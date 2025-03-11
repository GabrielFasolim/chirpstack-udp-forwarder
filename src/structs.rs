use std::convert::TryInto;
use std::time::{Duration, SystemTime};

use anyhow::Result;
use base64::{engine::general_purpose, Engine as _};
use chrono::{DateTime, Utc};
use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

use chirpstack_api::gw;

const PROTOCOL_VERSION: u8 = 0x02;

// =====================
// Semtech / ChirpStack structs
// =====================

pub enum Crc {
    Ok,
    Invalid,
    Missing,
}

impl Serialize for Crc {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Crc::Ok => serializer.serialize_i32(1),
            Crc::Invalid => serializer.serialize_i32(-1),
            Crc::Missing => serializer.serialize_i32(0),
        }
    }
}

pub enum Modulation {
    Lora,
    Fsk,
}

impl Serialize for Modulation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Modulation::Lora => serializer.serialize_str("LORA"),
            Modulation::Fsk => serializer.serialize_str("FSK"),
        }
    }
}

impl<'de> Deserialize<'de> for Modulation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "LORA" => Ok(Modulation::Lora),
            "FSK" => Ok(Modulation::Fsk),
            _ => Err(D::Error::custom("unexpected value"))?,
        }
    }
}

pub enum DataRate {
    Lora(u32, u32), // SF and BW (kHz)
    Fsk(u32),       // bitrate
}

impl Serialize for DataRate {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            DataRate::Lora(sf, bw) => serializer.serialize_str(&format!("SF{}BW{}", sf, bw / 1000)),
            DataRate::Fsk(bitrate) => serializer.serialize_u32(*bitrate),
        }
    }
}

impl<'de> Deserialize<'de> for DataRate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match Value::deserialize(deserializer)? {
            Value::String(v) => {
                let s: Vec<&str> = v.split(char::is_alphabetic).collect();
                if s.len() != 5 {
                    return Err(D::Error::custom("invalid datarate string"));
                }

                let sf: u32 = match s[2].parse() {
                    Ok(v) => v,
                    Err(err) => {
                        return Err(D::Error::custom(format!("parse sf error: {}", err)));
                    }
                };
                let bw: u32 = match s[4].parse() {
                    Ok(v) => v,
                    Err(err) => {
                        return Err(D::Error::custom(format!("parse bw error: {}", err)));
                    }
                };

                Ok(DataRate::Lora(sf, bw * 1000))
            }
            Value::Number(v) => {
                let br = v.as_u64().unwrap();
                Ok(DataRate::Fsk(br as u32))
            }
            _ => Err(D::Error::custom("unexpected type")),
        }
    }
}

#[derive(Clone, Copy)]
pub enum CodeRate {
    Undefined,
    LoRa4_5,
    LoRa4_6,
    LoRa4_7,
    LoRa4_8,
}

impl Serialize for CodeRate {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            CodeRate::LoRa4_5 => serializer.serialize_str("4/5"),
            CodeRate::LoRa4_6 => serializer.serialize_str("4/6"),
            CodeRate::LoRa4_7 => serializer.serialize_str("4/7"),
            CodeRate::LoRa4_8 => serializer.serialize_str("4/8"),
            _ => serializer.serialize_none(),
        }
    }
}

impl<'de> Deserialize<'de> for CodeRate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "4/5" => Ok(CodeRate::LoRa4_5),
            "4/6" => Ok(CodeRate::LoRa4_6),
            "4/7" => Ok(CodeRate::LoRa4_7),
            "4/8" => Ok(CodeRate::LoRa4_8),
            _ => Ok(CodeRate::Undefined),
        }
    }
}

// -------------
// PushData
// -------------
pub struct PushData {
    pub random_token: u16,
    pub gateway_id: [u8; 8],
    pub payload: PushDataPayload,
}

impl PushData {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut b = Vec::new();

        b.push(PROTOCOL_VERSION);
        b.append(&mut self.random_token.to_be_bytes().to_vec());
        b.push(0x00);
        b.append(&mut self.gateway_id.to_vec());

        let mut j = serde_json::to_vec(&self.payload).unwrap();
        b.append(&mut j);

        b
    }
}

#[derive(Serialize)]
pub struct PushDataPayload {
    pub rxpk: Vec<RxPk>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stat: Option<Stat>,
}

// -------------
// RxPk
// -------------
#[derive(Serialize)]
pub struct RxPk {
    // UTC time of pkt RX, us precision, ISO 8601 'compact' format
    #[serde(with = "compact_time_format")]
    pub time: DateTime<Utc>,
    // GPS time of pkt RX, number of milliseconds since 06.Jan.1980
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tmms: Option<u64>,
    // Internal timestamp of "RX finished" event (32b unsigned)
    pub tmst: u32,
    // RX central frequency in MHz
    pub freq: f64,
    pub chan: u32,
    pub rfch: u32,
    pub stat: Crc,
    pub modu: Modulation,
    pub datr: DataRate,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub codr: Option<CodeRate>,
    pub rssi: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lsnr: Option<f32>,
    pub size: u8,
    pub data: String,
}

impl RxPk {
    pub fn from_proto(up: &chirpstack_api::gw::UplinkFrame) -> Result<Self> {
        let rx_info = match &up.rx_info {
            Some(v) => v,
            None => {
                return Err(anyhow!("rx_info must not be None"));
            }
        };

        let tx_info = match &up.tx_info {
            Some(v) => v,
            None => {
                return Err(anyhow!("tx_info must not be None"));
            }
        };

        Ok(RxPk {
            time: match &rx_info.gw_time {
                Some(v) => match TryInto::<SystemTime>::try_into(*v) {
                    Ok(v) => v.into(),
                    Err(_) => Utc::now(),
                },
                None => Utc::now(),
            },
            tmms: rx_info
                .time_since_gps_epoch
                .as_ref()
                .map(|v| (v.seconds * 1000) as u64 + (v.nanos / 1000000) as u64),
            tmst: {
                let mut bytes: [u8; 4] = [0; 4];
                bytes.copy_from_slice(&rx_info.context);
                u32::from_be_bytes(bytes)
            },
            freq: tx_info.frequency as f64 / 1_000_000.0,
            chan: rx_info.channel,
            rfch: rx_info.rf_chain,
            stat: match rx_info.crc_status() {
                gw::CrcStatus::CrcOk => Crc::Ok,
                gw::CrcStatus::BadCrc => Crc::Invalid,
                gw::CrcStatus::NoCrc => Crc::Missing,
            },
            modu: match &tx_info.modulation {
                Some(m) => match &m.parameters {
                    Some(gw::modulation::Parameters::Lora(_)) => Modulation::Lora,
                    Some(gw::modulation::Parameters::Fsk(_)) => Modulation::Fsk,
                    Some(gw::modulation::Parameters::LrFhss(_)) => {
                        return Err(anyhow!("unsupported modulation"));
                    }
                    None => return Err(anyhow!("parameters must not be None")),
                },
                None => return Err(anyhow!("modulation_info must not be None")),
            },

            datr: match &tx_info.modulation {
                Some(m) => match &m.parameters {
                    Some(gw::modulation::Parameters::Lora(v)) => {
                        DataRate::Lora(v.spreading_factor, v.bandwidth)
                    }
                    Some(gw::modulation::Parameters::Fsk(v)) => DataRate::Fsk(v.datarate),
                    Some(gw::modulation::Parameters::LrFhss(_)) => {
                        return Err(anyhow!("unsupported modulation"));
                    }
                    None => return Err(anyhow!("parameters must not be None")),
                },
                None => return Err(anyhow!("modulation_info must not be None")),
            },

            codr: match &tx_info.modulation {
                Some(gw::Modulation {
                    parameters: Some(gw::modulation::Parameters::Lora(v)),
                }) => Some(match v.code_rate() {
                    gw::CodeRate::Cr45 => CodeRate::LoRa4_5,
                    gw::CodeRate::Cr46 => CodeRate::LoRa4_6,
                    gw::CodeRate::Cr47 => CodeRate::LoRa4_7,
                    gw::CodeRate::Cr48 => CodeRate::LoRa4_8,
                    _ => CodeRate::Undefined,
                }),
                _ => None,
            },
            rssi: rx_info.rssi,
            lsnr: match &tx_info.modulation {
                Some(gw::Modulation {
                    parameters: Some(gw::modulation::Parameters::Lora(_)),
                }) => Some(rx_info.snr),
                _ => None,
            },
            size: up.phy_payload.len() as u8,
            data: general_purpose::STANDARD.encode(up.phy_payload.clone()),
        })
    }
}

// -------------
// Stat
// -------------
#[derive(Serialize)]
pub struct Stat {
    /// UTC 'system' time of the gateway, ISO 8601 'expanded' format.
    #[serde(with = "expanded_time_format")]
    pub time: DateTime<Utc>,
    /// GPS latitude do gateway.
    pub lati: f64,
    /// GPS longitude do gateway.
    pub long: f64,
    /// GPS altitude do gateway em metros.
    pub alti: u32,
    /// Número de pacotes de rádio recebidos.
    pub rxnb: u32,
    /// Número de pacotes de rádio recebidos com um PHY Crc válido.
    pub rxok: u32,
    /// Número de pacotes de rádio encaminhados.
    pub rxfw: u32,
    /// Porcentagem de datagramas upstream que foram reconhecidos.
    pub ackr: f32,
    /// Número de datagramas de downlink recebidos.
    pub dwnb: u32,
    /// Número de pacotes emitidos.
    pub txnb: u32,
}

impl Stat {
    pub fn from_proto(stats: &gw::GatewayStats) -> Result<Self> {
        Ok(Stat {
            time: match &stats.time {
                Some(v) => match TryInto::<SystemTime>::try_into(*v) {
                    Ok(v) => v.into(),
                    Err(_) => Utc::now(),
                },
                None => Utc::now(),
            },
            lati: stats.location.as_ref().map_or(0.0, |loc| loc.latitude),
            long: stats.location.as_ref().map_or(0.0, |loc| loc.longitude),
            alti: stats.location.as_ref().map_or(0, |loc| loc.altitude as u32),
            rxnb: stats.rx_packets_received,
            rxok: stats.rx_packets_received_ok,
            rxfw: 0,
            ackr: 0.0,
            dwnb: stats.tx_packets_received,
            txnb: stats.tx_packets_emitted,
        })
    }
}

// -------------
// PushAck
// -------------
pub struct PushAck {
    pub random_token: u16,
}

impl PushAck {
    pub fn from_bytes(b: &[u8]) -> Result<Self> {
        if b.len() != 4 {
            return Err(anyhow!("expected 4 bytes, got: {}", b.len()));
        }

        if b[0] != PROTOCOL_VERSION {
            return Err(anyhow!(
                "expected protocol version: {}, got: {}",
                PROTOCOL_VERSION,
                b[0]
            ));
        }

        if b[3] != 0x01 {
            return Err(anyhow!("invalid identifier: {}", b[3]));
        }

        let mut rt: [u8; 2] = [0; 2];
        rt.copy_from_slice(&b[1..3]);

        Ok(PushAck {
            random_token: u16::from_be_bytes(rt),
        })
    }
}

// -------------
// PullData
// -------------
pub struct PullData {
    pub random_token: u16,
    pub gateway_id: [u8; 8],
}

impl PullData {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut b: Vec<u8> = Vec::with_capacity(12);
        b.push(PROTOCOL_VERSION);
        b.append(&mut self.random_token.to_be_bytes().to_vec());
        b.push(0x02);
        b.append(&mut self.gateway_id.to_vec());
        b
    }
}

// -------------
// PullAck
// -------------
pub struct PullAck {
    pub random_token: u16,
}

impl PullAck {
    pub fn from_bytes(b: &[u8]) -> Result<Self> {
        if b.len() != 4 {
            return Err(anyhow!("expected 4 bytes, got: {}", b.len()));
        }

        if b[0] != PROTOCOL_VERSION {
            return Err(anyhow!(
                "expected protocol version: {}, got: {}",
                PROTOCOL_VERSION,
                b[0]
            ));
        }

        if b[3] != 0x04 {
            return Err(anyhow!("invalid identifier: {}", b[3]));
        }

        let mut rt: [u8; 2] = [0; 2];
        rt.copy_from_slice(&b[1..3]);

        Ok(PullAck {
            random_token: u16::from_be_bytes(rt),
        })
    }
}

// -------------
// PullResp
// -------------
pub struct PullResp {
    pub random_token: u16,
    pub payload: PullRespPayload,
}

impl PullResp {
    pub fn from_bytes(b: &[u8]) -> Result<Self> {
        if b.len() < 5 {
            return Err(anyhow!("expected at least 5 bytes, got: {}", b.len()));
        }

        if b[0] != PROTOCOL_VERSION {
            return Err(anyhow!(
                "expected protocol version: {}, got: {}",
                PROTOCOL_VERSION,
                b[0]
            ));
        }

        if b[3] != 0x03 {
            return Err(anyhow!("invalid identifier: {}", b[3]));
        }

        let mut rt: [u8; 2] = [0; 2];
        rt.copy_from_slice(&b[1..3]);

        let pl: PullRespPayload = serde_json::from_slice(&b[4..])?;

        Ok(PullResp {
            random_token: u16::from_be_bytes(rt),
            payload: pl,
        })
    }
}

#[derive(Deserialize)]
pub struct PullRespPayload {
    pub txpk: TxPk,
}

// -------------
// TxPk
// -------------
#[derive(Deserialize)]
pub struct TxPk {
    pub imme: Option<bool>,
    pub tmst: Option<u32>,
    pub tmms: Option<u64>,
    pub freq: f64,
    pub powe: u8,
    pub modu: Modulation,
    pub datr: DataRate,
    pub codr: Option<CodeRate>,
    pub fdev: Option<u32>,
    pub ipol: Option<bool>,
    pub prea: Option<u8>,
    pub data: String,
    pub ncrc: Option<bool>,
}

impl TxPk {
    pub fn to_proto(
        &self,
        downlink_id: u32,
        gateway_id: Vec<u8>,
    ) -> Result<chirpstack_api::gw::DownlinkFrame> {
        let tx_info = chirpstack_api::gw::DownlinkTxInfo {
            frequency: (self.freq * 1_000_000.0) as u32,
            power: self.powe as i32,
            modulation: Some(gw::Modulation {
                parameters: Some(match self.modu {
                    Modulation::Lora => match self.datr {
                        DataRate::Lora(sf, bw) => {
                            gw::modulation::Parameters::Lora(gw::LoraModulationInfo {
                                bandwidth: bw,
                                spreading_factor: sf,
                                code_rate: match self.codr {
                                    Some(CodeRate::LoRa4_5) => gw::CodeRate::Cr45,
                                    Some(CodeRate::LoRa4_6) => gw::CodeRate::Cr46,
                                    Some(CodeRate::LoRa4_7) => gw::CodeRate::Cr47,
                                    Some(CodeRate::LoRa4_8) => gw::CodeRate::Cr48,
                                    Some(CodeRate::Undefined) | None => gw::CodeRate::CrUndefined,
                                }
                                .into(),
                                polarization_inversion: self.ipol.unwrap_or(true),
                                preamble: self.prea.map(|v| v as u32).unwrap_or_default(),
                                no_crc: self.ncrc.unwrap_or_default(),
                                code_rate_legacy: "".into(),
                            })
                        }
                        _ => {
                            return Err(anyhow!("LoRa DataRate expected"));
                        }
                    },
                    Modulation::Fsk => match self.datr {
                        DataRate::Fsk(v) => {
                            gw::modulation::Parameters::Fsk(gw::FskModulationInfo {
                                datarate: v,
                                frequency_deviation: self.fdev.unwrap_or(0),
                            })
                        }
                        _ => {
                            return Err(anyhow!("FSK DataRate expected"));
                        }
                    },
                }),
            }),
            board: 0,
            antenna: 0,
            timing: Some(gw::Timing {
                parameters: Some(if self.imme.unwrap_or(false) {
                    gw::timing::Parameters::Immediately(gw::ImmediatelyTimingInfo {})
                } else if self.tmst.is_some() {
                    gw::timing::Parameters::Delay(gw::DelayTimingInfo {
                        delay: Some(prost_types::Duration {
                            seconds: 0,
                            nanos: 0,
                        }),
                    })
                } else if let Some(v) = self.tmms {
                    gw::timing::Parameters::GpsEpoch(gw::GpsEpochTimingInfo {
                        time_since_gps_epoch: Some(Duration::from_millis(v).try_into()?),
                    })
                } else {
                    return Err(anyhow!("no timing information found"));
                }),
            }),
            context: self.tmst.map(|v| v.to_be_bytes().to_vec()).unwrap_or_default(),
        };

        Ok(chirpstack_api::gw::DownlinkFrame {
            downlink_id,
            gateway_id: hex::encode(gateway_id),
            items: vec![chirpstack_api::gw::DownlinkFrameItem {
                tx_info: Some(tx_info),
                phy_payload: match general_purpose::STANDARD.decode(&self.data) {
                    Ok(v) => v,
                    Err(err) => {
                        return Err(anyhow!("base64 decode payload error: {}", err));
                    }
                },
                ..Default::default()
            }],
            ..Default::default()
        })
    }
}

// -------------
// TxAck
// -------------
pub struct TxAck {
    pub random_token: u16,
    pub gateway_id: [u8; 8],
    pub payload: TxAckPayload,
}

impl TxAck {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut b = Vec::new();

        b.push(PROTOCOL_VERSION);
        b.append(&mut self.random_token.to_be_bytes().to_vec());
        b.push(0x05);
        b.append(&mut self.gateway_id.to_vec());

        let mut j = serde_json::to_vec(&self.payload).unwrap();
        b.append(&mut j);

        b
    }
}

#[derive(Serialize)]
pub struct TxAckPayload {
    pub txpk_ack: TxAckPayloadError,
}

#[derive(Serialize)]
pub struct TxAckPayloadError {
    pub error: String,
}

// ===============
// Formatação custom de datas
// ===============
mod expanded_time_format {
    use chrono::{DateTime, Utc};
    use serde::{self, Serializer};

    const FORMAT: &str = "%Y-%m-%d %H:%M:%S %Z";

    pub fn serialize<S>(date: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format!("{}", date.format(FORMAT));
        serializer.serialize_str(&s)
    }
}

mod compact_time_format {
    use chrono::{DateTime, Utc};
    use serde::{self, Serializer};

    const FORMAT: &str = "%+";

    pub fn serialize<S>(date: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format!("{}", date.format(FORMAT));
        serializer.serialize_str(&s)
    }
}

// ============================
// NOVAS STRUCTS PARA PROTOCOLO MESH (JSON) - "rxpk", "stat", "txpk"
// ============================

/// Mensagem de Uplink MESH, contendo zero ou mais `rxpk` e opcionalmente um `stat`.
#[derive(Debug, Deserialize)]
pub struct CustomUplink {
    /// Zero ou mais objetos rxpk.
    #[serde(default)]
    pub rxpk: Vec<CustomRxpk>,

    /// Objeto stat opcional.
    #[serde(default)]
    pub stat: Option<CustomStat>,
}

/// Objeto rxpk (pacote recebido).
/// Exemplo de JSON:
/// "rxpk": {
///   "tmst": 60000,
///   "rssi": 50,
///   "snr": -5,
///   "size": 31,
///   "data": "AgDiAAAZGhwbGAA..."
/// }
#[derive(Debug, Deserialize)]
pub struct CustomRxpk {
    pub tmst: u32,

    #[serde(default)]
    pub rssi: Option<u8>, // se omitido, é None

    #[serde(default)]
    pub snr: Option<i8>,  // se omitido, é None

    pub size: u8,
    pub data: String,
}

/// Objeto stat (estatísticas do concentrador).
/// Exemplo de JSON:
/// "stat": {
///   "tmst":40000,
///   "lati":-25.41068,
///   "long":-49.20843,
///   "upns":10,
///   "dwns":9,
///   "temp":32,
///   "volt":33
/// }
#[derive(Debug, Deserialize)]
pub struct CustomStat {
    pub tmst: u32,
    #[serde(default)]
    pub lati: Option<f64>,
    #[serde(default)]
    pub long: Option<f64>,

    pub upns: u32,
    pub dwns: u32,

    pub temp: i8,
    pub volt: u8,
}

/// Mensagem de Downlink MESH, contendo um objeto `txpk`.
#[derive(Debug, Deserialize)]
pub struct CustomDownlink {
    pub txpk: CustomTxpk,
}

/// Objeto txpk (pacote para enviar ao rádio).
/// Exemplo de JSON:
/// "txpk": {
///   "size":8,
///   "data":"AADiAAAAFbg="
/// }
#[derive(Debug, Deserialize)]
pub struct CustomTxpk {
    pub size: u8,
    pub data: String,
}

// ============================
// Testes
// ============================

#[cfg(test)]
mod tests {
    use super::*;

    use std::str;
    use std::time::{Duration, SystemTime};

    use chirpstack_api::{common, gw};

    #[test]
    fn test_push_data_rxpk_lora() {
        let rx_info = gw::UplinkRxInfo {
            gateway_id: "0102030405060708".into(),
            gw_time: Some(SystemTime::UNIX_EPOCH.try_into().unwrap()),
            time_since_gps_epoch: Some(Duration::from_secs(1).try_into().unwrap()),
            rssi: -160,
            snr: 5.5,
            board: 2,
            channel: 1,
            rf_chain: 1,
            antenna: 3,
            context: vec![1, 2, 3, 4],
            crc_status: gw::CrcStatus::CrcOk.into(),
            ..Default::default()
        };

        let tx_info = gw::UplinkTxInfo {
            frequency: 868300000,
            modulation: Some(gw::Modulation {
                parameters: Some(gw::modulation::Parameters::Lora(gw::LoraModulationInfo {
                    bandwidth: 125000,
                    spreading_factor: 12,
                    code_rate: gw::CodeRate::Cr45.into(),
                    polarization_inversion: true,
                    ..Default::default()
                })),
            }),
        };

        let uf = gw::UplinkFrame {
            rx_info: Some(rx_info),
            tx_info: Some(tx_info),
            phy_payload: vec![1, 2, 3],
            ..Default::default()
        };

        let rxpk = RxPk::from_proto(&uf).unwrap();
        let pd = PushData {
            random_token: 123,
            gateway_id: [1, 2, 3, 4, 5, 6, 7, 8],
            payload: PushDataPayload {
                rxpk: vec![rxpk],
                stat: None,
            },
        };

        let b = pd.to_bytes();
        assert_eq!(
            b[0..12].to_vec(),
            vec![2, 0, 123, 0, 1, 2, 3, 4, 5, 6, 7, 8]
        );

        assert_eq!(
            str::from_utf8(&b[12..]).unwrap(),
            r#"{"rxpk":[{"time":"1970-01-01T00:00:00+00:00","tmms":1000,"tmst":16909060,"freq":868.3,"chan":1,"rfch":1,"stat":1,"modu":"LORA","datr":"SF12BW125","codr":"4/5","rssi":-160,"lsnr":5.5,"size":3,"data":"AQID"}]}"#
        );
    }

    #[test]
    fn test_push_data_rxpk_fsk() {
        let rx_info = gw::UplinkRxInfo {
            gateway_id: "0102030405060708".into(),
            gw_time: Some(SystemTime::UNIX_EPOCH.try_into().unwrap()),
            time_since_gps_epoch: Some(Duration::from_secs(1).try_into().unwrap()),
            rssi: -160,
            channel: 1,
            rf_chain: 2,
            board: 3,
            antenna: 4,
            context: vec![1, 2, 3, 4],
            crc_status: gw::CrcStatus::CrcOk.into(),
            ..Default::default()
        };

        let tx_info = gw::UplinkTxInfo {
            frequency: 868300000,
            modulation: Some(gw::Modulation {
                parameters: Some(gw::modulation::Parameters::Fsk(gw::FskModulationInfo {
                    datarate: 50000,
                    ..Default::default()
                })),
            }),
        };

        let uf = gw::UplinkFrame {
            rx_info: Some(rx_info),
            tx_info: Some(tx_info),
            phy_payload: vec![1, 2, 3],
            ..Default::default()
        };

        let rxpk = RxPk::from_proto(&uf).unwrap();
        let pd = PushData {
            random_token: 123,
            gateway_id: [1, 2, 3, 4, 5, 6, 7, 8],
            payload: PushDataPayload {
                rxpk: vec![rxpk],
                stat: None,
            },
        };

        let b = pd.to_bytes();
        assert_eq!(
            b[0..12].to_vec(),
            vec![2, 0, 123, 0, 1, 2, 3, 4, 5, 6, 7, 8]
        );

        assert_eq!(
            str::from_utf8(&b[12..]).unwrap(),
            r#"{"rxpk":[{"time":"1970-01-01T00:00:00+00:00","tmms":1000,"tmst":16909060,"freq":868.3,"chan":1,"rfch":2,"stat":1,"modu":"FSK","datr":50000,"rssi":-160,"size":3,"data":"AQID"}]}"#
        );
    }

    #[test]
    fn test_push_data_stat() {
        let gs = gw::GatewayStats {
            gateway_id: "0102030405060708".into(),
            time: Some(SystemTime::UNIX_EPOCH.try_into().unwrap()),
            location: Some(common::Location {
                latitude: 1.123,
                longitude: 2.123,
                altitude: 3.123,
                ..Default::default()
            }),
            rx_packets_received: 10,
            rx_packets_received_ok: 5,
            tx_packets_received: 14,
            tx_packets_emitted: 7,
            ..Default::default()
        };

        let stat = Stat::from_proto(&gs).unwrap();
        let pd = PushData {
            random_token: 123,
            gateway_id: [1, 2, 3, 4, 5, 6, 7, 8],
            payload: PushDataPayload {
                rxpk: vec![],
                stat: Some(stat),
            },
        };

        let b = pd.to_bytes();
        assert_eq!(
            b[0..12].to_vec(),
            vec![2, 0, 123, 0, 1, 2, 3, 4, 5, 6, 7, 8]
        );

        assert_eq!(
            str::from_utf8(&b[12..]).unwrap(),
            r#"{"rxpk":[],"stat":{"time":"1970-01-01 00:00:00 UTC","lati":1.123,"long":2.123,"alti":3,"rxnb":10,"rxok":5,"rxfw":0,"ackr":0.0,"dwnb":14,"txnb":7}}"#
        );
    }

    #[test]
    fn test_push_ack() {
        let b: [u8; 4] = [2, 0, 123, 1];

        let push_ack = PushAck::from_bytes(&b).unwrap();
        assert_eq!(push_ack.random_token, 123);
    }

    #[test]
    fn test_pull_data() {
        let pull_data = PullData {
            random_token: 123,
            gateway_id: [1, 2, 3, 4, 5, 6, 7, 8],
        };

        let b = pull_data.to_bytes();
        assert_eq!(b, [2, 0, 123, 2, 1, 2, 3, 4, 5, 6, 7, 8]);
    }

    #[test]
    fn test_pull_ack() {
        let b: [u8; 4] = [2, 0, 123, 4];

        let pull_ack = PullAck::from_bytes(&b).unwrap();
        assert_eq!(pull_ack.random_token, 123);
    }

    #[test]
    fn test_pull_resp_lora_immediately() {
        let txpk = r#"{"txpk":{
            "imme":true,
            "freq":864.123456,
            "rfch":0,
            "powe":14,
            "modu":"LORA",
            "datr":"SF11BW125",
            "codr":"4/6",
            "ipol":false,
            "size":32,
            "data":"H3P3N2i9qc4yt7rK7ldqoeCVJGBybzPY5h1Dd7P7p8s="}}"#;
        let mut txpk = txpk.as_bytes().to_vec();

        let mut b: Vec<u8> = vec![2, 0, 123, 3];
        b.append(&mut txpk);

        let pull_resp = PullResp::from_bytes(&b).unwrap();

        assert_eq!(pull_resp.random_token, 123);

        let downlink_frame = pull_resp
            .payload
            .txpk
            .to_proto(0, vec![1, 2, 3, 4, 5, 6, 7, 8])
            .unwrap();

        let tx_info = gw::DownlinkTxInfo {
            frequency: 864123456,
            power: 14,
            board: 0,
            antenna: 0,
            context: vec![],
            timing: Some(gw::Timing {
                parameters: Some(gw::timing::Parameters::Immediately(
                    gw::ImmediatelyTimingInfo {},
                )),
            }),
            modulation: Some(gw::Modulation {
                parameters: Some(gw::modulation::Parameters::Lora(gw::LoraModulationInfo {
                    bandwidth: 125000,
                    spreading_factor: 11,
                    code_rate: gw::CodeRate::Cr46.into(),
                    polarization_inversion: false,
                    ..Default::default()
                })),
            }),
            ..Default::default()
        };

        assert_eq!(
            downlink_frame,
            gw::DownlinkFrame {
                downlink_id: 0,
                gateway_id: "0102030405060708".into(),
                items: vec![gw::DownlinkFrameItem {
                    phy_payload: general_purpose::STANDARD
                        .decode("H3P3N2i9qc4yt7rK7ldqoeCVJGBybzPY5h1Dd7P7p8s=")
                        .unwrap(),
                    tx_info: Some(tx_info),
                    ..Default::default()
                }],
                ..Default::default()
            }
        );
    }

    #[test]
    fn test_pull_resp_lora_delay() {
        let txpk = r#"{"txpk":{
            "freq":864.123456,
            "rfch":0,
            "powe":14,
            "modu":"LORA",
            "datr":"SF11BW125",
            "codr":"4/5",
            "ipol":false,
            "size":32,
            "tmst": 5000000,
            "data":"H3P3N2i9qc4yt7rK7ldqoeCVJGBybzPY5h1Dd7P7p8s="}}"#;
        let mut txpk = txpk.as_bytes().to_vec();

        let mut b: Vec<u8> = vec![2, 0, 123, 3];
        b.append(&mut txpk);

        let pull_resp = PullResp::from_bytes(&b).unwrap();

        assert_eq!(pull_resp.random_token, 123);

        let downlink_frame = pull_resp
            .payload
            .txpk
            .to_proto(0, vec![1, 2, 3, 4, 5, 6, 7, 8])
            .unwrap();

        let tx_info = gw::DownlinkTxInfo {
            frequency: 864123456,
            power: 14,
            board: 0,
            antenna: 0,
            context: vec![0, 76, 75, 64], // == 5000000
            timing: Some(gw::Timing {
                parameters: Some(gw::timing::Parameters::Delay(gw::DelayTimingInfo {
                    delay: Some(Duration::from_secs(0).try_into().unwrap()),
                })),
            }),
            modulation: Some(gw::Modulation {
                parameters: Some(gw::modulation::Parameters::Lora(gw::LoraModulationInfo {
                    bandwidth: 125000,
                    spreading_factor: 11,
                    code_rate: gw::CodeRate::Cr45.into(),
                    polarization_inversion: false,
                    ..Default::default()
                })),
            }),
            ..Default::default()
        };

        assert_eq!(
            downlink_frame,
            gw::DownlinkFrame {
                downlink_id: 0,
                gateway_id: "0102030405060708".into(),
                items: vec![gw::DownlinkFrameItem {
                    phy_payload: general_purpose::STANDARD
                        .decode("H3P3N2i9qc4yt7rK7ldqoeCVJGBybzPY5h1Dd7P7p8s=")
                        .unwrap(),
                    tx_info: Some(tx_info),
                    ..Default::default()
                }],
                ..Default::default()
            }
        );
    }

    #[test]
    fn test_pull_resp_lora_gps() {
        let txpk = r#"{"txpk":{
            "freq":864.123456,
            "rfch":0,
            "powe":14,
            "modu":"LORA",
            "datr":"SF11BW125",
            "codr":"4/5",
            "ipol":false,
            "size":32,
            "tmms": 5000000,
            "data":"H3P3N2i9qc4yt7rK7ldqoeCVJGBybzPY5h1Dd7P7p8s="}}"#;
        let mut txpk = txpk.as_bytes().to_vec();

        let mut b: Vec<u8> = vec![2, 0, 123, 3];
        b.append(&mut txpk);

        let pull_resp = PullResp::from_bytes(&b).unwrap();

        assert_eq!(pull_resp.random_token, 123);

        let downlink_frame = pull_resp
            .payload
            .txpk
            .to_proto(0, vec![1, 2, 3, 4, 5, 6, 7, 8])
            .unwrap();

        let tx_info = gw::DownlinkTxInfo {
            frequency: 864123456,
            power: 14,
            board: 0,
            antenna: 0,
            context: vec![],
            timing: Some(gw::Timing {
                parameters: Some(gw::timing::Parameters::GpsEpoch(gw::GpsEpochTimingInfo {
                    time_since_gps_epoch: Some(Duration::from_secs(5000).try_into().unwrap()),
                })),
            }),
            modulation: Some(gw::Modulation {
                parameters: Some(gw::modulation::Parameters::Lora(gw::LoraModulationInfo {
                    bandwidth: 125000,
                    spreading_factor: 11,
                    code_rate: gw::CodeRate::Cr45.into(),
                    polarization_inversion: false,
                    ..Default::default()
                })),
            }),
            ..Default::default()
        };

        assert_eq!(
            downlink_frame,
            gw::DownlinkFrame {
                downlink_id: 0,
                gateway_id: "0102030405060708".into(),
                items: vec![gw::DownlinkFrameItem {
                    phy_payload: general_purpose::STANDARD
                        .decode("H3P3N2i9qc4yt7rK7ldqoeCVJGBybzPY5h1Dd7P7p8s=")
                        .unwrap(),
                    tx_info: Some(tx_info),
                    ..Default::default()
                }],
                ..Default::default()
            }
        );
    }

    #[test]
    fn test_pull_resp_fsk_delay() {
        let txpk = r#"{"txpk":{
            "freq":861.3,
            "rfch":0,
            "powe":12,
            "modu":"FSK",
            "datr":50000,
            "fdev":3000,
            "size":32,
            "tmst": 5000000,
            "data":"H3P3N2i9qc4yt7rK7ldqoeCVJGBybzPY5h1Dd7P7p8s="}}"#;
        let mut txpk = txpk.as_bytes().to_vec();

        let mut b: Vec<u8> = vec![2, 0, 123, 3];
        b.append(&mut txpk);

        let pull_resp = PullResp::from_bytes(&b).unwrap();

        assert_eq!(pull_resp.random_token, 123);

        let downlink_frame = pull_resp
            .payload
            .txpk
            .to_proto(0, vec![1, 2, 3, 4, 5, 6, 7, 8])
            .unwrap();

        let tx_info = gw::DownlinkTxInfo {
            frequency: 861300000,
            power: 12,
            board: 0,
            antenna: 0,
            context: vec![0, 76, 75, 64], // == 5000000
            timing: Some(gw::Timing {
                parameters: Some(gw::timing::Parameters::Delay(gw::DelayTimingInfo {
                    delay: Some(Duration::from_secs(0).try_into().unwrap()),
                })),
            }),
            modulation: Some(gw::Modulation {
                parameters: Some(gw::modulation::Parameters::Fsk(gw::FskModulationInfo {
                    frequency_deviation: 3000,
                    datarate: 50000,
                })),
            }),
            ..Default::default()
        };

        assert_eq!(
            downlink_frame,
            gw::DownlinkFrame {
                downlink_id: 0,
                gateway_id: "0102030405060708".into(),
                items: vec![gw::DownlinkFrameItem {
                    phy_payload: general_purpose::STANDARD
                        .decode("H3P3N2i9qc4yt7rK7ldqoeCVJGBybzPY5h1Dd7P7p8s=")
                        .unwrap(),
                    tx_info: Some(tx_info),
                    ..Default::default()
                }],
                ..Default::default()
            }
        );
    }

    #[test]
    fn test_tx_ack() {
        let tx_ack = TxAck {
            random_token: 123,
            gateway_id: [1, 2, 3, 4, 5, 6, 7, 8],
            payload: TxAckPayload {
                txpk_ack: TxAckPayloadError {
                    error: "TOO_LATE".to_string(),
                },
            },
        };

        let b = tx_ack.to_bytes();
        assert_eq!(
            b[0..12].to_vec(),
            vec![2, 0, 123, 5, 1, 2, 3, 4, 5, 6, 7, 8],
        );

        assert_eq!(
            str::from_utf8(&b[12..]).unwrap(),
            r#"{"txpk_ack":{"error":"TOO_LATE"}}"#,
        );
    }
}
