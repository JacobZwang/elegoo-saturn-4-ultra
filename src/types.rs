use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ProtocolFlavor {
    LegacyMqtt,
    SdcpV3Websocket,
}

impl ProtocolFlavor {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LegacyMqtt => "legacy-mqtt",
            Self::SdcpV3Websocket => "sdcp-v3-websocket",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum CurrentStatus {
    Ready = 0,
    Busy = 1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum PrintInfoStatus {
    Exposure = 2,
    Retracting = 3,
    Lowering = 4,
    Complete = 16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum FileStatus {
    None = 0,
    Done = 2,
    Error = 3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum Command {
    Cmd0 = 0,
    Cmd1 = 1,
    Disconnect = 64,
    StartPrinting = 128,
    PausePrinting = 129,
    StopPrinting = 130,
    ContinuePrinting = 131,
    UploadFile = 256,
    SetMysteryTimePeriod = 512,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrinterStatus {
    pub status: i64,
    pub filename: String,
    pub current_layer: i64,
    pub total_layers: i64,
    pub print_status: i64,
    pub protocol: String,
    pub transport: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileTransferProgress {
    pub current_offset: i64,
    pub total_size: i64,
    pub filename: String,
}
