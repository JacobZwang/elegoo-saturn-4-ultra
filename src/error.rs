use thiserror::Error;

#[derive(Debug, Error)]
pub enum CassiniError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Websocket error: {0}")]
    Websocket(#[from] tokio_tungstenite::tungstenite::Error),

    #[error("Timeout while waiting for {0}")]
    Timeout(&'static str),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Channel closed: {0}")]
    Channel(&'static str),
}

pub type Result<T> = std::result::Result<T, CassiniError>;
