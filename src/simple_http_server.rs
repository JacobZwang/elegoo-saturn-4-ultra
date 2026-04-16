use std::collections::HashMap;
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

use crate::error::{CassiniError, Result};

#[derive(Debug, Clone)]
pub struct FileRoute {
    pub file: PathBuf,
    pub size: u64,
    pub md5: String,
}

pub struct SimpleHttpServer {
    host: String,
    port: u16,
    listener: Option<TcpListener>,
    routes: Arc<RwLock<HashMap<String, FileRoute>>>,
}

impl SimpleHttpServer {
    pub const BUFFER_SIZE: usize = 1_024_768;

    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
            listener: None,
            routes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub async fn start(&mut self) -> Result<()> {
        let listener = TcpListener::bind((self.host.as_str(), self.port)).await?;
        self.port = listener.local_addr()?.port();
        self.listener = Some(listener);
        Ok(())
    }

    pub async fn serve_forever(&mut self) -> Result<()> {
        let listener = self
            .listener
            .take()
            .ok_or_else(|| CassiniError::Protocol("HTTP server was not started".to_string()))?;

        loop {
            let (stream, _) = listener.accept().await?;
            let routes = Arc::clone(&self.routes);
            tokio::spawn(async move {
                if let Err(err) = Self::handle_client(stream, routes).await {
                    eprintln!("HTTP handler error: {err}");
                }
            });
        }
    }

    pub fn register_file_route(&self, path: impl Into<String>, filename: impl Into<PathBuf>) -> Result<FileRoute> {
        let path = path.into();
        let file = filename.into();

        let mut f = std::fs::File::open(&file)?;
        let mut hasher = md5::Context::new();
        let mut buf = [0u8; 8192];

        loop {
            let n = f.read(&mut buf)?;
            if n == 0 {
                break;
            }
            hasher.consume(&buf[..n]);
        }

        let metadata = std::fs::metadata(&file)?;
        let route = FileRoute {
            file,
            size: metadata.len(),
            md5: format!("{:x}", hasher.compute()),
        };

        self.routes.blocking_write().insert(path, route.clone());
        Ok(route)
    }

    pub async fn unregister_file_route(&self, path: &str) {
        self.routes.write().await.remove(path);
    }

    async fn handle_client(stream: TcpStream, routes: Arc<RwLock<HashMap<String, FileRoute>>>) -> Result<()> {
        let (mut reader, mut writer) = stream.into_split();
        let mut data = Vec::new();
        let mut buf = [0u8; 1024];

        loop {
            let n = reader.read(&mut buf).await?;
            if n == 0 {
                return Ok(());
            }
            data.extend_from_slice(&buf[..n]);
            if data.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
            if data.len() > 16 * 1024 {
                return Err(CassiniError::Protocol("HTTP headers too large".to_string()));
            }
        }

        let request = String::from_utf8_lossy(&data);
        let request_line = request
            .lines()
            .next()
            .ok_or_else(|| CassiniError::Protocol("missing HTTP request line".to_string()))?;
        let mut parts = request_line.split_whitespace();
        let method = parts
            .next()
            .ok_or_else(|| CassiniError::Protocol("missing HTTP method".to_string()))?;
        let path = parts
            .next()
            .ok_or_else(|| CassiniError::Protocol("missing HTTP path".to_string()))?;

        let route = {
            let map = routes.read().await;
            map.get(path).cloned()
        };

        let Some(route) = route else {
            writer.write_all(b"HTTP/1.1 404 Not Found\r\n\r\n").await?;
            writer.flush().await?;
            return Ok(());
        };

        let header = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=utf-8\r\nEtag: {}\r\nContent-Length: {}\r\n\r\n",
            route.md5, route.size
        );
        writer.write_all(header.as_bytes()).await?;

        if method == "GET" {
            let mut file = tokio::fs::File::open(route.file).await?;
            let mut chunk = vec![0u8; Self::BUFFER_SIZE];
            loop {
                let n = file.read(&mut chunk).await?;
                if n == 0 {
                    break;
                }
                writer.write_all(&chunk[..n]).await?;
            }
        }

        writer.flush().await?;
        Ok(())
    }
}
