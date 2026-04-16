use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use rand::Rng;
use serde_json::{json, Value};
use tokio::net::UdpSocket;
use tokio::sync::watch;
use tokio::time::{timeout, Duration, Instant};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use crate::error::{CassiniError, Result};
use crate::simple_http_server::SimpleHttpServer;
use crate::simple_mqtt_server::SimpleMqttServer;
use crate::types::{Command, CurrentStatus, FileStatus, FileTransferProgress, PrinterStatus, ProtocolFlavor};

const SATURN_UDP_PORT: u16 = 3000;
const SDCP_V3_WS_PORTS: [u16; 2] = [3030, 80];

#[derive(Debug, Clone)]
pub struct DiscoveryOptions {
    pub timeout: Duration,
    pub broadcast: Option<IpAddr>,
}

impl Default for DiscoveryOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(1),
            broadcast: None,
        }
    }
}

pub struct SaturnPrinter {
    pub addr: SocketAddr,
    pub timeout: Duration,
    pub id: String,
    pub name: String,
    pub machine_name: String,
    pub protocol_version: String,
    pub protocol_flavor: ProtocolFlavor,
    pub current_status: i64,
    pub busy: bool,
    pub desc: Value,
    pub last_status: Option<Value>,
    mqtt: Option<Arc<SimpleMqttServer>>,
    http: Option<Arc<SimpleHttpServer>>,
    file_transfer_rx: Option<watch::Receiver<FileTransferProgress>>,
}

impl SaturnPrinter {
    pub fn new(addr: SocketAddr, desc: Value, timeout: Duration) -> Self {
        let mut printer = Self {
            addr,
            timeout,
            id: String::new(),
            name: "Unknown".to_string(),
            machine_name: "Unknown".to_string(),
            protocol_version: String::new(),
            protocol_flavor: ProtocolFlavor::LegacyMqtt,
            current_status: 0,
            busy: false,
            desc: Value::Null,
            last_status: None,
            mqtt: None,
            http: None,
            file_transfer_rx: None,
        };
        printer.set_desc(desc);
        printer
    }

    pub async fn find_printers(options: DiscoveryOptions) -> Result<Vec<Self>> {
        let broadcast = options
            .broadcast
            .unwrap_or(IpAddr::V4(Ipv4Addr::new(255, 255, 255, 255)));
        let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).await?;
        socket.set_broadcast(true)?;
        socket
            .send_to(b"M99999", SocketAddr::new(broadcast, SATURN_UDP_PORT))
            .await?;

        let start = Instant::now();
        let mut out = Vec::new();
        let mut buf = [0u8; 8192];

        while start.elapsed() < options.timeout {
            let remaining = options
                .timeout
                .checked_sub(start.elapsed())
                .unwrap_or_else(|| Duration::from_millis(1));

            let recv = timeout(remaining, socket.recv_from(&mut buf)).await;
            let Ok(Ok((n, addr))) = recv else {
                break;
            };

            if let Ok(value) = serde_json::from_slice::<Value>(&buf[..n]) {
                out.push(Self::new(addr, value, options.timeout));
            }
        }

        Ok(out)
    }

    pub async fn find_printer(addr: IpAddr, timeout_duration: Duration) -> Result<Option<Self>> {
        let printers = Self::find_printers(DiscoveryOptions {
            timeout: timeout_duration,
            broadcast: Some(addr),
        })
        .await?;

        Ok(printers.into_iter().find(|printer| printer.addr.ip() == addr))
    }

    pub async fn refresh(&mut self, timeout_duration: Duration) -> Result<bool> {
        let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).await?;
        socket
            .send_to(b"M99999", SocketAddr::new(self.addr.ip(), SATURN_UDP_PORT))
            .await?;

        let mut buf = [0u8; 8192];
        let recv = timeout(timeout_duration, socket.recv_from(&mut buf)).await;
        let Ok(Ok((n, _))) = recv else {
            return Ok(false);
        };

        let payload = serde_json::from_slice::<Value>(&buf[..n])?;
        self.set_desc(payload);
        Ok(true)
    }

    pub async fn refresh_live_status(&mut self) -> Result<bool> {
        if self.protocol_flavor == ProtocolFlavor::SdcpV3Websocket {
            return self.refresh_v3_status().await;
        }
        self.refresh(self.timeout).await
    }

    pub fn set_desc(&mut self, desc: Value) {
        self.desc = desc;
        let data = self
            .desc
            .get("Data")
            .cloned()
            .unwrap_or_else(|| Value::Object(Default::default()));

        let (attrs, last_status) = if data.get("Attributes").is_some() {
            (
                data.get("Attributes")
                    .cloned()
                    .unwrap_or_else(|| Value::Object(Default::default())),
                data.get("Status").cloned(),
            )
        } else {
            (data, None)
        };

        self.last_status = last_status;

        self.protocol_version = attrs
            .get("ProtocolVersion")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        self.protocol_flavor = if self.protocol_version.starts_with("V3") {
            ProtocolFlavor::SdcpV3Websocket
        } else {
            ProtocolFlavor::LegacyMqtt
        };

        self.id = attrs
            .get("MainboardID")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        self.name = attrs
            .get("Name")
            .and_then(Value::as_str)
            .unwrap_or("Unknown")
            .to_string();
        self.machine_name = attrs
            .get("MachineName")
            .and_then(Value::as_str)
            .unwrap_or(&self.name)
            .to_string();

        let status = self.status();
        self.current_status = status.status;
        self.busy = self.current_status > 0;
    }

    pub fn describe(&self) -> String {
        format!("{} ({})", self.name, self.machine_name)
    }

    pub fn status(&self) -> PrinterStatus {
        let status_obj = self
            .last_status
            .as_ref()
            .cloned()
            .unwrap_or_else(|| Value::Object(Default::default()));
        let print_info = status_obj
            .get("PrintInfo")
            .cloned()
            .unwrap_or_else(|| Value::Object(Default::default()));

        PrinterStatus {
            status: Self::normalize_scalar(status_obj.get("CurrentStatus")).unwrap_or(0),
            filename: print_info
                .get("Filename")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            current_layer: Self::normalize_scalar(print_info.get("CurrentLayer")).unwrap_or(0),
            total_layers: Self::normalize_scalar(print_info.get("TotalLayer")).unwrap_or(0),
            print_status: Self::normalize_scalar(print_info.get("Status")).unwrap_or(0),
            protocol: self.protocol_version.clone(),
            transport: self.protocol_flavor.as_str().to_string(),
        }
    }

    pub fn file_transfer_progress(&self) -> Option<watch::Receiver<FileTransferProgress>> {
        self.file_transfer_rx.as_ref().map(watch::Receiver::clone)
    }

    pub async fn connect(
        &mut self,
        mqtt: Arc<SimpleMqttServer>,
        http: Arc<SimpleHttpServer>,
    ) -> Result<bool> {
        if self.protocol_flavor != ProtocolFlavor::LegacyMqtt {
            return Err(CassiniError::Protocol(
                "legacy MQTT connect flow is not used for SDCP V3 printers".to_string(),
            ));
        }

        self.mqtt = Some(Arc::clone(&mqtt));
        self.http = Some(http);

        let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).await?;
        let payload = format!("M66666 {}", mqtt.port());
        socket
            .send_to(payload.as_bytes(), SocketAddr::new(self.addr.ip(), SATURN_UDP_PORT))
            .await?;

        let client_id = mqtt.wait_for_client_connection(self.timeout).await?;
        if client_id != self.id {
            return Err(CassiniError::Protocol(format!(
                "client ID mismatch: {client_id} != {}",
                self.id
            )));
        }

        let _ = mqtt.wait_for_client_subscribed(self.timeout).await?;

        self.send_command_and_wait(Command::Cmd0, None, true).await?;
        self.send_command_and_wait(Command::Cmd1, None, true).await?;
        self.send_command_and_wait(
            Command::SetMysteryTimePeriod,
            Some(json!({"TimePeriod": 5000})),
            true,
        )
        .await?;

        Ok(true)
    }

    pub async fn disconnect(&mut self) -> Result<Value> {
        self.send_command_and_wait(Command::Disconnect, Some(json!({})), true)
            .await
    }

    async fn send_command_v3_and_wait(
        &mut self,
        cmdid: Command,
        data: Option<Value>,
        abort_on_bad_ack: bool,
    ) -> Result<Value> {
        let request_id = random_hexstr();
        let payload = json!({
            "Id": self.desc.get("Id").and_then(Value::as_str).unwrap_or(""),
            "Data": {
                "Cmd": cmdid as i32,
                "Data": data.unwrap_or_else(|| json!({})),
                "From": 1,
                "MainboardID": self.id,
                "RequestID": request_id,
                "TimeStamp": Self::unix_timestamp_secs(),
            },
            "Topic": format!("sdcp/request/{}", self.id),
        });

        let mut last_error: Option<CassiniError> = None;

        for port in SDCP_V3_WS_PORTS {
            let uri = format!("ws://{}:{port}/websocket", self.addr.ip());
            let deadline = Instant::now() + self.timeout;

            match connect_async(&uri).await {
                Ok((mut ws, _)) => {
                    ws.send(Message::Text(payload.to_string())).await?;

                    loop {
                        let now = Instant::now();
                        if now >= deadline {
                            break;
                        }

                        let remaining = deadline - now;
                        let incoming = timeout(remaining, ws.next()).await;
                        let Ok(Some(Ok(message))) = incoming else {
                            break;
                        };

                        let raw = match message {
                            Message::Text(text) => text,
                            Message::Binary(bytes) => String::from_utf8_lossy(&bytes).to_string(),
                            _ => continue,
                        };

                        let msg: Value = serde_json::from_str(&raw)?;
                        let msg_data = msg
                            .get("Data")
                            .cloned()
                            .unwrap_or_else(|| Value::Object(Default::default()));

                        if let Some(status_payload) = msg
                            .get("Status")
                            .cloned()
                            .or_else(|| msg_data.get("Status").cloned())
                        {
                            self.incoming_status(status_payload);
                        }

                        let topic = msg.get("Topic").and_then(Value::as_str).unwrap_or_default();
                        if !topic.contains("/response/") || !topic.ends_with(&format!("/{}", self.id)) {
                            continue;
                        }

                        let req = msg_data
                            .get("RequestID")
                            .and_then(Value::as_str)
                            .unwrap_or_default();
                        if req != request_id {
                            continue;
                        }

                        let result = msg_data.get("Data").cloned().unwrap_or_else(|| json!({}));
                        let ack = result.get("Ack").and_then(Value::as_i64).unwrap_or(0);
                        if abort_on_bad_ack && ack != 0 {
                            return Err(CassiniError::Protocol(format!(
                                "command {} returned Ack={ack} payload={result}",
                                cmdid as i32
                            )));
                        }
                        return Ok(result);
                    }
                }
                Err(err) => {
                    last_error = Some(CassiniError::from(err));
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            CassiniError::Protocol(format!("unable to reach SDCP V3 websocket for {}", self.addr.ip()))
        }))
    }

    pub async fn refresh_v3_status(&mut self) -> Result<bool> {
        if self.protocol_flavor != ProtocolFlavor::SdcpV3Websocket {
            return Ok(false);
        }

        let request_id = random_hexstr();
        let payload = json!({
            "Id": self.desc.get("Id").and_then(Value::as_str).unwrap_or(""),
            "Data": {
                "Cmd": Command::Cmd0 as i32,
                "Data": {},
                "From": 1,
                "MainboardID": self.id,
                "RequestID": request_id,
                "TimeStamp": Self::unix_timestamp_secs(),
            },
            "Topic": format!("sdcp/request/{}", self.id),
        });

        for port in SDCP_V3_WS_PORTS {
            let uri = format!("ws://{}:{port}/websocket", self.addr.ip());
            if let Ok((mut ws, _)) = connect_async(&uri).await {
                ws.send(Message::Text(payload.to_string())).await?;
                let deadline = Instant::now() + self.timeout;
                let mut got_ack = false;

                loop {
                    let now = Instant::now();
                    if now >= deadline {
                        break;
                    }
                    let remaining = deadline - now;
                    let incoming = timeout(remaining, ws.next()).await;
                    let Ok(Some(Ok(message))) = incoming else {
                        break;
                    };

                    let raw = match message {
                        Message::Text(text) => text,
                        Message::Binary(bytes) => String::from_utf8_lossy(&bytes).to_string(),
                        _ => continue,
                    };
                    let msg: Value = serde_json::from_str(&raw)?;
                    let msg_data = msg
                        .get("Data")
                        .cloned()
                        .unwrap_or_else(|| Value::Object(Default::default()));

                    if let Some(status_payload) = msg
                        .get("Status")
                        .cloned()
                        .or_else(|| msg_data.get("Status").cloned())
                    {
                        self.incoming_status(status_payload);
                        return Ok(true);
                    }

                    let topic = msg.get("Topic").and_then(Value::as_str).unwrap_or_default();
                    let req = msg_data
                        .get("RequestID")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    if topic.contains("/response/") && topic.ends_with(&format!("/{}", self.id)) && req == request_id {
                        got_ack = true;
                    }
                }

                if got_ack {
                    return Ok(self.last_status.is_some());
                }
            }
        }

        Ok(false)
    }

    pub async fn upload_file(&mut self, filename: &str, start_printing: bool) -> Result<()> {
        if self.protocol_flavor != ProtocolFlavor::LegacyMqtt {
            return Err(CassiniError::Protocol(
                "upload flow is implemented only for legacy MQTT printers".to_string(),
            ));
        }

        self.upload_file_inner(filename, start_printing).await
    }

    async fn upload_file_inner(&mut self, filename: &str, start_printing: bool) -> Result<()> {
        let mqtt = self
            .mqtt
            .as_ref()
            .cloned()
            .ok_or_else(|| CassiniError::Protocol("MQTT server is not connected".to_string()))?;
        let http = self
            .http
            .as_ref()
            .cloned()
            .ok_or_else(|| CassiniError::Protocol("HTTP server is not connected".to_string()))?;

        let basename = std::path::Path::new(filename)
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or_else(|| CassiniError::Protocol("invalid file name".to_string()))?
            .to_string();
        let ext = basename
            .split('.')
            .next_back()
            .unwrap_or_default()
            .to_ascii_lowercase();

        let httpname = format!("{}.{}", random_hexstr(), ext);
        let route = http.register_file_route(format!("/{httpname}"), filename)?;

        let cmd_data = json!({
            "Check": 0,
            "CleanCache": 1,
            "Compress": 0,
            "FileSize": route.size,
            "Filename": basename,
            "MD5": route.md5,
            "URL": format!("http://${{ipaddr}}:{}/{httpname}", http.port()),
            "StartPrinting": if start_printing { 1 } else { 0 },
        });

        self.send_command_and_wait(Command::UploadFile, Some(cmd_data), true)
            .await?;

        let (tx, rx) = watch::channel(FileTransferProgress {
            current_offset: 0,
            total_size: route.size as i64,
            filename: basename.clone(),
        });
        self.file_transfer_rx = Some(rx);

        loop {
            let reply = timeout(self.timeout * 2, mqtt.next_published_message())
                .await
                .map_err(|_| CassiniError::Timeout("upload status"))??;

            if reply.topic != format!("/sdcp/status/{}", self.id) {
                continue;
            }

            let data: Value = serde_json::from_str(&reply.payload)?;
            let status = data
                .get("Data")
                .and_then(|d| d.get("Status"))
                .cloned()
                .unwrap_or_else(|| Value::Object(Default::default()));
            self.incoming_status(status.clone());

            let file_info = status
                .get("FileTransferInfo")
                .cloned()
                .unwrap_or_else(|| Value::Object(Default::default()));
            let current_offset = Self::normalize_scalar(file_info.get("DownloadOffset")).unwrap_or(0);
            let total_size = Self::normalize_scalar(file_info.get("FileTotalSize")).unwrap_or(route.size as i64);
            let file_name = file_info
                .get("Filename")
                .and_then(Value::as_str)
                .unwrap_or(&basename)
                .to_string();

            let _ = tx.send(FileTransferProgress {
                current_offset,
                total_size,
                filename: file_name.clone(),
            });

            let ready = Self::normalize_scalar(status.get("CurrentStatus")).unwrap_or(0) == CurrentStatus::Ready as i64;
            if ready {
                let file_status = Self::normalize_scalar(file_info.get("Status")).unwrap_or(FileStatus::None as i64);
                if file_status != FileStatus::Done as i64 {
                    http.unregister_file_route(&format!("/{httpname}")).await;
                    return Err(CassiniError::Protocol(format!(
                        "file transfer failed with status code {file_status}"
                    )));
                }
                break;
            }
        }

        http.unregister_file_route(&format!("/{httpname}")).await;
        Ok(())
    }

    pub async fn print_file(&mut self, filename: &str) -> Result<bool> {
        let result = self
            .send_command_and_wait(
                Command::StartPrinting,
                Some(json!({"Filename": filename, "StartLayer": 0})),
                false,
            )
            .await?;

        Ok(result.get("Ack").and_then(Value::as_i64).unwrap_or(0) == 0)
    }

    pub async fn pause_print(&mut self) -> Result<Value> {
        self.send_command_and_wait(Command::PausePrinting, Some(json!({})), false)
            .await
    }

    pub async fn resume_print(&mut self) -> Result<Value> {
        self.send_command_and_wait(Command::ContinuePrinting, Some(json!({})), false)
            .await
    }

    pub async fn stop_print(&mut self) -> Result<Value> {
        self.send_command_and_wait(Command::StopPrinting, Some(json!({})), false)
            .await
    }

    pub fn incoming_status(&mut self, status: Value) {
        self.last_status = Some(status.clone());
        self.current_status = Self::normalize_scalar(status.get("CurrentStatus")).unwrap_or(self.current_status);
        self.busy = self.current_status > 0;
    }

    pub async fn send_command_and_wait(
        &mut self,
        cmdid: Command,
        data: Option<Value>,
        abort_on_bad_ack: bool,
    ) -> Result<Value> {
        if self.protocol_flavor == ProtocolFlavor::SdcpV3Websocket {
            return self
                .send_command_v3_and_wait(cmdid, data, abort_on_bad_ack)
                .await;
        }

        let req = self.send_command(cmdid, data)?;
        let mqtt = self
            .mqtt
            .as_ref()
            .cloned()
            .ok_or_else(|| CassiniError::Protocol("MQTT server is not connected".to_string()))?;

        loop {
            let reply = timeout(self.timeout, mqtt.next_published_message())
                .await
                .map_err(|_| CassiniError::Timeout("command response"))??;
            let payload: Value = serde_json::from_str(&reply.payload)?;

            if reply.topic == format!("/sdcp/response/{}", self.id) {
                let request_id = payload
                    .get("Data")
                    .and_then(|d| d.get("RequestID"))
                    .and_then(Value::as_str)
                    .unwrap_or_default();

                if request_id == req {
                    let result = payload
                        .get("Data")
                        .and_then(|d| d.get("Data"))
                        .cloned()
                        .unwrap_or_else(|| json!({}));
                    let ack = result.get("Ack").and_then(Value::as_i64).unwrap_or(0);
                    if abort_on_bad_ack && ack != 0 {
                        return Err(CassiniError::Protocol(format!(
                            "command {} returned bad ack: {result}",
                            cmdid as i32
                        )));
                    }
                    return Ok(result);
                }
            } else if reply.topic == format!("/sdcp/status/{}", self.id) {
                if let Some(status) = payload.get("Data").and_then(|d| d.get("Status")).cloned() {
                    self.incoming_status(status);
                }
            }
        }
    }

    pub fn send_command(&self, cmdid: Command, data: Option<Value>) -> Result<String> {
        if self.protocol_flavor != ProtocolFlavor::LegacyMqtt {
            return Err(CassiniError::Protocol(
                "MQTT publish path is not used for SDCP V3 printers".to_string(),
            ));
        }

        let mqtt = self
            .mqtt
            .as_ref()
            .ok_or_else(|| CassiniError::Protocol("MQTT server is not connected".to_string()))?;
        let req = random_hexstr();
        let timestamp = Self::unix_timestamp_millis();

        let cmd_data = json!({
            "Data": {
                "Cmd": cmdid as i32,
                "Data": data,
                "From": 0,
                "MainboardID": self.id,
                "RequestID": req,
                "TimeStamp": timestamp,
            },
            "Id": self.desc.get("Id").and_then(Value::as_str).unwrap_or("")
        });

        mqtt.publish(
            format!("/sdcp/request/{}", self.id),
            serde_json::to_string(&cmd_data)?,
        );

        Ok(req)
    }

    pub async fn connect_mqtt(&self, mqtt_host: IpAddr, mqtt_port: u16) -> Result<()> {
        let socket = UdpSocket::bind(SocketAddr::new(mqtt_host, 0)).await?;
        let payload = format!("M66666 {mqtt_port}");
        socket
            .send_to(payload.as_bytes(), SocketAddr::new(self.addr.ip(), SATURN_UDP_PORT))
            .await?;
        Ok(())
    }

    fn normalize_scalar(value: Option<&Value>) -> Option<i64> {
        match value {
            Some(Value::Array(values)) if values.len() == 1 => values[0].as_i64(),
            Some(v) => v.as_i64(),
            None => None,
        }
    }

    fn unix_timestamp_secs() -> i64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        now.as_secs() as i64
    }

    fn unix_timestamp_millis() -> i64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        now.as_millis() as i64
    }
}

fn random_hexstr() -> String {
    let mut rng = rand::thread_rng();
    let value: u128 = rng.gen();
    format!("{value:032x}")
}
