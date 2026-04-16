use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicU16, Ordering},
    Arc,
};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{tcp::OwnedWriteHalf, TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Mutex};
use tokio::time::{timeout, Duration};

use crate::error::{CassiniError, Result};

const MQTT_CONNECT: u8 = 1;
const MQTT_CONNACK: u8 = 2;
const MQTT_PUBLISH: u8 = 3;
const MQTT_PUBACK: u8 = 4;
const MQTT_SUBSCRIBE: u8 = 8;
const MQTT_SUBACK: u8 = 9;
const MQTT_DISCONNECT: u8 = 14;

#[derive(Debug, Clone)]
pub struct PublishedMessage {
    pub topic: String,
    pub payload: String,
}

#[derive(Debug, Clone)]
struct OutgoingMessage {
    topic: String,
    payload: String,
}

pub struct SimpleMqttServer {
    host: String,
    port: u16,
    listener: Option<TcpListener>,
    incoming_tx: mpsc::UnboundedSender<PublishedMessage>,
    incoming_rx: Mutex<mpsc::UnboundedReceiver<PublishedMessage>>,
    outgoing_tx: broadcast::Sender<OutgoingMessage>,
    client_connection_tx: broadcast::Sender<String>,
    client_connection_rx: Mutex<broadcast::Receiver<String>>,
    client_subscribed_tx: broadcast::Sender<String>,
    client_subscribed_rx: Mutex<broadcast::Receiver<String>>,
    connected_clients: Arc<Mutex<HashMap<String, SocketAddr>>>,
    next_pack_id: Arc<AtomicU16>,
}

impl SimpleMqttServer {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        let (incoming_tx, incoming_rx) = mpsc::unbounded_channel();
        let (outgoing_tx, _) = broadcast::channel(256);
        let (client_connection_tx, client_connection_rx) = broadcast::channel(16);
        let (client_subscribed_tx, client_subscribed_rx) = broadcast::channel(16);

        Self {
            host: host.into(),
            port,
            listener: None,
            incoming_tx,
            incoming_rx: Mutex::new(incoming_rx),
            outgoing_tx,
            client_connection_tx,
            client_connection_rx: Mutex::new(client_connection_rx),
            client_subscribed_tx,
            client_subscribed_rx: Mutex::new(client_subscribed_rx),
            connected_clients: Arc::new(Mutex::new(HashMap::new())),
            next_pack_id: Arc::new(AtomicU16::new(1)),
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
            .ok_or_else(|| CassiniError::Protocol("MQTT server was not started".to_string()))?;

        loop {
            let (stream, addr) = listener.accept().await?;
            let incoming_tx = self.incoming_tx.clone();
            let outgoing_tx = self.outgoing_tx.clone();
            let client_connection_tx = self.client_connection_tx.clone();
            let client_subscribed_tx = self.client_subscribed_tx.clone();
            let connected_clients = Arc::clone(&self.connected_clients);
            let next_pack_id = Arc::clone(&self.next_pack_id);

            tokio::spawn(async move {
                if let Err(err) = Self::handle_client(
                    stream,
                    addr,
                    incoming_tx,
                    outgoing_tx,
                    client_connection_tx,
                    client_subscribed_tx,
                    connected_clients,
                    next_pack_id,
                )
                .await
                {
                    eprintln!("MQTT client handler error: {err}");
                }
            });
        }
    }

    pub fn publish(&self, topic: impl Into<String>, payload: impl Into<String>) {
        let _ = self.outgoing_tx.send(OutgoingMessage {
            topic: topic.into(),
            payload: payload.into(),
        });
    }

    pub async fn next_published_message(&self) -> Result<PublishedMessage> {
        let mut rx = self.incoming_rx.lock().await;
        rx.recv()
            .await
            .ok_or(CassiniError::Channel("incoming MQTT message queue"))
    }

    pub async fn wait_for_client_connection(&self, timeout_duration: Duration) -> Result<String> {
        let mut rx = self.client_connection_rx.lock().await;
        timeout(timeout_duration, rx.recv())
            .await
            .map_err(|_| CassiniError::Timeout("MQTT client connection"))?
            .map_err(|_| CassiniError::Channel("MQTT client connection"))
    }

    pub async fn wait_for_client_subscribed(&self, timeout_duration: Duration) -> Result<String> {
        let mut rx = self.client_subscribed_rx.lock().await;
        timeout(timeout_duration, rx.recv())
            .await
            .map_err(|_| CassiniError::Timeout("MQTT client subscribe"))?
            .map_err(|_| CassiniError::Channel("MQTT client subscribe"))
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_client(
        stream: TcpStream,
        addr: SocketAddr,
        incoming_tx: mpsc::UnboundedSender<PublishedMessage>,
        outgoing_tx: broadcast::Sender<OutgoingMessage>,
        client_connection_tx: broadcast::Sender<String>,
        client_subscribed_tx: broadcast::Sender<String>,
        connected_clients: Arc<Mutex<HashMap<String, SocketAddr>>>,
        next_pack_id: Arc<AtomicU16>,
    ) -> Result<()> {
        let (mut reader, mut writer) = stream.into_split();
        let mut data = Vec::<u8>::new();
        let mut read_buf = [0u8; 1024];
        let mut subscribed_topics: HashMap<String, u8> = HashMap::new();
        let mut client_id: Option<String> = None;
        let mut out_rx = outgoing_tx.subscribe();

        loop {
            tokio::select! {
                read = reader.read(&mut read_buf) => {
                    let n = read?;
                    if n == 0 {
                        break;
                    }
                    data.extend_from_slice(&read_buf[..n]);

                    while let Some((msg_type, msg_flags, message, consumed_total)) = Self::try_next_message(&data)? {
                        data.drain(0..consumed_total);

                        match msg_type {
                            MQTT_CONNECT => {
                                if message.len() < 12 || &message[0..6] != b"\x00\x04MQTT" {
                                    return Err(CassiniError::Protocol("bad CONNECT packet".to_string()));
                                }

                                let client_id_len = u16::from_be_bytes([message[10], message[11]]) as usize;
                                let end = 12 + client_id_len;
                                if end > message.len() {
                                    return Err(CassiniError::Protocol("truncated CONNECT client ID".to_string()));
                                }

                                let id = String::from_utf8_lossy(&message[12..end]).to_string();
                                connected_clients.lock().await.insert(id.clone(), addr);
                                client_id = Some(id.clone());
                                let _ = client_connection_tx.send(id);

                                Self::send_msg(&mut writer, MQTT_CONNACK, 0, None, b"\x00\x00").await?;
                            }
                            MQTT_PUBLISH => {
                                let qos = (msg_flags >> 1) & 0x3;
                                let (topic, packet_id, content) = Self::parse_publish(&message)?;
                                let _ = incoming_tx.send(PublishedMessage { topic, payload: content });
                                if qos > 0 {
                                    Self::send_msg(&mut writer, MQTT_PUBACK, 0, Some(packet_id), &[]).await?;
                                }
                            }
                            MQTT_SUBSCRIBE => {
                                if message.len() < 3 {
                                    return Err(CassiniError::Protocol("truncated SUBSCRIBE packet".to_string()));
                                }
                                let packet_id = u16::from_be_bytes([message[0], message[1]]);
                                let (topic, qos) = Self::parse_subscribe(&message[2..])?;
                                subscribed_topics.insert(topic.clone(), qos);
                                let _ = client_subscribed_tx.send(topic);
                                Self::send_msg(&mut writer, MQTT_SUBACK, 0, Some(packet_id), &[qos]).await?;
                            }
                            MQTT_DISCONNECT => {
                                break;
                            }
                            _ => {
                                // Ignore message types not required by this protocol.
                            }
                        }
                    }
                }
                outbound = out_rx.recv() => {
                    match outbound {
                        Ok(msg) => {
                            if subscribed_topics.contains_key(&msg.topic) {
                                let packet_id = next_pack_id.fetch_add(1, Ordering::Relaxed);
                                let payload = Self::encode_publish(&msg.topic, &msg.payload, packet_id);
                                Self::send_msg(&mut writer, MQTT_PUBLISH, 0, None, &payload).await?;
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            // Drop old queued messages for this client.
                        }
                    }
                }
            }
        }

        if let Some(id) = client_id {
            connected_clients.lock().await.remove(&id);
        }

        Ok(())
    }

    fn try_next_message(data: &[u8]) -> Result<Option<(u8, u8, Vec<u8>, usize)>> {
        if data.len() < 2 {
            return Ok(None);
        }

        let msg_type = data[0] >> 4;
        let msg_flags = data[0] & 0x0f;
        let (msg_length, len_bytes_consumed) = Self::decode_length(&data[1..])?;
        let header_len = 1 + len_bytes_consumed;
        let total_len = header_len + msg_length;
        if total_len > data.len() {
            return Ok(None);
        }

        let message = data[header_len..total_len].to_vec();
        Ok(Some((msg_type, msg_flags, message, total_len)))
    }

    async fn send_msg(
        writer: &mut OwnedWriteHalf,
        msg_type: u8,
        flags: u8,
        packet_ident: Option<u16>,
        payload: &[u8],
    ) -> Result<()> {
        let mut head = vec![(msg_type << 4) | flags];
        let mut body = Vec::new();

        if let Some(packet_id) = packet_ident {
            body.extend_from_slice(&packet_id.to_be_bytes());
        }
        body.extend_from_slice(payload);
        head.extend_from_slice(&Self::encode_length(body.len()));

        writer.write_all(&head).await?;
        writer.write_all(&body).await?;
        writer.flush().await?;
        Ok(())
    }

    fn encode_length(mut length: usize) -> Vec<u8> {
        let mut encoded = Vec::new();
        loop {
            let mut digit = (length % 128) as u8;
            length /= 128;
            if length > 0 {
                digit |= 0x80;
            }
            encoded.push(digit);
            if length == 0 {
                break;
            }
        }
        encoded
    }

    fn decode_length(data: &[u8]) -> Result<(usize, usize)> {
        let mut multiplier: usize = 1;
        let mut value: usize = 0;
        let mut bytes_read: usize = 0;

        for byte in data {
            bytes_read += 1;
            value += usize::from(byte & 0x7f) * multiplier;
            if byte & 0x80 == 0 {
                return Ok((value, bytes_read));
            }
            multiplier *= 128;
            if multiplier > 2_097_152 {
                return Err(CassiniError::Protocol("Malformed Remaining Length".to_string()));
            }
        }

        Err(CassiniError::Protocol(
            "Truncated packet while decoding Remaining Length".to_string(),
        ))
    }

    fn parse_publish(data: &[u8]) -> Result<(String, u16, String)> {
        if data.len() < 4 {
            return Err(CassiniError::Protocol("truncated PUBLISH packet".to_string()));
        }

        let topic_len = u16::from_be_bytes([data[0], data[1]]) as usize;
        if data.len() < 2 + topic_len + 2 {
            return Err(CassiniError::Protocol("truncated PUBLISH topic".to_string()));
        }

        let topic = String::from_utf8_lossy(&data[2..2 + topic_len]).to_string();
        let packet_id = u16::from_be_bytes([data[2 + topic_len], data[3 + topic_len]]);
        let message = String::from_utf8_lossy(&data[4 + topic_len..]).to_string();
        Ok((topic, packet_id, message))
    }

    fn parse_subscribe(data: &[u8]) -> Result<(String, u8)> {
        if data.len() < 3 {
            return Err(CassiniError::Protocol("truncated SUBSCRIBE topic".to_string()));
        }

        let topic_len = u16::from_be_bytes([data[0], data[1]]) as usize;
        if data.len() < 2 + topic_len + 1 {
            return Err(CassiniError::Protocol("truncated SUBSCRIBE topic payload".to_string()));
        }

        let topic = String::from_utf8_lossy(&data[2..2 + topic_len]).to_string();
        let qos = data[2 + topic_len];
        Ok((topic, qos))
    }

    fn encode_publish(topic: &str, message: &str, packet_id: u16) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&(topic.len() as u16).to_be_bytes());
        payload.extend_from_slice(topic.as_bytes());
        payload.extend_from_slice(&packet_id.to_be_bytes());
        payload.extend_from_slice(message.as_bytes());
        payload
    }
}
