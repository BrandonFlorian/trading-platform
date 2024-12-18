use crate::error::AppError;
use backoff::{backoff::Backoff, ExponentialBackoff};
use futures_util::{stream::StreamExt, SinkExt};
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async, tungstenite::Message, MaybeTlsStream,
    WebSocketStream as TungsteniteWebSocketStream,
};
use tracing::{debug, error, info, warn};

type WsStream = TungsteniteWebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug, Clone)]
pub struct WebSocketConfig {
    pub health_check_interval: Duration,
    pub connection_timeout: Duration,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for WebSocketConfig {
    fn default() -> Self {
        Self {
            health_check_interval: Duration::from_secs(30),
            connection_timeout: Duration::from_secs(5),
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(60),
        }
    }
}

#[derive(Debug)]
enum ConnectionState {
    Connected(WsStream),
    Disconnected,
    Connecting,
}

pub struct WebSocketConnectionManager {
    ws_url: String,
    config: WebSocketConfig,
    backoff: ExponentialBackoff,
    last_connection_attempt: Option<Instant>,
    last_health_check: Option<Instant>,
    state: ConnectionState,
}

impl WebSocketConnectionManager {
    pub fn new(ws_url: String, config: Option<WebSocketConfig>) -> Self {
        let config = config.unwrap_or_default();
        let mut backoff = ExponentialBackoff::default();
        backoff.max_elapsed_time = None;
        backoff.initial_interval = config.initial_backoff;
        backoff.max_interval = config.max_backoff;

        Self {
            ws_url,
            config,
            backoff,
            last_connection_attempt: None,
            last_health_check: None,
            state: ConnectionState::Disconnected,
        }
    }

    pub async fn ensure_connection(&mut self) -> Result<&mut WsStream, AppError> {
        match self.state {
            ConnectionState::Disconnected => {
                debug!("No connection exists, establishing new connection");
                self.establish_connection().await
            }
            ConnectionState::Connecting => {
                debug!("Connection attempt in progress, waiting...");
                tokio::time::sleep(Duration::from_secs(1)).await;
                Err(AppError::WebSocketConnectionError(
                    "Connection attempt in progress".to_string(),
                ))
            }
            ConnectionState::Connected(_) => {
                if !self.needs_health_check() {
                    if let ConnectionState::Connected(ref mut conn) = self.state {
                        return Ok(conn);
                    }
                }

                // Take ownership of the connection temporarily
                if let ConnectionState::Connected(conn) =
                    std::mem::replace(&mut self.state, ConnectionState::Disconnected)
                {
                    let (is_healthy, conn) = self.check_connection_health(conn).await;

                    if is_healthy {
                        self.state = ConnectionState::Connected(conn);
                        if let ConnectionState::Connected(ref mut conn) = self.state {
                            return Ok(conn);
                        }
                    }
                    // If not healthy, fall through to establish_connection
                }

                self.establish_connection().await
            }
        }
    }

    async fn establish_connection(&mut self) -> Result<&mut WsStream, AppError> {
        let now = Instant::now();

        // Only apply backoff if we're not recovering from Connecting state
        if matches!(self.state, ConnectionState::Disconnected) {
            if let Some(last_attempt) = self.last_connection_attempt {
                if let Some(wait_time) = self.backoff.next_backoff() {
                    let elapsed = now.duration_since(last_attempt);
                    if elapsed < wait_time {
                        debug!("Applying backoff delay of {:?}", wait_time - elapsed);
                        tokio::time::sleep(wait_time - elapsed).await;
                    }
                }
            }
        }

        self.last_connection_attempt = Some(now);
        self.state = ConnectionState::Connecting;

        match connect_async(&self.ws_url).await {
            Ok((stream, _)) => {
                info!("Successfully established WebSocket connection");
                self.backoff.reset();
                self.last_health_check = Some(now);
                self.state = ConnectionState::Connected(stream);
                match &mut self.state {
                    ConnectionState::Connected(conn) => Ok(conn),
                    _ => unreachable!(),
                }
            }
            Err(e) => {
                error!("Failed to establish WebSocket connection: {}", e);
                self.state = ConnectionState::Disconnected;
                Err(AppError::WebSocketConnectionError(format!(
                    "Failed to establish connection: {}",
                    e
                )))
            }
        }
    }

    fn needs_health_check(&self) -> bool {
        match self.last_health_check {
            Some(last_check) => {
                Instant::now().duration_since(last_check) >= self.config.health_check_interval
            }
            None => true,
        }
    }

    async fn check_connection_health(&mut self, mut conn: WsStream) -> (bool, WsStream) {
        debug!("Performing connection health check");
        let message = Message::Ping(vec![].into());

        let is_healthy = match conn.send(message).await {
            Ok(_) => {
                match tokio::time::timeout(self.config.connection_timeout, conn.next()).await {
                    Ok(Some(Ok(Message::Pong(_)))) => {
                        debug!("Health check successful");
                        self.last_health_check = Some(Instant::now());
                        true
                    }
                    Ok(Some(Ok(_))) => {
                        warn!("Received unexpected message during health check");
                        false
                    }
                    Ok(None) => {
                        warn!("Connection closed during health check");
                        false
                    }
                    Ok(Some(Err(e))) => {
                        error!("Error during health check: {}", e);
                        false
                    }
                    Err(_) => {
                        warn!("Health check timed out");
                        false
                    }
                }
            }
            Err(e) => {
                error!("Failed to send ping message: {}", e);
                false
            }
        };

        (is_healthy, conn)
    }

    pub async fn send_message(&mut self, message: Message) -> Result<(), AppError> {
        let conn = self.ensure_connection().await?;
        conn.send(message)
            .await
            .map_err(|e| AppError::WebSocketSendError(e.to_string()))
    }

    pub async fn receive_message(&mut self) -> Result<Option<Message>, AppError> {
        let conn = self.ensure_connection().await?;
        match conn.next().await {
            Some(Ok(msg)) => Ok(Some(msg)),
            Some(Err(e)) => Err(AppError::WebSocketReceiveError(e.to_string())),
            None => Ok(None),
        }
    }

    pub async fn cleanup_connection(&mut self) {
        if let ConnectionState::Connected(mut conn) =
            std::mem::replace(&mut self.state, ConnectionState::Disconnected)
        {
            let _ = conn.close(None).await;
        }
    }

    pub async fn shutdown(&mut self) -> Result<(), AppError> {
        debug!("Initiating graceful shutdown");
        self.cleanup_connection().await;
        Ok(())
    }
}
