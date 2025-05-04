#[derive(Debug, Clone)]
pub enum WebSocketMessage {
    Text(String),
    Binary(Vec<u8>),
    Ping(Vec<u8>),
    Pong(Vec<u8>),
    Close,
}

// Add conversion implementations
impl From<WebSocketMessage> for axum::extract::ws::Message {
    fn from(msg: WebSocketMessage) -> Self {
        match msg {
            WebSocketMessage::Text(text) => Self::Text(text.into()),
            WebSocketMessage::Binary(data) => Self::Binary(data.into()),
            WebSocketMessage::Ping(data) => Self::Ping(data.into()),
            WebSocketMessage::Pong(data) => Self::Pong(data.into()),
            WebSocketMessage::Close => Self::Close(None),
        }
    }
}

impl From<axum::extract::ws::Message> for WebSocketMessage {
    fn from(msg: axum::extract::ws::Message) -> Self {
        match msg {
            axum::extract::ws::Message::Text(text) => Self::Text(text.to_string()),
            axum::extract::ws::Message::Binary(data) => Self::Binary(data.to_vec()),
            axum::extract::ws::Message::Ping(data) => Self::Ping(data.to_vec()),
            axum::extract::ws::Message::Pong(data) => Self::Pong(data.to_vec()),
            axum::extract::ws::Message::Close(_) => Self::Close,
        }
    }
}
