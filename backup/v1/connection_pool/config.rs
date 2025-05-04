use std::time::Duration;

/// Configuration for the connection pool
#[derive(Debug, Clone)]
pub struct ConnectionPoolConfig {
    pub min_connections: usize,
    pub max_connections: usize,
    pub connection_timeout: Duration,
    pub idle_timeout: Duration,
    pub max_lifetime: Duration,
    pub health_check_interval: Duration,
    pub retry_initial_interval: Duration,
    pub retry_max_interval: Duration,
    pub retry_max_attempts: u32,
}

impl Default for ConnectionPoolConfig {
    fn default() -> Self {
        Self {
            min_connections: 2,
            max_connections: 10,
            connection_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(300),
            max_lifetime: Duration::from_secs(3600),
            health_check_interval: Duration::from_secs(30),
            retry_initial_interval: Duration::from_millis(100),
            retry_max_interval: Duration::from_secs(5),
            retry_max_attempts: 3,
        }
    }
}
