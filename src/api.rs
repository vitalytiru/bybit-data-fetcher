use anyhow::{Context, Result};
use ring::hmac;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct Api {
    public: String,
    secret: String,
}

impl Api {
    pub fn read_api_keys(&mut self) -> Result<Self> {
        let secret = std::fs::read_to_string("/home/lastgosu/.ssh/bybit")
            .context("Failed to load secret")?;
        let public = "bdebUavw0ZabTR".to_string();
        Ok(Api { public, secret })
    }

    pub fn generate_signature(&self, message: String) -> (u64, String) {
        let expires = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 1000;
        let key = hmac::Key::new(hmac::HMAC_SHA256, self.secret.as_bytes());
        let sig = hmac::sign(&key, message.as_bytes());
        (expires, hex::encode(sig))
    }
}
