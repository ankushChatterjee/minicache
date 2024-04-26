use anyhow::{Context, Result};
use log::info;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::Db;

pub async fn clean(cache: Db) -> Result<()> {
    info!("Starting Cleanup");
    let keys = cache.iter();
    let mut keys_to_remove: Vec<String> = Vec::new();
    for item in keys {
        let (expiry, _) = item.value();
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("TIME ERROR")?
            .as_millis();
        if current_time > *expiry && *expiry != 0 {
            keys_to_remove.push(item.key().to_string());
        }
    }

    for key in &keys_to_remove {
        cache.remove(key);
    }
    info!("Cleanup Complete. Cleaned {} keys", keys_to_remove.len());
    Ok(())
}
