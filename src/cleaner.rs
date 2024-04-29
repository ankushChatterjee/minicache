use anyhow::{anyhow, Context, Result};
use log::info;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::{error::CleanupError, Db};

const CLEAN_RATIO: f32 = 0.10;

pub async fn clean(cache: Db) -> Result<()> {
    /*
     * Look at 10% keys. If 25% of the keys are evictable repear the process.
     * Repeat the process until less than 25% keys sampled are evicted.
     * Note: An iteration on a hashmap picks random keys.
     */
    let num_clean = (cache.len() as f32 * CLEAN_RATIO) as usize;
    info!("Starting Cleaup for {} keys", num_clean);
    let keys = cache.iter();
    let mut keys_to_remove: Vec<String> = Vec::new();
    for item in keys {
        let db_item = item.value();
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("TIME ERROR")?
            .as_millis();
        if current_time > db_item.expiry_timestamp && db_item.expiry_timestamp != 0 {
            keys_to_remove.push(item.key().to_string());
            if keys_to_remove.len() == num_clean {
                break;
            }
        }
    }

    for key in &keys_to_remove {
        cache.remove(key);
    }
    info!("Cleanup Complete. Cleaned {} keys", keys_to_remove.len());

    // Determine if function needs to be called again
    if keys_to_remove.len() > (0.25 * num_clean as f32) as usize {
        info!("Cleanup needs to be repeated");
        return Err(anyhow!(CleanupError::NeedToRepeat));
    }
    Ok(())
}
