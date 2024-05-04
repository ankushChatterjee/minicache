use std::sync::{Arc, RwLock};

use anyhow::{Context, Result};
use bytes::Bytes;
use clap::{command, Parser};
use dashmap::DashMap;
use error::{CleanupError, NetError};
use log::{error, info};
use tokio::time::{sleep, Duration};

use crate::connection::Connection;

mod cleaner;
mod connection;
mod error;
mod executor;
mod instruction;

const NUM_SHARDS: usize = 32;
const CLEANUP_GAP: u64 = 10;

struct DBItem {
    expiry_timestamp: u128,
    expiry_secs: u128,
    value: Bytes,
}

type Db = Arc<DashMap<String, DBItem>>;
type LockManager = Arc<DashMap<String, RwLock<bool>>>;

#[derive(Parser, Debug)]
#[command(author="Ankush", version="0.1.0", about = None, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "11211")]
    port: Option<u16>,
}

#[tokio::main]
async fn main() {
    print_ascii_art();
    env_logger::init();
    let cache: Db = Arc::new(DashMap::with_shard_amount(NUM_SHARDS));
    let lock_manager: LockManager = Arc::new(DashMap::with_shard_amount(NUM_SHARDS));
    let args = Args::parse();
    start_cleanup_daemon(cache.clone(), lock_manager.clone()).await;
    // Start tokio TCP Server
    match start_server(args.port.unwrap(), cache.clone(), lock_manager.clone()).await {
        Ok(_) => (),
        Err(e) => error!("{e}"),
    };
}

fn print_ascii_art() {
    let art = "

    ███╗   ███╗██╗███╗   ██╗██╗ ██████╗ █████╗  ██████╗██╗  ██╗███████╗
    ████╗ ████║██║████╗  ██║██║██╔════╝██╔══██╗██╔════╝██║  ██║██╔════╝
    ██╔████╔██║██║██╔██╗ ██║██║██║     ███████║██║     ███████║█████╗  
    ██║╚██╔╝██║██║██║╚██╗██║██║██║     ██╔══██║██║     ██╔══██║██╔══╝  
    ██║ ╚═╝ ██║██║██║ ╚████║██║╚██████╗██║  ██║╚██████╗██║  ██║███████╗
    ╚═╝     ╚═╝╚═╝╚═╝  ╚═══╝╚═╝ ╚═════╝╚═╝  ╚═╝ ╚═════╝╚═╝  ╚═╝╚══════╝
    
    ";
    print!("{}", art);
}

async fn start_server(port: u16, cache: Db, lock_manager: LockManager) -> Result<()> {
    let addr = format!("127.0.0.1:{}", port);
    info!("Starting server on {addr}");
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .context(format!("Can't bind {addr}"))?;
    loop {
        let (stream, _) = listener.accept().await?;
        let cloned_cache = cache.clone();
        let cloned_lock_manager = lock_manager.clone();

        info!("Accepted new connection");
        tokio::spawn(async move {
            let mut connection = Connection::new(stream);
            loop {
                let ins = connection.read_instruction().await;
                match ins {
                    Ok(ins) => {
                        match executor::execute(
                            ins,
                            cloned_cache.clone(),
                            cloned_lock_manager.clone(),
                        ) {
                            Ok(res) => {
                                connection.write_line(res).await.unwrap();
                            }
                            Err(e) => match e.downcast_ref() {
                                Some(NetError::ConnClosedByClient) => {
                                    break;
                                }
                                _ => match connection.write_line(e.to_string()).await {
                                    Ok(_) => {
                                        continue;
                                    }
                                    Err(_) => error!("Failed to write"),
                                },
                            },
                        };
                    }
                    Err(e) => match e.downcast_ref() {
                        Some(NetError::ConnClosedByClient) => {
                            break;
                        }
                        _ => match connection.write_line(e.to_string()).await {
                            Ok(_) => {
                                continue;
                            }
                            Err(_) => error!("Failed to write"),
                        },
                    },
                };
            }
            info!("Dropped Connection");
        });
    }
}

async fn start_cleanup_daemon(cache: Db, lock_manager: LockManager) {
    let cache = cache.clone();
    tokio::spawn(async move {
        loop {
            let cache = cache.clone();
            let lock_manager = lock_manager.clone();
            sleep(Duration::from_secs(CLEANUP_GAP)).await;

            match cleaner::clean(cache, lock_manager).await {
                Ok(_) => sleep(Duration::from_secs(CLEANUP_GAP)).await,
                Err(e) => match e.downcast_ref() {
                    Some(CleanupError::NeedToRepeat) => {
                        continue;
                    }
                    _ => {
                        error!("{e}");
                        sleep(Duration::from_secs(CLEANUP_GAP)).await;
                    }
                },
            };
        }
    });
}
