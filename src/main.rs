use std::sync::Arc;

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

type Db = Arc<DashMap<String, (u128, Bytes)>>;

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
    let args = Args::parse();
    start_cleanup_daemon(cache.clone()).await;
    // Start tokio TCP Server
    match start_server(args.port.unwrap(), cache.clone()).await {
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

async fn start_server(port: u16, cache: Db) -> Result<()> {
    let addr = format!("127.0.0.1:{}", port);
    info!("Starting server on {addr}");
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .context(format!("Can't bind {addr}"))?;
    loop {
        let (stream, _) = listener.accept().await?;
        let cloned_cache = cache.clone();
        info!("Accepted new connection");
        tokio::spawn(async move {
            let mut connection = Connection::new(stream);
            loop {
                let ins = connection.read_instruction().await;
                match ins {
                    Ok(ins) => {
                        match executor::execute(ins, cloned_cache.clone()) {
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

async fn start_cleanup_daemon(cache: Db) {
    let cache = cache.clone();
    tokio::spawn(async move {
        loop {
            let cache = cache.clone();
            sleep(Duration::from_secs(CLEANUP_GAP)).await;

            match cleaner::clean(cache).await {
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
