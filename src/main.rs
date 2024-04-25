use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use clap::{command, Parser};
use dashmap::DashMap;
use error::NetError;
use log::{error, info};

mod connection;
mod error;
mod executor;
mod instruction;

const NUM_SHARDS: usize = 32;

type Db = Arc<DashMap<String, Bytes>>;

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
    let args = Args::parse();
    // Start tokio TCP Server
    match start_server(args.port.unwrap()).await {
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

async fn start_server(port: u16) -> Result<()> {
    let addr = format!("127.0.0.1:{}", port);
    info!("Starting server on {addr}");
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .context(format!("Can't bind {addr}"))?;
    let cache: Db = Arc::new(DashMap::with_shard_amount(NUM_SHARDS));
    loop {
        let (stream, _) = listener.accept().await?;
        let cloned_cache = cache.clone();
        info!("Accepted new connection");
        tokio::spawn(async move {
            let mut connection = connection::new(stream);
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
                    Err(e) => match connection.write_line(e.to_string()).await {
                        Ok(_) => {
                            continue;
                        }
                        Err(_) => match e.downcast_ref() {
                            Some(NetError::ConnClosedByClient) => {
                                break;
                            }
                            _ => error!("Failed to write"),
                        },
                    },
                };
            }
            info!("Dropped Connection");
        });
    }
}
