use crate::commands::parse_command;
use anyhow::{bail, Result};
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

mod commands;
mod protocol;

type Map = Arc<Mutex<HashMap<String, Bytes>>>;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let map: Map = Arc::new(Mutex::new(HashMap::new()));
    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let map = map.clone();
        tokio::spawn(async move {
            handle_client(stream, map).await.unwrap();
        });
    }
}

async fn handle_client(stream: TcpStream, map: Map) -> Result<()> {
    println!("accepted new connection");
    let mut reader = BufReader::new(stream);
    loop {
        let mut decoder = protocol::Decoder::new(&mut reader);
        let packets = match decoder.parse().await {
            Ok(packets) => packets,
            Err(err) => match err.downcast_ref() {
                Some(protocol::ScanError::StreamClosed) => break,
                _ => bail!(err),
            },
        };
        for packet in packets {
            let cmd = parse_command(packet).unwrap();
            println!("received command: {:?}", cmd);
            let response = cmd.execute(map.clone()).unwrap();
            reader
                .write(response.encode().unwrap().as_slice())
                .await
                .unwrap();
        }
    }
    println!("done");
    Ok(())
}
