use crate::commands::parse_command;
use crate::db::Map;
use crate::decoders::v1::{Decoder, ScanError};
use crate::decoders::v2::{ParseError, StreamDecoder};

use anyhow::{bail, Result};
use std::collections::HashMap;
use std::env;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio_stream::StreamExt;

mod commands;
mod db;
mod decoders;
mod protocol;

fn get_client_version() -> u8 {
    return match env::var("REDIS_DECODER_VERSION") {
        Ok(v) => match v.parse() {
            Ok(v) => v,
            _ => DEFAULT_DECODER_VERSION,
        },
        _ => DEFAULT_DECODER_VERSION,
    };
}

const DEFAULT_DECODER_VERSION: u8 = 2;

#[tokio::main]
async fn main() {
    let decoder_version = get_client_version();
    let bind_address = "127.0.0.1:6379";
    let listener = TcpListener::bind(&bind_address).await.unwrap();
    println!("server started at {}", bind_address);
    let map: Map = Arc::new(Mutex::new(HashMap::new()));
    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let map = map.clone();
        tokio::spawn(async move {
            match decoder_version {
                1 => handle_client_v1(stream, map).await.unwrap(),
                2 => handle_client_v2(stream, map).await.unwrap(),
                _ => panic!("unkown client {}", decoder_version),
            }
        });
    }
}

/// handles connection using decoders::v1
async fn handle_client_v1(stream: TcpStream, map: Map) -> Result<()> {
    println!("accepted new connection");
    let mut reader = BufReader::new(stream);
    loop {
        let mut decoder = Decoder::new(&mut reader);
        let packets = match decoder.parse().await {
            Ok(packets) => packets,
            Err(err) => match err.downcast_ref() {
                Some(ScanError::StreamClosed) => break,
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

/// handles connection using decoders::v2
async fn handle_client_v2(stream: TcpStream, map: Map) -> Result<()> {
    println!("accepted new connection");
    let (rh, mut wh) = stream.into_split();
    let mut reader = BufReader::new(rh);
    let mut decoder = StreamDecoder::new(&mut reader);
    let mut stream = Box::pin(decoder.as_stream());
    while let Some(packet) = stream.next().await {
        println!("received packet: {:?}", packet);
        match packet {
            Ok(dt) => {
                let cmd = parse_command(dt).unwrap();
                println!("received command: {:?}", cmd);
                let response = cmd.execute(map.clone()).unwrap();
                wh.write(response.encode().unwrap().as_slice())
                    .await
                    .unwrap();
            }
            Err(e) => match e.downcast_ref() {
                Some(ParseError::StreamClosed) => return Ok(()),
                _ => bail!(e),
            },
        }
    }
    println!("done");
    Ok(())
}
