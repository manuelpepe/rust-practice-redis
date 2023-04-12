use crate::commands::parse_command;
use anyhow::{bail, Result};
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

mod commands;
mod protocol;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    loop {
        let (stream, _) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            handle_client(stream).await.unwrap();
        });
    }
}

async fn handle_client(stream: TcpStream) -> Result<()> {
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
            let response = cmd.get_response().unwrap();
            reader
                .write(response.encode().unwrap().as_slice())
                .await
                .unwrap();
        }
    }
    println!("done");
    Ok(())
}
