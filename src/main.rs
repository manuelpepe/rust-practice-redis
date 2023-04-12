use std::{io::Write, net::TcpListener};

use crate::commands::parse_command;

mod commands;
mod protocol;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                let mut p = protocol::Decoder::new(&mut stream);
                let ops = p.parse().unwrap();
                for op in ops {
                    let cmd = parse_command(op).unwrap();
                    println!("received command: {:?}", cmd);
                    let response = cmd.get_response().unwrap();
                    stream.write(response.encode().unwrap().as_slice()).unwrap();
                }
                println!("Done");
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
