use std::net::TcpListener;

mod protocol;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                let mut p = protocol::Parser::new(stream);
                let ops = p.parse().unwrap();
                for op in ops {
                    println!("received: {:?}", op);
                }
                println!("Done");
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
