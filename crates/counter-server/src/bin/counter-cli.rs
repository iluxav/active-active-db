use counter_proto::counter::v1::{
    counter_service_client::CounterServiceClient, GetRequest, IncrByRequest, MGetRequest,
};
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        print_usage();
        return Ok(());
    }

    // Get server address from environment or use default
    let addr = env::var("COUNTER_ADDR").unwrap_or_else(|_| "http://127.0.0.1:9000".to_string());

    let mut client = CounterServiceClient::connect(addr.clone()).await?;

    match args[1].as_str() {
        "incr" | "incrby" => {
            if args.len() < 3 {
                eprintln!("Usage: counter-cli incr <key> [amount]");
                return Ok(());
            }
            let key = &args[2];
            let amount: u64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(1);

            let response = client
                .incr_by(IncrByRequest {
                    key: key.clone(),
                    amount,
                })
                .await?;

            println!("{}", response.into_inner().value);
        }

        "get" => {
            if args.len() < 3 {
                eprintln!("Usage: counter-cli get <key>");
                return Ok(());
            }
            let key = &args[2];

            let response = client
                .get(GetRequest { key: key.clone() })
                .await?;

            println!("{}", response.into_inner().value);
        }

        "mget" => {
            if args.len() < 3 {
                eprintln!("Usage: counter-cli mget <key1> <key2> ...");
                return Ok(());
            }
            let keys: Vec<String> = args[2..].to_vec();

            let response = client.m_get(MGetRequest { keys }).await?;

            for value in response.into_inner().values {
                println!("{}", value);
            }
        }

        _ => {
            print_usage();
        }
    }

    Ok(())
}

fn print_usage() {
    eprintln!(
        r#"Active-Active Counter Store CLI

Usage:
  counter-cli incr <key> [amount]   Increment counter (default amount: 1)
  counter-cli get <key>             Get counter value
  counter-cli mget <key1> <key2>... Get multiple counter values

Environment:
  COUNTER_ADDR   Server address (default: http://127.0.0.1:9000)

Examples:
  counter-cli incr requests:api:2024 5
  counter-cli get requests:api:2024
  counter-cli mget errors:svc1 errors:svc2 errors:svc3

  COUNTER_ADDR=http://localhost:9010 counter-cli get mykey"#
    );
}
