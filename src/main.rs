use alpaca::{clock::get_clock, orders::OrderIntent, AlpacaConfig, Side};
use anyhow::Result;
use clap::{App, Arg};
use futures::{future, StreamExt};
use log::info;
use polygon_data_relay::PolygonMessage;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{stream_consumer::StreamConsumer, Consumer};
use rdkafka::message::OwnedMessage;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::Message;
use serde_json;
use std::collections::HashMap;
use std::convert::TryInto;
use std::env;

fn evaluate_quote<'a>(msg: OwnedMessage) -> Option<OrderIntent> {
    match msg.payload_view::<str>() {
        Some(Ok(payload)) => {
            let agg: PolygonMessage = serde_json::from_str(payload).ok()?;
            info!("{:#?}", &agg);
            if let PolygonMessage::MinuteAggregate {
                symbol,
                vwap,
                close,
                ..
            } = agg
            {
                let direction = if close > vwap { Side::Buy } else { Side::Sell };
                let shares = f64::trunc(10000.0 / close) as u32;
                let order_intent = OrderIntent::new(&symbol).qty(shares).side(direction);
                Some(order_intent)
            } else {
                None
            }
        }
        _ => None,
    }
}

fn update_positions(positions: &HashMap<String, i32>, msg: OwnedMessage) {
    todo!()
}

async fn run_async_processor(
    client: AlpacaConfig,
    brokers: String,
    group_id: String,
    input_topics: Vec<String>,
    output_topic: String,
) -> Result<()> {
    let clock = get_clock(&client).await?;
    info!("{:?}", &clock);
    if !clock.is_open {
        let duration = clock.next_open - clock.timestamp;
        info!("Market closed. Sleeping until {:?}", clock.next_open);
        tokio::time::delay_for(std::time::Duration::from_secs(
            duration.num_seconds().try_into().unwrap(),
        ))
        .await;
    }
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", &group_id)
        .set("bootstrap.servers", &brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .create()
        .expect("Consumer creation failed");

    let input: Vec<&str> = input_topics.iter().map(|x| x.as_str()).collect();
    consumer
        .subscribe(&input)
        .expect("Can't subscribe to specified topics");

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    //let mut positions = HashMap::new();

    consumer
        .start()
        .for_each(|borrowed_message| {
            let producer = producer.clone();
            let output_topic = output_topic.to_string();
            let owned_message = borrowed_message.unwrap().detach();
            tokio::spawn(async {
                match owned_message.topic() {
                    "minute-aggregates" => {
                        let order_intent = evaluate_quote(owned_message);
                        if let Some(oi) = order_intent {
                            let produce_future = producer.send(
                                FutureRecord::to(&output_topic).key(&oi.symbol).payload(
                                    &serde_json::to_string(&oi)
                                        .expect("Failed to serialize order intent"),
                                ),
                                0,
                            );
                            match produce_future.await {
                                Ok(_delivery) => info!("Sent: {:#?}", &oi),
                                _ => info!("Error"),
                            }
                            let mut oi2 = oi.clone();
                            tokio::spawn(async move {
                                tokio::time::delay_for(std::time::Duration::from_secs(30)).await;
                                oi2.side = match oi.side {
                                    Side::Buy => Side::Sell,
                                    Side::Sell => Side::Buy,
                                    _ => Side::Buy,
                                };
                                producer.send(
                                    FutureRecord::to(&output_topic).key(&oi2.symbol).payload(
                                        &serde_json::to_string(&oi2)
                                            .expect("failed to serialize order intent"),
                                    ),
                                    0,
                                )
                            });
                        }
                    }
                    "positions" => {
                        () //update_positions(&positions, owned_message);
                    }
                    _ => (),
                }
            });
            future::ready(())
        })
        .await;
    Ok(())
}

fn main() -> Result<()> {
    env_logger::builder().format_timestamp_micros().init();
    let matches = App::new("Async example")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Asynchronous computation example")
        .arg(
            Arg::with_name("url")
                .long("url")
                .takes_value(true)
                .default_value("https://paper-api.alpaca.markets/v2/"),
        )
        .arg(
            Arg::with_name("brokers")
                .short("b")
                .long("brokers")
                .help("Broker list in kafka format")
                .takes_value(true)
                .default_value("localhost:9092"),
        )
        .arg(
            Arg::with_name("group-id")
                .short("g")
                .long("group-id")
                .help("Consumer group id")
                .takes_value(true)
                .default_value("naive_strategy"),
        )
        .arg(
            Arg::with_name("input-topics")
                .long("input-topics")
                .help("Input topics")
                .required(true)
                .min_values(1),
        )
        .arg(
            Arg::with_name("output-topic")
                .long("output-topic")
                .help("Output topic")
                .takes_value(true)
                .required(true),
        )
        .get_matches();

    let url = matches
        .value_of("url")
        .expect("Has default value so unwrap is always safe");
    let brokers = matches
        .value_of("brokers")
        .expect("Has default value so unwrap is always safe");
    let group_id = matches
        .value_of("group-id")
        .expect("Has default value so unwrap is always safe");
    let input_topics: Vec<String> = matches
        .values_of("input-topics")
        .expect("Required value so unwrap is always safe")
        .map(|x| x.to_string())
        .collect();
    let output_topic = matches
        .value_of("output-topic")
        .expect("Required value so unwrap is always safe");

    let client = AlpacaConfig::new(
        url.to_string(),
        env::var("APCA_API_KEY_ID")?,
        env::var("APCA_API_SECRET_KEY")?,
    )?;

    let mut rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        run_async_processor(
            client,
            brokers.to_owned(),
            group_id.to_owned(),
            input_topics.to_owned(),
            output_topic.to_owned(),
        )
        .await?;
        Ok::<(), anyhow::Error>(())
    })
}
