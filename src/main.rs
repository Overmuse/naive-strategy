use alpaca::{orders::OrderIntent, Side};
use chrono::{Local, NaiveTime};
use clap::{value_t, App, Arg};
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryStreamExt};
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
            } = agg {
                let direction = if close > vwap { Side::Buy } else { Side::Sell };
                let shares = f64::trunc(10000.0 / close) as u32;
                let order_intent = OrderIntent::new(&symbol)
                    .qty(shares)
                    .side(direction);
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
    brokers: String,
    group_id: String,
    input_topics: Vec<String>,
    output_topic: String,
) {
    let local_time = Local::now();
    let market_open = NaiveTime::from_hms(09, 30, 0);
    if local_time.time() < market_open {
        let duration = market_open - local_time.time();
        info!("Market closed. Sleeping for {:?} seconds", duration);
        std::thread::sleep(std::time::Duration::from_secs(duration.num_seconds().try_into().unwrap()));
    }
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", &group_id)
        .set("bootstrap.servers", &brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .create()
        .expect("Consumer creation failed");

    let input: Vec<&str> = input_topics
        .iter()
        .map(|x| {x.as_str()})
        .collect();
    consumer
        .subscribe(&input)
        .expect("Can't subscribe to specified topics");

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    //let mut positions = HashMap::new();

    let stream_processor = consumer.start().try_for_each(|borrowed_message| {
        let producer = producer.clone();
        let output_topic = output_topic.to_string();
        async move {
            let owned_message = borrowed_message.detach();
            tokio::spawn(async move {
                match owned_message.topic() {
                    "minute-aggregates" => {
                        let order_intent = evaluate_quote(owned_message);
                        if let Some(oi) = order_intent {
                            let produce_future = producer.send(
                                FutureRecord::to(&output_topic).key(&oi.symbol).payload(
                                    &serde_json::to_string(&oi).expect("Failed to serialize order intent"),
                                ),
                                0,
                            );
                            match produce_future.await {
                                Ok(_delivery) => info!("Sent: {:#?}", &oi),
                                _ => info!("Error"),
                            }
                        }
                    },
                    "positions" => {
                        () //update_positions(&positions, owned_message);
                    },
                    _ => () 
            }});
            Ok(())
        }
    });

    info!("Starting event loop");
    stream_processor.await.expect("stream processing failed");
    info!("Stream processing terminated");
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let matches = App::new("Async example")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Asynchronous computation example")
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
                .min_values(1)
        )
        .arg(
            Arg::with_name("output-topic")
                .long("output-topic")
                .help("Output topic")
                .takes_value(true)
                .required(true),
        )
        .get_matches();

    let brokers = matches
        .value_of("brokers")
        .expect("Has default value so unwrap is always safe");
    let group_id = matches
        .value_of("group-id")
        .expect("Has default value so unwrap is always safe");
    let input_topics: Vec<String> = matches
        .values_of("input-topics")
        .expect("Required value so unwrap is always safe")
        .map(|x| {x.to_string()})
        .collect();
    let output_topic = matches
        .value_of("output-topic")
        .expect("Required value so unwrap is always safe");

    run_async_processor(
                brokers.to_owned(),
                group_id.to_owned(),
                input_topics.to_owned(),
                output_topic.to_owned(),
            ).await
}
