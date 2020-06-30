use alpaca::orders::OrderIntent;
use alpaca::Side;
use clap::{value_t, App, Arg};
use polygon_data_relay::PolygonMessage;
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryStreamExt};
use log::info;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::message::OwnedMessage;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::Message;
use serde_json;

fn evaluate_quote<'a>(msg: OwnedMessage) -> Option<OrderIntent> {
    match msg.payload_view::<str>() {
        Some(Ok(payload)) => {
            let agg: PolygonMessage = serde_json::from_str(payload).ok()?;
            println!("{:?}", &agg);
            if let PolygonMessage::MinuteAggregate { symbol, vwap, close, .. } = agg {
                let direction = if close > vwap { Side::Buy } else { Side::Sell };
                let order_intent = OrderIntent {
                    symbol: symbol,
                    qty: 1,
                    side: direction,
                    ..Default::default()
                };
                Some(order_intent)
            } else {
                None
            }
        }
        _ => None,
    }
}

// Creates all the resources and runs the event loop. The event loop will:
//   1) receive a stream of messages from the `StreamConsumer`.
//   2) filter out eventual Kafka errors.
//   3) send the message to a thread pool for processing.
//   4) produce the result to the output topic.
// `tokio::spawn` is used to handle IO-bound tasks in parallel (e.g., producing
// the messages), while `tokio::task::spawn_blocking` is used to handle the
// simulated CPU-bound task.
async fn run_async_processor(
    brokers: String,
    group_id: String,
    input_topic: String,
    output_topic: String,
) {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", &group_id)
        .set("bootstrap.servers", &brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&[&input_topic])
        .expect("Can't subscribe to specified topic");

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    let stream_processor = consumer.start().try_for_each(|borrowed_message| {
        let producer = producer.clone();
        let output_topic = output_topic.to_string();
        async move {
            let owned_message = borrowed_message.detach();
            tokio::spawn(async move {
                let order_intent = evaluate_quote(owned_message);
                if let Some(oi) = order_intent {
                    let produce_future = producer.send(
                        FutureRecord::to(&output_topic).key(&oi.symbol).payload(
                            &serde_json::to_string(&oi).expect("Failed to serialize order intent"),
                        ),
                        0,
                    );
                    match produce_future.await {
                        Ok(delivery) => println!("Sent: {:?}", delivery),
                        _ => println!("Error"),
                    }
                }
            });
            Ok(())
        }
    });

    info!("Starting event loop");
    stream_processor.await.expect("stream processing failed");
    info!("Stream processing terminated");
}

#[tokio::main]
async fn main() {
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
                .default_value("example_consumer_group_id"),
        )
        .arg(
            Arg::with_name("input-topic")
                .long("input-topic")
                .help("Input topic")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("output-topic")
                .long("output-topic")
                .help("Output topic")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("num-workers")
                .long("num-workers")
                .help("Number of workers")
                .takes_value(true)
                .default_value("1"),
        )
        .get_matches();

    let brokers = matches.value_of("brokers").expect("Has default value so unwrap is always safe");
    let group_id = matches.value_of("group-id").expect("Has default value so unwrap is always safe");
    let input_topic = matches.value_of("input-topic").expect("Required value so unwrap is always safe");
    let output_topic = matches.value_of("output-topic").expect("Required value so unwrap is always safe");
    let num_workers = value_t!(matches, "num-workers", usize).expect("Has default value so unwrap is always safe");

    (0..num_workers)
        .map(|_| {
            tokio::spawn(run_async_processor(
                brokers.to_owned(),
                group_id.to_owned(),
                input_topic.to_owned(),
                output_topic.to_owned(),
            ))
        })
        .collect::<FuturesUnordered<_>>()
        .for_each(|_| async { () })
        .await
}
