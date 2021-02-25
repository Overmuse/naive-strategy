use alpaca::orders::{OrderIntent, Side};
use anyhow::{Context, Result};
use dotenv::dotenv;
use futures::{future, StreamExt};
use log::info;
use polygon::ws::PolygonMessage;
use rdkafka::{
    config::ClientConfig,
    consumer::{stream_consumer::StreamConsumer, Consumer},
    message::OwnedMessage,
    producer::{FutureProducer, FutureRecord},
    Message,
};
use std::env;

fn evaluate_quote(msg: OwnedMessage) -> Option<OrderIntent> {
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

async fn run_async_processor() -> Result<()> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", &env::var("GROUP_ID")?)
        .set("bootstrap.servers", &env::var("BOOTSTRAP_SERVERS")?)
        .set("security.protocol", "SASL_SSL")
        .set("sasl.mechanisms", "PLAIN")
        .set("sasl.username", &env::var("SASL_USERNAME")?)
        .set("sasl.password", &env::var("SASL_PASSWORD")?)
        .set("enable.ssl.certificate.verification", "false")
        .set("message.timeout.ms", "5000")
        .create()
        .context("Failed to create Kafka consumer")?;

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &env::var("BOOTSTRAP_SERVERS")?)
        .set("security.protocol", "SASL_SSL")
        .set("sasl.mechanisms", "PLAIN")
        .set("sasl.username", &env::var("SASL_USERNAME")?)
        .set("sasl.password", &env::var("SASL_PASSWORD")?)
        .set("enable.ssl.certificate.verification", "false")
        .set("message.timeout.ms", "5000")
        .create()
        .context("Failed to create Kafka producer")?;

    let input_topic = env::var("INPUT_TOPIC")?;
    let output_topic = env::var("OUTPUT_TOPIC")?;

    consumer
        .subscribe(&[&input_topic])
        .expect("Can't subscribe to specified topic");

    consumer
        .start()
        .for_each(|borrowed_message| {
            let producer = producer.clone();
            let owned_message = borrowed_message.unwrap().detach();
            let output_topic = output_topic.clone();
            tokio::spawn(async {
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
                    let mut oi2 = oi.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                        oi2.side = match oi.side {
                            Side::Buy => Side::Sell,
                            Side::Sell => Side::Buy,
                        };
                        producer
                            .send(
                                FutureRecord::to(&output_topic).key(&oi2.symbol).payload(
                                    &serde_json::to_string(&oi2)
                                        .expect("failed to serialize order intent"),
                                ),
                                0,
                            )
                            .await
                    });
                }
            });
            future::ready(())
        })
        .await;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenv();
    env_logger::builder().format_timestamp_micros().init();
    info!("Starting naive strategy");

    run_async_processor().await
}
