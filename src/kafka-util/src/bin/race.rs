use rdkafka::TopicPartitionList;
use structopt::StructOpt;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{Consumer, DefaultConsumerContext};

#[derive(StructOpt)]
struct Args {
    // == Kafka configuration arguments. ==
    /// Address of one or more Kafka nodes, comma separated, in the Kafka
    /// cluster to connect to.
    #[structopt(short = "b", long, default_value = "localhost:9092")]
    bootstrap_servers: String,
    /// Topic from which to read records.
    #[structopt(short = "t", long = "topic")]
    topic: String,
}

#[tokio::main]
async fn main() {
    let args: Args = ore::cli::parse_args();

    let consumer: StreamConsumer<DefaultConsumerContext> = ClientConfig::new()
        .set("group.id", "blah")
        .set("bootstrap.servers", args.bootstrap_servers)
        .create_with_context(DefaultConsumerContext)
        .unwrap();

    let tps = TopicPartitionList::from_topic_map(
        &vec![((args.topic.clone(), 0), rdkafka::Offset::Beginning)]
            .into_iter()
            .collect(),
    )
    .unwrap();
    consumer.assign(&tps).unwrap();
    loop {
        consumer.recv().await.unwrap();
    }
}
