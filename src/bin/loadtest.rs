use clap::Parser;
use pgboss::{Client, Error};
use serde_json::json;
use std::sync::{atomic, Arc};

lazy_static::lazy_static! {
    static ref SCHEMA_NAME: String = format!("schema_{}", uuid::Uuid::new_v4().as_simple());
}

static QUEUES: &[&str] = &["qname"];

#[derive(Parser)]
#[command(version, about = "Loadtest for Rust implementation of PgBoss job queueing service.", long_about = None)]
struct Cli {
    #[arg(short, long, default_value_t = 30_000)]
    jobs_count: usize,

    #[arg(short, long, default_value_t = 10)]
    threads_count: usize,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let cli = Cli::parse();
    log::info!("Running a loadtest with the following settings: jobs_count={}, threads_count={}. Schema name will be {}", cli.jobs_count, cli.threads_count, SCHEMA_NAME.as_str());

    let jobs_sent = Arc::new(atomic::AtomicUsize::new(0));
    let jobs_fetched = Arc::new(atomic::AtomicUsize::new(0));

    let c = Client::builder()
        .schema(SCHEMA_NAME.as_str())
        .connect()
        .await
        .expect("connected and installed app");
    for &q in QUEUES {
        c.create_standard_queue(q).await.unwrap();
    }

    let start = std::time::Instant::now();

    let mut set = tokio::task::JoinSet::new();
    let threads_count = cli.threads_count;
    let _: Vec<_> = (0..threads_count)
        .map(|_| {
            let jobs_sent = jobs_sent.clone();
            let jobs_fetched = jobs_fetched.clone();
            set.spawn(async move {
                let c = Client::builder()
                    .schema(SCHEMA_NAME.as_str())
                    .connect()
                    .await?;
                for idx in 0..cli.jobs_count {
                    if idx % 2 == 0 {
                        let _id = c.send_data(QUEUES[0], json!({"key": "value"})).await?;
                        if jobs_sent.fetch_add(1, atomic::Ordering::SeqCst) >= cli.jobs_count {
                            return Ok(idx);
                        }
                    } else {
                        let _maybe_job = c.fetch_job(QUEUES[0]).await?;
                        if jobs_fetched.fetch_add(1, atomic::Ordering::SeqCst) >= cli.jobs_count {
                            return Ok(idx);
                        }
                    }
                }
                Ok::<usize, Error>(cli.jobs_count)
            })
        })
        .collect();

    let mut results = Vec::with_capacity(threads_count);
    while let Some(res) = set.join_next().await {
        results.push(res.unwrap());
    }

    let time_elapsed = start.elapsed();
    let seconds_elapsed = (time_elapsed.as_secs() * 1_000_000_000
        + time_elapsed.subsec_nanos() as u64) as f64
        / 1_000_000_000.0;

    log::info!(
        "Sent {} jobs and consumed {} jobs in {:.2} seconds, rate: {} jobs per second. Results: {:?}",
        jobs_sent.load(atomic::Ordering::SeqCst),
        jobs_fetched.load(atomic::Ordering::SeqCst),
        seconds_elapsed,
        cli.jobs_count as f64 / seconds_elapsed,
        results,
    );
}
