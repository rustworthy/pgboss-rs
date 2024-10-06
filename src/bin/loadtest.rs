use clap::Parser;

#[derive(Parser)]
#[command(version, about = "Loadtest for Rust implementation of PgBoss job queueing service.", long_about = None)]
struct Cli {
    #[arg(short, long, default_value_t = 1)]
    schemas_count: usize,

    #[arg(short, long, default_value_t = 30_000)]
    jobs_count: usize,

    #[arg(short, long, default_value_t = 10)]
    threads_count: usize,
}

fn main() {
    tracing_subscriber::fmt::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let cli = Cli::parse();
    log::info!("Running a loadtest with the following settings schemas_count={}, jobs_count={}, threads_count={}", cli.schemas_count, cli.jobs_count, cli.threads_count);
}
