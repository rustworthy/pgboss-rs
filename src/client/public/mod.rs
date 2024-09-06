mod connect_ops;
mod job_ops;
mod queue_ops;
use super::{builder::ClientBuilder, opts, Client};

impl Client {
    /// Create an instance of [`ClientBuilder`]
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }
}
