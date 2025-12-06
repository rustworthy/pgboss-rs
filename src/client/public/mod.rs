mod connect_ops;
mod job_ops;
pub mod maintain_ops;
mod queue_ops;

use super::{Client, builder::ClientBuilder, opts};

impl Client {
    /// Create an instance of [`ClientBuilder`]
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }
}
