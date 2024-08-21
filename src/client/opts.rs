#[derive(Debug, Clone)]
pub(crate) struct ClientOptions {
    pub(crate) schema: String,
}

impl Default for ClientOptions {
    fn default() -> Self {
        ClientOptions {
            schema: "pgboss".to_string(),
        }
    }
}
