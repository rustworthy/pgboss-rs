use serde::Serialize;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all(serialize = "lowercase"))]
#[non_exhaustive]
pub enum QueuePolicy {
    #[default]
    Standard,
    Short,
    Singleton,
    Stately,
}

impl std::fmt::Display for QueuePolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Standard => "standard",
            Self::Short => "short",
            Self::Singleton => "singleton",
            Self::Stately => "stately",
        };
        write!(f, "{}", s)
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct QueueOptions {
    pub policy: QueuePolicy,
}
