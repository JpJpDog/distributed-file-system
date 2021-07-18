use std::fmt::Display;

#[derive(Debug)]
pub struct MetaError {
    pub inner: String,
}

impl Display for MetaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}
