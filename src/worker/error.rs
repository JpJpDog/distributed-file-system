use std::fmt::Display;

#[derive(Debug)]
pub struct DataError {
    pub inner: String,
}

impl Display for DataError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}
