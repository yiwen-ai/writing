mod model_creation;

pub mod scylladb;

pub use model_creation::Creation;

pub trait ToAnyhowError {
    fn to_anyhow_error(self) -> anyhow::Error;
}
