mod model_collection;
mod model_content;
mod model_creation;
mod model_publication;

use model_content::Content;

pub mod scylladb;

pub use model_collection::Collection;
pub use model_creation::{Creation, CreationIndex};
pub use model_publication::Publication;

pub static USER_JARVIS: &str = "0000000000000jarvis0"; // system user
pub static USER_ANON: &str = "000000000000000anon0"; // anonymous user
