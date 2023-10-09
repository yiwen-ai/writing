mod model_bookmark;
mod model_content;
mod model_creation;
mod model_message;
mod model_publication;

use model_content::Content;

pub mod meili;
pub mod scylladb;

pub use model_bookmark::Bookmark;
pub use model_creation::{Creation, CreationIndex};
pub use model_message::Message;
pub use model_publication::{Publication, PublicationIndex};

pub static USER_JARVIS: &str = "0000000000000jarvis0"; // system user
pub static USER_ANON: &str = "000000000000000anon0"; // anonymous user
pub static DEFAULT_MODEL: &str = "gpt-3.5"; // default model
pub static MAX_ID: xid::Id = xid::Id([255; 12]);
pub static MIN_ID: xid::Id = xid::Id([0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255]);

pub fn xid_day(xid: xid::Id) -> i32 {
    let raw = xid.as_bytes();
    let unix_ts = u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]);
    (unix_ts / (3600 * 24)) as i32
}
