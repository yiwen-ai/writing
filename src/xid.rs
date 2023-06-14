use std::default::Default;

pub struct Xid(pub xid::Id);

impl Default for Xid {
    fn default() -> Self {
        Xid(xid::new())
    }
}
