//! structured data store support for `sled` db
//! # Examples
//!
//! ```
//! use serde::{Deserialize, Serialize};

//! #[derive(Serialize, Deserialize, Debug)]
//! struct Info {
//!     name: String,
//!     age: u8,
//! }

//! let linus_walker = Info { name: "Linus Walker".into(), age: 14 };
//! let db = crate::open("db").unwrap();
//! db.insert_json("linus_walker", &linus_walker).unwrap();
//! let resul = db.get_structued::<Info>(&"linus_walker".to_string()).unwrap();
//! println!("{resul:?}");
//!}
//! ```

use crate::{Db, IVec, Result};
use serde::{de::Deserialize, ser::Serialize};

impl Db {
    /// function implementation for `Db` to store structured data as json in db
    pub fn insert_json<K: AsRef<[u8]>, V: ?Sized + Serialize>(
        &self,
        key: K,
        value: &V,
    ) -> Result<Option<IVec>> {
        let value = serde_json::to_string(value).unwrap();
        self.insert(key, value.as_bytes())
    }

    /// function implementation to get structured value as output from database
    pub fn get_structured<T: for<'a> Deserialize<'a>>(
        &self,
        key: &dyn AsRef<[u8]>,
    ) -> std::result::Result<T, serde_json::Error> {
        let value = self.get(key.as_ref()).unwrap().unwrap();
        let string_value = String::from_utf8(value.to_vec()).unwrap();
        serde_json::from_str(&string_value)
    }
}
