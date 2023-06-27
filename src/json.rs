//! structured data store support for `sled` db
//! # Examples
//!
//! ```
//! use sled;
//! use serde::{Deserialize, Serialize};

//! #[derive(Serialize, Deserialize, Debug)]
//! struct Info {
//!     name: String,
//!     age: u8,
//! }

//! let linus_walker = Info { name: "Linus Walker".into(), age: 14 };
//! let db = sled::open("db").unwrap();
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
    pub fn get_structued<T: for<'a> DeDeserialize<'a>>(
        &self,
        key: &dyn AsRef<[u8]>,
    ) -> Result<Option<T>> {
        match self.db.get(key.as_ref())? {
            Some(v) => {
                let x = String::from_utf8(v.to_vec()).unwrap();
                match serde_json::from_str::<T>(&x) {
                    Ok(x_v) => Ok(Some(x_v)),
                    Err(_) => Err(sled::Error::ReportableBug(
                        "Error in Json parsing to given data structure!!"
                            .into(),
                    )),
                }
            }
            None => Ok(None),
        }
    }
}
