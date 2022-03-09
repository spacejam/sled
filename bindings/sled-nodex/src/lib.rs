#![allow(unused_mut)]
use nodex::prelude::*;

nodex::napi_module!(init);

mod db;

fn init(env: NapiEnv, mut exports: JsObject) -> NapiResult<()> {
    exports.set("sled", db::class(env)?)?;
    Ok(())
}
