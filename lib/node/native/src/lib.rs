#[macro_use]
extern crate neon;
extern crate sled;


use neon::vm::{Call, JsResult};
use neon::js::JsString;
use neon::js::JsNumber;
use neon::js::JsNull;
use neon::js::Value;
use neon::js::Variant;

fn threading_hint(call: Call) -> JsResult<JsNumber> {
    Ok(JsNumber::new(call.scope, num_cpus::get() as f64))
}

fn extract_arg(call: &mut Call, idx: i32) -> Result<String, ()> {
    let args = &call.arguments;
    let handle = args.get(call.scope, idx).ok_or(())?;
    Ok((*handle).to_string(call.scope).map_err(|_| ())?.value())
}

fn set(mut call: Call) -> JsResult<JsString> {
    let arg0 = extract_arg(&mut call, 0);
    let arg1 = extract_arg(&mut call, 1);

    println!("SET args {:?} {:?}", arg0, arg1);

    let t = sled::Config::default()
        .path("sled.db".to_owned())
        .tree();

    let k = arg0.unwrap().into_bytes();
    let v = arg1.unwrap().into_bytes();

    t.set(k.clone(), v);

    let from_db = t.get(&*k)
        .and_then(|from_db| {
            let str = unsafe { std::str::from_utf8_unchecked(&*from_db) };
            JsString::new(call.scope, str)
        })
        .unwrap_or_else(|| JsString::new(call.scope, "").unwrap());

    drop(t);

    Ok(from_db)
}

fn get(mut call: Call) -> JsResult<JsString> {
    let arg0 = extract_arg(&mut call, 0);

    println!("GET args {:?}", arg0);

    let t = sled::Config::default()
        .path("sled.db".to_owned())
        .tree();

    let k = arg0.unwrap().into_bytes();

    let from_db = t.get(&*k)
        .map( |from_db| {
            let str = unsafe { std::str::from_utf8_unchecked(&*from_db) };
            JsString::new(call.scope, str).unwrap()
        })
        .unwrap_or_else(|| JsString::new(call.scope, "").unwrap());

    Ok(from_db)
}

register_module!(m, {
    m.export("get", get);
    m.export("set", set);
});
