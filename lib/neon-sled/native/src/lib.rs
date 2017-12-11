#[macro_use]
extern crate neon;
extern crate sled;

use neon::vm::{Call, JsResult};
use neon::js::JsString;
use neon::js::JsNull;
use neon::js::Value;

fn extract_arg(call: &mut Call, idx: i32) -> Result<String, ()> {
    let args = &call.arguments;
    let handle = args.get(call.guard, idx).ok_or(())?;
    Ok((*handle).to_string(call.scope).map_err(|_| ())?.value())
}

fn create_db(mut call: Call) -> JsResult<JsString> {
    let path = extract_arg(&mut call, 0).unwrap();
    let t = sled::Config::default()
        .path(path)
        .tree();

    let ptr = Box::into_raw(Box::new(t));
    let ptr_string = format!("{}", ptr as usize);
    Ok(JsString::new(call.guard, &*ptr_string).unwrap())
}

fn cast_string_to_ptr<'a>(ptr_str: String) -> &'a sled::Tree {
    let ptr_from_str = ptr_str.parse::<usize>().unwrap();
    //println!("ptr_from_str: {}", ptr_from_str);

    let ptr = ptr_from_str as *mut sled::Tree;
    unsafe {
        &*ptr
    }
}

fn set(mut call: Call) -> JsResult<JsString> {
    let arg0 = extract_arg(&mut call, 0).unwrap();
    let arg1 = extract_arg(&mut call, 1);
    let arg2 = extract_arg(&mut call, 2);

    //println!("SET args {:?} {:?}", arg0, arg1);

    let t = cast_string_to_ptr(arg0);

    let k = arg1.unwrap().into_bytes();
    let v = arg2.unwrap().into_bytes();

    t.set(k.clone(), v);

    let from_db = t.get(&*k)
        .and_then(|from_db| {
            let str = unsafe { std::str::from_utf8_unchecked(&*from_db) };
            JsString::new(call.guard, str)
        })
        .unwrap_or_else(|| JsString::new(call.guard, "").unwrap());

    Ok(from_db)
}

fn get(mut call: Call) -> JsResult<JsString> {
    let arg0 = extract_arg(&mut call, 0).unwrap();
    let arg1 = extract_arg(&mut call, 1);

    //println!("GET args {:?}", arg0);

    let t = cast_string_to_ptr(arg0);
    let k = arg1.unwrap().into_bytes();

    let from_db = t.get(&*k)
        .map( |from_db| {
            let str = unsafe { std::str::from_utf8_unchecked(&*from_db) };
            JsString::new(call.guard, str).unwrap()
        })
        .unwrap_or_else(|| JsString::new(call.guard, "").unwrap());

    Ok(from_db)
}

fn del(mut call: Call) -> JsResult<JsNull> {
    let arg0 = extract_arg(&mut call, 0).unwrap();
    let arg1 = extract_arg(&mut call, 1);

    let t = cast_string_to_ptr(arg0);
    let k = arg1.unwrap().into_bytes();

    t.del(&*k);

    Ok(JsNull::new())
}

fn sync_and_close(mut call: Call) -> JsResult<JsNull> {
    let arg0 = extract_arg(&mut call, 0).unwrap();
    let ptr_from_str = arg0.parse::<usize>().unwrap();
    let ptr = ptr_from_str as *mut sled::Tree;

    unsafe {
        let t = Box::from_raw(ptr);
        drop(t);
    }
    Ok(JsNull::new())
}

register_module!(m, {
    m.export("get", get)?;
    m.export("set", set)?;
    m.export("del", del)?;
    m.export("createDb", create_db)?;
    m.export("syncAndClose", sync_and_close)
});
