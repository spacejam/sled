use either::Either;
use nodex::prelude::*;
use sled::*;
use std::sync::Arc;

type SledInner = Arc<Either<Db, Tree>>;

pub fn class(env: NapiEnv) -> NapiResult<JsClass> {
    JsClass::new(
        env,
        "sled",
        move |mut this, params: JsObject| {
            let env = this.env();
            let desc = [
                DescriptorMethodBuilder::new()
                    .with_utf8name("get")
                    .with_method(move |this, key: JsArrayBuffer| {
                        let env = this.env();
                        let inner = if let Some(inner) = this.unwrap::<SledInner>()? {
                            inner
                        } else {
                            env.throw_error("sled context missing")?;
                            return env.undefined().map(|u| u.value());
                        };

                        let key = key.buffer()?;
                        match &**inner {
                            Either::Left(ref db) => {
                                if let Some(value) = db.get(key).unwrap() {
                                    env.arraybuffer(value).map(|buffer| buffer.value())
                                } else {
                                    env.null().map(|u| u.value())
                                }
                            }
                            Either::Right(ref tree) => {
                                if let Some(value) = tree.get(key).unwrap() {
                                    env.arraybuffer(value).map(|buffer| buffer.value())
                                } else {
                                    env.null().map(|u| u.value())
                                }
                            }
                        }
                    })
                    .build()?,
                DescriptorMethodBuilder::new()
                    .with_utf8name("insert")
                    .with_method(move |this, (key, value): (JsArrayBuffer, JsArrayBuffer)| {
                        let env = this.env();
                        let inner = if let Some(inner) = this.unwrap::<SledInner>()? {
                            inner
                        } else {
                            env.throw_error("sled context missing")?;
                            return env.undefined().map(|u| u.value());
                        };

                        match &**inner {
                            Either::Left(ref db) => {
                                if let Some(value) =
                                    db.insert(key.buffer()?, value.buffer()?).unwrap()
                                {
                                    env.arraybuffer(value).map(|buffer| buffer.value())
                                } else {
                                    env.null().map(|u| u.value())
                                }
                            }
                            Either::Right(ref tree) => {
                                if let Some(value) =
                                    tree.insert(key.buffer()?, value.buffer()?).unwrap()
                                {
                                    env.arraybuffer(value).map(|buffer| buffer.value())
                                } else {
                                    env.null().map(|u| u.value())
                                }
                            }
                        }
                    })
                    .build()?,
                DescriptorMethodBuilder::new()
                    .with_utf8name("remove")
                    .with_method(move |this, key: JsArrayBuffer| {
                        let env = this.env();
                        let inner = if let Some(inner) = this.unwrap::<SledInner>()? {
                            inner
                        } else {
                            env.throw_error("sled context missing")?;
                            return env.undefined().map(|u| u.value());
                        };

                        let key = key.buffer()?;
                        match &**inner {
                            Either::Left(ref db) => {
                                if let Some(value) = db.remove(key).unwrap() {
                                    env.arraybuffer(value).map(|buffer| buffer.value())
                                } else {
                                    env.null().map(|u| u.value())
                                }
                            }
                            Either::Right(ref tree) => {
                                if let Some(value) = tree.remove(key).unwrap() {
                                    env.arraybuffer(value).map(|buffer| buffer.value())
                                } else {
                                    env.null().map(|u| u.value())
                                }
                            }
                        }
                    })
                    .build()?,
            ];

            let path: JsValue = params.get("path")?;
            let use_compression: JsValue = params.get("use_compression")?;

            let mut config = sled::Config::new();

            if let Ok(path) = path.cast_checked::<JsString>() {
                config = config.path(path.get()?);
            }

            if let Ok(use_compression) = use_compression.cast_checked::<JsBoolean>() {
                config = config.use_compression(use_compression.get()?);
            }

            let db = match config.open() {
                Ok(db) => db,
                Err(e) => {
                    env.throw_error(format!("{}", e))?;
                    return env.undefined();
                }
            };

            this.define_properties(&desc)?;

            let db1 = db.clone();
            this.set(
                "open_tree",
                env.func(move |this, tree_name: JsString| {
                    let env = this.env();
                    let name = tree_name.get()?;
                    let mut tree_obj = env.object()?;

                    let tree = db1.open_tree(name).unwrap();

                    tree_obj.wrap(
                        Arc::new(Either::<Db, Tree>::Right(tree)),
                        move |_, _: SledInner| Ok(()),
                    )?;

                    tree_obj.define_properties(&desc)?;

                    Ok(tree_obj)
                })?,
            )?;

            let db1 = db.clone();
            this.set(
                "tree_names",
                env.func(move |this, ()| {
                    let env = this.env();
                    let mut array = env.array()?;

                    for (idx, name) in db1.tree_names().into_iter().enumerate() {
                        let name = env.arraybuffer(&name)?;
                        array.set(idx as u32, name)?;
                    }

                    Ok(array)
                })?,
            )?;

            this.wrap(Arc::new(Either::<Db, Tree>::Left(db)), move |_, _| Ok(()))?;

            this.undefined()
        },
        &[],
    )
}
