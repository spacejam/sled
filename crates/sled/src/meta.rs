use super::*;

pub(crate) fn meta<'a>(
    pages: &PageCache<BLinkMaterializer, Frag, Recovery>,
    guard: &'a Guard,
) -> Result<&'a Meta, ()> {
    let meta_page_get =
        pages.get(META_PID, guard).map_err(|e| e.danger_cast())?;

    let meta = match meta_page_get {
        PageGet::Materialized(ref meta_ptr, ref _ptr) => {
            meta_ptr.unwrap_meta()
        }
        broken => {
            panic!("pagecache returned non-base node: {:?}", broken)
        }
    };

    Ok(meta)
}

pub(crate) fn pid_for_name(
    pages: &PageCache<BLinkMaterializer, Frag, Recovery>,
    name: &[u8],
    guard: &Guard,
) -> Result<Option<usize>, ()> {
    let m = meta(pages, guard)?;
    Ok(m.get_root(name))
}

pub(crate) fn cas_root(
    pages: &PageCache<BLinkMaterializer, Frag, Recovery>,
    name: Vec<u8>,
    old: Option<usize>,
    new: usize,
    guard: &Guard,
) -> Result<(), Option<usize>> {
    let meta_page_get =
        pages.get(META_PID, guard).map_err(|e| e.danger_cast())?;

    let (meta_key, meta) = match meta_page_get {
        PageGet::Materialized(ref meta_ptr, ref key) => {
            let meta = meta_ptr.unwrap_meta();
            (key, meta)
        }
        broken => {
            panic!("pagecache returned non-base node: {:?}", broken)
        }
    };

    let mut current: TreePtr = meta_key.clone();
    loop {
        let actual = meta.get_root(&name);
        if actual != old {
            return Err(Error::CasFailed(actual));
        }

        let mut new_meta = meta.clone();
        new_meta.set_root(name.clone(), new);

        let new_meta_frag = Frag::Meta(new_meta);
        let res = pages
            .replace(META_PID, current, new_meta_frag, &guard)
            .map_err(|e| e.danger_cast());

        match res {
            Ok(_) => return Ok(()),
            Err(Error::CasFailed(actual)) => current = actual,
            Err(other) => return Err(other.danger_cast()),
        }
    }
}
