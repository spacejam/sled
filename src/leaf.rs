use crate::*;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct Leaf<const LEAF_FANOUT: usize> {
    pub lo: InlineArray,
    pub hi: Option<InlineArray>,
    pub prefix_length: usize,
    data: stack_map::StackMap<InlineArray, InlineArray, LEAF_FANOUT>,
    pub in_memory_size: usize,
    pub mutation_count: u64,
    #[serde(skip)]
    pub dirty_flush_epoch: Option<FlushEpoch>,
    #[serde(skip)]
    pub page_out_on_flush: Option<FlushEpoch>,
    #[serde(skip)]
    pub deleted: Option<FlushEpoch>,
    #[serde(skip)]
    pub max_unflushed_epoch: Option<FlushEpoch>,
}

impl<const LEAF_FANOUT: usize> Leaf<LEAF_FANOUT> {
    pub(crate) fn empty() -> Leaf<LEAF_FANOUT> {
        Leaf {
            lo: InlineArray::default(),
            hi: None,
            prefix_length: 0,
            data: stack_map::StackMap::default(),
            // this does not need to be marked as dirty until it actually
            // receives inserted data
            dirty_flush_epoch: None,
            in_memory_size: std::mem::size_of::<Leaf<LEAF_FANOUT>>(),
            mutation_count: 0,
            page_out_on_flush: None,
            deleted: None,
            max_unflushed_epoch: None,
        }
    }

    pub(crate) const fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub(crate) fn set_dirty_epoch(&mut self, epoch: FlushEpoch) {
        assert!(self.deleted.is_none());
        if let Some(current_epoch) = self.dirty_flush_epoch {
            assert!(current_epoch <= epoch);
        }
        if self.page_out_on_flush < Some(epoch) {
            self.page_out_on_flush = None;
        }
        self.dirty_flush_epoch = Some(epoch);
    }

    fn prefix(&self) -> &[u8] {
        assert!(self.deleted.is_none());
        &self.lo[..self.prefix_length]
    }

    pub(crate) fn get(&self, key: &[u8]) -> Option<&InlineArray> {
        assert!(self.deleted.is_none());
        assert!(key.starts_with(self.prefix()));
        let prefixed_key = &key[self.prefix_length..];
        self.data.get(prefixed_key)
    }

    pub(crate) fn insert(
        &mut self,
        key: InlineArray,
        value: InlineArray,
    ) -> Option<InlineArray> {
        assert!(self.deleted.is_none());
        assert!(key.starts_with(self.prefix()));
        let prefixed_key = key[self.prefix_length..].into();
        self.data.insert(prefixed_key, value)
    }

    pub(crate) fn remove(&mut self, key: &[u8]) -> Option<InlineArray> {
        assert!(self.deleted.is_none());
        let prefix = self.prefix();
        assert!(key.starts_with(prefix));
        let partial_key = &key[self.prefix_length..];
        self.data.remove(partial_key)
    }

    pub(crate) fn merge_from(&mut self, other: &mut Self) {
        assert!(self.is_empty());

        self.hi = other.hi.clone();

        let new_prefix_len = if let Some(hi) = &self.hi {
            self.lo.iter().zip(hi.iter()).take_while(|(l, r)| l == r).count()
        } else {
            0
        };

        assert_eq!(self.lo[..new_prefix_len], other.lo[..new_prefix_len]);

        // self.prefix_length is not read because it's expected to be
        // initialized here.
        self.prefix_length = new_prefix_len;

        if self.prefix() == other.prefix() {
            self.data = std::mem::take(&mut other.data);
            return;
        }

        assert!(
            self.prefix_length < other.prefix_length,
            "self: {:?} other: {:?}",
            self,
            other
        );

        let unshifted_key_amount = other.prefix_length - self.prefix_length;
        let unshifted_prefix = &other.lo
            [other.prefix_length - unshifted_key_amount..other.prefix_length];

        for (k, v) in other.data.iter() {
            let mut unshifted_key =
                Vec::with_capacity(unshifted_prefix.len() + k.len());
            unshifted_key.extend_from_slice(unshifted_prefix);
            unshifted_key.extend_from_slice(k);
            self.data.insert(unshifted_key.into(), v.clone());
        }

        assert_eq!(other.data.len(), self.data.len());

        #[cfg(feature = "for-internal-testing-only")]
        assert_eq!(
            self.iter().collect::<Vec<_>>(),
            other.iter().collect::<Vec<_>>(),
            "self: {:#?} \n other: {:#?}\n",
            self,
            other
        );
    }

    pub(crate) fn iter(
        &self,
    ) -> impl Iterator<Item = (InlineArray, InlineArray)> {
        let prefix = self.prefix();
        self.data.iter().map(|(k, v)| {
            let mut unshifted_key = Vec::with_capacity(prefix.len() + k.len());
            unshifted_key.extend_from_slice(prefix);
            unshifted_key.extend_from_slice(k);
            (unshifted_key.into(), v.clone())
        })
    }

    pub(crate) fn serialize(&self, zstd_compression_level: i32) -> Vec<u8> {
        let mut ret = vec![];

        let mut zstd_enc =
            zstd::stream::Encoder::new(&mut ret, zstd_compression_level)
                .unwrap();

        bincode::serialize_into(&mut zstd_enc, self).unwrap();

        zstd_enc.finish().unwrap();

        ret
    }

    pub(crate) fn deserialize(
        buf: &[u8],
    ) -> std::io::Result<Box<Leaf<LEAF_FANOUT>>> {
        let zstd_decoded = zstd::stream::decode_all(buf).unwrap();
        let mut leaf: Box<Leaf<LEAF_FANOUT>> =
            bincode::deserialize(&zstd_decoded).unwrap();

        // use decompressed buffer length as a cheap proxy for in-memory size for now
        leaf.in_memory_size = zstd_decoded.len();

        Ok(leaf)
    }

    fn set_in_memory_size(&mut self) {
        self.in_memory_size = std::mem::size_of::<Leaf<LEAF_FANOUT>>()
            + self.hi.as_ref().map(|h| h.len()).unwrap_or(0)
            + self.lo.len()
            + self.data.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>();
    }

    pub(crate) fn split_if_full(
        &mut self,
        new_epoch: FlushEpoch,
        allocator: &ObjectCache<LEAF_FANOUT>,
        collection_id: CollectionId,
    ) -> Option<(InlineArray, Object<LEAF_FANOUT>)> {
        if self.data.is_full() {
            let original_len = self.data.len();

            let old_prefix_len = self.prefix_length;
            // split
            let split_offset = if self.lo.is_empty() {
                // split left-most shard almost at the beginning for
                // optimizing downward-growing workloads
                1
            } else if self.hi.is_none() {
                // split right-most shard almost at the end for
                // optimizing upward-growing workloads
                self.data.len() - 2
            } else {
                self.data.len() / 2
            };

            let data = self.data.split_off(split_offset);

            let left_max = &self.data.last().unwrap().0;
            let right_min = &data.first().unwrap().0;

            // suffix truncation attempts to shrink the split key
            // so that shorter keys bubble up into the index
            let splitpoint_length = right_min
                .iter()
                .zip(left_max.iter())
                .take_while(|(a, b)| a == b)
                .count()
                + 1;

            let mut split_vec =
                Vec::with_capacity(self.prefix_length + splitpoint_length);
            split_vec.extend_from_slice(self.prefix());
            split_vec.extend_from_slice(&right_min[..splitpoint_length]);
            let split_key = InlineArray::from(split_vec);

            let rhs_id = allocator.allocate_object_id(new_epoch);

            log::trace!(
                "split leaf {:?} at split key: {:?} into new {:?} at {:?}",
                self.lo,
                split_key,
                rhs_id,
                new_epoch,
            );

            let mut rhs = Leaf {
                dirty_flush_epoch: Some(new_epoch),
                hi: self.hi.clone(),
                lo: split_key.clone(),
                prefix_length: 0,
                in_memory_size: 0,
                data,
                mutation_count: 0,
                page_out_on_flush: None,
                deleted: None,
                max_unflushed_epoch: None,
            };

            rhs.shorten_keys_after_split(old_prefix_len);

            rhs.set_in_memory_size();

            self.hi = Some(split_key.clone());

            self.shorten_keys_after_split(old_prefix_len);

            self.set_in_memory_size();

            assert_eq!(self.hi.as_ref().unwrap(), &split_key);
            assert_eq!(rhs.lo, &split_key);
            assert_eq!(rhs.data.len() + self.data.len(), original_len);

            let rhs_node = Object {
                object_id: rhs_id,
                collection_id,
                low_key: split_key.clone(),
                inner: Arc::new(RwLock::new(CacheBox {
                    leaf: Some(Box::new(rhs)),
                    logged_index: BTreeMap::default(),
                })),
            };

            return Some((split_key, rhs_node));
        }

        None
    }

    pub(crate) fn shorten_keys_after_split(&mut self, old_prefix_len: usize) {
        let Some(hi) = self.hi.as_ref() else { return };

        let new_prefix_len =
            self.lo.iter().zip(hi.iter()).take_while(|(l, r)| l == r).count();

        assert_eq!(self.lo[..new_prefix_len], hi[..new_prefix_len]);

        // self.prefix_length is not read because it's expected to be
        // initialized here.
        self.prefix_length = new_prefix_len;

        if new_prefix_len == old_prefix_len {
            return;
        }

        assert!(
            new_prefix_len > old_prefix_len,
            "expected new prefix length of {} to be greater than the pre-split prefix length of {} for node {:?}",
            new_prefix_len,
            old_prefix_len,
            self
        );

        let key_shift = new_prefix_len - old_prefix_len;

        for (k, v) in std::mem::take(&mut self.data).iter() {
            self.data.insert(k[key_shift..].into(), v.clone());
        }
    }
}
