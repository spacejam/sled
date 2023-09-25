use std::{
    num::NonZeroU64,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use pagetable::PageTable;

use crate::{
    heap::{SlabAddress, N_SLABS},
    NodeId,
};

#[derive(Clone)]
struct SlabTenancy {
    inner: Arc<[SlabTenancyInner; N_SLABS]>,
}

struct SlabTenancyInner {
    slot_to_object_id: PageTable<AtomicU64>,
}

#[derive(Clone)]
pub(crate) struct ObjectLocationMap {
    pub(crate) object_id_to_location: PageTable<AtomicU64>,
    // location_to_object_id: SlabTenancy,
}

impl ObjectLocationMap {
    pub(crate) fn get_location_for_object(
        &self,
        object_id: NodeId,
    ) -> crate::SlabAddress {
        let location_u64 =
            self.object_id_to_location.get(object_id.0).load(Ordering::Acquire);

        let nzu = NonZeroU64::new(location_u64)
            .expect("node location metadata not present in pagetable");

        SlabAddress::from(nzu)
    }

    pub(crate) fn insert(
        &self,
        object_id: NodeId,
        new_location: SlabAddress,
    ) -> Option<crate::SlabAddress> {
        let location_nzu: NonZeroU64 = new_location.into();
        let location_u64 = location_nzu.get();

        let last_u64 = self
            .object_id_to_location
            .get(object_id.0)
            .swap(location_u64, Ordering::Release);

        if let Some(nzu) = NonZeroU64::new(last_u64) {
            let last_address = SlabAddress::from(nzu);
            Some(last_address)
        } else {
            None
        }
    }

    pub(crate) fn remove(
        &self,
        object_id: NodeId,
    ) -> Option<crate::SlabAddress> {
        let last_u64 = self
            .object_id_to_location
            .get(object_id.0)
            .swap(0, Ordering::Release);

        if let Some(nzu) = NonZeroU64::new(last_u64) {
            let last_address = SlabAddress::from(nzu);
            Some(last_address)
        } else {
            None
        }
    }
}
