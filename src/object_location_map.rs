use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use pagetable::PageTable;

use crate::{
    heap::{SlabAddress, N_SLABS},
    NodeId,
};

#[derive(Clone)]
struct SlabTenancy {
    inner: Arc<[PageTable<AtomicU64>; N_SLABS]>,
}

impl Default for SlabTenancy {
    fn default() -> SlabTenancy {
        SlabTenancy {
            inner: Arc::new(core::array::from_fn(|_| PageTable::default())),
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct ObjectLocationMap {
    object_id_to_location: PageTable<AtomicU64>,
    location_to_object_id: SlabTenancy,
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

    /// Returns the previous address for this object, if it is vacating one.
    ///
    /// # Panics
    ///
    /// Asserts that the new location is actually unoccupied. This is a major
    /// correctness violation if that isn't true.
    pub(crate) fn insert(
        &self,
        object_id: NodeId,
        new_location: SlabAddress,
    ) -> Option<SlabAddress> {
        // insert into object_id_to_location
        let location_nzu: NonZeroU64 = new_location.into();
        let location_u64 = location_nzu.get();

        let last_u64 = self
            .object_id_to_location
            .get(object_id.0)
            .swap(location_u64, Ordering::Release);

        let last_address_opt = if let Some(nzu) = NonZeroU64::new(last_u64) {
            let last_address = SlabAddress::from(nzu);
            Some(last_address)
        } else {
            None
        };

        // insert into location_to_object_id
        let slab = new_location.slab();
        let slot = new_location.slot();

        let last_oid_at_location = self.location_to_object_id.inner[slab]
            .get(slot)
            .swap(object_id.0, Ordering::Release);

        assert_eq!(0, last_oid_at_location);

        last_address_opt
    }

    /// Unmaps an object and returns its location.
    ///
    /// # Panics
    ///
    /// Asserts that the object was actually stored in a location.
    pub(crate) fn remove(&self, object_id: NodeId) -> SlabAddress {
        let last_u64 = self
            .object_id_to_location
            .get(object_id.0)
            .swap(0, Ordering::Release);

        assert_ne!(0, last_u64);

        let nzu = NonZeroU64::new(last_u64).unwrap();
        let last_address = SlabAddress::from(nzu);

        let slab = last_address.slab();
        let slot = last_address.slot();

        let last_oid_at_location = self.location_to_object_id.inner[slab]
            .get(slot)
            .swap(0, Ordering::Release);

        assert_eq!(object_id.0, last_oid_at_location);

        last_address
    }

    pub(crate) fn objects_to_defrag(&self) -> Vec<NodeId> {
        // TODO
        //todo!()
        vec![]
    }
}
