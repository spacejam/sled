use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use fnv::FnvHashSet;
use pagetable::PageTable;

use crate::{
    heap::{SlabAddress, UpdateMetadata, N_SLABS},
    Allocator, ObjectId,
};

#[derive(Debug, Default, Copy, Clone)]
pub struct AllocatorStats {
    pub objects_allocated: u64,
    pub objects_freed: u64,
    pub heap_slots_allocated: u64,
    pub heap_slots_freed: u64,
}

#[derive(Default)]
struct SlabTenancy {
    slot_to_object_id: PageTable<AtomicU64>,
    slot_allocator: Arc<Allocator>,
}

impl SlabTenancy {
    // returns (ObjectId, slot index) pairs
    fn objects_to_defrag(
        &self,
        target_fill_ratio: f32,
    ) -> Vec<(ObjectId, u64)> {
        let (frag_min, frag_max) = if let Some(frag) =
            self.slot_allocator.fragmentation_cutoff(target_fill_ratio)
        {
            frag
        } else {
            return vec![];
        };

        let mut ret = vec![];

        for fragmented_slot in frag_min..frag_max {
            let object_id_u64 = self
                .slot_to_object_id
                .get(fragmented_slot)
                .load(Ordering::Acquire);

            if let Some(object_id) = ObjectId::new(object_id_u64) {
                ret.push((object_id, fragmented_slot));
            }
        }

        ret
    }
}

#[derive(Clone)]
pub(crate) struct ObjectLocationMapper {
    object_id_to_location: PageTable<AtomicU64>,
    slab_tenancies: Arc<[SlabTenancy; N_SLABS]>,
    object_id_allocator: Arc<Allocator>,
    target_fill_ratio: f32,
}

impl ObjectLocationMapper {
    pub(crate) fn new(
        recovered_metadata: &[UpdateMetadata],
        target_fill_ratio: f32,
    ) -> ObjectLocationMapper {
        let mut ret = ObjectLocationMapper {
            object_id_to_location: PageTable::default(),
            slab_tenancies: Arc::new(core::array::from_fn(|_| {
                SlabTenancy::default()
            })),
            object_id_allocator: Arc::default(),
            target_fill_ratio,
        };

        let mut object_ids: FnvHashSet<u64> = Default::default();
        let mut slots_per_slab: [FnvHashSet<u64>; N_SLABS] =
            core::array::from_fn(|_| Default::default());

        for update_metadata in recovered_metadata {
            match update_metadata {
                UpdateMetadata::Store {
                    object_id,
                    collection_id: _,
                    location,
                    metadata: _,
                } => {
                    object_ids.insert(**object_id);
                    let slab_address = SlabAddress::from(*location);
                    slots_per_slab[slab_address.slab() as usize]
                        .insert(slab_address.slot());
                    ret.insert(*object_id, slab_address);
                }
                UpdateMetadata::Free { .. } => {
                    unreachable!()
                }
            }
        }

        ret.object_id_allocator =
            Arc::new(Allocator::from_allocated(&object_ids));

        let slabs = Arc::get_mut(&mut ret.slab_tenancies).unwrap();

        for i in 0..N_SLABS {
            let slab = &mut slabs[i];
            slab.slot_allocator =
                Arc::new(Allocator::from_allocated(&slots_per_slab[i]));
        }

        ret
    }

    pub(crate) fn get_max_allocated_per_slab(&self) -> Vec<(usize, u64)> {
        let mut ret = vec![];

        for (i, slab) in self.slab_tenancies.iter().enumerate() {
            if let Some(max_allocated) = slab.slot_allocator.max_allocated() {
                ret.push((i, max_allocated));
            }
        }

        ret
    }

    pub(crate) fn stats(&self) -> AllocatorStats {
        let (objects_allocated, objects_freed) =
            self.object_id_allocator.counters();

        let mut heap_slots_allocated = 0;
        let mut heap_slots_freed = 0;

        for slab_id in 0..N_SLABS {
            let (allocated, freed) =
                self.slab_tenancies[slab_id].slot_allocator.counters();
            heap_slots_allocated += allocated;
            heap_slots_freed += freed;
        }

        AllocatorStats {
            objects_allocated,
            objects_freed,
            heap_slots_allocated,
            heap_slots_freed,
        }
    }

    pub(crate) fn clone_object_id_allocator_arc(&self) -> Arc<Allocator> {
        self.object_id_allocator.clone()
    }

    pub(crate) fn allocate_object_id(&self) -> ObjectId {
        // object IDs wrap a NonZeroU64, so if we get 0, just re-allocate and leak the id

        let mut object_id = self.object_id_allocator.allocate();
        if object_id == 0 {
            object_id = self.object_id_allocator.allocate();
            assert_ne!(object_id, 0);
        }
        ObjectId::new(object_id).unwrap()
    }

    pub(crate) fn clone_slab_allocator_arc(
        &self,
        slab_id: u8,
    ) -> Arc<Allocator> {
        self.slab_tenancies[usize::from(slab_id)].slot_allocator.clone()
    }

    pub(crate) fn allocate_slab_slot(&self, slab_id: u8) -> SlabAddress {
        let slot =
            self.slab_tenancies[usize::from(slab_id)].slot_allocator.allocate();
        SlabAddress::from_slab_slot(slab_id, slot)
    }

    pub(crate) fn free_slab_slot(&self, slab_address: SlabAddress) {
        self.slab_tenancies[usize::from(slab_address.slab())]
            .slot_allocator
            .free(slab_address.slot())
    }

    pub(crate) fn get_location_for_object(
        &self,
        object_id: ObjectId,
    ) -> Option<crate::SlabAddress> {
        let location_u64 =
            self.object_id_to_location.get(*object_id).load(Ordering::Acquire);

        let nzu = NonZeroU64::new(location_u64)?;

        Some(SlabAddress::from(nzu))
    }

    /// Returns the previous address for this object, if it is vacating one.
    ///
    /// # Panics
    ///
    /// Asserts that the new location is actually unoccupied. This is a major
    /// correctness violation if that isn't true.
    pub(crate) fn insert(
        &self,
        object_id: ObjectId,
        new_location: SlabAddress,
    ) -> Option<SlabAddress> {
        // insert into object_id_to_location
        let location_nzu: NonZeroU64 = new_location.into();
        let location_u64 = location_nzu.get();

        let last_u64 = self
            .object_id_to_location
            .get(*object_id)
            .swap(location_u64, Ordering::Release);

        let last_address_opt = if let Some(nzu) = NonZeroU64::new(last_u64) {
            let last_address = SlabAddress::from(nzu);
            Some(last_address)
        } else {
            None
        };

        // insert into slab_tenancies
        let slab = new_location.slab();
        let slot = new_location.slot();

        let _last_oid_at_location = self.slab_tenancies[usize::from(slab)]
            .slot_to_object_id
            .get(slot)
            .swap(*object_id, Ordering::Release);

        // TODO add debug event verifier here assert_eq!(0, last_oid_at_location);

        last_address_opt
    }

    /// Unmaps an object and returns its location.
    ///
    /// # Panics
    ///
    /// Asserts that the object was actually stored in a location.
    pub(crate) fn remove(&self, object_id: ObjectId) -> Option<SlabAddress> {
        let last_u64 = self
            .object_id_to_location
            .get(*object_id)
            .swap(0, Ordering::Release);

        if let Some(nzu) = NonZeroU64::new(last_u64) {
            let last_address = SlabAddress::from(nzu);

            let slab = last_address.slab();
            let slot = last_address.slot();

            let last_oid_at_location = self.slab_tenancies[usize::from(slab)]
                .slot_to_object_id
                .get(slot)
                .swap(0, Ordering::Release);

            assert_eq!(*object_id, last_oid_at_location);

            Some(last_address)
        } else {
            None
        }
    }

    pub(crate) fn objects_to_defrag(&self) -> FnvHashSet<ObjectId> {
        let mut ret = FnvHashSet::default();

        for slab_id in 0..N_SLABS {
            let slab = &self.slab_tenancies[usize::from(slab_id)];

            for (object_id, slot) in
                slab.objects_to_defrag(self.target_fill_ratio)
            {
                let sa = SlabAddress::from_slab_slot(
                    u8::try_from(slab_id).unwrap(),
                    slot,
                );

                let rt_sa = if let Some(rt_raw_sa) = NonZeroU64::new(
                    self.object_id_to_location
                        .get(*object_id)
                        .load(Ordering::Acquire),
                ) {
                    SlabAddress::from(rt_raw_sa)
                } else {
                    // object has been removed but its slot has not yet been freed,
                    // hopefully due to a deferred write
                    // TODO test that with a testing event log
                    continue;
                };

                if sa == rt_sa {
                    let newly_inserted = ret.insert(object_id);
                    assert!(newly_inserted, "{object_id:?} present multiple times across slab objects_to_defrag");
                }
            }
        }

        ret
    }
}
