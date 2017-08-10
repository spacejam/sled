//  Copyright (c) 2017-present, Tyler Neely. All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree.
//
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE.rocksdb file in the root directory of this source tree.
//
//  Copyright (c) 2011 The LevelDB Authors. All rights reserved.
//  Use of this source code is governed by a BSD-style license that can be
//  found in the LICENSE.leveldb file.

use std::num::Wrapping;

/// Cheap hash for determining LRU shard slots.
pub fn hash(data: &[u8], raw_seed: u32) -> u32 {
    // Similar to murmur hash
    const M: Wrapping<u32> = Wrapping(0xc6a4_a793);
    const R: usize = 24;

    let seed = Wrapping(raw_seed);
    let n = Wrapping(data.len() as u32);

    let mut h = seed ^ (n * M);

    for chunk in data.chunks(4) {
        if chunk.len() == 4 {
            let w = Wrapping(slice_to_u32(chunk));
            h += w;
            h *= M;
            h ^= h >> 16;
            continue;
        }
        if chunk.len() == 3 {
            h += Wrapping(chunk[2] as i8 as u32) << 16;
        }
        if chunk.len() >= 2 {
            h += Wrapping(chunk[1] as i8 as u32) << 8;
        }
        if chunk.len() >= 1 {
            h += Wrapping(chunk[0] as i8 as u32);
            h *= M;
            h ^= h >> R;
        }
    }
    h.0
}

#[inline(always)]
fn slice_to_u32(w: &[u8]) -> u32 {
    // TODO if other endianness, reverse the array
    assert_eq!(w.len(), 4);
    ((w[3] as u32) << 24) | ((w[2] as u32) << 16) | ((w[1] as u32) << 8) | (w[0] as u32)
}

#[test]
fn test_hash() {
    let k_seed = 0xbc9f_1d34;  // Same as Bloomhash.

    assert_eq!(hash(b"", k_seed), 3_164_544_308);
    assert_eq!(hash(b"\x08", k_seed), 422_599_524);
    assert_eq!(hash(b"\x17", k_seed), 3_168_152_998);
    assert_eq!(hash(b"\x9a", k_seed), 3_195_034_349);
    assert_eq!(hash(b"\x1c", k_seed), 2_651_681_383);
    assert_eq!(hash(b"\x4d\x76", k_seed), 2_447_836_956);
    assert_eq!(hash(b"\x52\xd5", k_seed), 3_854_228_105);
    assert_eq!(hash(b"\x91\xf7", k_seed), 31_066_776);
    assert_eq!(hash(b"\xd6\x27", k_seed), 1_806_091_603);
    assert_eq!(hash(b"\x30\x46\x0b", k_seed), 3_808_221_797);
    assert_eq!(hash(b"\x56\xdc\xd6", k_seed), 2_157_698_265);
    assert_eq!(hash(b"\xd4\x52\x33", k_seed), 1_721_992_661);
    assert_eq!(hash(b"\x6a\xb5\xf4", k_seed), 2_469_105_222);
    assert_eq!(hash(b"\x67\x53\x81\x1c", k_seed), 118_283_265);
    assert_eq!(hash(b"\x69\xb8\xc0\x88", k_seed), 3_416_318_611);
    assert_eq!(hash(b"\x1e\x84\xaf\x2d", k_seed), 3_315_003_572);
    assert_eq!(hash(b"\x46\xdc\x54\xbe", k_seed), 447_346_355);
    assert_eq!(hash(b"\xd0\x7a\x6e\xea\x56", k_seed), 4_255_445_370);
    assert_eq!(hash(b"\x86\x83\xd5\xa4\xd8", k_seed), 2_390_603_402);
    assert_eq!(hash(b"\xb7\x46\xbb\x77\xce", k_seed), 2_048_907_743);
    assert_eq!(hash(b"\x6c\xa8\xbc\xe5\x99", k_seed), 2_177_978_500);
    assert_eq!(hash(b"\x5c\x5e\xe1\xa0\x73\x81", k_seed), 1_036_846_008);
    assert_eq!(hash(b"\x08\x5d\x73\x1c\xe5\x2e", k_seed), 229_980_482);
    assert_eq!(hash(b"\x42\xfb\xf2\x52\xb4\x10", k_seed), 3_655_585_422);
    assert_eq!(hash(b"\x73\xe1\xff\x56\x9c\xce", k_seed), 3_502_708_029);
    assert_eq!(hash(b"\x5c\xbe\x97\x75\x54\x9a\x52", k_seed), 815_120_748);
    assert_eq!(hash(b"\x16\x82\x39\x49\x88\x2b\x36", k_seed), 3_056_033_698);
    assert_eq!(hash(b"\x59\x77\xf0\xa7\x24\xf4\x78", k_seed), 587_205_227);
    assert_eq!(hash(b"\xd3\xa5\x7c\x0e\xc0\x02\x07", k_seed), 2_030_937_252);
    assert_eq!(hash(b"\x31\x1b\x98\x75\x96\x22\xd3\x9a", k_seed), 469_635_402);
    assert_eq!(hash(b"\x38\xd6\xf7\x28\x20\xb4\x8a\xe9", k_seed),
               3_530_274_698);
    assert_eq!(hash(b"\xbb\x18\x5d\xf4\x12\x03\xf7\x99", k_seed),
               1_974_545_809);
    assert_eq!(hash(b"\x80\xd4\x3b\x3b\xae\x22\xa2\x78", k_seed),
               3_563_570_120);
    assert_eq!(hash(b"\x1a\xb5\xd0\xfe\xab\xc3\x61\xb2\x99", k_seed),
               2_706_087_434);
    assert_eq!(hash(b"\x8e\x4a\xc3\x18\x20\x2f\x06\xe6\x3c", k_seed),
               1_534_654_151);
    assert_eq!(hash(b"\xb6\xc0\xdd\x05\x3f\xc4\x86\x4c\xef", k_seed),
               2_355_554_696);
    assert_eq!(hash(b"\x9a\x5f\x78\x0d\xaf\x50\xe1\x1f\x55", k_seed),
               1_400_800_912);
    assert_eq!(hash(b"\x22\x6f\x39\x1f\xf8\xdd\x4f\x52\x17\x94", k_seed),
               3_420_325_137);
    assert_eq!(hash(b"\x32\x89\x2a\x75\x48\x3a\x4a\x02\x69\xdd", k_seed),
               3_427_803_584);
    assert_eq!(hash(b"\x06\x92\x5c\xf4\x88\x0e\x7e\x68\x38\x3e", k_seed),
               1_152_407_945);
    assert_eq!(hash(b"\xbd\x2c\x63\x38\xbf\xe9\x78\xb7\xbf\x15", k_seed),
               3_382_479_516);
}
