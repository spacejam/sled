/* Redis uses the CRC64 variant with "Jones" coefficients and init value of 0.
 *
 * Specification of this CRC64 variant follows:
 * Name: crc-64-jones
 * Width: 64 bites
 * Poly: 0xad93_d235_94c9_35a9
 * Reflected In: True
 * Xor_In: 0xffff_ffff_ffff_ffff
 * Reflected_Out: True
 * Xor_Out: 0x0
 * Check("123456789"): 0xe9c6_d914_c4b8_d9ca
 *
 * Copyright (c) 2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * Copyright (c) 2017, Tyler Neely (ported to rust)
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE. */

#[cfg_attr(rustfmt, rustfmt_skip)]
const CRC64TAB: [u64; 256] = [
    0x0000_0000_0000_0000, 0x7ad8_70c8_3035_8979,
    0xf5b0_e190_606b_12f2, 0x8f68_9158_505e_9b8b,
    0xc038_e573_9841_b68f, 0xbae0_95bb_a874_3ff6,
    0x3588_04e3_f82a_a47d, 0x4f50_742b_c81f_2d04,
    0xab28_ecb4_6814_fe75, 0xd1f0_9c7c_5821_770c,
    0x5e98_0d24_087f_ec87, 0x2440_7dec_384a_65fe,
    0x6b10_09c7_f055_48fa, 0x11c8_790f_c060_c183,
    0x9ea0_e857_903e_5a08, 0xe478_989f_a00b_d371,
    0x7d08_ff3b_88be_6f81, 0x07d0_8ff3_b88b_e6f8,
    0x88b8_1eab_e8d5_7d73, 0xf260_6e63_d8e0_f40a,
    0xbd30_1a48_10ff_d90e, 0xc7e8_6a80_20ca_5077,
    0x4880_fbd8_7094_cbfc, 0x3258_8b10_40a1_4285,
    0xd620_138f_e0aa_91f4, 0xacf8_6347_d09f_188d,
    0x2390_f21f_80c1_8306, 0x5948_82d7_b0f4_0a7f,
    0x1618_f6fc_78eb_277b, 0x6cc0_8634_48de_ae02,
    0xe3a8_176c_1880_3589, 0x9970_67a4_28b5_bcf0,
    0xfa11_fe77_117c_df02, 0x80c9_8ebf_2149_567b,
    0x0fa1_1fe7_7117_cdf0, 0x7579_6f2f_4122_4489,
    0x3a29_1b04_893d_698d, 0x40f1_6bcc_b908_e0f4,
    0xcf99_fa94_e956_7b7f, 0xb541_8a5c_d963_f206,
    0x5139_12c3_7968_2177, 0x2be1_620b_495d_a80e,
    0xa489_f353_1903_3385, 0xde51_839b_2936_bafc,
    0x9101_f7b0_e129_97f8, 0xebd9_8778_d11c_1e81,
    0x64b1_1620_8142_850a, 0x1e69_66e8_b177_0c73,
    0x8719_014c_99c2_b083, 0xfdc1_7184_a9f7_39fa,
    0x72a9_e0dc_f9a9_a271, 0x0871_9014_c99c_2b08,
    0x4721_e43f_0183_060c, 0x3df9_94f7_31b6_8f75,
    0xb291_05af_61e8_14fe, 0xc849_7567_51dd_9d87,
    0x2c31_edf8_f1d6_4ef6, 0x56e9_9d30_c1e3_c78f,
    0xd981_0c68_91bd_5c04, 0xa359_7ca0_a188_d57d,
    0xec09_088b_6997_f879, 0x96d1_7843_59a2_7100,
    0x19b9_e91b_09fc_ea8b, 0x6361_99d3_39c9_63f2,
    0xdf7a_dabd_7a6e_2d6f, 0xa5a2_aa75_4a5b_a416,
    0x2aca_3b2d_1a05_3f9d, 0x5012_4be5_2a30_b6e4,
    0x1f42_3fce_e22f_9be0, 0x659a_4f06_d21a_1299,
    0xeaf2_de5e_8244_8912, 0x902a_ae96_b271_006b,
    0x7452_3609_127a_d31a, 0x0e8a_46c1_224f_5a63,
    0x81e2_d799_7211_c1e8, 0xfb3a_a751_4224_4891,
    0xb46a_d37a_8a3b_6595, 0xceb2_a3b2_ba0e_ecec,
    0x41da_32ea_ea50_7767, 0x3b02_4222_da65_fe1e,
    0xa272_2586_f2d0_42ee, 0xd8aa_554e_c2e5_cb97,
    0x57c2_c416_92bb_501c, 0x2d1a_b4de_a28e_d965,
    0x624a_c0f5_6a91_f461, 0x1892_b03d_5aa4_7d18,
    0x97fa_2165_0afa_e693, 0xed22_51ad_3acf_6fea,
    0x095a_c932_9ac4_bc9b, 0x7382_b9fa_aaf1_35e2,
    0xfcea_28a2_faaf_ae69, 0x8632_586a_ca9a_2710,
    0xc962_2c41_0285_0a14, 0xb3ba_5c89_32b0_836d,
    0x3cd2_cdd1_62ee_18e6, 0x460a_bd19_52db_919f,
    0x256b_24ca_6b12_f26d, 0x5fb3_5402_5b27_7b14,
    0xd0db_c55a_0b79_e09f, 0xaa03_b592_3b4c_69e6,
    0xe553_c1b9_f353_44e2, 0x9f8b_b171_c366_cd9b,
    0x10e3_2029_9338_5610, 0x6a3b_50e1_a30d_df69,
    0x8e43_c87e_0306_0c18, 0xf49b_b8b6_3333_8561,
    0x7bf3_29ee_636d_1eea, 0x012b_5926_5358_9793,
    0x4e7b_2d0d_9b47_ba97, 0x34a3_5dc5_ab72_33ee,
    0xbbcb_cc9d_fb2c_a865, 0xc113_bc55_cb19_211c,
    0x5863_dbf1_e3ac_9dec, 0x22bb_ab39_d399_1495,
    0xadd3_3a61_83c7_8f1e, 0xd70b_4aa9_b3f2_0667,
    0x985b_3e82_7bed_2b63, 0xe283_4e4a_4bd8_a21a,
    0x6deb_df12_1b86_3991, 0x1733_afda_2bb3_b0e8,
    0xf34b_3745_8bb8_6399, 0x8993_478d_bb8d_eae0,
    0x06fb_d6d5_ebd3_716b, 0x7c23_a61d_dbe6_f812,
    0x3373_d236_13f9_d516, 0x49ab_a2fe_23cc_5c6f,
    0xc6c3_33a6_7392_c7e4, 0xbc1b_436e_43a7_4e9d,
    0x95ac_9329_ac4b_c9b5, 0xef74_e3e1_9c7e_40cc,
    0x601c_72b9_cc20_db47, 0x1ac4_0271_fc15_523e,
    0x5594_765a_340a_7f3a, 0x2f4c_0692_043f_f643,
    0xa024_97ca_5461_6dc8, 0xdafc_e702_6454_e4b1,
    0x3e84_7f9d_c45f_37c0, 0x445c_0f55_f46a_beb9,
    0xcb34_9e0d_a434_2532, 0xb1ec_eec5_9401_ac4b,
    0xfebc_9aee_5c1e_814f, 0x8464_ea26_6c2b_0836,
    0x0b0c_7b7e_3c75_93bd, 0x71d4_0bb6_0c40_1ac4,
    0xe8a4_6c12_24f5_a634, 0x927c_1cda_14c0_2f4d,
    0x1d14_8d82_449e_b4c6, 0x67cc_fd4a_74ab_3dbf,
    0x289c_8961_bcb4_10bb, 0x5244_f9a9_8c81_99c2,
    0xdd2c_68f1_dcdf_0249, 0xa7f4_1839_ecea_8b30,
    0x438c_80a6_4ce1_5841, 0x3954_f06e_7cd4_d138,
    0xb63c_6136_2c8a_4ab3, 0xcce4_11fe_1cbf_c3ca,
    0x83b4_65d5_d4a0_eece, 0xf96c_151d_e495_67b7,
    0x7604_8445_b4cb_fc3c, 0x0cdc_f48d_84fe_7545,
    0x6fbd_6d5e_bd37_16b7, 0x1565_1d96_8d02_9fce,
    0x9a0d_8cce_dd5c_0445, 0xe0d5_fc06_ed69_8d3c,
    0xaf85_882d_2576_a038, 0xd55d_f8e5_1543_2941,
    0x5a35_69bd_451d_b2ca, 0x20ed_1975_7528_3bb3,
    0xc495_81ea_d523_e8c2, 0xbe4d_f122_e516_61bb,
    0x3125_607a_b548_fa30, 0x4bfd_10b2_857d_7349,
    0x04ad_6499_4d62_5e4d, 0x7e75_1451_7d57_d734,
    0xf11d_8509_2d09_4cbf, 0x8bc5_f5c1_1d3c_c5c6,
    0x12b5_9265_3589_7936, 0x686d_e2ad_05bc_f04f,
    0xe705_73f5_55e2_6bc4, 0x9ddd_033d_65d7_e2bd,
    0xd28d_7716_adc8_cfb9, 0xa855_07de_9dfd_46c0,
    0x273d_9686_cda3_dd4b, 0x5de5_e64e_fd96_5432,
    0xb99d_7ed1_5d9d_8743, 0xc345_0e19_6da8_0e3a,
    0x4c2d_9f41_3df6_95b1, 0x36f5_ef89_0dc3_1cc8,
    0x79a5_9ba2_c5dc_31cc, 0x037d_eb6a_f5e9_b8b5,
    0x8c15_7a32_a5b7_233e, 0xf6cd_0afa_9582_aa47,
    0x4ad6_4994_d625_e4da, 0x300e_395c_e610_6da3,
    0xbf66_a804_b64e_f628, 0xc5be_d8cc_867b_7f51,
    0x8aee_ace7_4e64_5255, 0xf036_dc2f_7e51_db2c,
    0x7f5e_4d77_2e0f_40a7, 0x0586_3dbf_1e3a_c9de,
    0xe1fe_a520_be31_1aaf, 0x9b26_d5e8_8e04_93d6,
    0x144e_44b0_de5a_085d, 0x6e96_3478_ee6f_8124,
    0x21c6_4053_2670_ac20, 0x5b1e_309b_1645_2559,
    0xd476_a1c3_461b_bed2, 0xaeae_d10b_762e_37ab,
    0x37de_b6af_5e9b_8b5b, 0x4d06_c667_6eae_0222,
    0xc26e_573f_3ef0_99a9, 0xb8b6_27f7_0ec5_10d0,
    0xf7e6_53dc_c6da_3dd4, 0x8d3e_2314_f6ef_b4ad,
    0x0256_b24c_a6b1_2f26, 0x788e_c284_9684_a65f,
    0x9cf6_5a1b_368f_752e, 0xe62e_2ad3_06ba_fc57,
    0x6946_bb8b_56e4_67dc, 0x139e_cb43_66d1_eea5,
    0x5cce_bf68_aece_c3a1, 0x2616_cfa0_9efb_4ad8,
    0xa97e_5ef8_cea5_d153, 0xd3a6_2e30_fe90_582a,
    0xb0c7_b7e3_c759_3bd8, 0xca1f_c72b_f76c_b2a1,
    0x4577_5673_a732_292a, 0x3faf_26bb_9707_a053,
    0x70ff_5290_5f18_8d57, 0x0a27_2258_6f2d_042e,
    0x854f_b300_3f73_9fa5, 0xff97_c3c8_0f46_16dc,
    0x1bef_5b57_af4d_c5ad, 0x6137_2b9f_9f78_4cd4,
    0xee5f_bac7_cf26_d75f, 0x9487_ca0f_ff13_5e26,
    0xdbd7_be24_370c_7322, 0xa10f_ceec_0739_fa5b,
    0x2e67_5fb4_5767_61d0, 0x54bf_2f7c_6752_e8a9,
    0xcdcf_48d8_4fe7_5459, 0xb717_3810_7fd2_dd20,
    0x387f_a948_2f8c_46ab, 0x42a7_d980_1fb9_cfd2,
    0x0df7_adab_d7a6_e2d6, 0x772f_dd63_e793_6baf,
    0xf847_4c3b_b7cd_f024, 0x829f_3cf3_87f8_795d,
    0x66e7_a46c_27f3_aa2c, 0x1c3f_d4a4_17c6_2355,
    0x9357_45fc_4798_b8de, 0xe98f_3534_77ad_31a7,
    0xa6df_411f_bfb2_1ca3, 0xdc07_31d7_8f87_95da,
    0x536f_a08f_dfd9_0e51, 0x29b7_d047_efec_8728,
];

pub fn crc64(s: &[u8]) -> u64 {
    let mut crc = 0;
    for byte in s {
        crc = CRC64TAB[((crc as u8) ^ byte) as usize] ^ (crc >> 8);
    }
    crc
}

#[test]
fn test_crc64() {
    assert_eq!(0xe9c6_d914_c4b8_d9ca, crc64(b"123456789"));
}
