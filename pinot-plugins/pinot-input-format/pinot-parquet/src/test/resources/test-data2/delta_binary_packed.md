<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

`delta_binary_packed.parquet` is generated with parquet-mr version 1.10.0.
The expected file contents are in `delta_binary_packed_expect.csv`.

Each column is DELTA_BINARY_PACKED-encoded, with a different delta bitwidth
for each column.

Each column has 200 rows including 1 first value and 2 blocks.  The first block
has 4 miniblocks, the second block has 3.  Each miniblock contains 32 values,
except the last one which only contains 7 values (1 + 6 \* 32 + 7 == 200).

Here is the file structure:
```
File Name: /home/antoine/parquet/testing/data/delta_binary_packed.parquet
Version: 1.0
Created By: parquet-mr version 1.10.0 (build 031a6654009e3b82020012a18434c582bd74c73a)
Total rows: 200
Number of RowGroups: 1
Number of Real Columns: 66
Number of Columns: 66
Number of Selected Columns: 66
Column 0: bitwidth0 (INT64)
Column 1: bitwidth1 (INT64)
Column 2: bitwidth2 (INT64)
Column 3: bitwidth3 (INT64)
Column 4: bitwidth4 (INT64)
Column 5: bitwidth5 (INT64)
Column 6: bitwidth6 (INT64)
Column 7: bitwidth7 (INT64)
Column 8: bitwidth8 (INT64)
Column 9: bitwidth9 (INT64)
Column 10: bitwidth10 (INT64)
Column 11: bitwidth11 (INT64)
Column 12: bitwidth12 (INT64)
Column 13: bitwidth13 (INT64)
Column 14: bitwidth14 (INT64)
Column 15: bitwidth15 (INT64)
Column 16: bitwidth16 (INT64)
Column 17: bitwidth17 (INT64)
Column 18: bitwidth18 (INT64)
Column 19: bitwidth19 (INT64)
Column 20: bitwidth20 (INT64)
Column 21: bitwidth21 (INT64)
Column 22: bitwidth22 (INT64)
Column 23: bitwidth23 (INT64)
Column 24: bitwidth24 (INT64)
Column 25: bitwidth25 (INT64)
Column 26: bitwidth26 (INT64)
Column 27: bitwidth27 (INT64)
Column 28: bitwidth28 (INT64)
Column 29: bitwidth29 (INT64)
Column 30: bitwidth30 (INT64)
Column 31: bitwidth31 (INT64)
Column 32: bitwidth32 (INT64)
Column 33: bitwidth33 (INT64)
Column 34: bitwidth34 (INT64)
Column 35: bitwidth35 (INT64)
Column 36: bitwidth36 (INT64)
Column 37: bitwidth37 (INT64)
Column 38: bitwidth38 (INT64)
Column 39: bitwidth39 (INT64)
Column 40: bitwidth40 (INT64)
Column 41: bitwidth41 (INT64)
Column 42: bitwidth42 (INT64)
Column 43: bitwidth43 (INT64)
Column 44: bitwidth44 (INT64)
Column 45: bitwidth45 (INT64)
Column 46: bitwidth46 (INT64)
Column 47: bitwidth47 (INT64)
Column 48: bitwidth48 (INT64)
Column 49: bitwidth49 (INT64)
Column 50: bitwidth50 (INT64)
Column 51: bitwidth51 (INT64)
Column 52: bitwidth52 (INT64)
Column 53: bitwidth53 (INT64)
Column 54: bitwidth54 (INT64)
Column 55: bitwidth55 (INT64)
Column 56: bitwidth56 (INT64)
Column 57: bitwidth57 (INT64)
Column 58: bitwidth58 (INT64)
Column 59: bitwidth59 (INT64)
Column 60: bitwidth60 (INT64)
Column 61: bitwidth61 (INT64)
Column 62: bitwidth62 (INT64)
Column 63: bitwidth63 (INT64)
Column 64: bitwidth64 (INT64)
Column 65: int_value (INT32)
--- Row Group: 0 ---
--- Total Bytes: 65467 ---
--- Total Compressed Bytes: 0 ---
--- Rows: 200 ---
Column 0
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 6374628540732951412, Min: 6374628540732951412
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 95, Compressed Size: 95
Column 1
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 0, Min: -104
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 114, Compressed Size: 114
Column 2
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 0, Min: -82
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 144, Compressed Size: 144
Column 3
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 0, Min: -96
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 172, Compressed Size: 172
Column 4
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 0, Min: -132
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 200, Compressed Size: 200
Column 5
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 24, Min: -290
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 228, Compressed Size: 228
Column 6
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 259, Min: -93
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 256, Compressed Size: 256
Column 7
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 476, Min: -64
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 284, Compressed Size: 284
Column 8
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 387, Min: -732
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 314, Compressed Size: 314
Column 9
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 194, Min: -1572
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 342, Compressed Size: 342
Column 10
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 5336, Min: -2353
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 370, Compressed Size: 370
Column 11
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 13445, Min: -8028
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 398, Compressed Size: 398
Column 12
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 2017, Min: -35523
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 426, Compressed Size: 426
Column 13
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 48649, Min: -4096
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 454, Compressed Size: 454
Column 14
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 65709, Min: -8244
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 482, Compressed Size: 482
Column 15
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 69786, Min: -106702
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 512, Compressed Size: 512
Column 16
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 162951, Min: -347012
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 540, Compressed Size: 540
Column 17
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 0, Min: -1054098
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 568, Compressed Size: 568
Column 18
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 664380, Min: -372793
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 596, Compressed Size: 596
Column 19
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 4001179, Min: -402775
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 624, Compressed Size: 624
Column 20
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 788039, Min: -4434785
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 652, Compressed Size: 652
Column 21
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 12455554, Min: -1070042
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 680, Compressed Size: 680
Column 22
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 2189135, Min: -17987827
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 710, Compressed Size: 710
Column 23
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 25967351, Min: -19361900
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 738, Compressed Size: 738
Column 24
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 95688064, Min: -17271207
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 766, Compressed Size: 766
Column 25
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 169215083, Min: -18759951
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 794, Compressed Size: 794
Column 26
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 163626565, Min: -168761837
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 822, Compressed Size: 822
Column 27
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 131734874, Min: -736933601
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 850, Compressed Size: 850
Column 28
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 913547745, Min: -490714808
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 878, Compressed Size: 878
Column 29
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 500305035, Min: -5834684238
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 908, Compressed Size: 908
Column 30
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 566280334, Min: -7728643109
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 936, Compressed Size: 936
Column 31
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 18831788461, Min: -2498101101
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 964, Compressed Size: 964
Column 32
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 23720914586, Min: -2147483648
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 992, Compressed Size: 992
Column 33
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 24075494509, Min: -4817999329
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 1020, Compressed Size: 1020
Column 34
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 12118456329, Min: -156025641218
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 1048, Compressed Size: 1048
Column 35
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 41614351758, Min: -114682966820
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 1076, Compressed Size: 1076
Column 36
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 82484946621, Min: -244178626927
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 1106, Compressed Size: 1106
Column 37
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 361459323159, Min: -275190620271
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 1134, Compressed Size: 1134
Column 38
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 1665294434042, Min: -420452598502
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 1162, Compressed Size: 1162
Column 39
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 110454290134, Min: -2926211785103
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 1190, Compressed Size: 1190
Column 40
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 3215717068302, Min: -4988823342986
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 1218, Compressed Size: 1218
Column 41
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 2166086616318, Min: -6488418568768
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 1246, Compressed Size: 1246
Column 42
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 10182365256028, Min: -8738522616121
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 1274, Compressed Size: 1274
Column 43
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 22909885827147, Min: -21214625470327
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 1304, Compressed Size: 1304
Column 44
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 67902133645749, Min: -9796939892175
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 1332, Compressed Size: 1332
Column 45
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 199494208930939, Min: -102473613757961
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 1360, Compressed Size: 1360
Column 46
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 18564971260296, Min: -359696498357610
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 1388, Compressed Size: 1388
Column 47
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 65624006999260, Min: -933995610201533
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 1416, Compressed Size: 1416
Column 48
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 983500521840940, Min: -878019827431629
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 1444, Compressed Size: 1444
Column 49
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 975533803684560, Min: -2091164446177739
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 1472, Compressed Size: 1472
Column 50
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 1276327559487856, Min: -5741928190724373
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 1502, Compressed Size: 1502
Column 51
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 0, Min: -15996275819941210
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 1530, Compressed Size: 1530
Column 52
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 12697545666077932, Min: -8823113595895130
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 1558, Compressed Size: 1558
Column 53
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 4785870085681342, Min: -24800000653307089
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 1586, Compressed Size: 1586
Column 54
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 26202576654140994, Min: -94647392931900711
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 1614, Compressed Size: 1614
Column 55
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 358302517069012889, Min: -32197353745654772
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 1642, Compressed Size: 1642
Column 56
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 678791154000627912, Min: -36028797018963968
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 1670, Compressed Size: 1670
Column 57
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 1444945950888122232, Min: -79246600304853010
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 1700, Compressed Size: 1700
Column 58
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 140747723990970254, Min: -1492687553985044679
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 1728, Compressed Size: 1728
Column 59
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 6318360990909070, Min: -3778424577629102559
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 1756, Compressed Size: 1756
Column 60
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 4574162334421819801, Min: -576460752303423488
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 1784, Compressed Size: 1784
Column 61
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 8803535686130338880, Min: -1155450847100943978
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 1812, Compressed Size: 1812
Column 62
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 9026687750017193101, Min: -4454039315625288390
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 1840, Compressed Size: 1840
Column 63
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 9150047972721273816, Min: -9220123451143279334
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 1868, Compressed Size: 1868
Column 64
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 8846115173408951296, Min: -9223372036854775808
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 1898, Compressed Size: 1898
Column 65
  Values: 200, Null Values: 0, Distinct Values: 0
  Max: 2142811258, Min: -2078683524
  Compression: UNCOMPRESSED, Encodings: DELTA_BINARY_PACKED
  Uncompressed Size: 980, Compressed Size: 980
```
