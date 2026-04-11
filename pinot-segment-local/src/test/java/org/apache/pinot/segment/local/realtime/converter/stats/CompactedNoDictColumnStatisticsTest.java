/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.local.realtime.converter.stats;

import java.math.BigDecimal;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.mutable.MutableForwardIndex;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Tests for [CompactedNoDictColumnStatistics], covering all data types (INT, LONG, FLOAT,  DOUBLE, BIG_DECIMAL,
/// STRING, BYTES), single-value and multi-value columns, with and without `sortedDocIds`, with partial and full valid
/// doc sets, and edge cases such as empty bitmaps and the `isSortedColumn` flag.
public class CompactedNoDictColumnStatisticsTest {

  // ======== INT SV ========

  @Test
  public void testIntSvSortedViaBitmap() {
    // docs: 0→10, 1→20, 2→30 — bitmap iterates in ascending doc order → sorted
    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.INT, true);
    when(forwardIndex.getInt(0)).thenReturn(10);
    when(forwardIndex.getInt(1)).thenReturn(20);
    when(forwardIndex.getInt(2)).thenReturn(30);

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1, 2);
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex), null, false, validDocIds);

    assertEquals(stats.getMinValue(), 10);
    assertEquals(stats.getMaxValue(), 30);
    assertTrue(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), 3);
  }

  @Test
  public void testIntSvUnsortedViaBitmap() {
    // docs: 0→10, 1→30, 2→20 — 30→20 is descending → unsorted
    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.INT, true);
    when(forwardIndex.getInt(0)).thenReturn(10);
    when(forwardIndex.getInt(1)).thenReturn(30);
    when(forwardIndex.getInt(2)).thenReturn(20);

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1, 2);
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex), null, false, validDocIds);

    assertEquals(stats.getMinValue(), 10);
    assertEquals(stats.getMaxValue(), 30);
    assertFalse(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), 3);
  }

  @Test
  public void testIntSvPartialValidDocs() {
    // 4 docs, validDocIds={1,2,3} — doc0 (value=50) excluded by compaction
    // iteration: doc1=10, doc2=30, doc3=20 → unsorted
    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.INT, true);
    when(forwardIndex.getInt(1)).thenReturn(10);
    when(forwardIndex.getInt(2)).thenReturn(30);
    when(forwardIndex.getInt(3)).thenReturn(20);

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(1, 2, 3);
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex), null, false, validDocIds);

    assertEquals(stats.getMinValue(), 10);
    assertEquals(stats.getMaxValue(), 30);
    assertFalse(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), 3);
  }

  @Test
  public void testIntSvWithSortedDocIdsSorted() {
    // sortedDocIds=[2,0,1]: doc2=10, doc0=20, doc1=30 → ascending → sorted
    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.INT, true);
    when(forwardIndex.getInt(2)).thenReturn(10);
    when(forwardIndex.getInt(0)).thenReturn(20);
    when(forwardIndex.getInt(1)).thenReturn(30);

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1, 2);
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex), new int[]{2, 0, 1}, false, validDocIds);

    assertEquals(stats.getMinValue(), 10);
    assertEquals(stats.getMaxValue(), 30);
    assertTrue(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), 3);
  }

  @Test
  public void testIntSvWithSortedDocIdsUnsorted() {
    // sortedDocIds=[0,1,2] but values are 30, 10, 20 → 30→10 is descending → unsorted
    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.INT, true);
    when(forwardIndex.getInt(0)).thenReturn(30);
    when(forwardIndex.getInt(1)).thenReturn(10);
    when(forwardIndex.getInt(2)).thenReturn(20);

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1, 2);
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex), new int[]{0, 1, 2}, false, validDocIds);

    assertEquals(stats.getMinValue(), 10);
    assertEquals(stats.getMaxValue(), 30);
    assertFalse(stats.isSorted());
  }

  @Test
  public void testIntSvWithSortedDocIdsAndPartialValidDocs() {
    // sortedDocIds=[1,3,0,2], validDocIds={0,1,2} (doc3 invalid, skipped)
    // filtered order: doc1=10, doc0=20, doc2=30 → sorted
    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.INT, true);
    when(forwardIndex.getInt(1)).thenReturn(10);
    when(forwardIndex.getInt(0)).thenReturn(20);
    when(forwardIndex.getInt(2)).thenReturn(30);

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1, 2);
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex), new int[]{1, 3, 0, 2}, false, validDocIds);

    assertEquals(stats.getMinValue(), 10);
    assertEquals(stats.getMaxValue(), 30);
    assertTrue(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), 3);
  }

  // ======== LONG SV ========

  @Test
  public void testLongSv() {
    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.LONG, true);
    when(forwardIndex.getLong(0)).thenReturn(100L);
    when(forwardIndex.getLong(1)).thenReturn(200L);

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex), null, false, validDocIds);

    assertEquals(stats.getMinValue(), 100L);
    assertEquals(stats.getMaxValue(), 200L);
    assertTrue(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), 2);
  }

  @Test
  public void testLongSvPartialValidDocs() {
    // 4 docs, validDocIds={0,2} — docs 1 and 3 excluded by compaction
    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.LONG, true);
    when(forwardIndex.getLong(0)).thenReturn(100L);
    when(forwardIndex.getLong(2)).thenReturn(300L);

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 2);
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex), null, false, validDocIds);

    assertEquals(stats.getMinValue(), 100L);
    assertEquals(stats.getMaxValue(), 300L);
    assertTrue(stats.isSorted()); // bitmap order: doc0=100, doc2=300 → ascending
    assertEquals(stats.getTotalNumberOfEntries(), 2);
  }

  // ======== FLOAT SV ========

  @Test
  public void testFloatSvUnsorted() {
    // doc0=1.5f, doc1=0.5f → 1.5→0.5 is descending → unsorted
    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.FLOAT, true);
    when(forwardIndex.getFloat(0)).thenReturn(1.5f);
    when(forwardIndex.getFloat(1)).thenReturn(0.5f);

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex), null, false, validDocIds);

    assertEquals(stats.getMinValue(), 0.5f);
    assertEquals(stats.getMaxValue(), 1.5f);
    assertFalse(stats.isSorted());
  }

  // ======== DOUBLE SV ========

  @Test
  public void testDoubleSv() {
    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.DOUBLE, true);
    when(forwardIndex.getDouble(0)).thenReturn(1.1);
    when(forwardIndex.getDouble(1)).thenReturn(2.2);

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex), null, false, validDocIds);

    assertEquals(stats.getMinValue(), 1.1);
    assertEquals(stats.getMaxValue(), 2.2);
    assertTrue(stats.isSorted());
  }

  // ======== BOOLEAN SV ========

  @Test
  public void testBooleanSv() {
    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.INT, true);
    when(forwardIndex.getInt(0)).thenReturn(0);
    when(forwardIndex.getInt(1)).thenReturn(1);

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex, DataType.BOOLEAN), null, false, validDocIds);

    assertEquals(stats.getMinValue(), 0);
    assertEquals(stats.getMaxValue(), 1);
    assertTrue(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), 2);
  }

  // ======== TIMESTAMP SV ========

  @Test
  public void testTimestampSv() {
    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.LONG, true);
    when(forwardIndex.getLong(0)).thenReturn(1000L);
    when(forwardIndex.getLong(1)).thenReturn(2000L);

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex, DataType.TIMESTAMP), null, false, validDocIds);

    assertEquals(stats.getMinValue(), 1000L);
    assertEquals(stats.getMaxValue(), 2000L);
    assertTrue(stats.isSorted());
  }

  // ======== BIG_DECIMAL SV ========

  @Test
  public void testBigDecimalSv() {
    BigDecimal v0 = new BigDecimal("10.5");
    BigDecimal v1 = new BigDecimal("20.5");

    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.BIG_DECIMAL, true);
    when(forwardIndex.getBigDecimal(0)).thenReturn(v0);
    when(forwardIndex.getBigDecimal(1)).thenReturn(v1);

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex), null, false, validDocIds);

    assertEquals(stats.getMinValue(), v0);
    assertEquals(stats.getMaxValue(), v1);
    assertTrue(stats.isSorted());
  }

  // ======== STRING SV ========

  @Test
  public void testStringSvUnsorted() {
    // "banana" → "apple" is descending → unsorted
    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.STRING, true);
    when(forwardIndex.getString(0)).thenReturn("banana");
    when(forwardIndex.getString(1)).thenReturn("apple");

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex), null, false, validDocIds);

    assertEquals(stats.getMinValue(), "apple");
    assertEquals(stats.getMaxValue(), "banana");
    assertFalse(stats.isSorted());
  }

  @Test
  public void testStringSvWithSortedDocIdsSorted() {
    // sortedDocIds=[1,0,2]: doc1="apple", doc0="banana", doc2="cherry" → ascending → sorted
    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.STRING, true);
    when(forwardIndex.getString(1)).thenReturn("apple");
    when(forwardIndex.getString(0)).thenReturn("banana");
    when(forwardIndex.getString(2)).thenReturn("cherry");

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1, 2);
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex), new int[]{1, 0, 2}, false, validDocIds);

    assertEquals(stats.getMinValue(), "apple");
    assertEquals(stats.getMaxValue(), "cherry");
    assertTrue(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), 3);
  }

  // ======== JSON SV ========

  @Test
  public void testJsonSv() {
    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.STRING, true);
    when(forwardIndex.getString(0)).thenReturn("null");
    when(forwardIndex.getString(1)).thenReturn("{\"a\":1}");

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex, DataType.JSON), null, false, validDocIds);

    assertEquals(stats.getMinValue(), "null");
    assertEquals(stats.getMaxValue(), "{\"a\":1}");
    assertTrue(stats.isSorted());
  }

  // ======== BYTES SV ========

  @Test
  public void testBytesSv() {
    byte[] b0 = new byte[]{1, 2};
    byte[] b1 = new byte[]{3, 4};

    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.BYTES, true);
    when(forwardIndex.getBytes(0)).thenReturn(b0);
    when(forwardIndex.getBytes(1)).thenReturn(b1);

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex), null, false, validDocIds);

    assertEquals(stats.getMinValue(), new ByteArray(b0));
    assertEquals(stats.getMaxValue(), new ByteArray(b1));
    assertTrue(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), 2);
  }

  @Test
  public void testBytesSvWithSortedDocIds() {
    // sortedDocIds=[1,0]: doc1={1,2}, doc0={3,4} → {1,2} < {3,4} → sorted
    byte[] b0 = new byte[]{3, 4};
    byte[] b1 = new byte[]{1, 2};

    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.BYTES, true);
    when(forwardIndex.getBytes(1)).thenReturn(b1);
    when(forwardIndex.getBytes(0)).thenReturn(b0);

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex), new int[]{1, 0}, false, validDocIds);

    assertEquals(stats.getMinValue(), new ByteArray(b1));
    assertEquals(stats.getMaxValue(), new ByteArray(b0));
    assertTrue(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), 2);
  }

  // ======== Multi-value ========

  @Test
  public void testIntMv() {
    // doc0=[10,30], doc1=[5,20] → totalEntries=4, maxMultiValues=2
    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.INT, false);
    when(forwardIndex.getIntMV(0)).thenReturn(new int[]{10, 30});
    when(forwardIndex.getIntMV(1)).thenReturn(new int[]{5, 20});

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex), null, false, validDocIds);

    assertEquals(stats.getMinValue(), 5);
    assertEquals(stats.getMaxValue(), 30);
    assertFalse(stats.isSorted()); // MV with totalEntries > 1 → always false
    assertEquals(stats.getTotalNumberOfEntries(), 4);
    assertEquals(stats.getMaxNumberOfMultiValues(), 2);
  }

  @Test
  public void testIntMvWithSortedDocIds() {
    // sortedDocIds provided for an MV column — sortedness is still always false for MV
    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.INT, false);
    when(forwardIndex.getIntMV(0)).thenReturn(new int[]{5, 15});
    when(forwardIndex.getIntMV(1)).thenReturn(new int[]{10, 20, 30});

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex), new int[]{0, 1}, false, validDocIds);

    assertEquals(stats.getMinValue(), 5);
    assertEquals(stats.getMaxValue(), 30);
    assertFalse(stats.isSorted()); // MV with totalEntries > 1 → always false
    assertEquals(stats.getTotalNumberOfEntries(), 5); // 2+3
    assertEquals(stats.getMaxNumberOfMultiValues(), 3);
  }

  @Test
  public void testLongMvPartialValidDocs() {
    // doc1 is invalid; only docs 0 and 2 accumulate entries
    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.LONG, false);
    when(forwardIndex.getLongMV(0)).thenReturn(new long[]{100L, 200L});
    when(forwardIndex.getLongMV(2)).thenReturn(new long[]{300L});

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 2);
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex), null, false, validDocIds);

    assertEquals(stats.getMinValue(), 100L);
    assertEquals(stats.getMaxValue(), 300L);
    assertFalse(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), 3);
    assertEquals(stats.getMaxNumberOfMultiValues(), 2);
  }

  @Test
  public void testFloatMv() {
    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.FLOAT, false);
    when(forwardIndex.getFloatMV(0)).thenReturn(new float[]{1.0f, 3.0f});
    when(forwardIndex.getFloatMV(1)).thenReturn(new float[]{2.0f});

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex), null, false, validDocIds);

    assertEquals(stats.getMinValue(), 1.0f);
    assertEquals(stats.getMaxValue(), 3.0f);
    assertFalse(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), 3);
  }

  @Test
  public void testDoubleMv() {
    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.DOUBLE, false);
    when(forwardIndex.getDoubleMV(0)).thenReturn(new double[]{1.5, 4.5});
    when(forwardIndex.getDoubleMV(1)).thenReturn(new double[]{2.5});

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex), null, false, validDocIds);

    assertEquals(stats.getMinValue(), 1.5);
    assertEquals(stats.getMaxValue(), 4.5);
    assertFalse(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), 3);
  }

  @Test
  public void testBooleanMv() {
    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.INT, false);
    when(forwardIndex.getIntMV(0)).thenReturn(new int[]{0, 1});
    when(forwardIndex.getIntMV(1)).thenReturn(new int[]{0});

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex, DataType.BOOLEAN), null, false, validDocIds);

    assertEquals(stats.getMinValue(), 0);
    assertEquals(stats.getMaxValue(), 1);
    assertFalse(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), 3);
    assertEquals(stats.getMaxNumberOfMultiValues(), 2);
  }

  @Test
  public void testTimestampMv() {
    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.LONG, false);
    when(forwardIndex.getLongMV(0)).thenReturn(new long[]{1000L, 2000L});
    when(forwardIndex.getLongMV(1)).thenReturn(new long[]{3000L});

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex, DataType.TIMESTAMP), null, false, validDocIds);

    assertEquals(stats.getMinValue(), 1000L);
    assertEquals(stats.getMaxValue(), 3000L);
    assertFalse(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), 3);
    assertEquals(stats.getMaxNumberOfMultiValues(), 2);
  }

  @Test
  public void testStringMvPartialValidDocs() {
    // doc1 invalid; only docs 0 and 2 accumulate entries
    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.STRING, false);
    when(forwardIndex.getStringMV(0)).thenReturn(new String[]{"a", "c"});
    when(forwardIndex.getStringMV(2)).thenReturn(new String[]{"b"});

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 2);
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex), null, false, validDocIds);

    assertEquals(stats.getMinValue(), "a");
    assertEquals(stats.getMaxValue(), "c");
    assertFalse(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), 3);
  }

  @Test
  public void testJsonMv() {
    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.STRING, false);
    when(forwardIndex.getStringMV(0)).thenReturn(new String[]{"null", "[]"});
    when(forwardIndex.getStringMV(1)).thenReturn(new String[]{"{}"});

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex, DataType.JSON), null, false, validDocIds);

    assertEquals(stats.getMinValue(), "[]");
    assertEquals(stats.getMaxValue(), "{}");
    assertFalse(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), 3);
    assertEquals(stats.getMaxNumberOfMultiValues(), 2);
  }

  @Test
  public void testBytesMv() {
    byte[] b0 = new byte[]{1};
    byte[] b1 = new byte[]{3};
    byte[] b2 = new byte[]{2};

    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.BYTES, false);
    when(forwardIndex.getBytesMV(0)).thenReturn(new byte[][]{b0, b1});
    when(forwardIndex.getBytesMV(1)).thenReturn(new byte[][]{b2});

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex), null, false, validDocIds);

    assertEquals(stats.getMinValue(), new ByteArray(b0));
    assertEquals(stats.getMaxValue(), new ByteArray(b1));
    assertFalse(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), 3);
    assertEquals(stats.getMaxNumberOfMultiValues(), 2);
  }

  // Empty validDocIds cases are now handled by EmptyColumnStatistics via RealtimeSegmentStatsContainer

  // ======== Edge cases ========

  @Test
  public void testSingleValidDoc() {
    // One valid doc → totalEntries=1 → isSorted=true
    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.INT, true);
    when(forwardIndex.getInt(0)).thenReturn(42);

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0);
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex), null, false, validDocIds);

    assertEquals(stats.getMinValue(), 42);
    assertEquals(stats.getMaxValue(), 42);
    assertTrue(stats.isSorted()); // totalEntries <= 1
    assertEquals(stats.getTotalNumberOfEntries(), 1);
  }

  @Test
  public void testIsSortedColumnFlagForcesSorted() {
    // isSortedColumn=true → isSorted=true even if data is descending
    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.INT, true);
    when(forwardIndex.getInt(0)).thenReturn(30);
    when(forwardIndex.getInt(1)).thenReturn(10); // descending

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex), null, true, validDocIds);

    assertTrue(stats.isSorted()); // isSortedColumn=true overrides scan result
  }

  // testMvWithEmptyValidDocsSorted removed — empty case handled by EmptyColumnStatistics

  // ======== Element length ========

  @Test
  public void testElementLengthFixedWidthInt() {
    // INT is fixed-width (4 bytes); element lengths must equal the fixed size
    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.INT, true);
    when(forwardIndex.getInt(0)).thenReturn(10);
    when(forwardIndex.getInt(1)).thenReturn(20);

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex), null, false, validDocIds);

    assertEquals(stats.getLengthOfShortestElement(), Integer.BYTES);
    assertEquals(stats.getLengthOfLongestElement(), Integer.BYTES);
    assertTrue(stats.isFixedLength());
  }

  @Test
  public void testElementLengthStringSv() {
    // Only valid docs contribute; "hi" (2 bytes) and "hello" (5 bytes) are valid
    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.STRING, true);
    when(forwardIndex.getString(0)).thenReturn("hi");
    when(forwardIndex.getString(1)).thenReturn("hello");

    // doc2 ("world!") is excluded by compaction
    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex), null, false, validDocIds);

    assertEquals(stats.getLengthOfShortestElement(), 2);
    assertEquals(stats.getLengthOfLongestElement(), 5);  // "hello"
    assertFalse(stats.isFixedLength());
  }

  @Test
  public void testElementLengthBytesSv() {
    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.BYTES, true);
    when(forwardIndex.getBytes(0)).thenReturn(new byte[]{1, 2, 3});    // 3 bytes
    when(forwardIndex.getBytes(1)).thenReturn(new byte[]{4, 5, 6, 7}); // 4 bytes

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex), null, false, validDocIds);

    assertEquals(stats.getLengthOfShortestElement(), 3);
    assertEquals(stats.getLengthOfLongestElement(), 4);
    assertFalse(stats.isFixedLength());
  }

  @Test
  public void testElementLengthStringMv() {
    // "hi" (2), "hello" (5), "hey" (3) — valid docs only
    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.STRING, false);
    when(forwardIndex.getStringMV(0)).thenReturn(new String[]{"hi", "hello"});
    when(forwardIndex.getStringMV(1)).thenReturn(new String[]{"hey"});

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex), null, false, validDocIds);

    assertEquals(stats.getLengthOfShortestElement(), 2);
    assertEquals(stats.getLengthOfLongestElement(), 5);  // "hello"
    assertFalse(stats.isFixedLength());
  }

  @Test
  public void testElementLengthBytesMv() {
    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.BYTES, false);
    when(forwardIndex.getBytesMV(0)).thenReturn(new byte[][]{new byte[]{1, 2}, new byte[]{3, 4, 5, 6}});
    when(forwardIndex.getBytesMV(1)).thenReturn(new byte[][]{new byte[]{7, 8, 9}});

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex), null, false, validDocIds);

    assertEquals(stats.getLengthOfShortestElement(), 2);
    assertEquals(stats.getLengthOfLongestElement(), 4);
    assertFalse(stats.isFixedLength());
  }

  // testElementLengthEmptyValidDocs removed — empty case handled by EmptyColumnStatistics

  @Test
  public void testElementLengthStringPartialValidDocs() {
    // doc1 ("superlongvalue", excluded) must not affect element lengths
    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.STRING, true);
    when(forwardIndex.getString(0)).thenReturn("short");
    when(forwardIndex.getString(2)).thenReturn("medium__");

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 2);
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex), null, false, validDocIds);

    assertEquals(stats.getLengthOfShortestElement(), 5);
    assertEquals(stats.getLengthOfLongestElement(), 8);  // "medium__"
    assertFalse(stats.isFixedLength());
  }

  // ======== isAscii ========

  @Test
  public void testIsAsciiStringAllAscii() {
    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.STRING, true);
    when(forwardIndex.getString(0)).thenReturn("hello");
    when(forwardIndex.getString(1)).thenReturn("world");

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex), null, false, validDocIds);

    assertTrue(stats.isAscii());
  }

  @Test
  public void testIsAsciiStringWithNonAscii() {
    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.STRING, true);
    when(forwardIndex.getString(0)).thenReturn("hello");
    when(forwardIndex.getString(1)).thenReturn("héllo");

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex), null, false, validDocIds);

    assertFalse(stats.isAscii());
  }

  @Test
  public void testIsAsciiStringMvWithNonAscii() {
    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.STRING, false);
    when(forwardIndex.getStringMV(0)).thenReturn(new String[]{"hello", "café"});
    when(forwardIndex.getStringMV(1)).thenReturn(new String[]{"world"});

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex), null, false, validDocIds);

    assertFalse(stats.isAscii());
  }

  @Test
  public void testIsAsciiIntType() {
    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.INT, true);
    when(forwardIndex.getInt(0)).thenReturn(42);

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0);
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex), null, false, validDocIds);

    assertFalse(stats.isAscii());
  }

  // ======== Helper ========

  private MutableForwardIndex mockForwardIndex(DataType dataType, boolean isSingleValue) {
    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.getStoredType()).thenReturn(dataType);
    when(forwardIndex.isSingleValue()).thenReturn(isSingleValue);
    return forwardIndex;
  }

  private DataSource mockDataSource(MutableForwardIndex forwardIndex) {
    DataType type = forwardIndex.getStoredType();
    boolean sv = forwardIndex.isSingleValue();
    FieldSpec fieldSpec = new DimensionFieldSpec("col", type, sv);

    DataSourceMetadata metadata = mock(DataSourceMetadata.class);
    when(metadata.getFieldSpec()).thenReturn(fieldSpec);
    when(metadata.getNumDocs()).thenReturn(1000); // arbitrary positive to satisfy precondition

    DataSource dataSource = mock(DataSource.class);
    when(dataSource.getDataSourceMetadata()).thenReturn(metadata);
    doReturn(forwardIndex).when(dataSource).getForwardIndex();
    return dataSource;
  }

  private DataSource mockDataSource(MutableForwardIndex forwardIndex, DataType logicalType) {
    boolean sv = forwardIndex.isSingleValue();
    FieldSpec fieldSpec = new DimensionFieldSpec("col", logicalType, sv);

    DataSourceMetadata metadata = mock(DataSourceMetadata.class);
    when(metadata.getFieldSpec()).thenReturn(fieldSpec);
    when(metadata.getNumDocs()).thenReturn(1000);

    DataSource dataSource = mock(DataSource.class);
    when(dataSource.getDataSourceMetadata()).thenReturn(metadata);
    doReturn(forwardIndex).when(dataSource).getForwardIndex();
    return dataSource;
  }
}
