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
import org.apache.pinot.segment.spi.index.mutable.MutableForwardIndex;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.Test;

import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
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
    assertEquals(stats.getTotalNumberOfEntries(), 3);
    assertTrue(stats.isSorted());
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
    assertEquals(stats.getTotalNumberOfEntries(), 3);
    assertFalse(stats.isSorted());
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
    assertEquals(stats.getTotalNumberOfEntries(), 3);
    assertFalse(stats.isSorted());
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
    assertEquals(stats.getTotalNumberOfEntries(), 3);
    assertTrue(stats.isSorted());
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

    assertEquals(stats.getTotalNumberOfEntries(), 3);
    assertEquals(stats.getMinValue(), 10);
    assertEquals(stats.getMaxValue(), 30);
    assertTrue(stats.isSorted());
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
    assertEquals(stats.getTotalNumberOfEntries(), 2);
    assertTrue(stats.isSorted());
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
    assertEquals(stats.getTotalNumberOfEntries(), 2);
    assertTrue(stats.isSorted()); // bitmap order: doc0=100, doc2=300 → ascending
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
    assertEquals(stats.getTotalNumberOfEntries(), 3);
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
    assertEquals(stats.getTotalNumberOfEntries(), 2);
    assertTrue(stats.isSorted());
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
    assertEquals(stats.getTotalNumberOfEntries(), 2);
    assertTrue(stats.isSorted());
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
    assertEquals(stats.getTotalNumberOfEntries(), 4);
    assertEquals(stats.getMaxNumberOfMultiValues(), 2);
    assertFalse(stats.isSorted()); // MV with totalEntries > 1 → always false
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
    assertEquals(stats.getTotalNumberOfEntries(), 5); // 2+3
    assertEquals(stats.getMaxNumberOfMultiValues(), 3);
    assertFalse(stats.isSorted()); // MV with totalEntries > 1 → always false
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
    assertEquals(stats.getTotalNumberOfEntries(), 3);
    assertEquals(stats.getMaxNumberOfMultiValues(), 2);
    assertFalse(stats.isSorted());
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
    assertEquals(stats.getTotalNumberOfEntries(), 3);
    assertFalse(stats.isSorted());
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
    assertEquals(stats.getTotalNumberOfEntries(), 3);
    assertFalse(stats.isSorted());
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
    assertEquals(stats.getTotalNumberOfEntries(), 3);
    assertFalse(stats.isSorted());
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
    assertEquals(stats.getTotalNumberOfEntries(), 3);
    assertEquals(stats.getMaxNumberOfMultiValues(), 2);
    assertFalse(stats.isSorted());
  }

  // ======== Edge cases ========

  @Test
  public void testEmptyValidDocs() {
    // RoaringBitmap is empty — no entries, min/max remain null, isSorted=true (totalEntries <= 1)
    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.INT, true);

    RoaringBitmap validDocIds = new RoaringBitmap();
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex), null, false, validDocIds);

    assertNull(stats.getMinValue());
    assertNull(stats.getMaxValue());
    assertEquals(stats.getTotalNumberOfEntries(), 0);
    assertTrue(stats.isSorted()); // totalEntries <= 1
  }

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
    assertEquals(stats.getTotalNumberOfEntries(), 1);
    assertTrue(stats.isSorted()); // totalEntries <= 1
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

  @Test
  public void testMvWithEmptyValidDocsSorted() {
    // MV column but zero valid docs → totalEntries=0 → isSorted=true (totalEntries <= 1)
    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.STRING, false);

    RoaringBitmap validDocIds = new RoaringBitmap();
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex), null, false, validDocIds);

    assertEquals(stats.getTotalNumberOfEntries(), 0);
    assertFalse(stats.isSorted());
  }

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
    assertEquals(stats.getLengthOfLargestElement(), Integer.BYTES);
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

    assertEquals(stats.getLengthOfShortestElement(), 2); // "hi"
    assertEquals(stats.getLengthOfLargestElement(), 5);  // "hello"
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
    assertEquals(stats.getLengthOfLargestElement(), 4);
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

    assertEquals(stats.getLengthOfShortestElement(), 2); // "hi"
    assertEquals(stats.getLengthOfLargestElement(), 5);  // "hello"
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
    assertEquals(stats.getLengthOfLargestElement(), 4);
  }

  @Test
  public void testElementLengthEmptyValidDocs() {
    // No valid docs → variable-width element lengths must be 0
    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.STRING, true);

    RoaringBitmap validDocIds = new RoaringBitmap();
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex), null, false, validDocIds);

    assertEquals(stats.getLengthOfShortestElement(), Integer.MAX_VALUE);
    assertEquals(stats.getLengthOfLargestElement(), 0);
  }

  @Test
  public void testElementLengthStringPartialValidDocs() {
    // doc1 ("superlongvalue", excluded) must not affect element lengths
    MutableForwardIndex forwardIndex = mockForwardIndex(DataType.STRING, true);
    when(forwardIndex.getString(0)).thenReturn("short");
    when(forwardIndex.getString(2)).thenReturn("medium__");

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 2);
    CompactedNoDictColumnStatistics stats =
        new CompactedNoDictColumnStatistics(mockDataSource(forwardIndex), null, false, validDocIds);

    assertEquals(stats.getLengthOfShortestElement(), 5); // "short"
    assertEquals(stats.getLengthOfLargestElement(), 8);  // "medium__"
  }

  // ======== Helper ========

  private MutableForwardIndex mockForwardIndex(DataType dataType, boolean isSingleValue) {
    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.getStoredType()).thenReturn(dataType);
    when(forwardIndex.isSingleValue()).thenReturn(isSingleValue);
    return forwardIndex;
  }

  private DataSource mockDataSource(MutableForwardIndex forwardIndex) {
    DataSource dataSource = mock(DataSource.class, RETURNS_DEEP_STUBS);
    doReturn(forwardIndex).when(dataSource).getForwardIndex();
    return dataSource;
  }
}
