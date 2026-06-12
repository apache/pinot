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
import org.apache.pinot.segment.spi.index.reader.Dictionary;
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


/// Tests for [CompactedColumnStatistics], covering all data types (INT, LONG, FLOAT, DOUBLE, BIG_DECIMAL, STRING,
/// BYTES), single-value and multi-value columns, with and without `sortedDocIds`, with partial and full valid doc sets,
/// and edge cases such as empty bitmaps and the `isSortedColumn` flag.
public class CompactedColumnStatisticsTest {

  // ======== INT SV ========

  @Test
  public void testIntSvSortedViaBitmap() {
    // docs: 0->id0(10), 1->id1(20), 2->id2(30) -- bitmap iterates in ascending doc order -> sorted
    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getDictId(0)).thenReturn(0);
    when(forwardIndex.getDictId(1)).thenReturn(1);
    when(forwardIndex.getDictId(2)).thenReturn(2);

    Dictionary dictionary = mockIntDictionary(new int[]{10, 20, 30});
    when(dictionary.compare(0, 1)).thenReturn(-1);
    when(dictionary.compare(1, 2)).thenReturn(-1);

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1, 2);
    CompactedColumnStatistics stats =
        new CompactedColumnStatistics(mockDataSource(forwardIndex, dictionary), null, false,
            validDocIds);

    assertEquals(stats.getMinValue(), 10);
    assertEquals(stats.getMaxValue(), 30);
    assertEquals((int[]) stats.getUniqueValuesSet(), new int[]{10, 20, 30});
    assertEquals(stats.getCardinality(), 3);
    assertTrue(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), 3);
  }

  @Test
  public void testIntSvUnsortedViaBitmap() {
    // docs: 0->id0(10), 1->id2(30), 2->id1(20) -- compare(id2,id1): 30>20 -> unsorted
    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getDictId(0)).thenReturn(0);
    when(forwardIndex.getDictId(1)).thenReturn(2);
    when(forwardIndex.getDictId(2)).thenReturn(1);

    Dictionary dictionary = mockIntDictionary(new int[]{10, 20, 30});
    when(dictionary.compare(0, 2)).thenReturn(-1); // 10 < 30 -- still sorted after doc1
    when(dictionary.compare(2, 1)).thenReturn(1);  // 30 > 20 -- unsorted at doc2

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1, 2);
    CompactedColumnStatistics stats =
        new CompactedColumnStatistics(mockDataSource(forwardIndex, dictionary), null, false,
            validDocIds);

    assertEquals(stats.getMinValue(), 10);
    assertEquals(stats.getMaxValue(), 30);
    assertEquals((int[]) stats.getUniqueValuesSet(), new int[]{10, 20, 30});
    assertEquals(stats.getCardinality(), 3);
    assertFalse(stats.isSorted());
  }

  @Test
  public void testIntSvSortedWithSortedDocIds() {
    // sortedDocIds=[2,0,1]: doc2->id0(10), doc0->id1(20), doc1->id2(30) -> sorted in that order
    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getDictId(2)).thenReturn(0);
    when(forwardIndex.getDictId(0)).thenReturn(1);
    when(forwardIndex.getDictId(1)).thenReturn(2);

    Dictionary dictionary = mockIntDictionary(new int[]{10, 20, 30});
    when(dictionary.compare(0, 1)).thenReturn(-1);
    when(dictionary.compare(1, 2)).thenReturn(-1);

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1, 2);
    CompactedColumnStatistics stats = new CompactedColumnStatistics(mockDataSource(forwardIndex, dictionary),
        new int[]{2, 0, 1}, false, validDocIds);

    assertEquals(stats.getMinValue(), 10);
    assertEquals(stats.getMaxValue(), 30);
    assertEquals(stats.getCardinality(), 3);
    assertTrue(stats.isSorted());
  }

  @Test
  public void testIntSvWithSortedDocIdsAndPartialValidDocs() {
    // sortedDocIds=[1,3,0,2], validDocIds={0,1,2} (doc3 is invalid, skipped)
    // filtered order: doc1->id0(10), doc0->id1(20), doc2->id2(30) -> sorted
    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getDictId(1)).thenReturn(0);
    when(forwardIndex.getDictId(0)).thenReturn(1);
    when(forwardIndex.getDictId(2)).thenReturn(2);

    Dictionary dictionary = mockIntDictionary(new int[]{10, 20, 30});
    when(dictionary.compare(0, 1)).thenReturn(-1);
    when(dictionary.compare(1, 2)).thenReturn(-1);

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1, 2);
    CompactedColumnStatistics stats = new CompactedColumnStatistics(mockDataSource(forwardIndex, dictionary),
        new int[]{1, 3, 0, 2}, false, validDocIds);

    assertEquals(stats.getMinValue(), 10);
    assertEquals(stats.getMaxValue(), 30);
    assertEquals(stats.getCardinality(), 3);
    assertTrue(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), 3);
  }

  // ======== LONG SV ========

  @Test
  public void testLongSvPartialValidDocs() {
    // 4 docs, only docs 0 and 2 are valid; docs 1 and 3 are excluded by compaction
    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getDictId(0)).thenReturn(0);
    when(forwardIndex.getDictId(2)).thenReturn(1);

    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.LONG);
    when(dictionary.getInternal(0)).thenReturn(100L);
    when(dictionary.getInternal(1)).thenReturn(300L);
    when(dictionary.getLongValue(0)).thenReturn(100L);
    when(dictionary.getLongValue(1)).thenReturn(300L);
    when(dictionary.compare(0, 1)).thenReturn(-1);

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 2);
    CompactedColumnStatistics stats =
        new CompactedColumnStatistics(mockDataSource(forwardIndex, dictionary), null, false,
            validDocIds);

    assertEquals(stats.getMinValue(), 100L);
    assertEquals(stats.getMaxValue(), 300L);
    assertEquals((long[]) stats.getUniqueValuesSet(), new long[]{100L, 300L});
    assertEquals(stats.getCardinality(), 2);
    assertTrue(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), 2);
  }

  // ======== FLOAT SV ========

  @Test
  public void testFloatSv() {
    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getDictId(0)).thenReturn(0);
    when(forwardIndex.getDictId(1)).thenReturn(1);

    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.FLOAT);
    when(dictionary.getInternal(0)).thenReturn(1.5f);
    when(dictionary.getInternal(1)).thenReturn(2.5f);
    when(dictionary.getFloatValue(0)).thenReturn(1.5f);
    when(dictionary.getFloatValue(1)).thenReturn(2.5f);
    when(dictionary.compare(0, 1)).thenReturn(-1);

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedColumnStatistics stats =
        new CompactedColumnStatistics(mockDataSource(forwardIndex, dictionary), null, false,
            validDocIds);

    assertEquals(stats.getMinValue(), 1.5f);
    assertEquals(stats.getMaxValue(), 2.5f);
    assertEquals((float[]) stats.getUniqueValuesSet(), new float[]{1.5f, 2.5f});
    assertEquals(stats.getCardinality(), 2);
    assertTrue(stats.isSorted());
  }

  // ======== DOUBLE SV ========

  @Test
  public void testDoubleSvUnsorted() {
    // doc0->id0(3.14), doc1->id1(2.71) -> compare(id0,id1): 3.14>2.71 -> unsorted
    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getDictId(0)).thenReturn(0);
    when(forwardIndex.getDictId(1)).thenReturn(1);

    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.DOUBLE);
    when(dictionary.getInternal(0)).thenReturn(3.14);
    when(dictionary.getInternal(1)).thenReturn(2.71);
    when(dictionary.getDoubleValue(0)).thenReturn(3.14);
    when(dictionary.getDoubleValue(1)).thenReturn(2.71);
    when(dictionary.compare(0, 1)).thenReturn(1); // 3.14 > 2.71

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedColumnStatistics stats =
        new CompactedColumnStatistics(mockDataSource(forwardIndex, dictionary), null, false,
            validDocIds);

    assertEquals(stats.getMinValue(), 2.71);
    assertEquals(stats.getMaxValue(), 3.14);
    assertEquals((double[]) stats.getUniqueValuesSet(), new double[]{2.71, 3.14});
    assertEquals(stats.getCardinality(), 2);
    assertFalse(stats.isSorted());
  }

  // ======== BOOLEAN SV ========

  @Test
  public void testBooleanSv() {
    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getDictId(0)).thenReturn(0);
    when(forwardIndex.getDictId(1)).thenReturn(1);

    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.INT);
    when(dictionary.getInternal(0)).thenReturn(0);
    when(dictionary.getInternal(1)).thenReturn(1);
    when(dictionary.getIntValue(0)).thenReturn(0);
    when(dictionary.getIntValue(1)).thenReturn(1);
    when(dictionary.compare(0, 1)).thenReturn(-1);

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedColumnStatistics stats =
        new CompactedColumnStatistics(mockDataSource(forwardIndex, dictionary, DataType.BOOLEAN), null, false,
            validDocIds);

    assertEquals(stats.getMinValue(), 0);
    assertEquals(stats.getMaxValue(), 1);
    assertEquals((int[]) stats.getUniqueValuesSet(), new int[]{0, 1});
    assertEquals(stats.getCardinality(), 2);
    assertTrue(stats.isSorted());
  }

  // ======== TIMESTAMP SV ========

  @Test
  public void testTimestampSv() {
    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getDictId(0)).thenReturn(0);
    when(forwardIndex.getDictId(1)).thenReturn(1);

    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.LONG);
    when(dictionary.getInternal(0)).thenReturn(1000L);
    when(dictionary.getInternal(1)).thenReturn(2000L);
    when(dictionary.getLongValue(0)).thenReturn(1000L);
    when(dictionary.getLongValue(1)).thenReturn(2000L);
    when(dictionary.compare(0, 1)).thenReturn(-1);

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedColumnStatistics stats =
        new CompactedColumnStatistics(mockDataSource(forwardIndex, dictionary, DataType.TIMESTAMP), null, false,
            validDocIds);

    assertEquals(stats.getMinValue(), 1000L);
    assertEquals(stats.getMaxValue(), 2000L);
    assertEquals(stats.getCardinality(), 2);
    assertTrue(stats.isSorted());
  }

  // ======== BIG_DECIMAL SV ========

  @Test
  public void testBigDecimalSv() {
    BigDecimal v0 = new BigDecimal("100.50");
    BigDecimal v1 = new BigDecimal("200.75");

    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getDictId(0)).thenReturn(0);
    when(forwardIndex.getDictId(1)).thenReturn(1);

    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.BIG_DECIMAL);
    when(dictionary.getInternal(0)).thenReturn(v0);
    when(dictionary.getInternal(1)).thenReturn(v1);
    when(dictionary.getBigDecimalValue(0)).thenReturn(v0);
    when(dictionary.getBigDecimalValue(1)).thenReturn(v1);
    when(dictionary.compare(0, 1)).thenReturn(-1);

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedColumnStatistics stats =
        new CompactedColumnStatistics(mockDataSource(forwardIndex, dictionary), null, false,
            validDocIds);

    assertEquals(stats.getMinValue(), v0);
    assertEquals(stats.getMaxValue(), v1);
    assertEquals((BigDecimal[]) stats.getUniqueValuesSet(), new BigDecimal[]{v0, v1});
    assertEquals(stats.getCardinality(), 2);
    assertTrue(stats.isSorted());
  }

  // ======== STRING SV ========

  @Test
  public void testStringSvSorted() {
    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getDictId(0)).thenReturn(0);
    when(forwardIndex.getDictId(1)).thenReturn(1);
    when(forwardIndex.getDictId(2)).thenReturn(2);

    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.STRING);
    when(dictionary.getInternal(0)).thenReturn("apple");
    when(dictionary.getInternal(1)).thenReturn("banana");
    when(dictionary.getInternal(2)).thenReturn("cherry");
    when(dictionary.getStringValue(0)).thenReturn("apple");
    when(dictionary.getStringValue(1)).thenReturn("banana");
    when(dictionary.getStringValue(2)).thenReturn("cherry");
    when(dictionary.compare(0, 1)).thenReturn(-1);
    when(dictionary.compare(1, 2)).thenReturn(-1);

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1, 2);
    CompactedColumnStatistics stats =
        new CompactedColumnStatistics(mockDataSource(forwardIndex, dictionary), null, false,
            validDocIds);

    assertEquals(stats.getMinValue(), "apple");
    assertEquals(stats.getMaxValue(), "cherry");
    assertEquals((String[]) stats.getUniqueValuesSet(), new String[]{"apple", "banana", "cherry"});
    assertEquals(stats.getCardinality(), 3);
    assertTrue(stats.isSorted());
  }

  // ======== JSON SV ========

  @Test
  public void testJsonSv() {
    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getDictId(0)).thenReturn(0);
    when(forwardIndex.getDictId(1)).thenReturn(1);

    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.STRING);
    when(dictionary.getInternal(0)).thenReturn("null");
    when(dictionary.getInternal(1)).thenReturn("{\"a\":1}");
    when(dictionary.getStringValue(0)).thenReturn("null");
    when(dictionary.getStringValue(1)).thenReturn("{\"a\":1}");
    when(dictionary.compare(0, 1)).thenReturn(-1);

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedColumnStatistics stats =
        new CompactedColumnStatistics(mockDataSource(forwardIndex, dictionary, DataType.JSON), null, false,
            validDocIds);

    assertEquals(stats.getMinValue(), "null");
    assertEquals(stats.getMaxValue(), "{\"a\":1}");
    assertEquals(stats.getCardinality(), 2);
    assertTrue(stats.isSorted());
  }

  // ======== BYTES SV ========

  @Test
  public void testBytesSv() {
    ByteArray ba0 = new ByteArray(new byte[]{1, 2});
    ByteArray ba1 = new ByteArray(new byte[]{3, 4});

    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getDictId(0)).thenReturn(0);
    when(forwardIndex.getDictId(1)).thenReturn(1);

    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.BYTES);
    when(dictionary.getInternal(0)).thenReturn(ba0);
    when(dictionary.getInternal(1)).thenReturn(ba1);
    when(dictionary.getByteArrayValue(0)).thenReturn(ba0);
    when(dictionary.getByteArrayValue(1)).thenReturn(ba1);
    when(dictionary.compare(0, 1)).thenReturn(-1);

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedColumnStatistics stats =
        new CompactedColumnStatistics(mockDataSource(forwardIndex, dictionary), null, false,
            validDocIds);

    assertEquals(stats.getMinValue(), ba0);
    assertEquals(stats.getMaxValue(), ba1);
    assertEquals((ByteArray[]) stats.getUniqueValuesSet(), new ByteArray[]{ba0, ba1});
    assertEquals(stats.getCardinality(), 2);
    assertTrue(stats.isSorted());
  }

  // ======== INT MV ========

  @Test
  public void testIntMvAllDocs() {
    // doc0=[20,30] (dictIds 1,2), doc1=[10] (dictId 0)
    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(false);
    when(forwardIndex.getDictIdMV(0)).thenReturn(new int[]{1, 2});
    when(forwardIndex.getDictIdMV(1)).thenReturn(new int[]{0});

    Dictionary dictionary = mockIntDictionary(new int[]{10, 20, 30});

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedColumnStatistics stats =
        new CompactedColumnStatistics(mockDataSource(forwardIndex, dictionary), null, false,
            validDocIds);

    assertEquals(stats.getMinValue(), 10);
    assertEquals(stats.getMaxValue(), 30);
    assertEquals((int[]) stats.getUniqueValuesSet(), new int[]{10, 20, 30});
    assertEquals(stats.getCardinality(), 3);
    assertFalse(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), 3); // 2+1
    assertEquals(stats.getMaxNumberOfMultiValues(), 2);
  }

  // ======== LONG MV ========

  @Test
  public void testLongMv() {
    // doc0=[100L,200L], doc1=[300L]
    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(false);
    when(forwardIndex.getDictIdMV(0)).thenReturn(new int[]{0, 1});
    when(forwardIndex.getDictIdMV(1)).thenReturn(new int[]{2});

    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.LONG);
    when(dictionary.getInternal(0)).thenReturn(100L);
    when(dictionary.getInternal(1)).thenReturn(200L);
    when(dictionary.getInternal(2)).thenReturn(300L);
    when(dictionary.getLongValue(0)).thenReturn(100L);
    when(dictionary.getLongValue(1)).thenReturn(200L);
    when(dictionary.getLongValue(2)).thenReturn(300L);

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedColumnStatistics stats =
        new CompactedColumnStatistics(mockDataSource(forwardIndex, dictionary), null, false,
            validDocIds);

    assertEquals(stats.getMinValue(), 100L);
    assertEquals(stats.getMaxValue(), 300L);
    assertEquals(stats.getCardinality(), 3);
    assertFalse(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), 3); // 2+1
    assertEquals(stats.getMaxNumberOfMultiValues(), 2);
  }

  // ======== FLOAT MV ========

  @Test
  public void testFloatMv() {
    // doc0=[1.5f,2.5f], doc1=[3.5f]
    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(false);
    when(forwardIndex.getDictIdMV(0)).thenReturn(new int[]{0, 1});
    when(forwardIndex.getDictIdMV(1)).thenReturn(new int[]{2});

    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.FLOAT);
    when(dictionary.getInternal(0)).thenReturn(1.5f);
    when(dictionary.getInternal(1)).thenReturn(2.5f);
    when(dictionary.getInternal(2)).thenReturn(3.5f);
    when(dictionary.getFloatValue(0)).thenReturn(1.5f);
    when(dictionary.getFloatValue(1)).thenReturn(2.5f);
    when(dictionary.getFloatValue(2)).thenReturn(3.5f);

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedColumnStatistics stats =
        new CompactedColumnStatistics(mockDataSource(forwardIndex, dictionary), null, false,
            validDocIds);

    assertEquals(stats.getMinValue(), 1.5f);
    assertEquals(stats.getMaxValue(), 3.5f);
    assertEquals(stats.getCardinality(), 3);
    assertFalse(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), 3); // 2+1
    assertEquals(stats.getMaxNumberOfMultiValues(), 2);
  }

  // ======== DOUBLE MV ========

  @Test
  public void testDoubleMv() {
    // doc0=[1.1,2.2], doc1=[3.3]
    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(false);
    when(forwardIndex.getDictIdMV(0)).thenReturn(new int[]{0, 1});
    when(forwardIndex.getDictIdMV(1)).thenReturn(new int[]{2});

    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.DOUBLE);
    when(dictionary.getInternal(0)).thenReturn(1.1);
    when(dictionary.getInternal(1)).thenReturn(2.2);
    when(dictionary.getInternal(2)).thenReturn(3.3);
    when(dictionary.getDoubleValue(0)).thenReturn(1.1);
    when(dictionary.getDoubleValue(1)).thenReturn(2.2);
    when(dictionary.getDoubleValue(2)).thenReturn(3.3);

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedColumnStatistics stats =
        new CompactedColumnStatistics(mockDataSource(forwardIndex, dictionary), null, false,
            validDocIds);

    assertEquals(stats.getMinValue(), 1.1);
    assertEquals(stats.getMaxValue(), 3.3);
    assertEquals(stats.getCardinality(), 3);
    assertFalse(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), 3); // 2+1
    assertEquals(stats.getMaxNumberOfMultiValues(), 2);
  }

  // ======== BOOLEAN MV ========

  @Test
  public void testBooleanMv() {
    // doc0=[0,1], doc1=[0]
    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(false);
    when(forwardIndex.getDictIdMV(0)).thenReturn(new int[]{0, 1});
    when(forwardIndex.getDictIdMV(1)).thenReturn(new int[]{0});

    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.INT);
    when(dictionary.getInternal(0)).thenReturn(0);
    when(dictionary.getInternal(1)).thenReturn(1);
    when(dictionary.getIntValue(0)).thenReturn(0);
    when(dictionary.getIntValue(1)).thenReturn(1);

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedColumnStatistics stats =
        new CompactedColumnStatistics(mockDataSource(forwardIndex, dictionary, DataType.BOOLEAN), null, false,
            validDocIds);

    assertEquals(stats.getMinValue(), 0);
    assertEquals(stats.getMaxValue(), 1);
    assertEquals(stats.getCardinality(), 2);
    assertFalse(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), 3); // 2+1
    assertEquals(stats.getMaxNumberOfMultiValues(), 2);
  }

  // ======== TIMESTAMP MV ========

  @Test
  public void testTimestampMv() {
    // doc0=[1000L,2000L], doc1=[3000L]
    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(false);
    when(forwardIndex.getDictIdMV(0)).thenReturn(new int[]{0, 1});
    when(forwardIndex.getDictIdMV(1)).thenReturn(new int[]{2});

    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.LONG);
    when(dictionary.getInternal(0)).thenReturn(1000L);
    when(dictionary.getInternal(1)).thenReturn(2000L);
    when(dictionary.getInternal(2)).thenReturn(3000L);
    when(dictionary.getLongValue(0)).thenReturn(1000L);
    when(dictionary.getLongValue(1)).thenReturn(2000L);
    when(dictionary.getLongValue(2)).thenReturn(3000L);

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedColumnStatistics stats =
        new CompactedColumnStatistics(mockDataSource(forwardIndex, dictionary, DataType.TIMESTAMP), null, false,
            validDocIds);

    assertEquals(stats.getMinValue(), 1000L);
    assertEquals(stats.getMaxValue(), 3000L);
    assertEquals(stats.getCardinality(), 3);
    assertFalse(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), 3); // 2+1
    assertEquals(stats.getMaxNumberOfMultiValues(), 2);
  }

  // ======== STRING MV ========

  @Test
  public void testStringMvPartialValidDocs() {
    // doc0=["a","b"], doc1=["c"] (invalid), doc2=["b","d"]
    // validDocIds={0,2} -> used dictIds: {0,1,3} -> values "a","b","d"
    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(false);
    when(forwardIndex.getDictIdMV(0)).thenReturn(new int[]{0, 1});
    when(forwardIndex.getDictIdMV(2)).thenReturn(new int[]{1, 3});

    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.STRING);
    when(dictionary.getInternal(0)).thenReturn("a");
    when(dictionary.getInternal(1)).thenReturn("b");
    when(dictionary.getInternal(3)).thenReturn("d");
    when(dictionary.getStringValue(0)).thenReturn("a");
    when(dictionary.getStringValue(1)).thenReturn("b");
    when(dictionary.getStringValue(3)).thenReturn("d");

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 2);
    CompactedColumnStatistics stats =
        new CompactedColumnStatistics(mockDataSource(forwardIndex, dictionary), null, false,
            validDocIds);

    assertEquals(stats.getMinValue(), "a");
    assertEquals(stats.getMaxValue(), "d");
    assertEquals((String[]) stats.getUniqueValuesSet(), new String[]{"a", "b", "d"});
    assertEquals(stats.getCardinality(), 3);
    assertFalse(stats.isSorted()); // MV -> always false when cardinality > 1
    assertEquals(stats.getTotalNumberOfEntries(), 4); // 2+2
    assertEquals(stats.getMaxNumberOfMultiValues(), 2);
  }

  @Test
  public void testStringMvWithSortedDocIds() {
    // sortedDocIds provided, MV -> isSorted must still be false when cardinality > 1
    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(false);
    when(forwardIndex.getDictIdMV(0)).thenReturn(new int[]{0, 1});
    when(forwardIndex.getDictIdMV(1)).thenReturn(new int[]{2});

    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.STRING);
    when(dictionary.getInternal(0)).thenReturn("a");
    when(dictionary.getInternal(1)).thenReturn("b");
    when(dictionary.getInternal(2)).thenReturn("c");
    when(dictionary.getStringValue(0)).thenReturn("a");
    when(dictionary.getStringValue(1)).thenReturn("b");
    when(dictionary.getStringValue(2)).thenReturn("c");

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedColumnStatistics stats = new CompactedColumnStatistics(mockDataSource(forwardIndex, dictionary),
        new int[]{0, 1}, false, validDocIds);

    assertEquals(stats.getCardinality(), 3);
    assertFalse(stats.isSorted()); // MV -> always false when cardinality > 1
    assertEquals(stats.getTotalNumberOfEntries(), 3);
  }

  // ======== JSON MV ========

  @Test
  public void testJsonMv() {
    // doc0=["null","{}"], doc1=["null"]
    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(false);
    when(forwardIndex.getDictIdMV(0)).thenReturn(new int[]{0, 1});
    when(forwardIndex.getDictIdMV(1)).thenReturn(new int[]{0});

    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.STRING);
    when(dictionary.getInternal(0)).thenReturn("null");
    when(dictionary.getInternal(1)).thenReturn("{}");
    when(dictionary.getStringValue(0)).thenReturn("null");
    when(dictionary.getStringValue(1)).thenReturn("{}");

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedColumnStatistics stats =
        new CompactedColumnStatistics(mockDataSource(forwardIndex, dictionary, DataType.JSON), null, false,
            validDocIds);

    assertEquals(stats.getMinValue(), "null");
    assertEquals(stats.getMaxValue(), "{}");
    assertEquals(stats.getCardinality(), 2);
    assertFalse(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), 3); // 2+1
    assertEquals(stats.getMaxNumberOfMultiValues(), 2);
  }

  // ======== BYTES MV ========

  @Test
  public void testBytesMv() {
    ByteArray ba0 = new ByteArray(new byte[]{1});
    ByteArray ba1 = new ByteArray(new byte[]{3});
    ByteArray ba2 = new ByteArray(new byte[]{2});

    // doc0=[ba0,ba1], doc1=[ba2]; all docs valid
    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(false);
    when(forwardIndex.getDictIdMV(0)).thenReturn(new int[]{0, 1});
    when(forwardIndex.getDictIdMV(1)).thenReturn(new int[]{2});

    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.BYTES);
    when(dictionary.getInternal(0)).thenReturn(ba0);
    when(dictionary.getInternal(1)).thenReturn(ba1);
    when(dictionary.getInternal(2)).thenReturn(ba2);
    when(dictionary.getByteArrayValue(0)).thenReturn(ba0);
    when(dictionary.getByteArrayValue(1)).thenReturn(ba1);
    when(dictionary.getByteArrayValue(2)).thenReturn(ba2);

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedColumnStatistics stats =
        new CompactedColumnStatistics(mockDataSource(forwardIndex, dictionary), null, false,
            validDocIds);

    assertEquals(stats.getMinValue(), ba0);
    assertEquals(stats.getMaxValue(), ba1);
    assertEquals((ByteArray[]) stats.getUniqueValuesSet(), new ByteArray[]{ba0, ba2, ba1}); // sorted: {1},{2},{3}
    assertEquals(stats.getCardinality(), 3);
    assertFalse(stats.isSorted()); // MV -> always false when cardinality > 1
    assertEquals(stats.getTotalNumberOfEntries(), 3); // 2+1
    assertEquals(stats.getMaxNumberOfMultiValues(), 2);
  }

  // Empty validDocIds cases are now handled by EmptyColumnStatistics via RealtimeSegmentStatsContainer

  // ======== Edge cases ========

  @Test
  public void testSingleValidDoc() {
    // One valid doc -> cardinality=1 -> isSorted=true regardless of isSortedColumn
    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getDictId(0)).thenReturn(0);

    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.STRING);
    when(dictionary.getInternal(0)).thenReturn("hello");
    when(dictionary.getStringValue(0)).thenReturn("hello");

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0);
    CompactedColumnStatistics stats =
        new CompactedColumnStatistics(mockDataSource(forwardIndex, dictionary), null, false,
            validDocIds);

    assertEquals(stats.getMinValue(), "hello");
    assertEquals(stats.getMaxValue(), "hello");
    assertEquals(stats.getCardinality(), 1);
    assertTrue(stats.isSorted()); // cardinality <= 1
    assertEquals(stats.getTotalNumberOfEntries(), 1);
  }

  @Test
  public void testIsSortedColumnFlagForcesSorted() {
    // isSortedColumn=true -> isSorted=true even if data is descending
    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getDictId(0)).thenReturn(0); // val=30
    when(forwardIndex.getDictId(1)).thenReturn(1); // val=10

    Dictionary dictionary = mockIntDictionary(new int[]{30, 10});
    when(dictionary.compare(0, 1)).thenReturn(1); // 30 > 10 -- would be unsorted if not overridden

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedColumnStatistics stats =
        new CompactedColumnStatistics(mockDataSource(forwardIndex, dictionary), null, true,
            validDocIds);

    assertTrue(stats.isSorted()); // isSortedColumn=true overrides scan result
  }

  @Test
  public void testDuplicateDictIdsReduceCardinality() {
    // 3 docs but only 2 distinct dictIds -> cardinality=2
    // The forward index has doc0 and doc2 both pointing to dictId=0 ("value1");
    // doc1 points to dictId=1 ("value2"). The dictionary maps each dictId to a unique value.
    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getDictId(0)).thenReturn(0);
    when(forwardIndex.getDictId(1)).thenReturn(1);
    when(forwardIndex.getDictId(2)).thenReturn(0); // same dictId as doc0

    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.STRING);
    when(dictionary.getInternal(0)).thenReturn("value1");
    when(dictionary.getInternal(1)).thenReturn("value2");
    when(dictionary.getStringValue(0)).thenReturn("value1");
    when(dictionary.getStringValue(1)).thenReturn("value2");
    // compare is called during the sorted scan; stub both orderings
    when(dictionary.compare(0, 1)).thenReturn(-1);
    when(dictionary.compare(1, 0)).thenReturn(1);

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1, 2);
    CompactedColumnStatistics stats =
        new CompactedColumnStatistics(mockDataSource(forwardIndex, dictionary), null, false,
            validDocIds);

    assertEquals((String[]) stats.getUniqueValuesSet(), new String[]{"value1", "value2"});
    assertEquals(stats.getCardinality(), 2);
    assertEquals(stats.getTotalNumberOfEntries(), 3);
  }

  @Test
  public void testIntSvUnsortedWithSortedDocIds() {
    // sortedDocIds=[0,1,2] but doc0->id0(30), doc1->id1(10) -> compare(id0,id1): 30>10 -> unsorted
    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getDictId(0)).thenReturn(0);
    when(forwardIndex.getDictId(1)).thenReturn(1);
    when(forwardIndex.getDictId(2)).thenReturn(2);

    Dictionary dictionary = mockIntDictionary(new int[]{30, 10, 20});
    when(dictionary.compare(0, 1)).thenReturn(1); // 30 > 10 -- unsorted at second doc

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1, 2);
    CompactedColumnStatistics stats = new CompactedColumnStatistics(mockDataSource(forwardIndex, dictionary),
        new int[]{0, 1, 2}, false, validDocIds);

    assertEquals(stats.getMinValue(), 10);
    assertEquals(stats.getMaxValue(), 30);
    assertEquals((int[]) stats.getUniqueValuesSet(), new int[]{10, 20, 30});
    assertEquals(stats.getCardinality(), 3);
    assertFalse(stats.isSorted());
  }

  // ======== Element length ========

  @Test
  public void testElementLengthFixedWidthInt() {
    // INT is fixed-width (4 bytes); min and max element length must equal the fixed size
    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getDictId(0)).thenReturn(0);
    when(forwardIndex.getDictId(1)).thenReturn(1);

    Dictionary dictionary = mockIntDictionary(new int[]{10, 20});
    when(dictionary.compare(0, 1)).thenReturn(-1);

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedColumnStatistics stats =
        new CompactedColumnStatistics(mockDataSource(forwardIndex, dictionary), null, false,
            validDocIds);

    assertEquals(stats.getLengthOfShortestElement(), Integer.BYTES);
    assertEquals(stats.getLengthOfLongestElement(), Integer.BYTES);
    assertTrue(stats.isFixedLength());
  }

  @Test
  public void testElementLengthString() {
    // "hi" (2 bytes UTF-8) and "hello" (5 bytes UTF-8); only compacted values count
    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getDictId(0)).thenReturn(0); // "hi"
    when(forwardIndex.getDictId(1)).thenReturn(1); // "hello"
    when(forwardIndex.getDictId(2)).thenReturn(2); // "world!" (invalid, excluded)

    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.STRING);
    when(dictionary.getInternal(0)).thenReturn("hi");
    when(dictionary.getInternal(1)).thenReturn("hello");
    when(dictionary.getStringValue(0)).thenReturn("hi");
    when(dictionary.getStringValue(1)).thenReturn("hello");
    when(dictionary.compare(0, 1)).thenReturn(-1);

    // Only docs 0 and 1 are valid; doc 2 (with "world!") is excluded by compaction
    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedColumnStatistics stats =
        new CompactedColumnStatistics(mockDataSource(forwardIndex, dictionary), null, false,
            validDocIds);

    assertEquals(stats.getLengthOfShortestElement(), 2);  // "hi"
    assertEquals(stats.getLengthOfLongestElement(), 5);  // "hello"
    assertFalse(stats.isFixedLength());
  }

  @Test
  public void testElementLengthBytes() {
    ByteArray ba0 = new ByteArray(new byte[]{1, 2, 3});     // 3 bytes
    ByteArray ba1 = new ByteArray(new byte[]{4, 5, 6, 7});  // 4 bytes

    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getDictId(0)).thenReturn(0);
    when(forwardIndex.getDictId(1)).thenReturn(1);

    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.BYTES);
    when(dictionary.getInternal(0)).thenReturn(ba0);
    when(dictionary.getInternal(1)).thenReturn(ba1);
    when(dictionary.getByteArrayValue(0)).thenReturn(ba0);
    when(dictionary.getByteArrayValue(1)).thenReturn(ba1);
    when(dictionary.compare(0, 1)).thenReturn(-1);

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedColumnStatistics stats =
        new CompactedColumnStatistics(mockDataSource(forwardIndex, dictionary), null, false,
            validDocIds);

    assertEquals(stats.getLengthOfShortestElement(), 3);
    assertEquals(stats.getLengthOfLongestElement(), 4);
    assertFalse(stats.isFixedLength());
  }

  // testElementLengthEmptyValidDocs removed -- empty case handled by EmptyColumnStatistics

  // ======== isAscii ========

  @Test
  public void testIsAsciiStringAllAscii() {
    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getDictId(0)).thenReturn(0);
    when(forwardIndex.getDictId(1)).thenReturn(1);

    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.STRING);
    when(dictionary.getInternal(0)).thenReturn("hello");
    when(dictionary.getInternal(1)).thenReturn("world");
    when(dictionary.getStringValue(0)).thenReturn("hello");
    when(dictionary.getStringValue(1)).thenReturn("world");
    when(dictionary.compare(0, 1)).thenReturn(-1);

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedColumnStatistics stats =
        new CompactedColumnStatistics(mockDataSource(forwardIndex, dictionary), null, false, validDocIds);

    assertTrue(stats.isAscii());
  }

  @Test
  public void testIsAsciiStringWithNonAscii() {
    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getDictId(0)).thenReturn(0);
    when(forwardIndex.getDictId(1)).thenReturn(1);

    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.STRING);
    when(dictionary.getInternal(0)).thenReturn("hello");
    when(dictionary.getInternal(1)).thenReturn("héllo");
    when(dictionary.getStringValue(0)).thenReturn("hello");
    when(dictionary.getStringValue(1)).thenReturn("héllo");
    when(dictionary.compare(0, 1)).thenReturn(-1);

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 1);
    CompactedColumnStatistics stats =
        new CompactedColumnStatistics(mockDataSource(forwardIndex, dictionary), null, false, validDocIds);

    assertFalse(stats.isAscii());
  }

  @Test
  public void testIsAsciiIntType() {
    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getDictId(0)).thenReturn(0);

    Dictionary dictionary = mockIntDictionary(new int[]{42});

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0);
    CompactedColumnStatistics stats =
        new CompactedColumnStatistics(mockDataSource(forwardIndex, dictionary), null, false, validDocIds);

    assertFalse(stats.isAscii());
  }

  @Test
  public void testIsAsciiBytesType() {
    ByteArray ba0 = new ByteArray(new byte[]{1, 2});

    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getDictId(0)).thenReturn(0);

    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.BYTES);
    when(dictionary.getInternal(0)).thenReturn(ba0);
    when(dictionary.getByteArrayValue(0)).thenReturn(ba0);

    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0);
    CompactedColumnStatistics stats =
        new CompactedColumnStatistics(mockDataSource(forwardIndex, dictionary), null, false, validDocIds);

    assertFalse(stats.isAscii());
  }

  // ======== Helpers ========

  /** Mocks a Dictionary with INT values at consecutive IDs starting from 0. */
  private Dictionary mockIntDictionary(int[] values) {
    Dictionary dict = mock(Dictionary.class);
    when(dict.getValueType()).thenReturn(DataType.INT);
    for (int i = 0; i < values.length; i++) {
      when(dict.getInternal(i)).thenReturn(values[i]);
      when(dict.getIntValue(i)).thenReturn(values[i]);
    }
    return dict;
  }

  private DataSource mockDataSource(MutableForwardIndex forwardIndex, Dictionary dictionary) {
    DataType type = dictionary.getValueType();
    boolean sv = forwardIndex.isSingleValue();
    FieldSpec fieldSpec = new DimensionFieldSpec("col", type, sv);

    DataSourceMetadata metadata = mock(DataSourceMetadata.class);
    when(metadata.getFieldSpec()).thenReturn(fieldSpec);
    when(metadata.getNumDocs()).thenReturn(1000); // arbitrary positive to satisfy precondition

    DataSource dataSource = mock(DataSource.class);
    when(dataSource.getDataSourceMetadata()).thenReturn(metadata);
    doReturn(forwardIndex).when(dataSource).getForwardIndex();
    when(dataSource.getDictionary()).thenReturn(dictionary);
    return dataSource;
  }

  private DataSource mockDataSource(MutableForwardIndex forwardIndex, Dictionary dictionary, DataType logicalType) {
    boolean sv = forwardIndex.isSingleValue();
    FieldSpec fieldSpec = new DimensionFieldSpec("col", logicalType, sv);

    DataSourceMetadata metadata = mock(DataSourceMetadata.class);
    when(metadata.getFieldSpec()).thenReturn(fieldSpec);
    when(metadata.getNumDocs()).thenReturn(1000);

    DataSource dataSource = mock(DataSource.class);
    when(dataSource.getDataSourceMetadata()).thenReturn(metadata);
    doReturn(forwardIndex).when(dataSource).getForwardIndex();
    when(dataSource.getDictionary()).thenReturn(dictionary);
    return dataSource;
  }
}
