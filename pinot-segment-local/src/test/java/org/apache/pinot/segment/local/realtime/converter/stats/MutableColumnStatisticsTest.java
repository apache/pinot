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
import org.apache.pinot.segment.local.PinotBuffersAfterClassCheckRule;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.realtime.impl.dictionary.BigDecimalOffHeapMutableDictionary;
import org.apache.pinot.segment.local.realtime.impl.dictionary.BigDecimalOnHeapMutableDictionary;
import org.apache.pinot.segment.local.realtime.impl.dictionary.BytesOffHeapMutableDictionary;
import org.apache.pinot.segment.local.realtime.impl.dictionary.BytesOnHeapMutableDictionary;
import org.apache.pinot.segment.local.realtime.impl.dictionary.DoubleOffHeapMutableDictionary;
import org.apache.pinot.segment.local.realtime.impl.dictionary.DoubleOnHeapMutableDictionary;
import org.apache.pinot.segment.local.realtime.impl.dictionary.FloatOffHeapMutableDictionary;
import org.apache.pinot.segment.local.realtime.impl.dictionary.FloatOnHeapMutableDictionary;
import org.apache.pinot.segment.local.realtime.impl.dictionary.IntOffHeapMutableDictionary;
import org.apache.pinot.segment.local.realtime.impl.dictionary.IntOnHeapMutableDictionary;
import org.apache.pinot.segment.local.realtime.impl.dictionary.LongOffHeapMutableDictionary;
import org.apache.pinot.segment.local.realtime.impl.dictionary.LongOnHeapMutableDictionary;
import org.apache.pinot.segment.local.realtime.impl.dictionary.StringOffHeapMutableDictionary;
import org.apache.pinot.segment.local.realtime.impl.dictionary.StringOnHeapMutableDictionary;
import org.apache.pinot.segment.local.segment.index.readers.ConstantValueBigDecimalDictionary;
import org.apache.pinot.segment.local.segment.index.readers.ConstantValueBytesDictionary;
import org.apache.pinot.segment.local.segment.index.readers.ConstantValueDoubleDictionary;
import org.apache.pinot.segment.local.segment.index.readers.ConstantValueFloatDictionary;
import org.apache.pinot.segment.local.segment.index.readers.ConstantValueIntDictionary;
import org.apache.pinot.segment.local.segment.index.readers.ConstantValueLongDictionary;
import org.apache.pinot.segment.local.segment.index.readers.ConstantValueStringDictionary;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.mutable.MutableDictionary;
import org.apache.pinot.segment.spi.index.mutable.MutableForwardIndex;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/// Tests for [MutableColumnStatistics] with real dictionary implementations: on-heap mutable,
/// off-heap mutable, and constant-value dictionaries.
///
/// Each test simulates the real ingestion flow: a value is first indexed into the dictionary
/// (which returns a dictId), then that dictId is written into the forward index. Values may
/// repeat across rows — the dictionary deduplicates and returns the same dictId.
@SuppressWarnings("rawtypes")
public class MutableColumnStatisticsTest implements PinotBuffersAfterClassCheckRule {
  private static final int EST_CARDINALITY = 10;

  private final PinotDataBufferMemoryManager _memoryManager =
      new DirectMemoryManager(MutableColumnStatisticsTest.class.getName());

  // ======== Data providers ========

  @DataProvider(name = "mutableDictTypes")
  public Object[][] mutableDictTypes() {
    return new Object[][]{{"onHeap"}, {"offHeap"}};
  }

  @DataProvider(name = "fixedWidthMutableCases")
  public Object[][] fixedWidthMutableCases() {
    DataType[] types = {DataType.INT, DataType.LONG, DataType.FLOAT, DataType.DOUBLE};
    Object[][] cases = new Object[types.length * 2][2];
    int i = 0;
    for (DataType type : types) {
      cases[i++] = new Object[]{type, "onHeap"};
      cases[i++] = new Object[]{type, "offHeap"};
    }
    return cases;
  }

  @DataProvider(name = "fixedWidthTypes")
  public Object[][] fixedWidthTypes() {
    return new Object[][]{{DataType.INT}, {DataType.LONG}, {DataType.FLOAT}, {DataType.DOUBLE}};
  }

  // ======== Fixed-width SV sorted (onHeap / offHeap) ========

  @Test(dataProvider = "fixedWidthMutableCases")
  public void testFixedWidthSvSorted(DataType type, String dictType)
      throws Exception {
    Comparable[] sorted = fixedWidthSortedValues(type);
    int typeSize = type.size();

    try (MutableDictionary dictionary = createMutableDictionary(type, dictType)) {
      // Simulate 5 rows arriving out of order, with duplicates
      int dictId0 = dictionary.index(sorted[2]);
      int dictId1 = dictionary.index(sorted[0]);
      int dictId2 = dictionary.index(sorted[1]);
      int dictId3 = dictionary.index(sorted[0]); // duplicate
      int dictId4 = dictionary.index(sorted[2]); // duplicate
      assertEquals(dictId3, dictId1); // same dictId for same value
      assertEquals(dictId4, dictId0);

      int numDocs = 5;
      // Forward index stores dictIds in row arrival order
      MutableForwardIndex forwardIndex = mockSvForwardIndex(dictId0, dictId1, dictId2, dictId3, dictId4);
      DataSourceMetadata metadata = mockMetadata(type, true, numDocs);
      when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(0);
      when(metadata.getMaxRowLengthInBytes()).thenReturn(typeSize);

      // sortedDocIds reorders to value-sorted: doc1(sorted[0]), doc3(sorted[0]),
      // doc2(sorted[1]), doc0(sorted[2]), doc4(sorted[2])
      MutableColumnStatistics stats =
          new MutableColumnStatistics(mockDataSource(metadata, forwardIndex, dictionary),
              new int[]{1, 3, 2, 0, 4}, false);

      assertEquals(stats.getMinValue(), sorted[0]);
      assertEquals(stats.getMaxValue(), sorted[2]);
      assertNotNull(stats.getUniqueValuesSet());
      assertEquals(stats.getCardinality(), 3);
      assertEquals(stats.getLengthOfShortestElement(), typeSize);
      assertEquals(stats.getLengthOfLongestElement(), typeSize);
      assertTrue(stats.isFixedLength());
      assertFalse(stats.isAscii());
      assertTrue(stats.isSorted());
      assertEquals(stats.getMaxNumberOfMultiValues(), 0);
      assertEquals(stats.getMaxRowLengthInBytes(), typeSize);
    }
  }

  // ======== Fixed-width constant value ========

  @Test(dataProvider = "fixedWidthTypes")
  public void testFixedWidthConstantValue(DataType type)
      throws Exception {
    Comparable value = fixedWidthSortedValues(type)[1];
    int typeSize = type.size();

    try (Dictionary dictionary = createConstantValueDictionary(type, value)) {
      MutableForwardIndex forwardIndex = mockSvForwardIndex(0, 0, 0);
      DataSourceMetadata metadata = mockMetadata(type, true, 3);
      when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(0);
      when(metadata.getMaxRowLengthInBytes()).thenReturn(typeSize);

      MutableColumnStatistics stats =
          new MutableColumnStatistics(mockDataSource(metadata, forwardIndex, dictionary), null, false);

      assertEquals(stats.getMinValue(), value);
      assertEquals(stats.getMaxValue(), value);
      assertEquals(stats.getCardinality(), 1);
      assertEquals(stats.getLengthOfShortestElement(), typeSize);
      assertEquals(stats.getLengthOfLongestElement(), typeSize);
      assertTrue(stats.isFixedLength());
      assertFalse(stats.isAscii());
      assertTrue(stats.isSorted());
    }
  }

  // ======== BigDecimal SV ========

  @Test(dataProvider = "mutableDictTypes")
  public void testBigDecimalSv(String dictType)
      throws Exception {
    BigDecimal min = new BigDecimal("10.5");
    BigDecimal max = new BigDecimal("20.75");

    try (MutableDictionary dictionary = createMutableDictionary(DataType.BIG_DECIMAL, dictType)) {
      // Rows arrive out of order with duplicates
      int dictId0 = dictionary.index(max);
      int dictId1 = dictionary.index(min);
      int dictId2 = dictionary.index(max); // duplicate
      int dictId3 = dictionary.index(min); // duplicate
      assertEquals(dictId2, dictId0);
      assertEquals(dictId3, dictId1);

      // Forward index stores dictIds in row arrival order
      MutableForwardIndex forwardIndex = mockSvForwardIndex(dictId0, dictId1, dictId2, dictId3);
      DataSourceMetadata metadata = mockMetadata(DataType.BIG_DECIMAL, true, 4);
      when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(0);
      when(metadata.getMaxRowLengthInBytes()).thenReturn(BigDecimalUtils.byteSize(max));

      // sortedDocIds: doc1(min), doc3(min), doc0(max), doc2(max)
      MutableColumnStatistics stats =
          new MutableColumnStatistics(mockDataSource(metadata, forwardIndex, dictionary),
              new int[]{1, 3, 0, 2}, false);

      assertEquals(stats.getMinValue(), min);
      assertEquals(stats.getMaxValue(), max);
      assertEquals(stats.getLengthOfShortestElement(), BigDecimalUtils.byteSize(min));
      assertEquals(stats.getLengthOfLongestElement(), BigDecimalUtils.byteSize(max));
      assertFalse(stats.isAscii());
      assertTrue(stats.isSorted());
    }
  }

  @Test
  public void testBigDecimalConstantValue()
      throws Exception {
    BigDecimal value = new BigDecimal("10.5");
    int byteSize = BigDecimalUtils.byteSize(value);

    try (Dictionary dictionary = new ConstantValueBigDecimalDictionary(value)) {
      MutableForwardIndex forwardIndex = mockSvForwardIndex(0, 0);
      DataSourceMetadata metadata = mockMetadata(DataType.BIG_DECIMAL, true, 2);
      when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(0);
      when(metadata.getMaxRowLengthInBytes()).thenReturn(byteSize);

      MutableColumnStatistics stats =
          new MutableColumnStatistics(mockDataSource(metadata, forwardIndex, dictionary), null, false);

      assertEquals(stats.getMinValue(), value);
      assertEquals(stats.getMaxValue(), value);
      assertEquals(stats.getLengthOfShortestElement(), byteSize);
      assertEquals(stats.getLengthOfLongestElement(), byteSize);
      assertTrue(stats.isFixedLength());
      assertFalse(stats.isAscii());
    }
  }

  // ======== String SV ========

  @Test(dataProvider = "mutableDictTypes")
  public void testStringSvAscii(String dictType)
      throws Exception {
    try (MutableDictionary dictionary = createMutableDictionary(DataType.STRING, dictType)) {
      // Rows arrive out of order with duplicates
      int dictId0 = dictionary.index("banana");
      int dictId1 = dictionary.index("apple");
      int dictId2 = dictionary.index("banana"); // duplicate
      int dictId3 = dictionary.index("apple"); // duplicate
      assertEquals(dictId2, dictId0);
      assertEquals(dictId3, dictId1);

      // Forward index stores dictIds in row arrival order
      MutableForwardIndex forwardIndex = mockSvForwardIndex(dictId0, dictId1, dictId2, dictId3);
      DataSourceMetadata metadata = mockMetadata(DataType.STRING, true, 4);
      when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(0);
      when(metadata.getMaxRowLengthInBytes()).thenReturn(6);

      // sortedDocIds: doc1(apple), doc3(apple), doc0(banana), doc2(banana)
      MutableColumnStatistics stats =
          new MutableColumnStatistics(mockDataSource(metadata, forwardIndex, dictionary),
              new int[]{1, 3, 0, 2}, false);

      assertEquals(stats.getMinValue(), "apple");
      assertEquals(stats.getMaxValue(), "banana");
      assertEquals(stats.getLengthOfShortestElement(), 5);
      assertEquals(stats.getLengthOfLongestElement(), 6);
      assertFalse(stats.isFixedLength());
      assertTrue(stats.isAscii());
      assertTrue(stats.isSorted());
    }
  }

  @Test(dataProvider = "mutableDictTypes")
  public void testStringSvNonAscii(String dictType)
      throws Exception {
    try (MutableDictionary dictionary = createMutableDictionary(DataType.STRING, dictType)) {
      // Rows arrive with non-ASCII and duplicates
      int dictId0 = dictionary.index("héllo"); // non-ASCII, 6 UTF-8 bytes
      int dictId1 = dictionary.index("hello");
      int dictId2 = dictionary.index("héllo"); // duplicate
      assertEquals(dictId2, dictId0);

      // Forward index in arrival order
      MutableForwardIndex forwardIndex = mockSvForwardIndex(dictId0, dictId1, dictId2);
      DataSourceMetadata metadata = mockMetadata(DataType.STRING, true, 3);
      when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(0);
      when(metadata.getMaxRowLengthInBytes()).thenReturn(6);

      MutableColumnStatistics stats =
          new MutableColumnStatistics(mockDataSource(metadata, forwardIndex, dictionary), null, false);

      assertFalse(stats.isAscii());
    }
  }

  @Test
  public void testStringConstantValueAscii()
      throws Exception {
    try (Dictionary dictionary = new ConstantValueStringDictionary("hello")) {
      MutableForwardIndex forwardIndex = mockSvForwardIndex(0, 0);
      DataSourceMetadata metadata = mockMetadata(DataType.STRING, true, 2);
      when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(0);
      when(metadata.getMaxRowLengthInBytes()).thenReturn(5);

      MutableColumnStatistics stats =
          new MutableColumnStatistics(mockDataSource(metadata, forwardIndex, dictionary), null, false);

      assertEquals(stats.getMinValue(), "hello");
      assertEquals(stats.getMaxValue(), "hello");
      assertEquals(stats.getLengthOfShortestElement(), 5);
      assertEquals(stats.getLengthOfLongestElement(), 5);
      assertTrue(stats.isFixedLength());
      assertTrue(stats.isAscii());
    }
  }

  @Test
  public void testStringConstantValueNonAscii()
      throws Exception {
    String nonAscii = "héllo";

    try (Dictionary dictionary = new ConstantValueStringDictionary(nonAscii)) {
      MutableForwardIndex forwardIndex = mockSvForwardIndex(0, 0);
      DataSourceMetadata metadata = mockMetadata(DataType.STRING, true, 2);
      when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(0);
      when(metadata.getMaxRowLengthInBytes()).thenReturn(6);

      MutableColumnStatistics stats =
          new MutableColumnStatistics(mockDataSource(metadata, forwardIndex, dictionary), null, false);

      assertFalse(stats.isAscii());
      assertEquals(stats.getLengthOfShortestElement(), 6);
      assertEquals(stats.getLengthOfLongestElement(), 6);
    }
  }

  // ======== Bytes SV ========

  @Test(dataProvider = "mutableDictTypes")
  public void testBytesSv(String dictType)
      throws Exception {
    byte[] smaller = {1, 2};
    byte[] larger = {3, 4, 5};

    try (MutableDictionary dictionary = createMutableDictionary(DataType.BYTES, dictType)) {
      // Rows arrive out of order with duplicates
      int dictId0 = dictionary.index(larger);
      int dictId1 = dictionary.index(smaller);
      int dictId2 = dictionary.index(larger); // duplicate
      assertEquals(dictId2, dictId0);

      // Forward index in arrival order
      MutableForwardIndex forwardIndex = mockSvForwardIndex(dictId0, dictId1, dictId2);
      DataSourceMetadata metadata = mockMetadata(DataType.BYTES, true, 3);
      when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(0);
      when(metadata.getMaxRowLengthInBytes()).thenReturn(3);

      // sortedDocIds: doc1(smaller), doc0(larger), doc2(larger)
      MutableColumnStatistics stats =
          new MutableColumnStatistics(mockDataSource(metadata, forwardIndex, dictionary),
              new int[]{1, 0, 2}, false);

      assertEquals(stats.getMinValue(), new ByteArray(smaller));
      assertEquals(stats.getMaxValue(), new ByteArray(larger));
      assertEquals(stats.getLengthOfShortestElement(), 2);
      assertEquals(stats.getLengthOfLongestElement(), 3);
      assertFalse(stats.isFixedLength());
      assertFalse(stats.isAscii());
      assertTrue(stats.isSorted());
    }
  }

  @Test
  public void testBytesConstantValue()
      throws Exception {
    byte[] value = {1, 2, 3};

    try (Dictionary dictionary = new ConstantValueBytesDictionary(value)) {
      MutableForwardIndex forwardIndex = mockSvForwardIndex(0, 0);
      DataSourceMetadata metadata = mockMetadata(DataType.BYTES, true, 2);
      when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(0);
      when(metadata.getMaxRowLengthInBytes()).thenReturn(3);

      MutableColumnStatistics stats =
          new MutableColumnStatistics(mockDataSource(metadata, forwardIndex, dictionary), null, false);

      assertEquals(stats.getMinValue(), new ByteArray(value));
      assertEquals(stats.getMaxValue(), new ByteArray(value));
      assertEquals(stats.getLengthOfShortestElement(), 3);
      assertEquals(stats.getLengthOfLongestElement(), 3);
      assertTrue(stats.isFixedLength());
      assertFalse(stats.isAscii());
    }
  }

  // ======== Sorted / unsorted ========

  @Test
  public void testSvUnsorted()
      throws Exception {
    try (MutableDictionary dictionary = new IntOnHeapMutableDictionary()) {
      // Rows arrive out of order with duplicates
      int dictId0 = dictionary.index(30);
      int dictId1 = dictionary.index(10);
      int dictId2 = dictionary.index(20);
      int dictId3 = dictionary.index(10); // duplicate
      assertEquals(dictId3, dictId1);

      // Forward index NOT sorted by value: doc0=10, doc1=30, doc2=20, doc3=10
      MutableForwardIndex forwardIndex = mockSvForwardIndex(dictId1, dictId0, dictId2, dictId3);
      DataSourceMetadata metadata = mockMetadata(DataType.INT, true, 4);
      when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(0);
      when(metadata.getMaxRowLengthInBytes()).thenReturn(Integer.BYTES);

      MutableColumnStatistics stats =
          new MutableColumnStatistics(mockDataSource(metadata, forwardIndex, dictionary), null, false);

      assertFalse(stats.isSorted());
    }
  }

  @Test
  public void testSvSortedColumnFlag()
      throws Exception {
    try (MutableDictionary dictionary = new IntOnHeapMutableDictionary()) {
      int dictId0 = dictionary.index(20);
      int dictId1 = dictionary.index(10);

      // Forward index NOT sorted, but isSortedColumn=true overrides
      MutableForwardIndex forwardIndex = mockSvForwardIndex(dictId0, dictId1);
      DataSourceMetadata metadata = mockMetadata(DataType.INT, true, 2);
      when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(0);
      when(metadata.getMaxRowLengthInBytes()).thenReturn(Integer.BYTES);

      MutableColumnStatistics stats =
          new MutableColumnStatistics(mockDataSource(metadata, forwardIndex, dictionary), null, true);

      assertTrue(stats.isSorted()); // isSortedColumn=true overrides scan
    }
  }

  @Test
  public void testSvSortedWithSortedDocIds()
      throws Exception {
    try (MutableDictionary dictionary = new IntOnHeapMutableDictionary()) {
      int dictId0 = dictionary.index(30);
      int dictId1 = dictionary.index(10);
      int dictId2 = dictionary.index(20);

      // Physical forward index: doc0=30, doc1=20, doc2=10
      MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
      when(forwardIndex.isSingleValue()).thenReturn(true);
      when(forwardIndex.getDictId(0)).thenReturn(dictId0);
      when(forwardIndex.getDictId(1)).thenReturn(dictId2);
      when(forwardIndex.getDictId(2)).thenReturn(dictId1);

      // sortedDocIds=[2,1,0]: logical order doc2(10), doc1(20), doc0(30) — sorted by value
      DataSourceMetadata metadata = mockMetadata(DataType.INT, true, 3);
      when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(0);
      when(metadata.getMaxRowLengthInBytes()).thenReturn(Integer.BYTES);

      MutableColumnStatistics stats =
          new MutableColumnStatistics(mockDataSource(metadata, forwardIndex, dictionary),
              new int[]{2, 1, 0}, false);

      assertTrue(stats.isSorted());
    }
  }

  // ======== MV fixed-width ========

  @Test(dataProvider = "fixedWidthMutableCases")
  public void testFixedWidthMv(DataType type, String dictType)
      throws Exception {
    Comparable[] sorted = fixedWidthSortedValues(type);
    int typeSize = type.size();

    try (MutableDictionary dictionary = createMutableDictionary(type, dictType)) {
      // Index out of order with duplicates
      dictionary.index(sorted[2]);
      dictionary.index(sorted[0]);
      dictionary.index(sorted[1]);
      dictionary.index(sorted[0]); // dup

      MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
      when(forwardIndex.isSingleValue()).thenReturn(false);

      DataSourceMetadata metadata = mockMetadata(type, false, 2);
      when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(2);
      when(metadata.getMaxRowLengthInBytes()).thenReturn(typeSize * 2);
      when(metadata.getNumValues()).thenReturn(4);

      MutableColumnStatistics stats =
          new MutableColumnStatistics(mockDataSource(metadata, forwardIndex, dictionary), null, false);

      assertFalse(stats.isSingleValue());
      assertEquals(stats.getMinValue(), sorted[0]);
      assertEquals(stats.getMaxValue(), sorted[2]);
      assertEquals(stats.getCardinality(), 3);
      assertEquals(stats.getLengthOfShortestElement(), typeSize);
      assertEquals(stats.getLengthOfLongestElement(), typeSize);
      assertTrue(stats.isFixedLength());
      assertFalse(stats.isAscii());
      assertFalse(stats.isSorted());
      assertEquals(stats.getTotalNumberOfEntries(), 4);
      assertEquals(stats.getMaxNumberOfMultiValues(), 2);
    }
  }

  // ======== MV string ========

  @Test(dataProvider = "mutableDictTypes")
  public void testStringMv(String dictType)
      throws Exception {
    try (MutableDictionary dictionary = createMutableDictionary(DataType.STRING, dictType)) {
      // Index out of order with duplicates
      dictionary.index("c");
      dictionary.index("bb");
      dictionary.index("a");
      dictionary.index("bb"); // dup

      MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
      when(forwardIndex.isSingleValue()).thenReturn(false);

      DataSourceMetadata metadata = mockMetadata(DataType.STRING, false, 2);
      when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(2);
      when(metadata.getMaxRowLengthInBytes()).thenReturn(3);
      when(metadata.getNumValues()).thenReturn(4);

      MutableColumnStatistics stats =
          new MutableColumnStatistics(mockDataSource(metadata, forwardIndex, dictionary), null, false);

      assertFalse(stats.isSingleValue());
      assertEquals(stats.getLengthOfShortestElement(), 1);
      assertEquals(stats.getLengthOfLongestElement(), 2);
      assertTrue(stats.isAscii());
      assertFalse(stats.isSorted());
    }
  }

  // ======== MV bytes ========

  @Test(dataProvider = "mutableDictTypes")
  public void testBytesMv(String dictType)
      throws Exception {
    try (MutableDictionary dictionary = createMutableDictionary(DataType.BYTES, dictType)) {
      // Index out of order with duplicates
      dictionary.index(new byte[]{3, 4, 5});
      dictionary.index(new byte[]{1, 2});
      dictionary.index(new byte[]{3, 4, 5}); // dup

      MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
      when(forwardIndex.isSingleValue()).thenReturn(false);

      DataSourceMetadata metadata = mockMetadata(DataType.BYTES, false, 2);
      when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(2);
      when(metadata.getMaxRowLengthInBytes()).thenReturn(3 * 2);
      when(metadata.getNumValues()).thenReturn(4);

      MutableColumnStatistics stats =
          new MutableColumnStatistics(mockDataSource(metadata, forwardIndex, dictionary), null, false);

      assertFalse(stats.isSingleValue());
      assertEquals(stats.getLengthOfShortestElement(), 2);
      assertEquals(stats.getLengthOfLongestElement(), 3);
      assertFalse(stats.isAscii());
      assertFalse(stats.isSorted());
    }
  }

  // ======== Helpers ========

  @AfterClass
  public void tearDown()
      throws Exception {
    _memoryManager.close();
  }

  private MutableDictionary createMutableDictionary(DataType type, String dictType) {
    if ("onHeap".equals(dictType)) {
      return createOnHeapDictionary(type);
    } else if ("offHeap".equals(dictType)) {
      return createOffHeapDictionary(type);
    }
    throw new IllegalArgumentException("Unknown dict type: " + dictType);
  }

  private static MutableDictionary createOnHeapDictionary(DataType type) {
    switch (type) {
      case INT:
        return new IntOnHeapMutableDictionary();
      case LONG:
        return new LongOnHeapMutableDictionary();
      case FLOAT:
        return new FloatOnHeapMutableDictionary();
      case DOUBLE:
        return new DoubleOnHeapMutableDictionary();
      case BIG_DECIMAL:
        return new BigDecimalOnHeapMutableDictionary();
      case STRING:
        return new StringOnHeapMutableDictionary();
      case BYTES:
        return new BytesOnHeapMutableDictionary();
      default:
        throw new UnsupportedOperationException("Unsupported: " + type);
    }
  }

  private MutableDictionary createOffHeapDictionary(DataType type) {
    String ctx = type.name().toLowerCase() + "Column";
    switch (type) {
      case INT:
        return new IntOffHeapMutableDictionary(EST_CARDINALITY, 10, _memoryManager, ctx);
      case LONG:
        return new LongOffHeapMutableDictionary(EST_CARDINALITY, 10, _memoryManager, ctx);
      case FLOAT:
        return new FloatOffHeapMutableDictionary(EST_CARDINALITY, 10, _memoryManager, ctx);
      case DOUBLE:
        return new DoubleOffHeapMutableDictionary(EST_CARDINALITY, 10, _memoryManager, ctx);
      case BIG_DECIMAL:
        return new BigDecimalOffHeapMutableDictionary(EST_CARDINALITY, 10, _memoryManager, ctx, 32);
      case STRING:
        return new StringOffHeapMutableDictionary(EST_CARDINALITY, 10, _memoryManager, ctx, 32);
      case BYTES:
        return new BytesOffHeapMutableDictionary(EST_CARDINALITY, 10, _memoryManager, ctx, 32);
      default:
        throw new UnsupportedOperationException("Unsupported: " + type);
    }
  }

  private static Dictionary createConstantValueDictionary(DataType type, Object value) {
    switch (type) {
      case INT:
        return new ConstantValueIntDictionary((int) value);
      case LONG:
        return new ConstantValueLongDictionary((long) value);
      case FLOAT:
        return new ConstantValueFloatDictionary((float) value);
      case DOUBLE:
        return new ConstantValueDoubleDictionary((double) value);
      default:
        throw new UnsupportedOperationException("Unsupported: " + type);
    }
  }

  private static MutableForwardIndex mockSvForwardIndex(int... dictIds) {
    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    for (int doc = 0; doc < dictIds.length; doc++) {
      when(forwardIndex.getDictId(doc)).thenReturn(dictIds[doc]);
    }
    return forwardIndex;
  }

  private static DataSourceMetadata mockMetadata(DataType type, boolean sv, int numDocs) {
    FieldSpec fieldSpec = new DimensionFieldSpec("col", type, sv);
    DataSourceMetadata metadata = mock(DataSourceMetadata.class);
    when(metadata.getFieldSpec()).thenReturn(fieldSpec);
    when(metadata.getNumDocs()).thenReturn(numDocs);
    when(metadata.getNumValues()).thenReturn(numDocs);
    return metadata;
  }

  private static DataSource mockDataSource(DataSourceMetadata metadata, MutableForwardIndex forwardIndex,
      Dictionary dictionary) {
    DataSource ds = mock(DataSource.class);
    when(ds.getDataSourceMetadata()).thenReturn(metadata);
    when(ds.getDictionary()).thenReturn(dictionary);
    doReturn(forwardIndex).when(ds).getForwardIndex();
    return ds;
  }

  private static Comparable[] fixedWidthSortedValues(DataType type) {
    switch (type) {
      case INT:
        return new Comparable[]{10, 20, 30};
      case LONG:
        return new Comparable[]{100L, 200L, 300L};
      case FLOAT:
        return new Comparable[]{1.0f, 2.0f, 3.0f};
      case DOUBLE:
        return new Comparable[]{1.0, 2.0, 3.0};
      default:
        throw new IllegalArgumentException("Not a fixed-width type: " + type);
    }
  }
}
