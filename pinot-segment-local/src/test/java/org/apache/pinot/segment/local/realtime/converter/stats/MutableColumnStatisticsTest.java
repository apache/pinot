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

import com.google.common.base.Utf8;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.mutable.MutableForwardIndex;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.Utf8Utils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/// Tests for [MutableColumnStatistics], covering all value types, SV/MV, sorted/unsorted,
/// element lengths, and isAscii.
@SuppressWarnings("rawtypes")
public class MutableColumnStatisticsTest {

  // ======== Fixed-width SV sorted ========

  @DataProvider(name = "fixedWidthTypes")
  public Object[][] fixedWidthTypes() {
    return new Object[][]{{DataType.INT}, {DataType.LONG}, {DataType.FLOAT}, {DataType.DOUBLE}};
  }

  @Test(dataProvider = "fixedWidthTypes")
  public void testFixedWidthSvSorted(DataType type) {
    int numDocs = 3;
    Comparable[] values = fixedWidthValues(type);

    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(type);
    when(dictionary.length()).thenReturn(3);
    when(dictionary.getMinVal()).thenReturn(values[0]);
    when(dictionary.getMaxVal()).thenReturn(values[2]);
    when(dictionary.getSortedValues()).thenReturn(values);
    when(dictionary.compare(0, 1)).thenReturn(-1);
    when(dictionary.compare(1, 2)).thenReturn(-1);

    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getDictId(0)).thenReturn(0);
    when(forwardIndex.getDictId(1)).thenReturn(1);
    when(forwardIndex.getDictId(2)).thenReturn(2);

    DataSourceMetadata metadata = mockMetadata(type, true, numDocs);
    when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(0);
    when(metadata.getMaxRowLengthInBytes()).thenReturn(type.size());

    MutableColumnStatistics stats =
        new MutableColumnStatistics(mockDataSource(metadata, forwardIndex, dictionary), null, false);

    assertEquals(stats.getMinValue(), values[0]);
    assertEquals(stats.getMaxValue(), values[2]);
    assertNotNull(stats.getUniqueValuesSet());
    assertEquals(stats.getCardinality(), 3);
    assertEquals(stats.getLengthOfShortestElement(), type.size());
    assertEquals(stats.getLengthOfLongestElement(), type.size());
    assertTrue(stats.isFixedLength());
    assertFalse(stats.isAscii());
    assertTrue(stats.isSorted());
    assertEquals(stats.getMaxNumberOfMultiValues(), 0);
    assertEquals(stats.getMaxRowLengthInBytes(), type.size());
  }

  // ======== BigDecimal SV ========

  @Test
  public void testBigDecimalSv() {
    BigDecimal v0 = new BigDecimal("10.5");
    BigDecimal v1 = new BigDecimal("20.75");
    int numDocs = 2;

    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.BIG_DECIMAL);
    when(dictionary.length()).thenReturn(2);
    when(dictionary.getMinVal()).thenReturn(v0);
    when(dictionary.getMaxVal()).thenReturn(v1);
    when(dictionary.compare(0, 1)).thenReturn(-1);
    when(dictionary.getValueSize(0)).thenReturn(BigDecimalUtils.byteSize(v0));
    when(dictionary.getValueSize(1)).thenReturn(BigDecimalUtils.byteSize(v1));

    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getDictId(0)).thenReturn(0);
    when(forwardIndex.getDictId(1)).thenReturn(1);

    DataSourceMetadata metadata = mockMetadata(DataType.BIG_DECIMAL, true, numDocs);
    when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(0);
    when(metadata.getMaxRowLengthInBytes()).thenReturn(BigDecimalUtils.byteSize(v1));

    MutableColumnStatistics stats =
        new MutableColumnStatistics(mockDataSource(metadata, forwardIndex, dictionary), null, false);

    assertEquals(stats.getMinValue(), v0);
    assertEquals(stats.getMaxValue(), v1);
    assertEquals(stats.getLengthOfShortestElement(), BigDecimalUtils.byteSize(v0));
    assertEquals(stats.getLengthOfLongestElement(), BigDecimalUtils.byteSize(v1));
    assertFalse(stats.isAscii());
    assertTrue(stats.isSorted());
  }

  // ======== Boolean SV ========

  @Test
  public void testBooleanSvSorted() {
    int numDocs = 2;

    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.INT);
    when(dictionary.length()).thenReturn(2);
    when(dictionary.getMinVal()).thenReturn(0);
    when(dictionary.getMaxVal()).thenReturn(1);
    when(dictionary.getSortedValues()).thenReturn(new Comparable[]{0, 1});
    when(dictionary.compare(0, 1)).thenReturn(-1);

    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getDictId(0)).thenReturn(0);
    when(forwardIndex.getDictId(1)).thenReturn(1);

    DataSourceMetadata metadata = mockMetadata(DataType.BOOLEAN, true, numDocs);
    when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(0);
    when(metadata.getMaxRowLengthInBytes()).thenReturn(Integer.BYTES);

    MutableColumnStatistics stats =
        new MutableColumnStatistics(mockDataSource(metadata, forwardIndex, dictionary), null, false);

    assertEquals(stats.getMinValue(), 0);
    assertEquals(stats.getMaxValue(), 1);
    assertNotNull(stats.getUniqueValuesSet());
    assertEquals(stats.getCardinality(), 2);
    assertEquals(stats.getLengthOfShortestElement(), Integer.BYTES);
    assertEquals(stats.getLengthOfLongestElement(), Integer.BYTES);
    assertTrue(stats.isFixedLength());
    assertFalse(stats.isAscii());
    assertTrue(stats.isSorted());
    assertEquals(stats.getMaxNumberOfMultiValues(), 0);
    assertEquals(stats.getMaxRowLengthInBytes(), Integer.BYTES);
  }

  // ======== Timestamp SV ========

  @Test
  public void testTimestampSvSorted() {
    int numDocs = 2;

    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.LONG);
    when(dictionary.length()).thenReturn(2);
    when(dictionary.getMinVal()).thenReturn(1000L);
    when(dictionary.getMaxVal()).thenReturn(2000L);
    when(dictionary.getSortedValues()).thenReturn(new Comparable[]{1000L, 2000L});
    when(dictionary.compare(0, 1)).thenReturn(-1);

    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getDictId(0)).thenReturn(0);
    when(forwardIndex.getDictId(1)).thenReturn(1);

    DataSourceMetadata metadata = mockMetadata(DataType.TIMESTAMP, true, numDocs);
    when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(0);
    when(metadata.getMaxRowLengthInBytes()).thenReturn(Long.BYTES);

    MutableColumnStatistics stats =
        new MutableColumnStatistics(mockDataSource(metadata, forwardIndex, dictionary), null, false);

    assertEquals(stats.getMinValue(), 1000L);
    assertEquals(stats.getMaxValue(), 2000L);
    assertNotNull(stats.getUniqueValuesSet());
    assertEquals(stats.getCardinality(), 2);
    assertEquals(stats.getLengthOfShortestElement(), Long.BYTES);
    assertEquals(stats.getLengthOfLongestElement(), Long.BYTES);
    assertTrue(stats.isFixedLength());
    assertFalse(stats.isAscii());
    assertTrue(stats.isSorted());
    assertEquals(stats.getMaxNumberOfMultiValues(), 0);
    assertEquals(stats.getMaxRowLengthInBytes(), Long.BYTES);
  }

  // ======== String SV ========

  @Test
  public void testStringSvAscii() {
    int numDocs = 2;
    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.STRING);
    when(dictionary.length()).thenReturn(2);
    when(dictionary.getMinVal()).thenReturn("apple");
    when(dictionary.getMaxVal()).thenReturn("banana");
    when(dictionary.getSortedValues()).thenReturn(new String[]{"apple", "banana"});
    when(dictionary.compare(0, 1)).thenReturn(-1);
    // For element length scanning: getBytesValue returns UTF-8 bytes, getValueSize returns byte length
    when(dictionary.getBytesValue(0)).thenReturn("apple".getBytes(StandardCharsets.UTF_8));
    when(dictionary.getBytesValue(1)).thenReturn("banana".getBytes(StandardCharsets.UTF_8));
    when(dictionary.getValueSize(0)).thenReturn(5);
    when(dictionary.getValueSize(1)).thenReturn(6);

    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getDictId(0)).thenReturn(0);
    when(forwardIndex.getDictId(1)).thenReturn(1);

    DataSourceMetadata metadata = mockMetadata(DataType.STRING, true, numDocs);
    when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(0);
    when(metadata.getMaxRowLengthInBytes()).thenReturn(6);

    MutableColumnStatistics stats =
        new MutableColumnStatistics(mockDataSource(metadata, forwardIndex, dictionary), null, false);

    assertEquals(stats.getMinValue(), "apple");
    assertEquals(stats.getMaxValue(), "banana");
    assertEquals(stats.getLengthOfShortestElement(), 5);
    assertEquals(stats.getLengthOfLongestElement(), 6);
    assertFalse(stats.isFixedLength());
    assertTrue(stats.isAscii());
    assertTrue(stats.isSorted());
  }

  @Test
  public void testStringSvNonAscii() {
    int numDocs = 2;
    String nonAscii = "héllo";
    byte[] nonAsciiBytes = nonAscii.getBytes(StandardCharsets.UTF_8);

    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.STRING);
    when(dictionary.length()).thenReturn(2);
    when(dictionary.getMinVal()).thenReturn("hello");
    when(dictionary.getMaxVal()).thenReturn(nonAscii);
    when(dictionary.compare(0, 1)).thenReturn(-1);
    when(dictionary.getBytesValue(0)).thenReturn("hello".getBytes(StandardCharsets.UTF_8));
    when(dictionary.getBytesValue(1)).thenReturn(nonAsciiBytes);
    when(dictionary.getValueSize(0)).thenReturn(5);
    when(dictionary.getValueSize(1)).thenReturn(nonAsciiBytes.length);

    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getDictId(0)).thenReturn(0);
    when(forwardIndex.getDictId(1)).thenReturn(1);

    DataSourceMetadata metadata = mockMetadata(DataType.STRING, true, numDocs);
    when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(0);
    when(metadata.getMaxRowLengthInBytes()).thenReturn(nonAsciiBytes.length);

    MutableColumnStatistics stats =
        new MutableColumnStatistics(mockDataSource(metadata, forwardIndex, dictionary), null, false);

    assertFalse(stats.isAscii());
  }

  // ======== JSON SV ========

  @Test
  public void testJsonSvSorted() {
    int numDocs = 2;
    String v0 = "null";
    String v1 = "{\"a\":1}";

    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.STRING);
    when(dictionary.length()).thenReturn(2);
    when(dictionary.getMinVal()).thenReturn(v0);
    when(dictionary.getMaxVal()).thenReturn(v1);
    when(dictionary.getSortedValues()).thenReturn(new Comparable[]{v0, v1});
    when(dictionary.compare(0, 1)).thenReturn(-1);
    when(dictionary.getBytesValue(0)).thenReturn(v0.getBytes(StandardCharsets.UTF_8));
    when(dictionary.getBytesValue(1)).thenReturn(v1.getBytes(StandardCharsets.UTF_8));
    when(dictionary.getValueSize(0)).thenReturn(4);
    when(dictionary.getValueSize(1)).thenReturn(7);

    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getDictId(0)).thenReturn(0);
    when(forwardIndex.getDictId(1)).thenReturn(1);

    DataSourceMetadata metadata = mockMetadata(DataType.JSON, true, numDocs);
    when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(0);
    when(metadata.getMaxRowLengthInBytes()).thenReturn(7);

    MutableColumnStatistics stats =
        new MutableColumnStatistics(mockDataSource(metadata, forwardIndex, dictionary), null, false);

    assertEquals(stats.getMinValue(), v0);
    assertEquals(stats.getMaxValue(), v1);
    assertEquals(stats.getLengthOfShortestElement(), 4);
    assertEquals(stats.getLengthOfLongestElement(), 7);
    assertFalse(stats.isFixedLength());
    assertTrue(stats.isAscii());
    assertTrue(stats.isSorted());
  }

  // ======== Bytes SV ========

  @Test
  public void testBytesSv() {
    int numDocs = 2;
    ByteArray ba0 = new ByteArray(new byte[]{1, 2});
    ByteArray ba1 = new ByteArray(new byte[]{3, 4, 5});

    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.BYTES);
    when(dictionary.length()).thenReturn(2);
    when(dictionary.getMinVal()).thenReturn(ba0);
    when(dictionary.getMaxVal()).thenReturn(ba1);
    when(dictionary.compare(0, 1)).thenReturn(-1);
    when(dictionary.getValueSize(0)).thenReturn(2);
    when(dictionary.getValueSize(1)).thenReturn(3);

    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getDictId(0)).thenReturn(0);
    when(forwardIndex.getDictId(1)).thenReturn(1);

    DataSourceMetadata metadata = mockMetadata(DataType.BYTES, true, numDocs);
    when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(0);
    when(metadata.getMaxRowLengthInBytes()).thenReturn(3);

    MutableColumnStatistics stats =
        new MutableColumnStatistics(mockDataSource(metadata, forwardIndex, dictionary), null, false);

    assertEquals(stats.getMinValue(), ba0);
    assertEquals(stats.getMaxValue(), ba1);
    assertEquals(stats.getLengthOfShortestElement(), 2);
    assertEquals(stats.getLengthOfLongestElement(), 3);
    assertFalse(stats.isFixedLength());
    assertFalse(stats.isAscii());
    assertTrue(stats.isSorted());
  }

  // ======== Sorted / unsorted / sortedColumnFlag ========

  @Test
  public void testIntSvUnsorted() {
    int numDocs = 3;
    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.INT);
    when(dictionary.length()).thenReturn(3);
    when(dictionary.getMinVal()).thenReturn(10);
    when(dictionary.getMaxVal()).thenReturn(30);

    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    // doc0→dictId2(30), doc1→dictId0(10), doc2→dictId1(20) — unsorted
    when(forwardIndex.getDictId(0)).thenReturn(2);
    when(forwardIndex.getDictId(1)).thenReturn(0);
    when(forwardIndex.getDictId(2)).thenReturn(1);
    when(dictionary.compare(2, 0)).thenReturn(1); // 30 > 10 — unsorted at doc1

    DataSourceMetadata metadata = mockMetadata(DataType.INT, true, numDocs);
    when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(0);
    when(metadata.getMaxRowLengthInBytes()).thenReturn(Integer.BYTES);

    MutableColumnStatistics stats =
        new MutableColumnStatistics(mockDataSource(metadata, forwardIndex, dictionary), null, false);

    assertFalse(stats.isSorted());
  }

  @Test
  public void testIntSvSortedColumnFlag() {
    int numDocs = 2;
    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.INT);
    when(dictionary.length()).thenReturn(2);

    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);

    DataSourceMetadata metadata = mockMetadata(DataType.INT, true, numDocs);
    when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(0);
    when(metadata.getMaxRowLengthInBytes()).thenReturn(Integer.BYTES);

    MutableColumnStatistics stats =
        new MutableColumnStatistics(mockDataSource(metadata, forwardIndex, dictionary), null, true);

    assertTrue(stats.isSorted()); // isSortedColumn=true overrides scan
  }

  @Test
  public void testIntSvSortedWithSortedDocIds() {
    int numDocs = 3;
    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.INT);
    when(dictionary.length()).thenReturn(3);
    when(dictionary.getMinVal()).thenReturn(10);
    when(dictionary.getMaxVal()).thenReturn(30);

    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    // sortedDocIds=[2,0,1]: doc2→dictId0(10), doc0→dictId1(20), doc1→dictId2(30) → sorted
    when(forwardIndex.getDictId(2)).thenReturn(0);
    when(forwardIndex.getDictId(0)).thenReturn(1);
    when(forwardIndex.getDictId(1)).thenReturn(2);
    when(dictionary.compare(0, 1)).thenReturn(-1);
    when(dictionary.compare(1, 2)).thenReturn(-1);

    DataSourceMetadata metadata = mockMetadata(DataType.INT, true, numDocs);
    when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(0);
    when(metadata.getMaxRowLengthInBytes()).thenReturn(Integer.BYTES);

    MutableColumnStatistics stats =
        new MutableColumnStatistics(mockDataSource(metadata, forwardIndex, dictionary),
            new int[]{2, 0, 1}, false);

    assertTrue(stats.isSorted());
  }

  // ======== MV ========

  @Test
  public void testIntMv() {
    int numDocs = 2;
    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.INT);
    when(dictionary.length()).thenReturn(3);
    when(dictionary.getMinVal()).thenReturn(10);
    when(dictionary.getMaxVal()).thenReturn(30);

    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(false);

    DataSourceMetadata metadata = mockMetadata(DataType.INT, false, numDocs);
    when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(2);
    when(metadata.getMaxRowLengthInBytes()).thenReturn(Integer.BYTES * 2);
    when(metadata.getNumValues()).thenReturn(4);

    MutableColumnStatistics stats =
        new MutableColumnStatistics(mockDataSource(metadata, forwardIndex, dictionary), null, false);

    assertFalse(stats.isSingleValue());
    assertEquals(stats.getMinValue(), 10);
    assertEquals(stats.getMaxValue(), 30);
    assertEquals(stats.getCardinality(), 3);
    assertEquals(stats.getLengthOfShortestElement(), Integer.BYTES);
    assertEquals(stats.getLengthOfLongestElement(), Integer.BYTES);
    assertTrue(stats.isFixedLength());
    assertFalse(stats.isAscii());
    assertFalse(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), 4);
    assertEquals(stats.getMaxNumberOfMultiValues(), 2);
  }

  @Test
  public void testLongMv() {
    int numDocs = 2;
    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.LONG);
    when(dictionary.length()).thenReturn(3);
    when(dictionary.getMinVal()).thenReturn(100L);
    when(dictionary.getMaxVal()).thenReturn(300L);

    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(false);

    DataSourceMetadata metadata = mockMetadata(DataType.LONG, false, numDocs);
    when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(2);
    when(metadata.getMaxRowLengthInBytes()).thenReturn(Long.BYTES * 2);
    when(metadata.getNumValues()).thenReturn(4);

    MutableColumnStatistics stats =
        new MutableColumnStatistics(mockDataSource(metadata, forwardIndex, dictionary), null, false);

    assertFalse(stats.isSingleValue());
    assertEquals(stats.getMinValue(), 100L);
    assertEquals(stats.getMaxValue(), 300L);
    assertEquals(stats.getCardinality(), 3);
    assertEquals(stats.getLengthOfShortestElement(), Long.BYTES);
    assertEquals(stats.getLengthOfLongestElement(), Long.BYTES);
    assertTrue(stats.isFixedLength());
    assertFalse(stats.isAscii());
    assertFalse(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), 4);
    assertEquals(stats.getMaxNumberOfMultiValues(), 2);
  }

  @Test
  public void testFloatMv() {
    int numDocs = 2;
    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.FLOAT);
    when(dictionary.length()).thenReturn(3);
    when(dictionary.getMinVal()).thenReturn(1.0f);
    when(dictionary.getMaxVal()).thenReturn(3.0f);

    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(false);

    DataSourceMetadata metadata = mockMetadata(DataType.FLOAT, false, numDocs);
    when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(2);
    when(metadata.getMaxRowLengthInBytes()).thenReturn(Float.BYTES * 2);
    when(metadata.getNumValues()).thenReturn(4);

    MutableColumnStatistics stats =
        new MutableColumnStatistics(mockDataSource(metadata, forwardIndex, dictionary), null, false);

    assertFalse(stats.isSingleValue());
    assertEquals(stats.getMinValue(), 1.0f);
    assertEquals(stats.getMaxValue(), 3.0f);
    assertEquals(stats.getCardinality(), 3);
    assertEquals(stats.getLengthOfShortestElement(), Float.BYTES);
    assertEquals(stats.getLengthOfLongestElement(), Float.BYTES);
    assertTrue(stats.isFixedLength());
    assertFalse(stats.isAscii());
    assertFalse(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), 4);
    assertEquals(stats.getMaxNumberOfMultiValues(), 2);
  }

  @Test
  public void testDoubleMv() {
    int numDocs = 2;
    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.DOUBLE);
    when(dictionary.length()).thenReturn(3);
    when(dictionary.getMinVal()).thenReturn(1.0);
    when(dictionary.getMaxVal()).thenReturn(3.0);

    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(false);

    DataSourceMetadata metadata = mockMetadata(DataType.DOUBLE, false, numDocs);
    when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(2);
    when(metadata.getMaxRowLengthInBytes()).thenReturn(Double.BYTES * 2);
    when(metadata.getNumValues()).thenReturn(4);

    MutableColumnStatistics stats =
        new MutableColumnStatistics(mockDataSource(metadata, forwardIndex, dictionary), null, false);

    assertFalse(stats.isSingleValue());
    assertEquals(stats.getMinValue(), 1.0);
    assertEquals(stats.getMaxValue(), 3.0);
    assertEquals(stats.getCardinality(), 3);
    assertEquals(stats.getLengthOfShortestElement(), Double.BYTES);
    assertEquals(stats.getLengthOfLongestElement(), Double.BYTES);
    assertTrue(stats.isFixedLength());
    assertFalse(stats.isAscii());
    assertFalse(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), 4);
    assertEquals(stats.getMaxNumberOfMultiValues(), 2);
  }

  @Test
  public void testBooleanMv() {
    int numDocs = 2;
    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.INT);
    when(dictionary.length()).thenReturn(2);
    when(dictionary.getMinVal()).thenReturn(0);
    when(dictionary.getMaxVal()).thenReturn(1);

    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(false);

    DataSourceMetadata metadata = mockMetadata(DataType.BOOLEAN, false, numDocs);
    when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(2);
    when(metadata.getMaxRowLengthInBytes()).thenReturn(Integer.BYTES * 2);
    when(metadata.getNumValues()).thenReturn(3);

    MutableColumnStatistics stats =
        new MutableColumnStatistics(mockDataSource(metadata, forwardIndex, dictionary), null, false);

    assertFalse(stats.isSingleValue());
    assertEquals(stats.getMinValue(), 0);
    assertEquals(stats.getMaxValue(), 1);
    assertEquals(stats.getCardinality(), 2);
    assertEquals(stats.getLengthOfShortestElement(), Integer.BYTES);
    assertEquals(stats.getLengthOfLongestElement(), Integer.BYTES);
    assertTrue(stats.isFixedLength());
    assertFalse(stats.isAscii());
    assertFalse(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), 3);
    assertEquals(stats.getMaxNumberOfMultiValues(), 2);
  }

  @Test
  public void testTimestampMv() {
    int numDocs = 2;
    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.LONG);
    when(dictionary.length()).thenReturn(2);
    when(dictionary.getMinVal()).thenReturn(1000L);
    when(dictionary.getMaxVal()).thenReturn(2000L);

    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(false);

    DataSourceMetadata metadata = mockMetadata(DataType.TIMESTAMP, false, numDocs);
    when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(2);
    when(metadata.getMaxRowLengthInBytes()).thenReturn(Long.BYTES * 2);
    when(metadata.getNumValues()).thenReturn(3);

    MutableColumnStatistics stats =
        new MutableColumnStatistics(mockDataSource(metadata, forwardIndex, dictionary), null, false);

    assertFalse(stats.isSingleValue());
    assertEquals(stats.getMinValue(), 1000L);
    assertEquals(stats.getMaxValue(), 2000L);
    assertEquals(stats.getCardinality(), 2);
    assertEquals(stats.getLengthOfShortestElement(), Long.BYTES);
    assertEquals(stats.getLengthOfLongestElement(), Long.BYTES);
    assertTrue(stats.isFixedLength());
    assertFalse(stats.isAscii());
    assertFalse(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), 3);
    assertEquals(stats.getMaxNumberOfMultiValues(), 2);
  }

  @Test
  public void testStringMv() {
    int numDocs = 2;
    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.STRING);
    when(dictionary.length()).thenReturn(3);
    when(dictionary.getMinVal()).thenReturn("a");
    when(dictionary.getMaxVal()).thenReturn("c");
    // All ASCII strings
    when(dictionary.getBytesValue(0)).thenReturn("a".getBytes(StandardCharsets.UTF_8));
    when(dictionary.getBytesValue(1)).thenReturn("bb".getBytes(StandardCharsets.UTF_8));
    when(dictionary.getBytesValue(2)).thenReturn("c".getBytes(StandardCharsets.UTF_8));

    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(false);

    DataSourceMetadata metadata = mockMetadata(DataType.STRING, false, numDocs);
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

  @Test
  public void testJsonMv() {
    int numDocs = 2;
    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.STRING);
    when(dictionary.length()).thenReturn(3);
    when(dictionary.getMinVal()).thenReturn("a");
    when(dictionary.getMaxVal()).thenReturn("c");
    when(dictionary.getBytesValue(0)).thenReturn("a".getBytes(StandardCharsets.UTF_8));
    when(dictionary.getBytesValue(1)).thenReturn("bb".getBytes(StandardCharsets.UTF_8));
    when(dictionary.getBytesValue(2)).thenReturn("c".getBytes(StandardCharsets.UTF_8));

    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(false);

    DataSourceMetadata metadata = mockMetadata(DataType.JSON, false, numDocs);
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

  @Test
  public void testBytesMv() {
    int numDocs = 2;
    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.getValueType()).thenReturn(DataType.BYTES);
    when(dictionary.length()).thenReturn(2);
    when(dictionary.getMinVal()).thenReturn(new ByteArray(new byte[]{1, 2}));
    when(dictionary.getMaxVal()).thenReturn(new ByteArray(new byte[]{3, 4, 5}));
    when(dictionary.getValueSize(0)).thenReturn(2);
    when(dictionary.getValueSize(1)).thenReturn(3);

    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(false);

    DataSourceMetadata metadata = mockMetadata(DataType.BYTES, false, numDocs);
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

  // ======== Element length randomized (original test) ========

  @Test
  public void testElementLength() {
    int numElements = 10;
    String[] elements = new String[numElements];
    int minElementLength = Integer.MAX_VALUE;
    int maxElementLength = 0;
    RandomStringUtils gen = RandomStringUtils.secure();
    for (int i = 0; i < numElements; i++) {
      String randomString = gen.next(100);
      elements[i] = randomString;
      int elementLength = Utf8.encodedLength(randomString);
      minElementLength = Math.min(minElementLength, elementLength);
      maxElementLength = Math.max(maxElementLength, elementLength);
    }

    FieldSpec fieldSpec = new DimensionFieldSpec("col", DataType.STRING, true);
    DataSourceMetadata metadata = mock(DataSourceMetadata.class);
    when(metadata.getFieldSpec()).thenReturn(fieldSpec);
    when(metadata.getNumDocs()).thenReturn(numElements);

    DataSource dataSource = mock(DataSource.class);
    when(dataSource.getDataSourceMetadata()).thenReturn(metadata);
    Dictionary dictionary = mock(Dictionary.class);
    when(dataSource.getDictionary()).thenReturn(dictionary);
    when(dictionary.getValueType()).thenReturn(DataType.STRING);
    when(dictionary.length()).thenReturn(numElements);
    when(dictionary.getValueSize(anyInt())).thenAnswer(
        invocation -> Utf8.encodedLength(elements[(int) invocation.getArgument(0)]));
    when(dictionary.getBytesValue(anyInt())).thenAnswer(
        invocation -> Utf8Utils.encode(elements[(int) invocation.getArgument(0)]));

    MutableColumnStatistics columnStatistics = new MutableColumnStatistics(dataSource, null, false);
    assertEquals(columnStatistics.isFixedLength(), minElementLength == maxElementLength);
    assertEquals(columnStatistics.getLengthOfLongestElement(), maxElementLength);
  }

  // ======== Helpers ========

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

  private static Comparable[] fixedWidthValues(DataType type) {
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
