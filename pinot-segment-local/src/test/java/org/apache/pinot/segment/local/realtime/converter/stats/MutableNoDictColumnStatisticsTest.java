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
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.mutable.MutableForwardIndex;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.segment.spi.Constants.UNKNOWN_CARDINALITY;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Tests for [MutableNoDictColumnStatistics], covering all value types, SV/MV, sorted/unsorted,
/// element lengths, and isAscii.
@SuppressWarnings("rawtypes")
public class MutableNoDictColumnStatisticsTest {

  // ======== Fixed-width SV sorted ========

  @DataProvider(name = "fixedWidthTypes")
  public Object[][] fixedWidthTypes() {
    return new Object[][]{{DataType.INT}, {DataType.LONG}, {DataType.FLOAT}, {DataType.DOUBLE}};
  }

  @Test(dataProvider = "fixedWidthTypes")
  public void testFixedWidthSvSorted(DataType type) {
    int numDocs = 3;
    FieldSpec fieldSpec = new DimensionFieldSpec("col", type, true);
    Comparable[] values = fixedWidthValues(type);

    DataSourceMetadata metadata = mockMetadata(fieldSpec, numDocs);
    when(metadata.getMinValue()).thenReturn(values[0]);
    when(metadata.getMaxValue()).thenReturn(values[2]);
    when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(0);
    when(metadata.getMaxRowLengthInBytes()).thenReturn(type.size());

    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getLengthOfShortestElement()).thenReturn(type.size());
    when(forwardIndex.getLengthOfLongestElement()).thenReturn(type.size());
    when(forwardIndex.isAscii()).thenReturn(false);
    stubForwardIndexReads(forwardIndex, type, values);

    MutableNoDictColumnStatistics stats =
        new MutableNoDictColumnStatistics(mockNoDictDataSource(metadata, forwardIndex), null, false);

    assertEquals(stats.getMinValue(), values[0]);
    assertEquals(stats.getMaxValue(), values[2]);
    assertNull(stats.getUniqueValuesSet());
    assertEquals(stats.getCardinality(), UNKNOWN_CARDINALITY);
    assertEquals(stats.getLengthOfShortestElement(), type.size());
    assertEquals(stats.getLengthOfLongestElement(), type.size());
    assertTrue(stats.isFixedLength());
    assertFalse(stats.isAscii());
    assertTrue(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), numDocs);
    assertEquals(stats.getMaxNumberOfMultiValues(), 0);
    assertEquals(stats.getMaxRowLengthInBytes(), type.size());
  }

  // ======== BigDecimal SV ========

  @Test
  public void testBigDecimalSv() {
    BigDecimal v0 = new BigDecimal("10.5");
    BigDecimal v1 = new BigDecimal("20.75");
    int numDocs = 2;
    FieldSpec fieldSpec = new DimensionFieldSpec("col", DataType.BIG_DECIMAL, true);

    DataSourceMetadata metadata = mockMetadata(fieldSpec, numDocs);
    when(metadata.getMinValue()).thenReturn(v0);
    when(metadata.getMaxValue()).thenReturn(v1);
    when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(0);
    when(metadata.getMaxRowLengthInBytes()).thenReturn(BigDecimalUtils.byteSize(v1));

    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getLengthOfShortestElement()).thenReturn(BigDecimalUtils.byteSize(v0));
    when(forwardIndex.getLengthOfLongestElement()).thenReturn(BigDecimalUtils.byteSize(v1));
    when(forwardIndex.isAscii()).thenReturn(false);
    when(forwardIndex.getBigDecimal(0)).thenReturn(v0);
    when(forwardIndex.getBigDecimal(1)).thenReturn(v1);

    MutableNoDictColumnStatistics stats =
        new MutableNoDictColumnStatistics(mockNoDictDataSource(metadata, forwardIndex), null, false);

    assertEquals(stats.getMinValue(), v0);
    assertEquals(stats.getMaxValue(), v1);
    assertNull(stats.getUniqueValuesSet());
    assertEquals(stats.getCardinality(), UNKNOWN_CARDINALITY);
    assertEquals(stats.getLengthOfShortestElement(), BigDecimalUtils.byteSize(v0));
    assertEquals(stats.getLengthOfLongestElement(), BigDecimalUtils.byteSize(v1));
    assertFalse(stats.isAscii());
    assertTrue(stats.isSorted());
  }

  // ======== Boolean SV ========

  @Test
  public void testBooleanSvSorted() {
    int numDocs = 2;
    FieldSpec fieldSpec = new DimensionFieldSpec("col", DataType.BOOLEAN, true);

    DataSourceMetadata metadata = mockMetadata(fieldSpec, numDocs);
    when(metadata.getMinValue()).thenReturn(0);
    when(metadata.getMaxValue()).thenReturn(1);
    when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(0);
    when(metadata.getMaxRowLengthInBytes()).thenReturn(Integer.BYTES);

    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getInt(0)).thenReturn(0);
    when(forwardIndex.getInt(1)).thenReturn(1);
    when(forwardIndex.getLengthOfShortestElement()).thenReturn(Integer.BYTES);
    when(forwardIndex.getLengthOfLongestElement()).thenReturn(Integer.BYTES);
    when(forwardIndex.isAscii()).thenReturn(false);

    MutableNoDictColumnStatistics stats =
        new MutableNoDictColumnStatistics(mockNoDictDataSource(metadata, forwardIndex), null, false);

    assertEquals(stats.getMinValue(), 0);
    assertEquals(stats.getMaxValue(), 1);
    assertNull(stats.getUniqueValuesSet());
    assertEquals(stats.getCardinality(), UNKNOWN_CARDINALITY);
    assertEquals(stats.getLengthOfShortestElement(), Integer.BYTES);
    assertEquals(stats.getLengthOfLongestElement(), Integer.BYTES);
    assertTrue(stats.isFixedLength());
    assertFalse(stats.isAscii());
    assertTrue(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), numDocs);
    assertEquals(stats.getMaxNumberOfMultiValues(), 0);
    assertEquals(stats.getMaxRowLengthInBytes(), Integer.BYTES);
  }

  // ======== Timestamp SV ========

  @Test
  public void testTimestampSvSorted() {
    int numDocs = 2;
    FieldSpec fieldSpec = new DimensionFieldSpec("col", DataType.TIMESTAMP, true);

    DataSourceMetadata metadata = mockMetadata(fieldSpec, numDocs);
    when(metadata.getMinValue()).thenReturn(1000L);
    when(metadata.getMaxValue()).thenReturn(2000L);
    when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(0);
    when(metadata.getMaxRowLengthInBytes()).thenReturn(Long.BYTES);

    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getLong(0)).thenReturn(1000L);
    when(forwardIndex.getLong(1)).thenReturn(2000L);
    when(forwardIndex.getLengthOfShortestElement()).thenReturn(Long.BYTES);
    when(forwardIndex.getLengthOfLongestElement()).thenReturn(Long.BYTES);
    when(forwardIndex.isAscii()).thenReturn(false);

    MutableNoDictColumnStatistics stats =
        new MutableNoDictColumnStatistics(mockNoDictDataSource(metadata, forwardIndex), null, false);

    assertEquals(stats.getMinValue(), 1000L);
    assertEquals(stats.getMaxValue(), 2000L);
    assertNull(stats.getUniqueValuesSet());
    assertEquals(stats.getCardinality(), UNKNOWN_CARDINALITY);
    assertEquals(stats.getLengthOfShortestElement(), Long.BYTES);
    assertEquals(stats.getLengthOfLongestElement(), Long.BYTES);
    assertTrue(stats.isFixedLength());
    assertFalse(stats.isAscii());
    assertTrue(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), numDocs);
    assertEquals(stats.getMaxNumberOfMultiValues(), 0);
    assertEquals(stats.getMaxRowLengthInBytes(), Long.BYTES);
  }

  // ======== String SV ========

  @Test
  public void testStringSvAscii() {
    int numDocs = 2;
    FieldSpec fieldSpec = new DimensionFieldSpec("col", DataType.STRING, true);

    DataSourceMetadata metadata = mockMetadata(fieldSpec, numDocs);
    when(metadata.getMinValue()).thenReturn("apple");
    when(metadata.getMaxValue()).thenReturn("banana");
    when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(0);
    when(metadata.getMaxRowLengthInBytes()).thenReturn(6);

    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getLengthOfShortestElement()).thenReturn(5);
    when(forwardIndex.getLengthOfLongestElement()).thenReturn(6);
    when(forwardIndex.isAscii()).thenReturn(true);
    when(forwardIndex.getString(0)).thenReturn("apple");
    when(forwardIndex.getString(1)).thenReturn("banana");

    MutableNoDictColumnStatistics stats =
        new MutableNoDictColumnStatistics(mockNoDictDataSource(metadata, forwardIndex), null, false);

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
    FieldSpec fieldSpec = new DimensionFieldSpec("col", DataType.STRING, true);

    DataSourceMetadata metadata = mockMetadata(fieldSpec, numDocs);
    when(metadata.getMinValue()).thenReturn("hello");
    when(metadata.getMaxValue()).thenReturn("héllo");
    when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(0);
    when(metadata.getMaxRowLengthInBytes()).thenReturn(Utf8.encodedLength("héllo"));

    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getLengthOfShortestElement()).thenReturn(5);
    when(forwardIndex.getLengthOfLongestElement()).thenReturn(Utf8.encodedLength("héllo"));
    when(forwardIndex.isAscii()).thenReturn(false);
    when(forwardIndex.getString(0)).thenReturn("hello");
    when(forwardIndex.getString(1)).thenReturn("héllo");

    MutableNoDictColumnStatistics stats =
        new MutableNoDictColumnStatistics(mockNoDictDataSource(metadata, forwardIndex), null, false);

    assertFalse(stats.isAscii());
  }

  // ======== JSON SV ========

  @Test
  public void testJsonSvSorted() {
    int numDocs = 2;
    FieldSpec fieldSpec = new DimensionFieldSpec("col", DataType.JSON, true);

    DataSourceMetadata metadata = mockMetadata(fieldSpec, numDocs);
    when(metadata.getMinValue()).thenReturn("null");
    when(metadata.getMaxValue()).thenReturn("{\"a\":1}");
    when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(0);
    when(metadata.getMaxRowLengthInBytes()).thenReturn(7);

    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getLengthOfShortestElement()).thenReturn(4);
    when(forwardIndex.getLengthOfLongestElement()).thenReturn(7);
    when(forwardIndex.isAscii()).thenReturn(true);
    when(forwardIndex.getString(0)).thenReturn("null");
    when(forwardIndex.getString(1)).thenReturn("{\"a\":1}");

    MutableNoDictColumnStatistics stats =
        new MutableNoDictColumnStatistics(mockNoDictDataSource(metadata, forwardIndex), null, false);

    assertEquals(stats.getMinValue(), "null");
    assertEquals(stats.getMaxValue(), "{\"a\":1}");
    assertNull(stats.getUniqueValuesSet());
    assertEquals(stats.getCardinality(), UNKNOWN_CARDINALITY);
    assertEquals(stats.getLengthOfShortestElement(), 4);
    assertEquals(stats.getLengthOfLongestElement(), 7);
    assertFalse(stats.isFixedLength());
    assertTrue(stats.isAscii());
    assertTrue(stats.isSorted());
  }

  // ======== Bytes SV ========

  @Test
  public void testBytesSv() {
    byte[] b0 = new byte[]{1, 2};
    byte[] b1 = new byte[]{3, 4, 5};
    int numDocs = 2;
    FieldSpec fieldSpec = new DimensionFieldSpec("col", DataType.BYTES, true);

    DataSourceMetadata metadata = mockMetadata(fieldSpec, numDocs);
    when(metadata.getMinValue()).thenReturn(new ByteArray(b0));
    when(metadata.getMaxValue()).thenReturn(new ByteArray(b1));
    when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(0);
    when(metadata.getMaxRowLengthInBytes()).thenReturn(3);

    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getLengthOfShortestElement()).thenReturn(2);
    when(forwardIndex.getLengthOfLongestElement()).thenReturn(3);
    when(forwardIndex.isAscii()).thenReturn(false);
    when(forwardIndex.getBytes(0)).thenReturn(b0);
    when(forwardIndex.getBytes(1)).thenReturn(b1);

    MutableNoDictColumnStatistics stats =
        new MutableNoDictColumnStatistics(mockNoDictDataSource(metadata, forwardIndex), null, false);

    assertEquals(stats.getMinValue(), new ByteArray(b0));
    assertEquals(stats.getMaxValue(), new ByteArray(b1));
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
    FieldSpec fieldSpec = new DimensionFieldSpec("col", DataType.INT, true);

    DataSourceMetadata metadata = mockMetadata(fieldSpec, numDocs);
    when(metadata.getMinValue()).thenReturn(10);
    when(metadata.getMaxValue()).thenReturn(30);
    when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(0);
    when(metadata.getMaxRowLengthInBytes()).thenReturn(Integer.BYTES);

    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getLengthOfShortestElement()).thenReturn(Integer.BYTES);
    when(forwardIndex.getLengthOfLongestElement()).thenReturn(Integer.BYTES);
    when(forwardIndex.isAscii()).thenReturn(false);
    when(forwardIndex.getInt(0)).thenReturn(30);
    when(forwardIndex.getInt(1)).thenReturn(10);
    when(forwardIndex.getInt(2)).thenReturn(20);

    MutableNoDictColumnStatistics stats =
        new MutableNoDictColumnStatistics(mockNoDictDataSource(metadata, forwardIndex), null, false);

    assertFalse(stats.isSorted());
  }

  @Test
  public void testIntSvSortedColumnFlag() {
    int numDocs = 2;
    FieldSpec fieldSpec = new DimensionFieldSpec("col", DataType.INT, true);

    DataSourceMetadata metadata = mockMetadata(fieldSpec, numDocs);
    when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(0);
    when(metadata.getMaxRowLengthInBytes()).thenReturn(Integer.BYTES);

    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getLengthOfShortestElement()).thenReturn(Integer.BYTES);
    when(forwardIndex.getLengthOfLongestElement()).thenReturn(Integer.BYTES);
    when(forwardIndex.isAscii()).thenReturn(false);

    MutableNoDictColumnStatistics stats =
        new MutableNoDictColumnStatistics(mockNoDictDataSource(metadata, forwardIndex), null, true);

    assertTrue(stats.isSorted()); // isSortedColumn=true overrides scan
  }

  // ======== MV ========

  @Test
  public void testIntMv() {
    int numDocs = 2;
    FieldSpec fieldSpec = new DimensionFieldSpec("col", DataType.INT, false);

    DataSourceMetadata metadata = mockMetadata(fieldSpec, numDocs);
    when(metadata.getMinValue()).thenReturn(10);
    when(metadata.getMaxValue()).thenReturn(30);
    when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(2);
    when(metadata.getMaxRowLengthInBytes()).thenReturn(Integer.BYTES * 2);

    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(false);
    when(forwardIndex.getLengthOfShortestElement()).thenReturn(Integer.BYTES);
    when(forwardIndex.getLengthOfLongestElement()).thenReturn(Integer.BYTES);
    when(forwardIndex.isAscii()).thenReturn(false);

    MutableNoDictColumnStatistics stats =
        new MutableNoDictColumnStatistics(mockNoDictDataSource(metadata, forwardIndex), null, false);

    assertFalse(stats.isSingleValue());
    assertEquals(stats.getMinValue(), 10);
    assertEquals(stats.getMaxValue(), 30);
    assertNull(stats.getUniqueValuesSet());
    assertEquals(stats.getCardinality(), UNKNOWN_CARDINALITY);
    assertEquals(stats.getLengthOfShortestElement(), Integer.BYTES);
    assertEquals(stats.getLengthOfLongestElement(), Integer.BYTES);
    assertTrue(stats.isFixedLength());
    assertFalse(stats.isAscii());
    assertFalse(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), numDocs);
    assertEquals(stats.getMaxNumberOfMultiValues(), 2);
    assertEquals(stats.getMaxRowLengthInBytes(), Integer.BYTES * 2);
  }

  @Test
  public void testLongMv() {
    int numDocs = 2;
    FieldSpec fieldSpec = new DimensionFieldSpec("col", DataType.LONG, false);

    DataSourceMetadata metadata = mockMetadata(fieldSpec, numDocs);
    when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(2);
    when(metadata.getMaxRowLengthInBytes()).thenReturn(Long.BYTES * 2);

    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(false);
    when(forwardIndex.getLengthOfShortestElement()).thenReturn(Long.BYTES);
    when(forwardIndex.getLengthOfLongestElement()).thenReturn(Long.BYTES);
    when(forwardIndex.isAscii()).thenReturn(false);

    MutableNoDictColumnStatistics stats =
        new MutableNoDictColumnStatistics(mockNoDictDataSource(metadata, forwardIndex), null, false);

    assertFalse(stats.isSingleValue());
    assertNull(stats.getUniqueValuesSet());
    assertEquals(stats.getCardinality(), UNKNOWN_CARDINALITY);
    assertEquals(stats.getLengthOfShortestElement(), Long.BYTES);
    assertEquals(stats.getLengthOfLongestElement(), Long.BYTES);
    assertTrue(stats.isFixedLength());
    assertFalse(stats.isAscii());
    assertFalse(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), numDocs);
    assertEquals(stats.getMaxNumberOfMultiValues(), 2);
    assertEquals(stats.getMaxRowLengthInBytes(), Long.BYTES * 2);
  }

  @Test
  public void testFloatMv() {
    int numDocs = 2;
    FieldSpec fieldSpec = new DimensionFieldSpec("col", DataType.FLOAT, false);

    DataSourceMetadata metadata = mockMetadata(fieldSpec, numDocs);
    when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(2);
    when(metadata.getMaxRowLengthInBytes()).thenReturn(Float.BYTES * 2);

    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(false);
    when(forwardIndex.getLengthOfShortestElement()).thenReturn(Float.BYTES);
    when(forwardIndex.getLengthOfLongestElement()).thenReturn(Float.BYTES);
    when(forwardIndex.isAscii()).thenReturn(false);

    MutableNoDictColumnStatistics stats =
        new MutableNoDictColumnStatistics(mockNoDictDataSource(metadata, forwardIndex), null, false);

    assertFalse(stats.isSingleValue());
    assertNull(stats.getUniqueValuesSet());
    assertEquals(stats.getCardinality(), UNKNOWN_CARDINALITY);
    assertEquals(stats.getLengthOfShortestElement(), Float.BYTES);
    assertEquals(stats.getLengthOfLongestElement(), Float.BYTES);
    assertTrue(stats.isFixedLength());
    assertFalse(stats.isAscii());
    assertFalse(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), numDocs);
    assertEquals(stats.getMaxNumberOfMultiValues(), 2);
    assertEquals(stats.getMaxRowLengthInBytes(), Float.BYTES * 2);
  }

  @Test
  public void testDoubleMv() {
    int numDocs = 2;
    FieldSpec fieldSpec = new DimensionFieldSpec("col", DataType.DOUBLE, false);

    DataSourceMetadata metadata = mockMetadata(fieldSpec, numDocs);
    when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(2);
    when(metadata.getMaxRowLengthInBytes()).thenReturn(Double.BYTES * 2);

    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(false);
    when(forwardIndex.getLengthOfShortestElement()).thenReturn(Double.BYTES);
    when(forwardIndex.getLengthOfLongestElement()).thenReturn(Double.BYTES);
    when(forwardIndex.isAscii()).thenReturn(false);

    MutableNoDictColumnStatistics stats =
        new MutableNoDictColumnStatistics(mockNoDictDataSource(metadata, forwardIndex), null, false);

    assertFalse(stats.isSingleValue());
    assertNull(stats.getUniqueValuesSet());
    assertEquals(stats.getCardinality(), UNKNOWN_CARDINALITY);
    assertEquals(stats.getLengthOfShortestElement(), Double.BYTES);
    assertEquals(stats.getLengthOfLongestElement(), Double.BYTES);
    assertTrue(stats.isFixedLength());
    assertFalse(stats.isAscii());
    assertFalse(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), numDocs);
    assertEquals(stats.getMaxNumberOfMultiValues(), 2);
    assertEquals(stats.getMaxRowLengthInBytes(), Double.BYTES * 2);
  }

  @Test
  public void testBooleanMv() {
    int numDocs = 2;
    FieldSpec fieldSpec = new DimensionFieldSpec("col", DataType.BOOLEAN, false);

    DataSourceMetadata metadata = mockMetadata(fieldSpec, numDocs);
    when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(2);
    when(metadata.getMaxRowLengthInBytes()).thenReturn(Integer.BYTES * 2);

    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(false);
    when(forwardIndex.getLengthOfShortestElement()).thenReturn(Integer.BYTES);
    when(forwardIndex.getLengthOfLongestElement()).thenReturn(Integer.BYTES);
    when(forwardIndex.isAscii()).thenReturn(false);

    MutableNoDictColumnStatistics stats =
        new MutableNoDictColumnStatistics(mockNoDictDataSource(metadata, forwardIndex), null, false);

    assertFalse(stats.isSingleValue());
    assertNull(stats.getUniqueValuesSet());
    assertEquals(stats.getCardinality(), UNKNOWN_CARDINALITY);
    assertEquals(stats.getLengthOfShortestElement(), Integer.BYTES);
    assertEquals(stats.getLengthOfLongestElement(), Integer.BYTES);
    assertTrue(stats.isFixedLength());
    assertFalse(stats.isAscii());
    assertFalse(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), numDocs);
    assertEquals(stats.getMaxNumberOfMultiValues(), 2);
    assertEquals(stats.getMaxRowLengthInBytes(), Integer.BYTES * 2);
  }

  @Test
  public void testTimestampMv() {
    int numDocs = 2;
    FieldSpec fieldSpec = new DimensionFieldSpec("col", DataType.TIMESTAMP, false);

    DataSourceMetadata metadata = mockMetadata(fieldSpec, numDocs);
    when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(2);
    when(metadata.getMaxRowLengthInBytes()).thenReturn(Long.BYTES * 2);

    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(false);
    when(forwardIndex.getLengthOfShortestElement()).thenReturn(Long.BYTES);
    when(forwardIndex.getLengthOfLongestElement()).thenReturn(Long.BYTES);
    when(forwardIndex.isAscii()).thenReturn(false);

    MutableNoDictColumnStatistics stats =
        new MutableNoDictColumnStatistics(mockNoDictDataSource(metadata, forwardIndex), null, false);

    assertFalse(stats.isSingleValue());
    assertNull(stats.getUniqueValuesSet());
    assertEquals(stats.getCardinality(), UNKNOWN_CARDINALITY);
    assertEquals(stats.getLengthOfShortestElement(), Long.BYTES);
    assertEquals(stats.getLengthOfLongestElement(), Long.BYTES);
    assertTrue(stats.isFixedLength());
    assertFalse(stats.isAscii());
    assertFalse(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), numDocs);
    assertEquals(stats.getMaxNumberOfMultiValues(), 2);
    assertEquals(stats.getMaxRowLengthInBytes(), Long.BYTES * 2);
  }

  @Test
  public void testStringMv() {
    int numDocs = 2;
    FieldSpec fieldSpec = new DimensionFieldSpec("col", DataType.STRING, false);

    DataSourceMetadata metadata = mockMetadata(fieldSpec, numDocs);
    when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(3);
    when(metadata.getMaxRowLengthInBytes()).thenReturn(20);

    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(false);
    when(forwardIndex.getLengthOfShortestElement()).thenReturn(2);
    when(forwardIndex.getLengthOfLongestElement()).thenReturn(8);
    when(forwardIndex.isAscii()).thenReturn(true);

    MutableNoDictColumnStatistics stats =
        new MutableNoDictColumnStatistics(mockNoDictDataSource(metadata, forwardIndex), null, false);

    assertFalse(stats.isSingleValue());
    assertEquals(stats.getLengthOfShortestElement(), 2);
    assertEquals(stats.getLengthOfLongestElement(), 8);
    assertFalse(stats.isFixedLength());
    assertTrue(stats.isAscii());
    assertFalse(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), numDocs);
    assertEquals(stats.getMaxNumberOfMultiValues(), 3);
    assertEquals(stats.getMaxRowLengthInBytes(), 20);
  }

  @Test
  public void testJsonMv() {
    int numDocs = 2;
    FieldSpec fieldSpec = new DimensionFieldSpec("col", DataType.JSON, false);

    DataSourceMetadata metadata = mockMetadata(fieldSpec, numDocs);
    when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(2);
    when(metadata.getMaxRowLengthInBytes()).thenReturn(15);

    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(false);
    when(forwardIndex.getLengthOfShortestElement()).thenReturn(2);
    when(forwardIndex.getLengthOfLongestElement()).thenReturn(8);
    when(forwardIndex.isAscii()).thenReturn(true);

    MutableNoDictColumnStatistics stats =
        new MutableNoDictColumnStatistics(mockNoDictDataSource(metadata, forwardIndex), null, false);

    assertFalse(stats.isSingleValue());
    assertNull(stats.getUniqueValuesSet());
    assertEquals(stats.getCardinality(), UNKNOWN_CARDINALITY);
    assertEquals(stats.getLengthOfShortestElement(), 2);
    assertEquals(stats.getLengthOfLongestElement(), 8);
    assertFalse(stats.isFixedLength());
    assertTrue(stats.isAscii());
    assertFalse(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), numDocs);
    assertEquals(stats.getMaxNumberOfMultiValues(), 2);
    assertEquals(stats.getMaxRowLengthInBytes(), 15);
  }

  @Test
  public void testBytesMv() {
    int numDocs = 2;
    FieldSpec fieldSpec = new DimensionFieldSpec("col", DataType.BYTES, false);

    DataSourceMetadata metadata = mockMetadata(fieldSpec, numDocs);
    when(metadata.getMaxNumValuesPerMVEntry()).thenReturn(2);
    when(metadata.getMaxRowLengthInBytes()).thenReturn(5);

    MutableForwardIndex forwardIndex = mock(MutableForwardIndex.class);
    when(forwardIndex.isSingleValue()).thenReturn(false);
    when(forwardIndex.getLengthOfShortestElement()).thenReturn(2);
    when(forwardIndex.getLengthOfLongestElement()).thenReturn(3);
    when(forwardIndex.isAscii()).thenReturn(false);

    MutableNoDictColumnStatistics stats =
        new MutableNoDictColumnStatistics(mockNoDictDataSource(metadata, forwardIndex), null, false);

    assertFalse(stats.isSingleValue());
    assertNull(stats.getUniqueValuesSet());
    assertEquals(stats.getCardinality(), UNKNOWN_CARDINALITY);
    assertEquals(stats.getLengthOfShortestElement(), 2);
    assertEquals(stats.getLengthOfLongestElement(), 3);
    assertFalse(stats.isFixedLength());
    assertFalse(stats.isAscii());
    assertFalse(stats.isSorted());
    assertEquals(stats.getTotalNumberOfEntries(), numDocs);
    assertEquals(stats.getMaxNumberOfMultiValues(), 2);
    assertEquals(stats.getMaxRowLengthInBytes(), 5);
  }

  // ======== Helpers ========

  private static DataSourceMetadata mockMetadata(FieldSpec fieldSpec, int numDocs) {
    DataSourceMetadata metadata = mock(DataSourceMetadata.class);
    when(metadata.getFieldSpec()).thenReturn(fieldSpec);
    when(metadata.getNumDocs()).thenReturn(numDocs);
    return metadata;
  }

  private static DataSource mockNoDictDataSource(DataSourceMetadata metadata, MutableForwardIndex forwardIndex) {
    DataSource ds = mock(DataSource.class);
    when(ds.getDataSourceMetadata()).thenReturn(metadata);
    when(ds.getDictionary()).thenReturn(null);
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

  private static void stubForwardIndexReads(MutableForwardIndex fi, DataType type, Comparable[] values) {
    for (int i = 0; i < values.length; i++) {
      switch (type) {
        case INT:
          when(fi.getInt(i)).thenReturn((Integer) values[i]);
          break;
        case LONG:
          when(fi.getLong(i)).thenReturn((Long) values[i]);
          break;
        case FLOAT:
          when(fi.getFloat(i)).thenReturn((Float) values[i]);
          break;
        case DOUBLE:
          when(fi.getDouble(i)).thenReturn((Double) values[i]);
          break;
        default:
          throw new IllegalArgumentException("Use type-specific stub for: " + type);
      }
    }
  }
}
