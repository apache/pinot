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
package org.apache.pinot.segment.local.segment.index.loader.defaultcolumn;

import com.google.common.base.Utf8;
import java.math.BigDecimal;
import java.util.Arrays;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/// Tests for [DefaultColumnStatistics], covering all value types for SV and MV cases.
public class DefaultColumnStatisticsTest {

  @DataProvider(name = "allTypesSV")
  public Object[][] allTypesSV() {
    DataType[] types = {
        DataType.INT, DataType.LONG, DataType.FLOAT, DataType.DOUBLE, DataType.BIG_DECIMAL, DataType.BOOLEAN,
        DataType.TIMESTAMP, DataType.STRING, DataType.JSON, DataType.BYTES
    };
    return Arrays.stream(types).map(t -> new Object[]{t}).toArray(Object[][]::new);
  }

  @DataProvider(name = "allTypesMV")
  public Object[][] allTypesMV() {
    DataType[] types = {
        DataType.INT, DataType.LONG, DataType.FLOAT, DataType.DOUBLE, DataType.BOOLEAN, DataType.TIMESTAMP,
        DataType.STRING, DataType.JSON, DataType.BYTES
    };
    return Arrays.stream(types).map(t -> new Object[]{t}).toArray(Object[][]::new);
  }

  @Test(dataProvider = "allTypesSV")
  public void testSV(DataType type) {
    int numDocs = 100;
    FieldSpec fieldSpec = new DimensionFieldSpec("col", type, true);
    DefaultColumnStatistics stats = new DefaultColumnStatistics(fieldSpec, numDocs);
    DataType storedType = type.getStoredType();

    // Assertions follow ColumnStatistics API order

    // getFieldSpec, getValueType, isSingleValue
    assertEquals(stats.getFieldSpec(), fieldSpec);
    assertEquals(stats.getValueType(), storedType);
    assertTrue(stats.isSingleValue());

    // getMinValue, getMaxValue — single default value: min == max
    assertNotNull(stats.getMinValue());
    assertEquals(stats.getMinValue(), stats.getMaxValue());

    // getUniqueValuesSet, getCardinality
    assertNotNull(stats.getUniqueValuesSet());
    assertEquals(stats.getCardinality(), 1);

    // getLengthOfShortestElement, getLengthOfLongestElement, isFixedLength, isAscii per stored type
    // (switch ordered by DataType enum: BIG_DECIMAL, STRING, BYTES)
    if (storedType.isFixedWidth()) {
      assertEquals(stats.getLengthOfShortestElement(), storedType.size());
      assertEquals(stats.getLengthOfLongestElement(), storedType.size());
      assertTrue(stats.isFixedLength());
      assertFalse(stats.isAscii());
    } else {
      int expectedLength;
      switch (storedType) {
        case BIG_DECIMAL:
          expectedLength = BigDecimalUtils.byteSize((BigDecimal) fieldSpec.getDefaultNullValue());
          assertFalse(stats.isAscii());
          break;
        case STRING:
          expectedLength = Utf8.encodedLength((String) fieldSpec.getDefaultNullValue());
          assertTrue(stats.isAscii()); // default "null" is ASCII
          break;
        case BYTES:
          expectedLength = 0; // default is empty byte array
          assertFalse(stats.isAscii());
          break;
        default:
          throw new IllegalStateException("Unexpected var-width type: " + storedType);
      }
      assertEquals(stats.getLengthOfShortestElement(), expectedLength);
      assertEquals(stats.getLengthOfLongestElement(), expectedLength);
      assertTrue(stats.isFixedLength());
    }

    // isSorted
    assertTrue(stats.isSorted());

    // getTotalNumberOfEntries, getMaxNumberOfMultiValues, getMaxRowLengthInBytes
    assertEquals(stats.getTotalNumberOfEntries(), numDocs);
    assertEquals(stats.getMaxNumberOfMultiValues(), 0);
    assertEquals(stats.getMaxRowLengthInBytes(), stats.getLengthOfLongestElement());
  }

  @Test(dataProvider = "allTypesMV")
  public void testMV(DataType type) {
    int numDocs = 50;
    FieldSpec fieldSpec = new DimensionFieldSpec("col", type, false);
    DefaultColumnStatistics stats = new DefaultColumnStatistics(fieldSpec, numDocs);
    DataType storedType = type.getStoredType();

    // Assertions follow ColumnStatistics API order

    // getFieldSpec, getValueType, isSingleValue
    assertEquals(stats.getFieldSpec(), fieldSpec);
    assertEquals(stats.getValueType(), storedType);
    assertFalse(stats.isSingleValue());

    // getMinValue, getMaxValue — single default value: min == max
    assertNotNull(stats.getMinValue());
    assertEquals(stats.getMinValue(), stats.getMaxValue());

    // getUniqueValuesSet, getCardinality
    assertNotNull(stats.getUniqueValuesSet());
    assertEquals(stats.getCardinality(), 1);

    // getLengthOfShortestElement, getLengthOfLongestElement, isFixedLength, isAscii per stored type
    if (storedType.isFixedWidth()) {
      assertEquals(stats.getLengthOfShortestElement(), storedType.size());
      assertEquals(stats.getLengthOfLongestElement(), storedType.size());
      assertTrue(stats.isFixedLength());
      assertFalse(stats.isAscii());
    } else {
      int expectedLength;
      switch (storedType) {
        case STRING:
          expectedLength = Utf8.encodedLength((String) fieldSpec.getDefaultNullValue());
          assertTrue(stats.isAscii()); // default "null" is ASCII
          break;
        case BYTES:
          expectedLength = 0; // default is empty byte array
          assertFalse(stats.isAscii());
          break;
        default:
          throw new IllegalStateException("Unexpected var-width type: " + storedType);
      }
      assertEquals(stats.getLengthOfShortestElement(), expectedLength);
      assertEquals(stats.getLengthOfLongestElement(), expectedLength);
      assertTrue(stats.isFixedLength());
    }

    // isSorted — MV is never sorted
    assertFalse(stats.isSorted());

    // getTotalNumberOfEntries, getMaxNumberOfMultiValues, getMaxRowLengthInBytes
    assertEquals(stats.getTotalNumberOfEntries(), numDocs);
    assertEquals(stats.getMaxNumberOfMultiValues(), 1); // each row has 1 default element
    assertEquals(stats.getMaxRowLengthInBytes(), stats.getLengthOfLongestElement());
  }
}
