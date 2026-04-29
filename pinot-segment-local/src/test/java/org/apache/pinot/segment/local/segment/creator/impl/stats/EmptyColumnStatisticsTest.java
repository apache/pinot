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
package org.apache.pinot.segment.local.segment.creator.impl.stats;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Tests for [EmptyColumnStatistics], covering all value types and SV/MV.
public class EmptyColumnStatisticsTest {

  private static final DataType[] ALL_TYPES = {
      DataType.INT, DataType.LONG, DataType.FLOAT, DataType.DOUBLE, DataType.BIG_DECIMAL, DataType.BOOLEAN,
      DataType.TIMESTAMP, DataType.STRING, DataType.JSON, DataType.BYTES, DataType.MAP
  };

  @DataProvider(name = "allTypesAndSV")
  public Object[][] allTypesAndSV() {
    List<Object[]> params = new ArrayList<>();
    for (DataType type : ALL_TYPES) {
      params.add(new Object[]{type, true});
      // MAP does not support MV
      if (type != DataType.MAP) {
        params.add(new Object[]{type, false});
      }
    }
    return params.toArray(new Object[0][]);
  }

  @Test(dataProvider = "allTypesAndSV")
  public void testEmptyColumnStatistics(DataType type, boolean sv) {
    FieldSpec fieldSpec = type != DataType.MAP ? new DimensionFieldSpec("col", type, sv)
        : new ComplexFieldSpec("col", DataType.MAP, true, Map.of());
    EmptyColumnStatistics stats = new EmptyColumnStatistics(fieldSpec, null, null);
    DataType storedType = type.getStoredType();

    // Identity
    assertEquals(stats.getFieldSpec(), fieldSpec);
    assertEquals(stats.getValueType(), storedType);
    assertEquals(stats.isSingleValue(), sv);

    // Empty: no values
    assertNull(stats.getMinValue());
    assertNull(stats.getMaxValue());
    assertNull(stats.getUniqueValuesSet());
    assertEquals(stats.getCardinality(), 0);
    assertEquals(stats.getTotalNumberOfEntries(), 0);
    assertEquals(stats.getMaxNumberOfMultiValues(), 0);
    assertEquals(stats.getMaxRowLengthInBytes(), 0);

    // Sorted: empty SV is sorted, empty MV is not
    assertEquals(stats.isSorted(), sv);

    // Element lengths: fixed-width -> type size; var-width -> 0
    if (storedType.isFixedWidth()) {
      assertEquals(stats.getLengthOfShortestElement(), storedType.size());
      assertEquals(stats.getLengthOfLongestElement(), storedType.size());
    } else {
      assertEquals(stats.getLengthOfShortestElement(), 0);
      assertEquals(stats.getLengthOfLongestElement(), 0);
    }
    assertTrue(stats.isFixedLength());
    assertFalse(stats.isAscii());

    // Partition: empty
    assertNull(stats.getPartitionFunction());
    assertNull(stats.getPartitions());
  }
}
