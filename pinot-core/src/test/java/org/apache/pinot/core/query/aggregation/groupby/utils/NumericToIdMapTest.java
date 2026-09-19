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
package org.apache.pinot.core.query.aggregation.groupby.utils;

import java.util.LinkedHashMap;
import java.util.Map;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Verifies numeric key identity and contiguous IDs across insertion, repetition, and map growth.
public class NumericToIdMapTest {
  @DataProvider(name = "numericMaps")
  public Object[][] numericMaps() {
    return new Object[][]{
        {new IntToIdMap(), new Object[]{Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE}},
        {new LongToIdMap(), new Object[]{Long.MIN_VALUE, -1L, 0L, 1L, Long.MAX_VALUE, 9007199254740993L}},
        {new FloatToIdMap(), new Object[]{Float.NEGATIVE_INFINITY, -Float.MAX_VALUE, -0.0f, 0.0f, Float.MIN_VALUE,
            Float.MAX_VALUE, Float.POSITIVE_INFINITY, Float.NaN}},
        {new DoubleToIdMap(), new Object[]{Double.NEGATIVE_INFINITY, -Double.MAX_VALUE, -0.0d, 0.0d, Double.MIN_VALUE,
            Double.MAX_VALUE, Double.POSITIVE_INFINITY, Double.NaN}},
        {new FloatToIdMap(), new Object[]{Float.intBitsToFloat(0x7fc00001)}},
        {new DoubleToIdMap(), new Object[]{Double.longBitsToDouble(0x7ff8000000000001L)}}
    };
  }

  @Test(dataProvider = "numericMaps")
  public void testNumericKeys(ValueToIdMap map, Object[] values) {
    Map<Object, Integer> expected = new LinkedHashMap<>();
    for (Object value : values) {
      assertEquals(map.getId(value), expected.getOrDefault(value, ValueToIdMap.INVALID_KEY).intValue());
      int expectedId = expected.computeIfAbsent(value, key -> expected.size());
      assertEquals(map.put(value), expectedId);
      assertEquals(map.put(value), expectedId);
      assertEquals(map.getId(value), expectedId);
      assertEquals(map.get(expectedId), value);
    }
    for (int i = 1; i <= 4096; i++) {
      Object value;
      if (map instanceof IntToIdMap) {
        value = Integer.valueOf(i);
      } else if (map instanceof LongToIdMap) {
        value = Long.valueOf(i);
      } else if (map instanceof FloatToIdMap) {
        value = Float.valueOf(i);
      } else {
        value = Double.valueOf(i);
      }
      int expectedId = expected.computeIfAbsent(value, key -> expected.size());
      assertEquals(map.put(value), expectedId);
    }
    expected.forEach((value, id) -> {
      assertEquals(map.put(value), id.intValue());
      assertEquals(map.getId(value), id.intValue());
      assertEquals(map.get(id), value);
    });
  }
}
