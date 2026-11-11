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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.function.IntFunction;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Verifies key identity and contiguous IDs across insertion, repetition, and map growth.
public class ValueToIdMapTest {
  @DataProvider(name = "maps")
  public Object[][] maps() {
    return new Object[][]{
        {new IntToIdMap(), new Object[]{Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE},
            (IntFunction<Object>) Integer::valueOf},
        {new LongToIdMap(), new Object[]{Long.MIN_VALUE, -1L, 0L, 1L, Long.MAX_VALUE, 9007199254740993L},
            (IntFunction<Object>) Long::valueOf},
        // fastutil preserves raw floating-point bits: signed zeros and different NaN payloads are distinct keys.
        {new FloatToIdMap(), new Object[]{Float.NEGATIVE_INFINITY, -Float.MAX_VALUE, -0.0f, 0.0f, Float.MIN_VALUE,
            Float.MAX_VALUE, Float.POSITIVE_INFINITY, Float.NaN, Float.intBitsToFloat(0x7fc00001)},
            (IntFunction<Object>) Float::valueOf},
        {new DoubleToIdMap(), new Object[]{Double.NEGATIVE_INFINITY, -Double.MAX_VALUE, -0.0d, 0.0d, Double.MIN_VALUE,
            Double.MAX_VALUE, Double.POSITIVE_INFINITY, Double.NaN, Double.longBitsToDouble(0x7ff8000000000001L)},
            (IntFunction<Object>) Double::valueOf},
        {ValueToIdMapFactory.get(DataType.STRING), new Object[]{null, "", "a", "b"},
            (IntFunction<Object>) String::valueOf},
        {ValueToIdMapFactory.get(DataType.BYTES),
            new Object[]{new ByteArray(new byte[0]), new ByteArray(new byte[]{1})},
            (IntFunction<Object>) value -> new ByteArray(ByteBuffer.allocate(Integer.BYTES).putInt(value).array())},
        {ValueToIdMapFactory.get(DataType.BIG_DECIMAL), new Object[]{BigDecimal.ZERO, new BigDecimal("1.0"),
            new BigDecimal("1.00")}, (IntFunction<Object>) BigDecimal::valueOf}
    };
  }

  @Test(dataProvider = "maps")
  public void testKeys(ValueToIdMap map, Object[] values, IntFunction<Object> valueFactory) {
    for (int i = 0; i < values.length; i++) {
      assertEquals(map.getId(values[i]), ValueToIdMap.INVALID_KEY);
      assertMapping(map, values[i], i);
    }
    // These values are disjoint from the initial keys and force several rehashes.
    for (int i = 0; i < 4096; i++) {
      Object value = valueFactory.apply(1000 + i);
      assertEquals(map.getId(value), ValueToIdMap.INVALID_KEY);
      assertMapping(map, value, values.length + i);
    }
    for (int i = 0; i < values.length; i++) {
      assertMapping(map, values[i], i);
    }
    for (int i = 0; i < 4096; i++) {
      assertMapping(map, valueFactory.apply(1000 + i), values.length + i);
    }
  }

  private static void assertMapping(ValueToIdMap map, Object value, int expectedId) {
    assertEquals(map.put(value), expectedId);
    assertEquals(map.put(value), expectedId);
    assertEquals(map.getId(value), expectedId);
    assertEquals(map.get(expectedId), value);
    assertEquals(map.getId(map.get(expectedId)), expectedId);
  }
}
