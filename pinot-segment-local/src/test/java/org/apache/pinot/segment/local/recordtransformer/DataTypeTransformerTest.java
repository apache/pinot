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
package org.apache.pinot.segment.local.recordtransformer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.segment.local.utils.DataTypeTransformerUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertEqualsNoOrder;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;


public class DataTypeTransformerTest {
  private static final String COLUMN = "testColumn";

  @Test
  public void testStandardize() {
    /**
     * Tests for Map
     */

    // Empty Map
    Map<String, Object> map = Map.of();
    assertNull(DataTypeTransformerUtils.standardize(COLUMN, map, true));
    assertNull(DataTypeTransformerUtils.standardize(COLUMN, map, false));

    // Map with single entry
    String expectedValue = "testValue";
    map = Map.of("testKey", expectedValue);
    assertEquals(DataTypeTransformerUtils.standardize(COLUMN, map, true), expectedValue);
    assertEquals(DataTypeTransformerUtils.standardize(COLUMN, map, false), expectedValue);

    // Map with multiple entries
    Object[] expectedValues = new Object[]{"testValue1", "testValue2"};
    map = new HashMap<>();
    map.put("testKey1", "testValue1");
    map.put("testKey2", "testValue2");
    try {
      // Should fail because Map with multiple entries cannot be standardized as single value
      DataTypeTransformerUtils.standardize(COLUMN, map, true);
      fail();
    } catch (Exception e) {
      // Expected
    }
    assertEqualsNoOrder((Object[]) DataTypeTransformerUtils.standardize(COLUMN, map, false), expectedValues);

    /**
     * Tests for List
     */

    // Empty List
    List<Object> list = List.of();
    assertNull(DataTypeTransformerUtils.standardize(COLUMN, list, true));
    assertNull(DataTypeTransformerUtils.standardize(COLUMN, list, false));

    // List with single entry
    list = List.of(expectedValue);
    assertEquals(DataTypeTransformerUtils.standardize(COLUMN, list, true), expectedValue);
    assertEquals(DataTypeTransformerUtils.standardize(COLUMN, list, false), expectedValue);

    // List with multiple entries
    list = Arrays.asList(expectedValues);
    try {
      // Should fail because List with multiple entries cannot be standardized as single value
      DataTypeTransformerUtils.standardize(COLUMN, list, true);
      fail();
    } catch (Exception e) {
      // Expected
    }
    assertEquals((Object[]) DataTypeTransformerUtils.standardize(COLUMN, list, false), expectedValues);

    /**
     * Tests for Object[]
     */

    // Empty Object[]
    Object[] values = new Object[0];
    assertNull(DataTypeTransformerUtils.standardize(COLUMN, values, true));
    assertNull(DataTypeTransformerUtils.standardize(COLUMN, values, false));

    // Object[] with single entry
    values = new Object[]{expectedValue};
    assertEquals(DataTypeTransformerUtils.standardize(COLUMN, values, true), expectedValue);
    assertEquals(DataTypeTransformerUtils.standardize(COLUMN, values, false), expectedValue);

    // Object[] with multiple entries
    values = new Object[]{"testValue1", "testValue2"};
    try {
      // Should fail because Object[] with multiple entries cannot be standardized as single value
      DataTypeTransformerUtils.standardize(COLUMN, values, true);
      fail();
    } catch (Exception e) {
      // Expected
    }
    assertEquals((Object[]) DataTypeTransformerUtils.standardize(COLUMN, values, false), expectedValues);

    /**
     * Tests for nested Map/List/Object[]
     */

    // Map with empty List
    map = Map.of("testKey", List.of());
    assertNull(DataTypeTransformerUtils.standardize(COLUMN, map, true));
    assertNull(DataTypeTransformerUtils.standardize(COLUMN, map, false));

    // Map with single-entry List
    map = Map.of("testKey", List.of(expectedValue));
    assertEquals(DataTypeTransformerUtils.standardize(COLUMN, map, true), expectedValue);
    assertEquals(DataTypeTransformerUtils.standardize(COLUMN, map, false), expectedValue);

    // Map with one empty Map and one single-entry Map
    map = new HashMap<>();
    map.put("testKey1", Map.of());
    map.put("testKey2", Map.of("testKey", expectedValue));
    // Can be standardized into single value because empty Map should be ignored
    assertEquals(DataTypeTransformerUtils.standardize(COLUMN, map, true), expectedValue);
    assertEquals(DataTypeTransformerUtils.standardize(COLUMN, map, false), expectedValue);

    // Map with multi-entries List
    map = Map.of("testKey", Arrays.asList(expectedValues));
    try {
      // Should fail because Map with multiple entries cannot be standardized as single value
      DataTypeTransformerUtils.standardize(COLUMN, map, true);
      fail();
    } catch (Exception e) {
      // Expected
    }
    assertEqualsNoOrder((Object[]) DataTypeTransformerUtils.standardize(COLUMN, map, false), expectedValues);

    // Map with one empty Map, one single-entry List and one single-entry Object[]
    map = new HashMap<>();
    map.put("testKey1", Map.of());
    map.put("testKey2", List.of("testValue1"));
    map.put("testKey3", new Object[]{"testValue2"});
    try {
      // Should fail because Map with multiple entries cannot be standardized as single value
      DataTypeTransformerUtils.standardize(COLUMN, map, true);
      fail();
    } catch (Exception e) {
      // Expected
    }
    assertEqualsNoOrder((Object[]) DataTypeTransformerUtils.standardize(COLUMN, map, false), expectedValues);

    // List with two single-entry Maps and one empty Map
    list = Arrays
        .asList(Map.of("testKey", "testValue1"), Map.of("testKey", "testValue2"),
            Map.of());
    try {
      // Should fail because List with multiple entries cannot be standardized as single value
      DataTypeTransformerUtils.standardize(COLUMN, list, true);
      fail();
    } catch (Exception e) {
      // Expected
    }
    assertEquals((Object[]) DataTypeTransformerUtils.standardize(COLUMN, list, false), expectedValues);

    // Object[] with two single-entry Maps
    values = new Object[]{
        Map.of("testKey", "testValue1"), Map.of("testKey", "testValue2")
    };
    try {
      // Should fail because Object[] with multiple entries cannot be standardized as single value
      DataTypeTransformerUtils.standardize(COLUMN, values, true);
      fail();
    } catch (Exception e) {
      // Expected
    }
    assertEqualsNoOrder((Object[]) DataTypeTransformerUtils.standardize(COLUMN, values, false), expectedValues);

    // Object[] with one empty Object[], one multi-entries List of nested Map/List/Object[]
    values = new Object[]{
        new Object[0], List.of(Map.of("testKey", "testValue1")),
        Map.of("testKey", Arrays.asList(new Object[]{"testValue2"}, Map.of()))
    };
    try {
      DataTypeTransformerUtils.standardize(COLUMN, values, true);
      fail();
    } catch (Exception e) {
      // Expected
    }
    assertEqualsNoOrder((Object[]) DataTypeTransformerUtils.standardize(COLUMN, values, false), expectedValues);
  }
}
