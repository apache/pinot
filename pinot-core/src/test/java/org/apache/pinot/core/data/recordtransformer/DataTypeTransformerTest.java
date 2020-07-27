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
package org.apache.pinot.core.data.recordtransformer;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertEqualsNoOrder;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;


public class DataTypeTransformerTest {
  private static final String COLUMN = "testColumn";

  @Test
  public void testStandardize() {
    // Tests for Map
    Map<String, Object> map = Collections.emptyMap();
    assertNull(DataTypeTransformer.standardize(COLUMN, map, true));
    assertNull(DataTypeTransformer.standardize(COLUMN, map, false));
    String expectedValue = "testValue";
    map = Collections.singletonMap("testKey", expectedValue);
    assertEquals(DataTypeTransformer.standardize(COLUMN, map, true), expectedValue);
    assertEquals(DataTypeTransformer.standardize(COLUMN, map, false), expectedValue);
    Object[] expectedValues = new Object[]{"testValue1", "testValue2"};
    map = new HashMap<>();
    map.put("testKey1", "testValue1");
    map.put("testKey2", "testValue2");
    try {
      DataTypeTransformer.standardize(COLUMN, map, true);
      fail();
    } catch (Exception e) {
      // Expected
    }
    assertEqualsNoOrder((Object[]) DataTypeTransformer.standardize(COLUMN, map, false), expectedValues);

    // Tests for List
    List<Object> list = Collections.emptyList();
    assertNull(DataTypeTransformer.standardize(COLUMN, list, true));
    assertNull(DataTypeTransformer.standardize(COLUMN, list, false));
    list = Collections.singletonList(expectedValue);
    assertEquals(DataTypeTransformer.standardize(COLUMN, list, true), expectedValue);
    assertEquals(DataTypeTransformer.standardize(COLUMN, list, false), expectedValue);
    list = Arrays.asList(expectedValues);
    try {
      DataTypeTransformer.standardize(COLUMN, list, true);
      fail();
    } catch (Exception e) {
      // Expected
    }
    assertEquals((Object[]) DataTypeTransformer.standardize(COLUMN, list, false), expectedValues);

    // Tests for Object[]
    Object[] values = new Object[0];
    assertNull(DataTypeTransformer.standardize(COLUMN, values, true));
    assertNull(DataTypeTransformer.standardize(COLUMN, values, false));
    values = new Object[]{expectedValue};
    assertEquals(DataTypeTransformer.standardize(COLUMN, values, true), expectedValue);
    assertEquals(DataTypeTransformer.standardize(COLUMN, values, false), expectedValue);
    values = new Object[]{"testValue1", "testValue2"};
    try {
      DataTypeTransformer.standardize(COLUMN, values, true);
      fail();
    } catch (Exception e) {
      // Expected
    }
    assertEquals((Object[]) DataTypeTransformer.standardize(COLUMN, values, false), expectedValues);

    // Tests for nested Map/List/Object[]
    map = Collections.singletonMap("testKey", Collections.emptyList());
    assertNull(DataTypeTransformer.standardize(COLUMN, map, true));
    assertNull(DataTypeTransformer.standardize(COLUMN, map, false));

    map = Collections.singletonMap("testKey", Collections.singletonList(expectedValue));
    assertEquals(DataTypeTransformer.standardize(COLUMN, map, true), expectedValue);
    assertEquals(DataTypeTransformer.standardize(COLUMN, map, false), expectedValue);

    map = new HashMap<>();
    map.put("testKey1", Collections.emptyMap());
    map.put("testKey2", Collections.singletonMap("testKey", expectedValue));
    assertEquals(DataTypeTransformer.standardize(COLUMN, map, true), expectedValue);
    assertEquals(DataTypeTransformer.standardize(COLUMN, map, false), expectedValue);

    map = Collections.singletonMap("testKey", Arrays.asList(expectedValues));
    try {
      DataTypeTransformer.standardize(COLUMN, map, true);
      fail();
    } catch (Exception e) {
      // Expected
    }
    assertEqualsNoOrder((Object[]) DataTypeTransformer.standardize(COLUMN, map, false), expectedValues);

    map = new HashMap<>();
    map.put("testKey1", Collections.emptyMap());
    map.put("testKey2", Collections.singletonList("testValue1"));
    map.put("testKey3", new Object[]{"testValue2"});
    try {
      DataTypeTransformer.standardize(COLUMN, map, true);
      fail();
    } catch (Exception e) {
      // Expected
    }
    assertEqualsNoOrder((Object[]) DataTypeTransformer.standardize(COLUMN, map, false), expectedValues);

    list = Arrays
        .asList(Collections.singletonMap("testKey", "testValue1"), Collections.singletonMap("testKey", "testValue2"),
            Collections.emptyMap());
    try {
      DataTypeTransformer.standardize(COLUMN, list, true);
      fail();
    } catch (Exception e) {
      // Expected
    }
    assertEquals((Object[]) DataTypeTransformer.standardize(COLUMN, list, false), expectedValues);

    values = new Object[]{new Object[0], Collections.singletonList(
        Collections.singletonMap("testKey", "testValue1")), Collections.singletonMap("testKey",
        Arrays.asList(new Object[]{"testValue2"}, Collections.emptyMap()))};
    try {
      DataTypeTransformer.standardize(COLUMN, values, true);
      fail();
    } catch (Exception e) {
      // Expected
    }
    assertEqualsNoOrder((Object[]) DataTypeTransformer.standardize(COLUMN, values, false), expectedValues);
  }
}
