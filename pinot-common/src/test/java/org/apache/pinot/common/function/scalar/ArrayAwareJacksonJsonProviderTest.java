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
package org.apache.pinot.common.function.scalar;

import com.jayway.jsonpath.JsonPathException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.apache.pinot.segment.spi.utils.JavaVersion;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class ArrayAwareJacksonJsonProviderTest {
  @Test
  public void testIsArray() {
    JsonFunctions.ArrayAwareJacksonJsonProvider jp = new JsonFunctions.ArrayAwareJacksonJsonProvider();

    assertFalse(jp.isArray(null));
    assertTrue(jp.isArray(new Object[0]));
    assertTrue(jp.isArray(new ArrayList<>(0)));
    assertFalse(jp.isArray("I'm a list"));
    assertFalse(jp.isArray(new HashMap<String, String>()));
  }

  @Test
  public void testArrayLength() {
    JsonFunctions.ArrayAwareJacksonJsonProvider jp = new JsonFunctions.ArrayAwareJacksonJsonProvider();

    Object[] dataInAry = new Object[]{"123", "456"};
    assertEquals(jp.length(dataInAry), 2);

    List<Object> dataInList = Arrays.asList("abc", "efg", "hij");
    assertEquals(jp.length(dataInList), 3);
  }

  @Test(expectedExceptions = JsonPathException.class)
  public void testArrayLengthThrowsForNullArray() {
    new JsonFunctions.ArrayAwareJacksonJsonProvider().length(null);
  }

  @Test
  public void testGetArrayIndex() {
    JsonFunctions.ArrayAwareJacksonJsonProvider jp = new JsonFunctions.ArrayAwareJacksonJsonProvider();

    Object[] dataInAry = new Object[]{"123", "456"};
    for (int i = 0; i < dataInAry.length; i++) {
      assertEquals(jp.getArrayIndex(dataInAry, i), dataInAry[i]);
    }
    try {
      jp.getArrayIndex(dataInAry, dataInAry.length);
      fail();
    } catch (IndexOutOfBoundsException e) {
      assertEquals(e.getMessage(), "Index 2 out of bounds for length 2");
    }

    List<Object> dataInList = Arrays.asList("abc", "efg", "hij");
    for (int i = 0; i < dataInList.size(); i++) {
      assertEquals(jp.getArrayIndex(dataInList, i), dataInList.get(i));
    }
    try {
      jp.getArrayIndex(dataInList, dataInList.size());
      fail();
    } catch (IndexOutOfBoundsException e) {
      assertEquals(e.getMessage(), "Index 3 out of bounds for length 3");
    }
  }

  @Test
  public void testToIterable() {
    JsonFunctions.ArrayAwareJacksonJsonProvider jp = new JsonFunctions.ArrayAwareJacksonJsonProvider();

    Object[] dataInAry = new Object[]{"123", "456"};
    int idx = 0;
    for (Object v : jp.toIterable(dataInAry)) {
      assertEquals(v, dataInAry[idx++]);
    }

    List<Object> dataInList = Arrays.asList("abc", "efg", "hij");
    idx = 0;
    for (Object v : jp.toIterable(dataInList)) {
      assertEquals(v, dataInList.get(idx++));
    }

    try {
      jp.toIterable(null);
      fail();
    } catch (NullPointerException e) {
      // It's supposed to get a JsonPathException, but JsonPath library actually
      // has a bug leading to NullPointerException while creating the JsonPathException.
      if (JavaVersion.VERSION < 14) {
        // In modern Java versions messages is something like "Cannot invoke "Object.getClass()" because "obj" is null"
        assertNull(e.getMessage());
      }
    }
  }
}
