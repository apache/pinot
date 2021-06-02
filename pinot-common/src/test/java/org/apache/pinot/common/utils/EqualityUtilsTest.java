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
package org.apache.pinot.common.utils;

import java.util.*;
import org.apache.pinot.spi.utils.EqualityUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class EqualityUtilsTest {

  @Test
  public void testPrimitiveType() {
    Assert.assertTrue(EqualityUtils.isEqual(Float.NaN, Float.NaN));
    Assert.assertTrue(EqualityUtils.isEqual(Double.NaN, Double.NaN));
  }

  @Test
  public void testObjectArray() {
    Integer[] integerArray1 = {1};
    Integer[] integerArray1Copy = {1};
    Integer[] integerArray2 = {1, 2};
    Assert.assertFalse(EqualityUtils.isSameReference(integerArray1, integerArray1Copy));
    Assert.assertTrue(EqualityUtils.isEqual(integerArray1, integerArray1Copy));
    Assert.assertFalse(EqualityUtils.isEqual(integerArray1, integerArray2));
  }

  @Test
  public void testListObject() {
    List<Integer> list1 = Arrays.asList(1, 2);
    List<Integer> list1Copy = Arrays.asList(1, 2);
    List<Integer> list2 = Arrays.asList(2, 1);
    Assert.assertFalse(EqualityUtils.isSameReference(list1, list1Copy));
    Assert.assertTrue(EqualityUtils.isEqual(list1, list1Copy));
    Assert.assertFalse(EqualityUtils.isEqual(list1, list2));
    Assert.assertTrue(EqualityUtils.isEqualIgnoreOrder(list1, list2));
  }

  @Test
  public void testSetObject() {
    Set<Integer> set1 = Collections.singleton(1);
    Set<Integer> set1Copy = Collections.singleton(1);
    Set<Long> set2 = Collections.singleton(1L);
    Assert.assertFalse(EqualityUtils.isSameReference(set1, set1Copy));
    Assert.assertTrue(EqualityUtils.isEqual(set1, set1Copy));
    Assert.assertFalse(EqualityUtils.isEqual(set1, set2));
  }

  @Test
  public void testMapObject() {
    Map<Integer, Integer> map1 = new HashMap<>();
    map1.put(1, 1);
    Map<Integer, Integer> map1Copy = new HashMap<>();
    map1Copy.put(1, 1);
    Map<Long, Integer> map2 = new HashMap<>();
    map2.put(1L, 1);
    Assert.assertFalse(EqualityUtils.isSameReference(map1, map1Copy));
    Assert.assertTrue(EqualityUtils.isEqual(map1, map1Copy));
    Assert.assertFalse(EqualityUtils.isEqual(map1, map2));
  }

  @Test
  public void testNestedMapCollections() {
    Map<Integer, Object> map1 = new HashMap<>();
    Map<Integer, Object> map1WithInt = new HashMap<>();
    map1WithInt.put(1, 1);
    Map<Integer, Object> map1WithArrayOfMap = new HashMap<>();
    map1WithArrayOfMap.put(1, Arrays.asList(1, map1WithInt).toArray());
    Map<Integer, Object> map1WithListOfMap = new HashMap<>();
    map1WithListOfMap.put(1, Arrays.asList(1, map1WithInt));
    Map<Integer, Object> map1WithByteArray = new HashMap<>();
    map1WithByteArray.put(1, "testString".getBytes());
    Map<Integer, Object> map1WithNull = new HashMap<>();
    map1WithNull.put(1, null);
    map1.put(1, map1WithArrayOfMap);
    map1.put(2, map1WithListOfMap);
    map1.put(3, map1WithByteArray);
    map1.put(4, map1WithNull);

    Map<Integer, Object> map1Copy = new HashMap<>();
    Map<Integer, Object> map1CopyWithInt = new HashMap<>();
    map1CopyWithInt.put(1, 1);
    Map<Integer, Object> map1CopyWithArrayOfMap = new HashMap<>();
    map1CopyWithArrayOfMap.put(1, Arrays.asList(1, map1CopyWithInt).toArray());
    Map<Integer, Object> map1CopyWithListOfMap = new HashMap<>();
    map1CopyWithListOfMap.put(1, Arrays.asList(1, map1CopyWithInt));
    Map<Integer, Object> map1CopyWithByteArray = new HashMap<>();
    map1CopyWithByteArray.put(1, "testString".getBytes());
    Map<Integer, Object> map1CopyWithNull = new HashMap<>();
    map1CopyWithNull.put(1, null);
    map1Copy.put(1, map1CopyWithArrayOfMap);
    map1Copy.put(2, map1CopyWithListOfMap);
    map1Copy.put(3, map1CopyWithByteArray);
    map1Copy.put(4, map1CopyWithNull);

    Assert.assertFalse(EqualityUtils.isSameReference(map1, map1Copy));
    Assert.assertTrue(EqualityUtils.isEqual(map1, map1Copy));

    // Place different data throughout the nested collections to ensure we're checking deep equality
    List<Integer> floatLocations = Arrays.asList(0, 1, 2, 3, 4);
    floatLocations.forEach(loc -> {
      Map<Integer, Object> map2 = new HashMap<>();
      Map<Integer, Object> map2WithInt = new HashMap<>();
      map2WithInt.put(1, 1);
      Map<Integer, Object> map2WithFloat = new HashMap<>();
      map2WithFloat.put(1, 1L);
      Map<Integer, Object> map2WithArrayOfMap = new HashMap<>();
      map2WithArrayOfMap.put(1, Arrays.asList(1, loc == 1 ? map2WithFloat : map2WithInt).toArray());
      Map<Integer, Object> map2WithByteArray = new HashMap<>();
      String testString = loc == 2 ? "newTestString" : "testString";
      map2WithByteArray.put(1, testString.getBytes());
      Map<Integer, Object> map2WithListOfMap = new HashMap<>();
      map2WithListOfMap.put(1, Arrays.asList(1, loc == 3 ? map2WithFloat : map2WithInt));
      Map<Integer, Object> map2WithNull = new HashMap<>();
      map2WithNull.put(1, loc == 4 ? map2WithInt : null);
      map2.put(1, map2WithArrayOfMap);
      map2.put(2, map2WithListOfMap);
      map2.put(3, map2WithByteArray);
      map2.put(4, map2WithNull);

      if (loc == 0) {
        Assert.assertTrue(EqualityUtils.isEqual(map1, map2), "Loc 0 should have no changes");
      } else {
        Assert.assertFalse(EqualityUtils.isEqual(map1, map2), "Failed at loc " + loc);
      }
    });

  }

  @Test
  public void testNull() {
    List list = new ArrayList();
    Assert.assertTrue(EqualityUtils.isEqual(null, null));
    Assert.assertTrue(EqualityUtils.isEqualIgnoreOrder(null, null));
    Assert.assertTrue(EqualityUtils.isSameReference(null, null));
    Assert.assertFalse(EqualityUtils.isEqual(list, null));
    Assert.assertFalse(EqualityUtils.isEqualIgnoreOrder(list, null));
    Assert.assertFalse(EqualityUtils.isSameReference(list, null));
    Assert.assertTrue(EqualityUtils.isNullOrNotSameClass(list, null));
    Assert.assertEquals(EqualityUtils.hashCodeOf(null), 0);
  }
}
