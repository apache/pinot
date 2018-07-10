/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
