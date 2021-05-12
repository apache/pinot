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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ComplexTypeTransformerTest {
  @Test
  public void testFlattenMap() {
    ComplexTypeTransformer transformer = new ComplexTypeTransformer(new ArrayList<>(), ".");

    // test flatten root-level tuples
    GenericRow genericRow = new GenericRow();
    genericRow.putValue("a", 1L);
    Map<String, Object> map1 = new HashMap<>();
    genericRow.putValue("map1", map1);
    map1.put("b", "v");
    Map<String, Object> innerMap1 = new HashMap<>();
    innerMap1.put("aa", 2);
    innerMap1.put("bb", "u");
    map1.put("im1", innerMap1);
    Map<String, Object> map2 = new HashMap<>();
    map2.put("c", 3);
    genericRow.putValue("map2", map2);

    transformer.transform(genericRow);
    Assert.assertEquals(genericRow.getValue("a"), 1L);
    Assert.assertEquals(genericRow.getValue("map1.b"), "v");
    Assert.assertEquals(genericRow.getValue("map1.im1.aa"), 2);
    Assert.assertEquals(genericRow.getValue("map1.im1.bb"), "u");
    Assert.assertEquals(genericRow.getValue("map2.c"), 3);

    // test flattening the tuple inside the collection
    transformer = new ComplexTypeTransformer(Arrays.asList("l1"), ".");
    genericRow = new GenericRow();
    List<Map<String, Object>> list1 = new ArrayList<>();
    list1.add(map1);
    genericRow.putValue("l1", list1);
    List<Integer> list2 = new ArrayList<>();
    list2.add(2);
    genericRow.putValue("l2", list2);
    transformer.flattenMap(genericRow, new ArrayList<>(genericRow.getFieldToValueMap().keySet()));
    Map<String, Object> map = (Map<String, Object>) ((Collection) genericRow.getValue("l1")).iterator().next();
    Assert.assertEquals(map.get("b"), "v");
    Assert.assertEquals(map.get("im1.aa"), 2);
    Assert.assertEquals(map.get("im1.bb"), "u");

    // test overriding delimiter
    transformer = new ComplexTypeTransformer(Arrays.asList("l1"), "_");
    genericRow = new GenericRow();
    innerMap1 = new HashMap<>();
    innerMap1.put("aa", 2);
    innerMap1.put("bb", "u");
    map1 = new HashMap<>();
    map1.put("im1", innerMap1);
    list1 = new ArrayList<>();
    list1.add(map1);
    genericRow.putValue("l1", list1);
    transformer.flattenMap(genericRow, new ArrayList<>(genericRow.getFieldToValueMap().keySet()));
    map = (Map<String, Object>) ((Collection) genericRow.getValue("l1")).iterator().next();
    Assert.assertEquals(map.get("im1_aa"), 2);
    Assert.assertEquals(map.get("im1_bb"), "u");
  }

  @Test
  public void testUnnestCollection() {
    // unnest root level collection
    //    {
    //      "array":[
    //      {
    //        "a":"v1"
    //      },
    //      {
    //        "a":"v2"
    //      }
    //   ]}
    //  ->
    //    [{
    //      "array.a":"v1"
    //    },
    //    {
    //      "array.a":"v2"
    //    }]
    ComplexTypeTransformer transformer = new ComplexTypeTransformer(Arrays.asList("array"), ".");
    GenericRow genericRow = new GenericRow();
    Object[] array = new Object[2];
    Map<String, Object> map1 = new HashMap<>();
    map1.put("a", "v1");
    Map<String, Object> map2 = new HashMap<>();
    map2.put("a", "v2");
    array[0] = map1;
    array[1] = map2;
    genericRow.putValue("array", array);
    transformer.transform(genericRow);
    Assert.assertNotNull(genericRow.getValue(GenericRow.MULTIPLE_RECORDS_KEY));
    Collection<GenericRow> collection = (Collection<GenericRow>) genericRow.getValue(GenericRow.MULTIPLE_RECORDS_KEY);
    Assert.assertEquals(2, collection.size());
    Iterator<GenericRow> itr = collection.iterator();
    Assert.assertEquals("v1", itr.next().getValue("array.a"));
    Assert.assertEquals("v2", itr.next().getValue("array.a"));

    // unnest sibling collections
    //    {
    //      "array":[
    //      {
    //        "a":"v1"
    //      },
    //      {
    //        "a":"v2"
    //      }],
    //      "array2":[
    //      {
    //        "b":"v3"
    //      },
    //      {
    //        "b":"v4"
    //      }]
    //    }
    // ->
    //  [
    //   {
    //      "array.a":"v1","array2.b":"v3"
    //   },
    //   {
    //      "array.a":"v1","array2.b":"v4"
    //   },
    //   {
    //      "array.a":"v2","array2.b":"v3"
    //   },
    //   {
    //      "array.a":"v2","array2.b":"v4"
    //   }]
    //
    transformer = new ComplexTypeTransformer(Arrays.asList("array", "array2"), ".");
    genericRow = new GenericRow();
    Object[] array2 = new Object[2];
    Map<String, Object> map3 = new HashMap<>();
    map3.put("b", "v3");
    Map<String, Object> map4 = new HashMap<>();
    map4.put("b", "v4");
    array2[0] = map3;
    array2[1] = map4;
    genericRow.putValue("array", array);
    genericRow.putValue("array2", array2);
    transformer.transform(genericRow);
    Assert.assertNotNull(genericRow.getValue(GenericRow.MULTIPLE_RECORDS_KEY));
    collection = (Collection<GenericRow>) genericRow.getValue(GenericRow.MULTIPLE_RECORDS_KEY);
    Assert.assertEquals(4, collection.size());
    itr = collection.iterator();
    GenericRow next = itr.next();
    Assert.assertEquals("v1", next.getValue("array.a"));
    Assert.assertEquals("v3", next.getValue("array2.b"));
    next = itr.next();
    Assert.assertEquals("v1", next.getValue("array.a"));
    Assert.assertEquals("v4", next.getValue("array2.b"));
    next = itr.next();
    Assert.assertEquals("v2", next.getValue("array.a"));
    Assert.assertEquals("v3", next.getValue("array2.b"));
    next = itr.next();
    Assert.assertEquals("v2", next.getValue("array.a"));
    Assert.assertEquals("v4", next.getValue("array2.b"));

    // unnest nested collection
    // {
    //   "array":[
    //      {
    //         "a":"v1",
    //         "array2":[
    //            {
    //               "b":"v3"
    //            },
    //            {
    //               "b":"v4"
    //            }
    //         ]
    //      },
    //      {
    //         "a":"v2",
    //         "array2":[
    //
    //         ]
    //      }
    //   ]}
    // ->
    // [
    //   {
    //      "array.a":"v1","array.array2.b":"v3"
    //   },
    //   {
    //      "array.a":"v1","array.array2.b":"v4"
    //   },
    //   {
    //      "array.a":"v2"
    //   }]
    transformer = new ComplexTypeTransformer(Arrays.asList("array", "array.array2"), ".");
    genericRow = new GenericRow();
    genericRow.putValue("array", array);
    map1.put("array2", array2);
    map2.put("array2", new Object[]{});
    transformer.transform(genericRow);
    Assert.assertNotNull(genericRow.getValue(GenericRow.MULTIPLE_RECORDS_KEY));
    collection = (Collection<GenericRow>) genericRow.getValue(GenericRow.MULTIPLE_RECORDS_KEY);
    Assert.assertEquals(3, collection.size());
    itr = collection.iterator();
    next = itr.next();
    Assert.assertEquals("v1", next.getValue("array.a"));
    Assert.assertEquals("v3", next.getValue("array.array2.b"));
    next = itr.next();
    Assert.assertEquals("v1", next.getValue("array.a"));
    Assert.assertEquals("v4", next.getValue("array.array2.b"));
    next = itr.next();
    Assert.assertEquals("v2", next.getValue("array.a"));
  }
}
