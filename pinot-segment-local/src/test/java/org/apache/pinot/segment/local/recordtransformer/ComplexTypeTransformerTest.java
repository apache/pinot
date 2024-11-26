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

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.config.table.ingestion.ComplexTypeConfig;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.pinot.segment.local.recordtransformer.ComplexTypeTransformer.DEFAULT_COLLECTION_TO_JSON_MODE;


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
    innerMap1.put("cc", new byte[]{1, 1});

    map1.put("im1", innerMap1);
    Map<String, Object> map2 = new HashMap<>();
    map2.put("c", 3);
    genericRow.putValue("map2", map2);

    transformer.transform(genericRow);
    Assert.assertEquals(genericRow.getValue("a"), 1L);
    Assert.assertEquals(genericRow.getValue("map1.b"), "v");
    Assert.assertEquals(genericRow.getValue("map1.im1.aa"), 2);
    Assert.assertEquals(genericRow.getValue("map1.im1.bb"), "u");
    Assert.assertEquals(genericRow.getValue("map1.im1.cc"), new byte[]{1, 1});

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
    Assert.assertEquals(collection.size(), 2);
    Iterator<GenericRow> itr = collection.iterator();
    Assert.assertEquals(itr.next().getValue("array.a"), "v1");
    Assert.assertEquals(itr.next().getValue("array.a"), "v2");

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
    Assert.assertEquals(collection.size(), 4);
    itr = collection.iterator();
    GenericRow next = itr.next();
    Assert.assertEquals(next.getValue("array.a"), "v1");
    Assert.assertEquals(next.getValue("array2.b"), "v3");
    next = itr.next();
    Assert.assertEquals(next.getValue("array.a"), "v1");
    Assert.assertEquals(next.getValue("array2.b"), "v4");
    next = itr.next();
    Assert.assertEquals(next.getValue("array.a"), "v2");
    Assert.assertEquals(next.getValue("array2.b"), "v3");
    next = itr.next();
    Assert.assertEquals(next.getValue("array.a"), "v2");
    Assert.assertEquals(next.getValue("array2.b"), "v4");

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
    Assert.assertEquals(collection.size(), 3);
    itr = collection.iterator();
    next = itr.next();
    Assert.assertEquals(next.getValue("array.a"), "v1");
    Assert.assertEquals(next.getValue("array.array2.b"), "v3");
    next = itr.next();
    Assert.assertEquals(next.getValue("array.a"), "v1");
    Assert.assertEquals(next.getValue("array.array2.b"), "v4");
    next = itr.next();
    Assert.assertEquals(next.getValue("array.a"), "v2");

    transformer = new ComplexTypeTransformer(Arrays.asList("array"), ".");
    genericRow = new GenericRow();
    genericRow.putValue("array", array);
    map1.put("array2", array2);
    map2.put("array2", new Object[]{});
    transformer.transform(genericRow);
    Assert.assertNotNull(genericRow.getValue(GenericRow.MULTIPLE_RECORDS_KEY));
    collection = (Collection<GenericRow>) genericRow.getValue(GenericRow.MULTIPLE_RECORDS_KEY);
    Assert.assertEquals(collection.size(), 2);
    itr = collection.iterator();
    next = itr.next();
    Assert.assertEquals(next.getValue("array.a"), "v1");
    Assert.assertEquals(next.getValue("array.array2"), "[{\"b\":\"v3\"},{\"b\":\"v4\"}]");
    next = itr.next();
    Assert.assertEquals(next.getValue("array.a"), "v2");
  }

  @Test
  public void testUnnestMultiLevelArray() {
    //    {
    //      "level1" : [ {
    //      "level2" : {
    //        "level3" : [ {
    //          "level4" : "foo_bar"
    //        }, {
    //          "level4" : "foo_bar"
    //        } ]
    //      }
    //    }, {
    //      "level2" : {
    //        "level3" : [ {
    //          "level4" : "foo_bar"
    //        }, {
    //          "level4" : "foo_bar"
    //        } ]
    //      }
    //    } ]
    //    }
    GenericRow genericRow = new GenericRow();
    Map<String, String> level3 = new HashMap<>();
    level3.put("level4", "foo_bar");

    Map<String, Object> level2 = new HashMap<>();
    Object[] level3Arr = new Object[]{level3, level3};
    level2.put("level3", level3Arr);

    Map<String, Object> level1 = new HashMap<>();
    level1.put("level2", level2);

    Object[] level1Arr = new Object[]{level1, level1};
    genericRow.putValue("level1", level1Arr);

    List<String> fieldsToUnnest = new ArrayList<>();
    fieldsToUnnest.add("level1");
    fieldsToUnnest.add("level1.level2.level3");

    ComplexTypeTransformer complexTypeTransformer = new ComplexTypeTransformer(fieldsToUnnest, ".");
    GenericRow result = complexTypeTransformer.transform(genericRow);

    Assert.assertNotNull(result.getValue(GenericRow.MULTIPLE_RECORDS_KEY));
    Collection<GenericRow> rows = (Collection<GenericRow>) result.getValue(GenericRow.MULTIPLE_RECORDS_KEY);
    Assert.assertEquals(rows.size(), 4);
    for (GenericRow row : rows) {
      Assert.assertEquals(row.getValue("level1.level2.level3.level4"), "foo_bar");
    }
  }

  @Test
  public void testConvertCollectionToString() {
    // json convert inner collections
    // {
    //   "array":[
    //      {
    //         "array1":[
    //            {
    //               "b":"v1"
    //            }
    //         ]
    //      }
    //   ]
    // }
    // is converted to
    // [{
    //   "array.array1":"[
    //            {
    //               "b":"v1"
    //            }
    //         ]"
    // }]
    ComplexTypeTransformer transformer = new ComplexTypeTransformer(Arrays.asList("array"), ".");
    GenericRow genericRow = new GenericRow();
    Map<String, Object> map = new HashMap<>();
    Object[] array1 = new Object[1];
    array1[0] = ImmutableMap.of("b", "v1");
    map.put("array1", array1);
    Object[] array = new Object[1];
    array[0] = map;
    genericRow.putValue("array", array);
    transformer.transform(genericRow);
    Assert.assertNotNull(genericRow.getValue(GenericRow.MULTIPLE_RECORDS_KEY));
    Collection<GenericRow> collection = (Collection<GenericRow>) genericRow.getValue(GenericRow.MULTIPLE_RECORDS_KEY);
    GenericRow row = collection.iterator().next();
    Assert.assertTrue(row.getValue("array.array1") instanceof String);

    // primitive array not converted
    // {
    //   "array":[1,2]
    // }
    transformer = new ComplexTypeTransformer(Arrays.asList(), ".");
    genericRow = new GenericRow();
    array = new Object[]{1, 2};
    genericRow.putValue("array", array);
    transformer.transform(genericRow);
    Assert.assertTrue(genericRow.getValue("array") instanceof Object[]);

    // primitive array converted
    // {
    //   "array":[1,2]
    // }
    // ->
    // {
    //   "array":"[1,2]"
    // }
    transformer = new ComplexTypeTransformer(Arrays.asList(), ".",
        ComplexTypeConfig.CollectionNotUnnestedToJson.ALL, new HashMap<>(), null);
    genericRow = new GenericRow();
    array = new Object[]{1, 2};
    genericRow.putValue("array", array);
    transformer.transform(genericRow);
    Assert.assertTrue(genericRow.getValue("array") instanceof String);

    // array under tuple converted
    // {
    //   "t": {
    //         "array1":[
    //            {
    //               "b":"v1"
    //            }
    //         ]
    //      }
    // to
    // {
    //   "t": "{
    //         "array1":[
    //            {
    //               "b":"v1"
    //            }
    //         ]
    //  }"
    genericRow = new GenericRow();
    genericRow.putValue("t", map);
    transformer.transform(genericRow);
    Assert.assertTrue(genericRow.getValue("t.array1") instanceof String);

    // array under tuple not converted
    // {
    //   "t": {
    //         "array1":[
    //            {
    //               "b":"v1"
    //            }
    //         ]
    //      }
    genericRow = new GenericRow();
    map = new HashMap<>();
    array1 = new Object[1];
    array1[0] = ImmutableMap.of("b", "v1");
    map.put("array1", array1);
    genericRow.putValue("t", map);
    transformer = new ComplexTypeTransformer(Arrays.asList(), ".",
        ComplexTypeConfig.CollectionNotUnnestedToJson.NONE, new HashMap<>(), null);
    transformer.transform(genericRow);
    Assert.assertTrue(ComplexTypeTransformer.isNonPrimitiveArray(genericRow.getValue("t.array1")));
  }

  @Test
  public void testRenamePrefixes() {
    HashMap<String, String> prefixesToRename = new HashMap<>();
    prefixesToRename.put("map1.", "");
    prefixesToRename.put("map2", "test");
    ComplexTypeTransformer transformer = new ComplexTypeTransformer(new ArrayList<>(), ".",
        DEFAULT_COLLECTION_TO_JSON_MODE, prefixesToRename, null);

    GenericRow genericRow = new GenericRow();
    genericRow.putValue("a", 1L);
    genericRow.putValue("map1.b", 2L);
    genericRow.putValue("map2.c", "u");
    transformer.renamePrefixes(genericRow);
    Assert.assertEquals(genericRow.getValue("a"), 1L);
    Assert.assertEquals(genericRow.getValue("b"), 2L);
    Assert.assertEquals(genericRow.getValue("test.c"), "u");

    // name conflict where there becomes duplicate field names after renaming
    prefixesToRename = new HashMap<>();
    prefixesToRename.put("test.", "");
    transformer = new ComplexTypeTransformer(new ArrayList<>(), ".",
        DEFAULT_COLLECTION_TO_JSON_MODE, prefixesToRename, null);
    genericRow = new GenericRow();
    genericRow.putValue("a", 1L);
    genericRow.putValue("test.a", 2L);
    try {
      transformer.renamePrefixes(genericRow);
      Assert.fail("Should fail due to name conflict after renaming");
    } catch (RuntimeException e) {
      // expected
    }

    // name conflict where there becomes an empty field name after renaming
    prefixesToRename = new HashMap<>();
    prefixesToRename.put("test", "");
    transformer = new ComplexTypeTransformer(new ArrayList<>(), ".",
        DEFAULT_COLLECTION_TO_JSON_MODE, prefixesToRename, null);
    genericRow = new GenericRow();
    genericRow.putValue("a", 1L);
    genericRow.putValue("test", 2L);
    try {
      transformer.renamePrefixes(genericRow);
      Assert.fail("Should fail due to empty name after renaming");
    } catch (RuntimeException e) {
      // expected
    }

    // case where nothing gets renamed
    prefixesToRename = new HashMap<>();
    transformer = new ComplexTypeTransformer(new ArrayList<>(), ".",
        DEFAULT_COLLECTION_TO_JSON_MODE, prefixesToRename, null);
    genericRow = new GenericRow();
    genericRow.putValue("a", 1L);
    genericRow.putValue("test", 2L);
    transformer.renamePrefixes(genericRow);
    Assert.assertEquals(genericRow.getValue("a"), 1L);
    Assert.assertEquals(genericRow.getValue("test"), 2L);
  }

  @Test
  public void testPrefixesToRename() {
    HashMap<String, String> prefixesToRename = new HashMap<>();
    prefixesToRename.put("map1.", "");
    prefixesToRename.put("map2", "test");
    ComplexTypeTransformer transformer = new ComplexTypeTransformer(new ArrayList<>(), ".",
        DEFAULT_COLLECTION_TO_JSON_MODE, prefixesToRename, null);

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
    Assert.assertEquals(genericRow.getValue("b"), "v");
    Assert.assertEquals(genericRow.getValue("im1.aa"), 2);
    Assert.assertEquals(genericRow.getValue("im1.bb"), "u");
    Assert.assertEquals(genericRow.getValue("test.c"), 3);
  }

  @Test
  public void testPrefixesToRename2() {
    HashMap<String, String> prefixesToRename = new HashMap<>();
    prefixesToRename.put("info.", "");
    prefixesToRename.put("class_teacher", "teacher");
    ComplexTypeTransformer transformer = new ComplexTypeTransformer(new ArrayList<>(), ".",
        DEFAULT_COLLECTION_TO_JSON_MODE, prefixesToRename, null);

    // test flatten root-level tuples
    GenericRow genericRow = new GenericRow();
    genericRow.putValue("name", "Jane");
    Map<String, Object> info = new HashMap<>();
    genericRow.putValue("info", Map.of(
        "id", "100",
        "address", Map.of("street", "1 Park Street", "city", "San Francisco", "state", "CA")
    ));
    genericRow.putValue("class_teacher", Map.of("name", "Max"));

    transformer.transform(genericRow);
    Assert.assertEquals(genericRow.getValue("name"), "Jane");
    Assert.assertEquals(genericRow.getValue("id"), "100");
    Assert.assertEquals(genericRow.getValue("address.street"), "1 Park Street");
    Assert.assertEquals(genericRow.getValue("address.city"), "San Francisco");
    Assert.assertEquals(genericRow.getValue("address.state"), "CA");
    Assert.assertEquals(genericRow.getValue("teacher.name"), "Max");
  }
}
