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
package org.apache.pinot.plugin.inputformat.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;



public class JSONRecordExtractorUtilsTest {

  @Test(dataProvider = "conversionTestData")
  public void testConversion(Object value, Object expectedConvertedValue)
      throws JsonProcessingException {
    Object convertedValue = JSONRecordExtractorUtils.convertValue(value);
    Assert.assertEquals(JsonUtils.objectToString(convertedValue), JsonUtils.objectToString(expectedConvertedValue));
  }

  @DataProvider(name = "conversionTestData")
  public Object[][] getConversionTestData()
      throws IOException {
    List<Object[]> input = new ArrayList<>();

    String jsonString = "{\n"
        + "  \"myInt\": 10,\n"
        + "  \"myLong\": 1588469340000,\n"
        + "  \"myDouble\": 10.2,\n"
        + "  \"myNull\": null,\n"
        + "  \"myString\": \"foo\",\n"
        + "  \"myIntArray\": [10, 20, 30],\n"
        + "  \"myStringArray\": [\"foo\", null, \"bar\"],\n"
        + "  \"myDoubleArray\": [10.2, 12.1, 1.1],\n"
        + "  \"myComplexArray1\": [{\"one\": 1, \"two\": \"too\"}, {\"one\": 11, \"two\": \"roo\"}],\n"
        + "  \"myComplexArray2\": [{\"one\":1, \"two\": {\"sub1\":1.1, \"sub2\": 1.2}, \"three\":[\"a\", \"b\"]}, {\"one\":11, \"two\": {\"sub1\":11.1, \"sub2\": 11.2}, \"three\":[\"aa\", \"bb\"]}],\n"
        + "  \"myMap1\": {\"k1\": \"foo\", \"k2\": \"bar\"},\n"
        + "  \"myMap2\": {\"k3\": {\"sub1\": 10, \"sub2\": 1.0}, \"k4\": \"baz\", \"k5\": [1,2,3]}\n" + "}";
    Map<String, Object> jsonNode = new ObjectMapper().readValue(jsonString, Map.class);
    input.add(new Object[]{jsonNode.get("myNull"), null});

    input.add(new Object[]{jsonNode.get("myInt"), 10});

    input.add(new Object[]{jsonNode.get("myLong"), 1588469340000L});

    input.add(new Object[]{jsonNode.get("myDouble"), 10.2});

    input.add(new Object[]{jsonNode.get("myString"), "foo"});

    input.add(new Object[]{jsonNode.get("myIntArray"), new Object[]{10, 20, 30}});

    input.add(new Object[]{jsonNode.get("myDoubleArray"), new Object[]{10.2, 12.1, 1.1}});

    input.add(new Object[]{jsonNode.get("myStringArray"), new Object[]{"foo", "bar"}});

    Map<String, Object> map1 = new HashMap<>();
    map1.put("one", 1);
    map1.put("two", "too");
    Map<String, Object> map2 = new HashMap<>();
    map2.put("one", 11);
    map2.put("two", "roo");
    input.add(new Object[]{jsonNode.get("myComplexArray1"), new Object[]{map1, map2}});

    Map<String, Object> map3 = new HashMap<>();
    map3.put("one", 1);
    Map<String, Object> map31 = new HashMap<>();
    map31.put("sub1", 1.1);
    map31.put("sub2", 1.2);
    map3.put("two", map31);
    map3.put("three", new Object[]{"a", "b"});
    Map<String, Object> map4 = new HashMap<>();
    map4.put("one", 11);
    Map<String, Object> map41 = new HashMap<>();
    map41.put("sub1", 11.1);
    map41.put("sub2", 11.2);
    map4.put("two", map41);
    map4.put("three", new Object[]{"aa", "bb"});
    input.add(new Object[]{jsonNode.get("myComplexArray2"), new Object[]{map3, map4}});

    Map<String, Object> map5 = new HashMap<>();
    map5.put("k1", "foo");
    map5.put("k2", "bar");
    input.add(new Object[]{jsonNode.get("myMap1"), map5});

    Map<String, Object> map6 = new HashMap<>();
    Map<String, Object> map61 = new HashMap<>();
    map61.put("sub1", 10);
    map61.put("sub2", 1.0);
    map6.put("k3", map61);
    map6.put("k4", "baz");
    map6.put("k5", new Object[]{1, 2, 3});
    input.add(new Object[]{jsonNode.get("myMap2"), map6});

    return input.toArray(new Object[0][]);
  }

}