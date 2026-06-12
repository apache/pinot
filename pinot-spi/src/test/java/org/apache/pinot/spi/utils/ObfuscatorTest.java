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
package org.apache.pinot.spi.utils;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ObfuscatorTest {
  private static final String VALUE = "VALUE";
  private static final String SECRET = "SECRET";

  private Obfuscator _obfuscator;

  private Map<String, Object> _map;
  private Map<String, Object> _nestedMap;

  @BeforeMethod
  public void setup() {
    _obfuscator = new Obfuscator();

    _map = new HashMap<>();
    _map.put("value", "VALUE");
    _map.put("secret", "SECRET");
    _map.put("a.secret", "SECRET");
    _map.put("mysecret", "SECRET");
    _map.put("mySecret", "SECRET");
    _map.put("password", "SECRET");
    _map.put("a.password", "SECRET");
    _map.put("mypassword", "SECRET");
    _map.put("myPassword", "SECRET");
    _map.put("keytab", "SECRET");
    _map.put("a.keytab", "SECRET");
    _map.put("mykeytab", "SECRET");
    _map.put("myKeytab", "SECRET");
    _map.put("token", "SECRET");
    _map.put("a.token", "SECRET");
    _map.put("mytoken", "SECRET");
    _map.put("myToken", "SECRET");

    _map.put("secretKey", "SECRET");
    _map.put("secretkey", "SECRET");
    _map.put("secret_key", "SECRET");
    _map.put("mysecretKey", "SECRET");
    _map.put("mySecretKey", "SECRET");
    _map.put("a.secretKey", "SECRET");

    _nestedMap = new HashMap<>();
    _nestedMap.put("value", "VALUE");
    _nestedMap.put("map", _map);
  }

  @Test
  public void testSimple() {
    String output = _obfuscator.toJsonString(_map);
    Assert.assertTrue(output.contains(VALUE));
    Assert.assertFalse(output.contains(SECRET));
  }

  @Test
  public void testNested() {
    String output = _obfuscator.toJsonString(_nestedMap);
    Assert.assertTrue(output.contains(VALUE));
    Assert.assertFalse(output.contains(SECRET));
  }

  @Test
  public void testComplexObject() {
    Object complex = Pair.of("nested", Pair.of("moreNested", Pair.of("mostNestedSecret", SECRET)));
    String output = _obfuscator.toJsonString(complex);
    Assert.assertFalse(output.contains(SECRET));
  }

  @Test
  public void testNull() {
    Assert.assertEquals(String.valueOf(_obfuscator.toJson(null)), "null");
  }

  @Test
  public void testNoop() {
    Object output = new Obfuscator("nope", Collections.emptyList()).toJson(_map);
    Assert.assertEquals(output, JsonUtils.objectToJsonNode(_map));
  }

  @Test
  public void testCustomPattern() {
    Obfuscator obfuscator = new Obfuscator("verycustomized", Collections.singletonList(Pattern.compile("^value$")));
    String output = obfuscator.toJsonString(_nestedMap);
    Assert.assertFalse(output.contains(VALUE));
    Assert.assertTrue(output.contains("verycustomized"));
    Assert.assertTrue(output.contains(SECRET));
  }

  @Test
  public void testJsonNode()
      throws IOException {
    JsonNode node = JsonUtils.stringToJsonNode("{\"key\":\"VALUE\",\"my.secret\":\"SECRET\"}");
    String output = _obfuscator.toJsonString(node);
    Assert.assertTrue(output.contains(VALUE));
    Assert.assertFalse(output.contains(SECRET));
  }

  @Test
  public void testJsonString() {
    String output = _obfuscator.toJsonString("{\"key\":\"VALUE\",\"my.secret\":\"SECRET\"}");
    Assert.assertTrue(output.contains(VALUE));
    Assert.assertFalse(output.contains(SECRET));
  }

  @Test
  public void testArrayOfObjects() {
    Map<String, Object> item1 = new HashMap<>();
    item1.put("name", "item1");
    item1.put("password", "SECRET");

    Map<String, Object> item2 = new HashMap<>();
    item2.put("name", "item2");
    item2.put("secret", "SECRET");

    Map<String, Object> root = new HashMap<>();
    root.put("items", Arrays.asList(item1, item2));

    String output = _obfuscator.toJsonString(root);
    Assert.assertTrue(output.contains("item1"));
    Assert.assertTrue(output.contains("item2"));
    Assert.assertFalse(output.contains(SECRET));
  }

  @Test
  public void testNestedArrayOfObjects() {
    Map<String, Object> innerItem = new HashMap<>();
    innerItem.put("token", "SECRET");
    innerItem.put("host", "localhost");

    Map<String, Object> outerItem = new HashMap<>();
    outerItem.put("connections", Collections.singletonList(innerItem));
    outerItem.put("label", "cluster-1");

    Map<String, Object> root = new HashMap<>();
    root.put("clusters", Collections.singletonList(outerItem));

    String output = _obfuscator.toJsonString(root);
    Assert.assertTrue(output.contains("localhost"));
    Assert.assertTrue(output.contains("cluster-1"));
    Assert.assertFalse(output.contains(SECRET));
  }

  @Test
  public void testArrayOfObjectsViaJsonString() {
    String json = "{\"items\":[{\"user\":\"admin\",\"password\":\"SECRET\"},"
        + "{\"user\":\"reader\",\"token\":\"SECRET\"}]}";
    String output = _obfuscator.toJsonString(json);
    Assert.assertTrue(output.contains("admin"));
    Assert.assertTrue(output.contains("reader"));
    Assert.assertFalse(output.contains(SECRET));
  }

  @Test
  public void testTopLevelArray()
      throws IOException {
    JsonNode node = JsonUtils.stringToJsonNode("[{\"password\":\"SECRET\",\"host\":\"db1\"},{\"token\":\"SECRET\","
        + "\"host\":\"db2\"}]");
    String output = _obfuscator.toJsonString(node);
    Assert.assertTrue(output.contains("db1"));
    Assert.assertTrue(output.contains("db2"));
    Assert.assertFalse(output.contains(SECRET));
  }

  @Test
  public void testArrayWithMixedContent() {
    Map<String, Object> root = new HashMap<>();
    root.put("tags", Arrays.asList("public", "production"));
    root.put("credentials", Arrays.asList(
        createEntry("apiKey", "SECRET"),
        createEntry("apiKey", "SECRET")
    ));

    String output = _obfuscator.toJsonString(root);
    Assert.assertTrue(output.contains("public"));
    Assert.assertTrue(output.contains("production"));
    Assert.assertFalse(output.contains(SECRET));
  }

  private static Map<String, Object> createEntry(String key, String value) {
    Map<String, Object> map = new HashMap<>();
    map.put(key, value);
    return map;
  }
}
