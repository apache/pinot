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
    _map.put("password", "SECRET");
    _map.put("a.password", "SECRET");
    _map.put("mypassword", "SECRET");
    _map.put("token", "SECRET");
    _map.put("a.token", "SECRET");
    _map.put("mytoken", "SECRET");

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
}
