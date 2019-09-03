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

package org.apache.pinot.thirdeye.config;

import org.apache.pinot.thirdeye.dashboard.resources.v2.ConfigResource;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ConfigNamespaceIntegrationTest {
  private final static Map<String, String> MAP_EXPECTED = new HashMap<>();
  private DAOTestBase testDAOProvider;
  static {
    MAP_EXPECTED.put("a", "A");
    MAP_EXPECTED.put("b", "B");
    MAP_EXPECTED.put("c", "C");
  }

  private final static Map<String, Object> NESTED_EXPECTED = new HashMap<>();
  static {
    Map<String, Integer> inner = new HashMap<>();
    inner.put("aa", 1);
    inner.put("bb", 2);
    inner.put("cc", 3);

    NESTED_EXPECTED.put("a", 1);
    NESTED_EXPECTED.put("b", Arrays.asList("A", "B", "C"));
    NESTED_EXPECTED.put("c", inner);
  }

  private final static Map<String, Map<String, Integer>> NESTED_TYPED_EXPECTED = new HashMap<>();
  static {
    Map<String, Integer> inner1 = new HashMap<>();
    inner1.put("aa", 11);
    inner1.put("ab", 12);

    Map<String, Integer> inner2 = new HashMap<>();
    inner2.put("ba", 21);
    inner2.put("bb", 22);

    Map<String, Integer> inner3 = new HashMap<>();
    inner3.put("ca", 31);
    inner3.put("cb", 32);

    NESTED_TYPED_EXPECTED.put("1", inner1);
    NESTED_TYPED_EXPECTED.put("2", inner2);
    NESTED_TYPED_EXPECTED.put("3", inner3);
  }

  @BeforeMethod
  public void beforeMethod() {
    testDAOProvider = DAOTestBase.getInstance();
  }

  @AfterMethod(alwaysRun = true)
  public void afterMethod() {
    testDAOProvider.cleanup();
  }

  public void setupViaResource() throws IOException {
    ConfigResource res = new ConfigResource(DAORegistry.getInstance().getConfigDAO());
    res.put("ns1", "string", "mystring");
    res.put("ns1", "list", "[1, 2, 3]");
    res.put("ns1", "listString", "[\"1\", \"2\", \"3\"]");
    res.put("ns1", "map", "{\"a\": \"A\", \"b\": \"B\", \"c\": \"C\"}");
    res.put("ns1", "nested", "{\"a\": 1, \"b\": [\"A\", \"B\", \"C\"], \"c\": {\"aa\": 1, \"bb\": 2, \"cc\": 3}}");
    res.put("ns1", "nestedTyped", "{\"1\": {\"aa\": 11, \"ab\": 12}, \"2\": {\"ba\": 21, \"bb\": 22}, \"3\": {\"ca\": 31, \"cb\": 32}}");
  }

  @Test
  public void testReadConfig() throws Exception {
    setupViaResource();

    ConfigNamespace ns = new ConfigNamespace("ns1", DAORegistry.getInstance().getConfigDAO());
    String string = ns.get("string");
    Assert.assertEquals(string, "mystring");

    List<Integer> list = ns.get("list");
    Assert.assertEquals(list, Arrays.asList(1, 2, 3));

    List<String> listString = ns.get("listString");
    Assert.assertEquals(listString, Arrays.asList("1", "2", "3"));

    Map<String, String> map = ns.get("map");
    Assert.assertEquals(map, MAP_EXPECTED);

    Map<String, Object> nested = ns.get("nested");
    Assert.assertEquals(nested, NESTED_EXPECTED);

    Map<Integer, Map<String, Integer>> nestedTyped = ns.get("nestedTyped");
    Assert.assertEquals(nestedTyped, NESTED_TYPED_EXPECTED);
  }
}
