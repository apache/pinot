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

import org.apache.pinot.thirdeye.datalayer.bao.ConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ConfigNamespaceTest {
  private ConfigNamespace cn;
  private DAOTestBase testDAOProvider;

  @BeforeMethod
  public void beforeMethod() {
    testDAOProvider = DAOTestBase.getInstance();
    this.cn = new ConfigNamespace("namespace", DAORegistry.getInstance().getConfigDAO());
  }

  @AfterMethod(alwaysRun = true)
  public void afterMethod() {
    testDAOProvider.cleanup();
  }

  @Test
  public void testOverride() {
    this.cn.put("a", "hello");
    this.cn.put("a", "world");
    this.cn.put("a", "!");

    Assert.assertEquals(this.cn.get("a"), "!");
  }

  @Test
  public void testListAll() {
    this.cn.put("a", "v1");
    this.cn.put("b", "v2");
    this.cn.put("c", "v3");

    Map<String, Object> config = this.cn.getAll();

    Assert.assertEquals(config.size(), 3);
    Assert.assertEquals(config.get("a"), "v1");
    Assert.assertEquals(config.get("b"), "v2");
    Assert.assertEquals(config.get("c"), "v3");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testNotFound() {
    this.cn.get("a");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testDelete() {
    this.cn.put("a", "v1");
    this.cn.delete("a");
    this.cn.get("a");
  }

  @Test
  public void testComplexValue() {
    Map<String, List<?>> in = new HashMap<>();
    in.put("a", Arrays.asList(1, 2, 3));
    in.put("b", Arrays.asList("a", "b", "c"));
    in.put("c", Arrays.asList("1", "2", "3"));
    in.put("d", Arrays.asList(true, false, false));

    this.cn.put("key", in);

    Map<String, List<?>> out = this.cn.get("key");
    Assert.assertEquals(out.get("a"), Arrays.asList(1, 2, 3));
    Assert.assertEquals(out.get("b"), Arrays.asList("a", "b", "c"));
    Assert.assertEquals(out.get("c"), Arrays.asList("1", "2", "3"));
    Assert.assertEquals(out.get("d"), Arrays.asList(true, false, false));
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testObjectValueFail() {
    this.cn.put("key", new ObjectValue("a", 1));
  }

  @Test
  public void testNamespace() {
    ConfigManager configDAO = DAORegistry.getInstance().getConfigDAO();
    ConfigNamespace cn1 = new ConfigNamespace("namespace", configDAO);
    ConfigNamespace cn2 = new ConfigNamespace("theOther", configDAO);

    this.cn.put("a", "value");
    cn2.put("a", "other");

    Assert.assertEquals(cn1.get("a"), "value"); // same namespace, different accessor
    Assert.assertEquals(cn2.get("a"), "other"); // different namespace
  }

  @SuppressWarnings("unused")
  @Test(expectedExceptions = ClassCastException.class)
  public void testClassCastFail() {
    this.cn.put("a", 1);
    String ignore = this.cn.get("a");
  }

  public static class ObjectValue {
    final String sval;
    final int ival;

    ObjectValue(String sval, int ival) {
      this.sval = sval;
      this.ival = ival;
    }
  }

}
