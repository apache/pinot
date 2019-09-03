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

package org.apache.pinot.thirdeye.datalayer.bao;

import org.apache.pinot.thirdeye.datalayer.DaoTestUtils;
import org.apache.pinot.thirdeye.datalayer.dto.ConfigDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestConfigManager {

  private DAOTestBase testDAOProvider;
  private ConfigManager configDAO;
  @BeforeMethod
  void beforeMethod() {
    testDAOProvider = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    configDAO = daoRegistry.getConfigDAO();
  }

  @AfterMethod(alwaysRun = true)
  void afterMethod() {
    testDAOProvider.cleanup();
  }

  @Test
  public void testCreateConfig() {
    ConfigDTO config = DaoTestUtils.getTestConfig("a", "b", "c");
    Assert.assertNotNull(this.configDAO.save(config));
  }

  @Test
  public void testOverrideConfigFail() throws Exception {
    ConfigDTO first = DaoTestUtils.getTestConfig("a", "b", "c");
    this.configDAO.save(first);

    ConfigDTO second = DaoTestUtils.getTestConfig("a", "b", "OTHER");
    Assert.assertNull(this.configDAO.save(second));
  }

  @Test
  public void testDeleteConfig() {
    ConfigDTO config = DaoTestUtils.getTestConfig("a", "b", "c");
    Assert.assertNotNull(this.configDAO.save(config));

    this.configDAO.deleteByNamespaceName("a", "b");

    Assert.assertNull(this.configDAO.findByNamespaceName("a", "b"));
  }

  @Test
  public void testOverrideWithDelete() {
    ConfigDTO config = DaoTestUtils.getTestConfig("a", "b", "c");
    Assert.assertNotNull(this.configDAO.save(config));

    this.configDAO.deleteByNamespaceName("a", "b");

    ConfigDTO config2 = DaoTestUtils.getTestConfig("a", "b", "xyz");
    Assert.assertNotNull(this.configDAO.save(config2));

    ConfigDTO out = this.configDAO.findByNamespaceName("a", "b");
    Assert.assertEquals(config2, out);
  }

  @Test
  public void testGetConfig() {
    ConfigDTO in = DaoTestUtils.getTestConfig("a", "b", "c");
    Assert.assertNotNull(this.configDAO.save(in));

    ConfigDTO out = this.configDAO.findByNamespaceName("a", "b");
    Assert.assertEquals(in, out);
  }

  @Test
  public void testNamespace() {
    this.configDAO.save(DaoTestUtils.getTestConfig("a", "a", "v1"));
    this.configDAO.save(DaoTestUtils.getTestConfig("a", "b", "v2"));
    this.configDAO.save(DaoTestUtils.getTestConfig("b", "a", "v3"));
    this.configDAO.save(DaoTestUtils.getTestConfig("", "a", "v4"));

    Assert.assertEquals(this.configDAO.findByNamespace("a").size(), 2);
    Assert.assertEquals(this.configDAO.findByNamespace("b").size(), 1);
  }
}
