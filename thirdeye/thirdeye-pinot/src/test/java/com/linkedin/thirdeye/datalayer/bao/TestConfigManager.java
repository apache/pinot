package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.DaoProvider;
import com.linkedin.thirdeye.datalayer.DaoTestUtils;
import com.linkedin.thirdeye.datalayer.dto.ConfigDTO;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestConfigManager {

  private DaoProvider DAO_REGISTRY;
  private ConfigManager configDAO;
  @BeforeClass
  void beforeClass() {
    DAO_REGISTRY = DAOTestBase.getInstance();
    configDAO = DAO_REGISTRY.getConfigDAO();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    DAO_REGISTRY.restart();
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
    cleanup();
    ConfigDTO config = DaoTestUtils.getTestConfig("a", "b", "c");
    Assert.assertNotNull(this.configDAO.save(config));

    this.configDAO.deleteByNamespaceName("a", "b");

    Assert.assertNull(this.configDAO.findByNamespaceName("a", "b"));
  }

  @Test
  public void testOverrideWithDelete() {
    cleanup();
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
    cleanup();
    this.configDAO.save(DaoTestUtils.getTestConfig("a", "a", "v1"));
    this.configDAO.save(DaoTestUtils.getTestConfig("a", "b", "v2"));
    this.configDAO.save(DaoTestUtils.getTestConfig("b", "a", "v3"));
    this.configDAO.save(DaoTestUtils.getTestConfig("", "a", "v4"));

    Assert.assertEquals(this.configDAO.findByNamespace("a").size(), 2);
    Assert.assertEquals(this.configDAO.findByNamespace("b").size(), 1);
  }

  private void cleanup(){
    List<ConfigDTO> configs = configDAO.findAll();
    for (ConfigDTO config : configs) {
      configDAO.delete(config);
    }
  }
}
