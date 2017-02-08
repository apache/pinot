package com.linkedin.thirdeye.datalayer.bao;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;

public class TestDatasetConfigManager extends AbstractManagerTestBase {

  private Long datasetConfigId1;
  private Long datasetConfigId2;
  private static String collection1 = "my dataset1";
  private static String collection2 = "my dataset2";

  @BeforeClass
  void beforeClass() {
    super.init();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    super.cleanup();
  }

  @Test
  public void testCreate() {

    datasetConfigId1 = datasetConfigDAO.save(getTestDatasetConfig(collection1));
    Assert.assertNotNull(datasetConfigId1);

    DatasetConfigDTO datasetConfig2 = getTestDatasetConfig(collection2);
    datasetConfig2.setActive(false);
    datasetConfigId2 = datasetConfigDAO.save(datasetConfig2);
    Assert.assertNotNull(datasetConfigId2);

    List<DatasetConfigDTO> datasetConfigs = datasetConfigDAO.findAll();
    Assert.assertEquals(datasetConfigs.size(), 2);

    datasetConfigs = datasetConfigDAO.findActive();
    Assert.assertEquals(datasetConfigs.size(), 1);
  }

  @Test(dependsOnMethods = {"testCreate"})
  public void testFindByDataset() {
    DatasetConfigDTO datasetConfigs = datasetConfigDAO.findByDataset(collection1);
    Assert.assertEquals(datasetConfigs.getDataset(), collection1);
  }

  @Test(dependsOnMethods = { "testFindByDataset" })
  public void testUpdate() {
    DatasetConfigDTO datasetConfig = datasetConfigDAO.findById(datasetConfigId1);
    Assert.assertNotNull(datasetConfig);
    Assert.assertFalse(datasetConfig.isMetricAsDimension());
    datasetConfig.setMetricAsDimension(true);
    datasetConfigDAO.update(datasetConfig);
    datasetConfig = datasetConfigDAO.findById(datasetConfigId1);
    Assert.assertNotNull(datasetConfig);
    Assert.assertTrue(datasetConfig.isMetricAsDimension());
  }

  @Test(dependsOnMethods = { "testUpdate" })
  public void testDelete() {
    datasetConfigDAO.deleteById(datasetConfigId2);
    DatasetConfigDTO datasetConfig = datasetConfigDAO.findById(datasetConfigId2);
    Assert.assertNull(datasetConfig);
  }
}
