package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

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

    DatasetConfigDTO datasetConfig1 = getTestDatasetConfig(collection1);
    datasetConfig1.setRequiresCompletenessCheck(true);
    datasetConfigId1 = datasetConfigDAO.save(datasetConfig1);
    Assert.assertNotNull(datasetConfigId1);

    DatasetConfigDTO datasetConfig2 = getTestDatasetConfig(collection2);
    datasetConfig2.setActive(false);
    datasetConfig2.setRequiresCompletenessCheck(true);
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
    Assert.assertFalse(datasetConfig.isRealtime());
    datasetConfig.setRealtime(true);
    datasetConfigDAO.update(datasetConfig);
    datasetConfig = datasetConfigDAO.findById(datasetConfigId1);
    Assert.assertNotNull(datasetConfig);
    Assert.assertTrue(datasetConfig.isRealtime());
  }

  @Test(dependsOnMethods = { "testUpdate" })
  public void testDelete() {
    datasetConfigDAO.deleteById(datasetConfigId2);
    DatasetConfigDTO datasetConfig = datasetConfigDAO.findById(datasetConfigId2);
    Assert.assertNull(datasetConfig);
  }

  @Test(dependsOnMethods = {"testCreate"})
  public void testActiveRequiresCompletenessCheck() {
    Assert.assertEquals(datasetConfigDAO.findActiveRequiresCompletenessCheck().size(), 1);
  }
}
