package com.linkedin.thirdeye.datalayer.bao;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.datalayer.dto.IngraphDashboardConfigDTO;

public class TestIngraphDatasetConfigManager extends AbstractManagerTestBase {

  private Long ingraphDashboardConfigId1 = null;
  private Long ingraphDashboardConfigId2 = null;
  private static String name1 = "dashboard1";
  private static String name2 = "dashboard2";

  @BeforeClass
  void beforeClass() {
    super.init();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    super.cleanup();
  }

  @Test
  public void testCreateIngraphDashboard() {

    IngraphDashboardConfigDTO ingraphDashboardConfigDTO1 = getTestIngraphDashboardConfig(name1);
    ingraphDashboardConfigId1 = ingraphDashboardConfigDAO.save(ingraphDashboardConfigDTO1);
    Assert.assertNotNull(ingraphDashboardConfigId1);

    IngraphDashboardConfigDTO ingraphDashboardConfigDTO2 = getTestIngraphDashboardConfig(name2);
    ingraphDashboardConfigId2 = ingraphDashboardConfigDAO.save(ingraphDashboardConfigDTO2);
    Assert.assertNotNull(ingraphDashboardConfigId2);

  }

  @Test(dependsOnMethods = {"testCreateIngraphDashboard"})
  public void testFindIngraphDashboard() {

    List<IngraphDashboardConfigDTO> ingraphDashboardConfigDTOs = ingraphDashboardConfigDAO.findAll();
    Assert.assertEquals(ingraphDashboardConfigDTOs.size(), 2);

    IngraphDashboardConfigDTO ingraphDashboardConfigDTO = ingraphDashboardConfigDAO.findByName(name1);
    Assert.assertEquals(ingraphDashboardConfigDTO.getName(), name1);

  }

  @Test(dependsOnMethods = { "testFindIngraphDashboard" })
  public void testUpdateIngraphDashboard() {
    IngraphDashboardConfigDTO ingraphDashboardConfigDTO = ingraphDashboardConfigDAO.findById(ingraphDashboardConfigId1);
    Assert.assertNotNull(ingraphDashboardConfigDTO);
    Assert.assertTrue(ingraphDashboardConfigDTO.isBootstrap());
    ingraphDashboardConfigDTO.setBootstrap(false);
    ingraphDashboardConfigDAO.update(ingraphDashboardConfigDTO);
    ingraphDashboardConfigDTO = ingraphDashboardConfigDAO.findById(ingraphDashboardConfigId1);
    Assert.assertNotNull(ingraphDashboardConfigDTO);
    Assert.assertFalse(ingraphDashboardConfigDTO.isBootstrap());

  }

  @Test(dependsOnMethods = { "testUpdateIngraphDashboard" })
  public void testDeleteIngraphDashboard() {
    ingraphDashboardConfigDAO.deleteById(ingraphDashboardConfigId1);
    IngraphDashboardConfigDTO ingraphDashboardConfigDTO = ingraphDashboardConfigDAO.findById(ingraphDashboardConfigId1);
    Assert.assertNull(ingraphDashboardConfigDTO);
  }
}
