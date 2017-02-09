package com.linkedin.thirdeye.datalayer.bao;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.datalayer.dto.IngraphMetricConfigDTO;

public class TestIngraphMetricConfigManager extends AbstractManagerTestBase {

  private Long ingraphMetricConfigId1 = null;
  private Long ingraphMetricConfigId2 = null;
  private static String rrd1 = "rrd1";
  private static String metric1 = "metric1";
  private static String rrd2 = "rrd2";
  private static String metric2 = "metric2";
  private static String dashboard1 = "dashboard1";
  private static String dashboard2 = "dashboard2";

  @BeforeClass
  void beforeClass() {
    super.init();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    super.cleanup();
  }

  @Test
  public void testCreateIngraphMetric() {

    IngraphMetricConfigDTO ingraphMetricConfigDTO1 = getTestIngraphMetricConfig(rrd1, metric1, dashboard1);
    ingraphMetricConfigId1 = ingraphMetricConfigDAO.save(ingraphMetricConfigDTO1);
    Assert.assertNotNull(ingraphMetricConfigId1);

    IngraphMetricConfigDTO ingraphMetricConfigDTO2 = getTestIngraphMetricConfig(rrd2, metric2, dashboard2);
    ingraphMetricConfigId2 = ingraphMetricConfigDAO.save(ingraphMetricConfigDTO2);
    Assert.assertNotNull(ingraphMetricConfigId2);

  }

  @Test(dependsOnMethods = {"testCreateIngraphMetric"})
  public void testFindIngraphMetric() {

    List<IngraphMetricConfigDTO> ingraphMetricConfigDTOs = ingraphMetricConfigDAO.findAll();
    Assert.assertEquals(ingraphMetricConfigDTOs.size(), 2);

    ingraphMetricConfigDTOs = ingraphMetricConfigDAO.findByDashboard(dashboard1);
    Assert.assertEquals(ingraphMetricConfigDTOs.size(), 1);

    IngraphMetricConfigDTO ingraphMetricConfigDTO = ingraphMetricConfigDAO.
        findByDashboardAndMetricName(dashboard1, metric1);
    Assert.assertEquals(ingraphMetricConfigDTO.getDashboardName(), dashboard1);
    Assert.assertEquals(ingraphMetricConfigDTO.getMetricName(), metric1);
    Assert.assertEquals(ingraphMetricConfigDTO.getRrdName(), rrd1);

    ingraphMetricConfigDTO = ingraphMetricConfigDAO.findByRrdName(rrd1);
    Assert.assertEquals(ingraphMetricConfigDTO.getDashboardName(), dashboard1);
    Assert.assertEquals(ingraphMetricConfigDTO.getMetricName(), metric1);
    Assert.assertEquals(ingraphMetricConfigDTO.getRrdName(), rrd1);

  }

  @Test(dependsOnMethods = { "testFindIngraphMetric" })
  public void testUpdateIngraphMetric() {
    IngraphMetricConfigDTO ingraphMetricConfigDTO = ingraphMetricConfigDAO.findById(ingraphMetricConfigId1);
    Assert.assertNotNull(ingraphMetricConfigDTO);
    Assert.assertEquals(ingraphMetricConfigDTO.getDashboardName(), dashboard1);
    ingraphMetricConfigDTO.setDashboardName(dashboard2);
    ingraphMetricConfigDAO.update(ingraphMetricConfigDTO);
    ingraphMetricConfigDTO = ingraphMetricConfigDAO.findById(ingraphMetricConfigId1);
    Assert.assertNotNull(ingraphMetricConfigDTO);
    Assert.assertEquals(ingraphMetricConfigDTO.getDashboardName(), dashboard2);

    List<IngraphMetricConfigDTO> ingraphMetricConfigDTOs = ingraphMetricConfigDAO.findByDashboard(dashboard2);
    Assert.assertEquals(ingraphMetricConfigDTOs.size(), 2);
  }

  @Test(dependsOnMethods = { "testUpdateIngraphMetric" })
  public void testDeleteIngraphMetric() {
    ingraphMetricConfigDAO.deleteById(ingraphMetricConfigId1);
    IngraphMetricConfigDTO ingraphMetricConfigDTO = ingraphMetricConfigDAO.findById(ingraphMetricConfigId1);
    Assert.assertNull(ingraphMetricConfigDTO);
  }
}
