package com.linkedin.thirdeye.datalayer.bao;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.datalayer.dto.AutometricsConfigDTO;

public class TestAutometricsConfigManager extends AbstractManagerTestBase {

  private Long autometricsConfigId1 = null;
  private Long autometricsConfigId2 = null;
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
  public void testCreateAutometrics() {

    AutometricsConfigDTO autometricsConfigDTO1 = getTestAutometricsConfig(rrd1, metric1, dashboard1);
    autometricsConfigId1 = autometricsConfigDAO.save(autometricsConfigDTO1);
    Assert.assertNotNull(autometricsConfigId1);

    AutometricsConfigDTO autometricsConfigDTO2 = getTestAutometricsConfig(rrd2, metric2, dashboard2);
    autometricsConfigId2 = autometricsConfigDAO.save(autometricsConfigDTO2);
    Assert.assertNotNull(autometricsConfigId2);

  }

  @Test(dependsOnMethods = {"testCreateAutometrics"})
  public void testFindAutometrics() {

    List<AutometricsConfigDTO> autometricsConfigDTOs = autometricsConfigDAO.findAll();
    Assert.assertEquals(autometricsConfigDTOs.size(), 2);

    autometricsConfigDTOs = autometricsConfigDAO.findByDashboard(dashboard1);
    Assert.assertEquals(autometricsConfigDTOs.size(), 1);

    AutometricsConfigDTO autometricsConfigDTO = autometricsConfigDAO.
        findByDashboardAndMetric(dashboard1, metric1);
    Assert.assertEquals(autometricsConfigDTO.getDashboard(), dashboard1);
    Assert.assertEquals(autometricsConfigDTO.getMetric(), metric1);
    Assert.assertEquals(autometricsConfigDTO.getRrd(), rrd1);

    autometricsConfigDTO = autometricsConfigDAO.findByRrd(rrd1);
    Assert.assertEquals(autometricsConfigDTO.getDashboard(), dashboard1);
    Assert.assertEquals(autometricsConfigDTO.getMetric(), metric1);
    Assert.assertEquals(autometricsConfigDTO.getRrd(), rrd1);

  }

  @Test(dependsOnMethods = { "testFindAutometrics" })
  public void testUpdateAutometrics() {
    AutometricsConfigDTO autometricsConfigDTO = autometricsConfigDAO.findById(autometricsConfigId1);
    Assert.assertNotNull(autometricsConfigDTO);
    Assert.assertEquals(autometricsConfigDTO.getDashboard(), dashboard1);
    autometricsConfigDTO.setDashboard(dashboard2);
    autometricsConfigDAO.update(autometricsConfigDTO);
    autometricsConfigDTO = autometricsConfigDAO.findById(autometricsConfigId1);
    Assert.assertNotNull(autometricsConfigDTO);
    Assert.assertEquals(autometricsConfigDTO.getDashboard(), dashboard2);

    List<AutometricsConfigDTO> autometricsConfigDTOs = autometricsConfigDAO.findByDashboard(dashboard2);
    Assert.assertEquals(autometricsConfigDTOs.size(), 2);
  }

  @Test(dependsOnMethods = { "testUpdateAutometrics" })
  public void testDeleteAutometrics() {
    autometricsConfigDAO.deleteById(autometricsConfigId1);
    AutometricsConfigDTO autometricsConfigDTO = autometricsConfigDAO.findById(autometricsConfigId1);
    Assert.assertNull(autometricsConfigDTO);
  }
}
