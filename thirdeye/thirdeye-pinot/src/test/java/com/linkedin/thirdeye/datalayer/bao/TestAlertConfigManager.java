package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.alert.commons.AnomalyFeedConfig;
import com.linkedin.thirdeye.datalayer.DaoTestUtils;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestAlertConfigManager{

  Long alertConfigid;

  private DAOTestBase testDAOProvider;
  private AlertConfigManager alertConfigDAO;
  @BeforeClass
  void beforeClass() {
    testDAOProvider = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    alertConfigDAO = daoRegistry.getAlertConfigDAO();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }

  @Test
  public void testCreateAlertConfig() {
    AlertConfigDTO request = new AlertConfigDTO();
    request.setActive(true);
    request.setName("my alert config");
    alertConfigid = alertConfigDAO.save(request);
    Assert.assertTrue(alertConfigid > 0);
  }

  @Test
  public void testFindNameEquals() {
    AlertConfigDTO sample = alertConfigDAO.findAll().get(0);
    Assert.assertNotNull(alertConfigDAO.findWhereNameEquals(sample.getName()));
  }

  @Test (dependsOnMethods = {"testDeleteAlertConfig"})
  public void testCreateAlertConfigWithAnomalyFeedConfig() {
    AlertConfigDTO dto = new AlertConfigDTO();
    dto.setActive(true);
    dto.setName("my alert config");
    dto.setAnomalyFeedConfig(DaoTestUtils.getTestAnomalyFeedConfig());
    long dtoId = alertConfigDAO.save(dto);
    AlertConfigDTO newDto = alertConfigDAO.findById(dtoId);
    AnomalyFeedConfig feedConfig = dto.getAnomalyFeedConfig();
    AnomalyFeedConfig newFeedConfig = newDto.getAnomalyFeedConfig();

    Assert.assertEquals(newFeedConfig.getAnomalyFetcherConfigs().size(), feedConfig.getAnomalyFetcherConfigs().size());
    Assert.assertEquals(newFeedConfig.getAlertFilterConfigs(), feedConfig.getAlertFilterConfigs());
    Assert.assertEquals(newFeedConfig.getAnomalySource(), feedConfig.getAnomalySource());
  }

  @Test (dependsOnMethods = {"testCreateAlertConfig"})
  public void testFetchAlertConfig() {
    // find by id
    AlertConfigDTO response = alertConfigDAO.findById(alertConfigid);
    Assert.assertNotNull(response);
    Assert.assertEquals(response.getId(), alertConfigid);
    Assert.assertEquals(alertConfigDAO.findAll().size(), 1);
  }

  @Test (dependsOnMethods = {"testFetchAlertConfig"})
  public void testDeleteAlertConfig() {
    alertConfigDAO.deleteById(alertConfigid);
    Assert.assertEquals(alertConfigDAO.findAll().size(), 0);
  }
}
