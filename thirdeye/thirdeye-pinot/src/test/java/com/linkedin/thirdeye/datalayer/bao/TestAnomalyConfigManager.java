package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestAnomalyConfigManager extends AbstractManagerTestBase {

  Long alertConfigid;

  @Test
  public void testCreateAlertConfig() {
    AlertConfigDTO request = new AlertConfigDTO();
    request.setActive(true);
    request.setName("my alert config");
    alertConfigid = alertConfigManager.save(request);
    Assert.assertTrue(alertConfigid > 0);

  }

  @Test (dependsOnMethods = {"testCreateAlertConfig"})
  public void testFetchAlertConfig() {
    // find by id
    AlertConfigDTO response = alertConfigManager.findById(alertConfigid);
    Assert.assertNotNull(response);
    Assert.assertEquals(response.getId(), alertConfigid);
    Assert.assertEquals(alertConfigManager.findAll().size(), 1);
  }

  @Test (dependsOnMethods = {"testFetchAlertConfig"})
  public void testDeleteAlertConfig() {
    alertConfigManager.deleteById(alertConfigid);
    Assert.assertEquals(alertConfigManager.findAll().size(), 0);
  }

  // TODO: add tests for Email Config

  // TODO: add tests for Report Config
}
