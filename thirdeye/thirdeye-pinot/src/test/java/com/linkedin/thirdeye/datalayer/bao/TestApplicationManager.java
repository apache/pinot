package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.dto.ApplicationDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestApplicationManager {

  Long applicationId;

  private DAOTestBase testDAOProvider;
  private ApplicationManager applicationDAO;
  @BeforeClass
  void beforeClass() {
    testDAOProvider = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    applicationDAO = daoRegistry.getApplicationDAO();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }

  @Test
  public void testCreateApplication() {
    ApplicationDTO request = new ApplicationDTO();
    request.setApplication("MY_APP");
    request.setRecipients("abc@abc.in");
    applicationId = applicationDAO.save(request);
    Assert.assertTrue(applicationId > 0);
  }

  @Test(dependsOnMethods = { "testCreateApplication" })
  public void testFetchApplication() {
    // find by id
    ApplicationDTO response = applicationDAO.findById(applicationId);
    Assert.assertNotNull(response);
    Assert.assertEquals(response.getId(), applicationId);
    Assert.assertEquals(applicationDAO.findAll().size(), 1);
  }

  @Test(dependsOnMethods = { "testFetchApplication" })
  public void testDeleteApplication() {
    applicationDAO.deleteById(applicationId);
    Assert.assertEquals(applicationDAO.findAll().size(), 0);
  }
}
