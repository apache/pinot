package com.linkedin.thirdeye.db.dao;

import com.linkedin.thirdeye.db.dao.AbstractDbTestBase;
import com.linkedin.thirdeye.db.entity.AnomalyFunctionSpec;
import com.linkedin.thirdeye.db.entity.EmailConfiguration;
import java.util.ArrayList;
import java.util.List;
import org.testng.annotations.Test;
import static org.testng.Assert.*;

public class TestEmailConfigurationDAO extends AbstractDbTestBase {

  Long emailConfigId, functionId;

  @Test
  public void testCreateEmailConfig() {
    EmailConfiguration request = getEmailConfiguration();
    emailConfigId = emailConfigurationDAO.save(request);
    assertNotNull(emailConfigId);
  }

  @Test (dependsOnMethods = {"testCreateEmailConfig"})
  public void testFunctionEmailAssignment() {
    // create function
    AnomalyFunctionSpec functionReq = getTestFunctionSpec("testMetric", "testCollection");
    functionId = anomalyFunctionDAO.save(functionReq);
    assertNotNull(functionId);

    // save function in EmailConfig
    EmailConfiguration emailConfiguration = emailConfigurationDAO.findById(emailConfigId);
    AnomalyFunctionSpec anomalyFunctionSpec = anomalyFunctionDAO.findById(functionId);

    assertEquals(emailConfiguration.getFunctions().size(), 0);

    List<AnomalyFunctionSpec> functionSpecList = new ArrayList<>();
    functionSpecList.add(anomalyFunctionSpec);
    emailConfiguration.setFunctions(functionSpecList);
    emailConfigurationDAO.save(emailConfiguration);

    // Validate relation in both Email and Function objects
    EmailConfiguration emailConfig1 = emailConfigurationDAO.findById(emailConfigId);

    assertEquals(emailConfig1.getFunctions().size(), 1);
  }

  @Test(dependsOnMethods = { "testFunctionEmailAssignment" })
  public void testFindByFunctionId() {
    List<EmailConfiguration> emailConfigurations =
        emailConfigurationDAO.findByFunctionId(functionId);
    assertEquals(emailConfigurations.size(), 1);
  }

  @Test(dependsOnMethods = { "testFindByFunctionId" })
  public void testDelete() {
    emailConfigurationDAO.deleteById(emailConfigId);
    EmailConfiguration emailConfiguration = emailConfigurationDAO.findById(emailConfigId);
    AnomalyFunctionSpec anomalyFunctionSpec = anomalyFunctionDAO.findById(functionId);

    // email configuration should be deleted and anomaly function should not.
    assertNull(emailConfiguration);
    assertNotNull(anomalyFunctionSpec);

    // now cleanup the anomaly function
    anomalyFunctionDAO.deleteById(anomalyFunctionSpec.getId());
  }
}
