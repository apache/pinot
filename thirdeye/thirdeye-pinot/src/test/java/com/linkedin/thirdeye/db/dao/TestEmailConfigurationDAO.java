package com.linkedin.thirdeye.db.dao;

import java.util.ArrayList;
import java.util.List;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.EmailConfigurationDTO;

import static org.testng.Assert.*;

public class TestEmailConfigurationDAO extends AbstractDbTestBase {

  Long emailConfigId, functionId;

  @Test
  public void testCreateEmailConfig() {
    EmailConfigurationDTO request = getEmailConfiguration();
    emailConfigId = emailConfigurationDAO.save(request);
    assertNotNull(emailConfigId);
  }

  @Test (dependsOnMethods = {"testCreateEmailConfig"})
  public void testFunctionEmailAssignment() {
    // create function
    AnomalyFunctionDTO functionReq = getTestFunctionSpec("testMetric", "testCollection");
    functionId = anomalyFunctionDAO.save(functionReq);
    assertNotNull(functionId);

    // save function in EmailConfig
    EmailConfigurationDTO emailConfiguration = emailConfigurationDAO.findById(emailConfigId);
    AnomalyFunctionDTO anomalyFunctionSpec = anomalyFunctionDAO.findById(functionId);

    assertEquals(emailConfiguration.getFunctions().size(), 0);

    List<AnomalyFunctionDTO> functionSpecList = new ArrayList<>();
    functionSpecList.add(anomalyFunctionSpec);
    emailConfiguration.setFunctions(functionSpecList);
    emailConfigurationDAO.save(emailConfiguration);

    // Validate relation in both Email and Function objects
    EmailConfigurationDTO emailConfig1 = emailConfigurationDAO.findById(emailConfigId);

    assertEquals(emailConfig1.getFunctions().size(), 1);
  }

  @Test(dependsOnMethods = { "testFunctionEmailAssignment" })
  public void testFindByFunctionId() {
    List<EmailConfigurationDTO> emailConfigurations =
        emailConfigurationDAO.findByFunctionId(functionId);
    assertEquals(emailConfigurations.size(), 1);
  }

  @Test(dependsOnMethods = { "testFindByFunctionId" })
  public void testDelete() {
    emailConfigurationDAO.deleteById(emailConfigId);
    EmailConfigurationDTO emailConfiguration = emailConfigurationDAO.findById(emailConfigId);
    AnomalyFunctionDTO anomalyFunctionSpec = anomalyFunctionDAO.findById(functionId);

    // email configuration should be deleted and anomaly function should not.
    assertNull(emailConfiguration);
    assertNotNull(anomalyFunctionSpec);

    // now cleanup the anomaly function
    anomalyFunctionDAO.deleteById(anomalyFunctionSpec.getId());
  }
}
