package com.linkedin.thirdeye.dashboard.resource;

import com.linkedin.thirdeye.dashboard.resources.AnomalyResource;
import com.linkedin.thirdeye.datalayer.bao.AlertConfigManager;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.DAOTestBase;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.pojo.AlertConfigBean;
import com.linkedin.thirdeye.datasource.DAORegistry;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class AnomalyResourceTest {
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyResourceTest.class);

  private DAOTestBase testDAOProvider;
  private DAORegistry daoRegistry;

  private long func1;
  private long func2;
  private long func3;
  private long func4;

  @BeforeClass
  public void beforeClass() {
    // Prepare database
    testDAOProvider = DAOTestBase.getInstance();
    daoRegistry = DAORegistry.getInstance();

    // Create anomaly functions
    AnomalyFunctionManager anomalyFunctionDAO = daoRegistry.getAnomalyFunctionDAO();
    AnomalyFunctionDTO anomalyFunctionDTO1 = new AnomalyFunctionDTO();
    anomalyFunctionDTO1.setFunctionName("test1");
    anomalyFunctionDAO.save(anomalyFunctionDTO1);
    AnomalyFunctionDTO anomalyFunctionDTO2 = new AnomalyFunctionDTO();
    anomalyFunctionDTO2.setFunctionName("test2");
    anomalyFunctionDAO.save(anomalyFunctionDTO2);
    AnomalyFunctionDTO anomalyFunctionDTO3 = new AnomalyFunctionDTO();
    anomalyFunctionDTO3.setFunctionName("test3");
    anomalyFunctionDAO.save(anomalyFunctionDTO3);
    AnomalyFunctionDTO anomalyFunctionDTO4 = new AnomalyFunctionDTO();
    anomalyFunctionDTO4.setFunctionName("test4");
    anomalyFunctionDAO.save(anomalyFunctionDTO4);

    List<AnomalyFunctionDTO> anomalyFuncList = daoRegistry.getAnomalyFunctionDAO().findAll();
    func1 = anomalyFuncList.get(0).getId();
    func2 = anomalyFuncList.get(1).getId();
    func3 = anomalyFuncList.get(2).getId();
    func4 = anomalyFuncList.get(3).getId();

    LOG.info("func1: " + func1 + " func2: " + func2 + " func3: " + func3 + " func4: " + func4);

    // Create alert groups
    AlertConfigManager alertDAO = daoRegistry.getAlertConfigDAO();
    AlertConfigDTO alertConfigDTO1 = new AlertConfigDTO();
    alertConfigDTO1.setName("test1");
    alertConfigDTO1.setApplication("test1");
    AlertConfigBean.EmailConfig emailConfig1 = new AlertConfigBean.EmailConfig();
    emailConfig1.setFunctionIds(Arrays.asList(func1, func2));
    alertConfigDTO1.setEmailConfig(emailConfig1);
    alertDAO.save(alertConfigDTO1);
    AlertConfigDTO alertConfigDTO2 = new AlertConfigDTO();
    alertConfigDTO2.setName("test2");
    alertConfigDTO2.setApplication("test2");
    AlertConfigBean.EmailConfig emailConfig2 = new AlertConfigBean.EmailConfig();
    emailConfig2.setFunctionIds(Arrays.asList(func2, func3));
    alertConfigDTO2.setEmailConfig(emailConfig2);
    alertDAO.save(alertConfigDTO2);

    // Create merged anomalies
    MergedAnomalyResultManager mergedAnomalyDAO = daoRegistry.getMergedAnomalyResultDAO();
    MergedAnomalyResultDTO mergedAnomalyResultDTO1 = new MergedAnomalyResultDTO();
    mergedAnomalyResultDTO1.setFunctionId(func2);
    mergedAnomalyResultDTO1.setStartTime(1000);
    mergedAnomalyDAO.save(mergedAnomalyResultDTO1);
    MergedAnomalyResultDTO mergedAnomalyResultDTO2 = new MergedAnomalyResultDTO();
    mergedAnomalyResultDTO2.setFunctionId(func2);
    mergedAnomalyResultDTO1.setStartTime(2000);
    mergedAnomalyDAO.save(mergedAnomalyResultDTO2);
    MergedAnomalyResultDTO mergedAnomalyResultDTO3 = new MergedAnomalyResultDTO();
    mergedAnomalyResultDTO3.setFunctionId(func3);
    mergedAnomalyResultDTO1.setStartTime(2000);
    mergedAnomalyDAO.save(mergedAnomalyResultDTO3);
    MergedAnomalyResultDTO mergedAnomalyResultDTO4 = new MergedAnomalyResultDTO();
    mergedAnomalyResultDTO4.setFunctionId(func4);
    mergedAnomalyResultDTO1.setStartTime(3000);
    mergedAnomalyDAO.save(mergedAnomalyResultDTO4);
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }

  @Test
  public void testDeleteAnomalyFunctions() {
    AnomalyResource anomalyResource = new AnomalyResource(null, null, null);
    anomalyResource.deleteAnomalyFunctions(func2 + "," + func3 + ", 9999");

    // Anomaly functions and the associated anomalies are cleaned up correctly
    Assert.assertEquals(daoRegistry.getAnomalyFunctionDAO().findAll().size(), 2);
    Assert.assertTrue(Arrays.asList(func1, func4).contains(daoRegistry.getAnomalyFunctionDAO().findAll().get(0).getId()));
    Assert.assertTrue(Arrays.asList(func1, func4).contains(daoRegistry.getAnomalyFunctionDAO().findAll().get(1).getId()));
    Assert.assertEquals(daoRegistry.getMergedAnomalyResultDAO().findAll().size(), 1);
    Assert.assertEquals(daoRegistry.getMergedAnomalyResultDAO().findAll().get(0).getFunctionId().longValue(), func4);

    // Subscription alert groups cleaned up correctly
    Assert.assertEquals(daoRegistry.getAlertConfigDAO().findAll().size(), 2);
    Assert.assertEquals(daoRegistry.getAlertConfigDAO().findAll().get(0).getEmailConfig().getFunctionIds().size(), 1);
    Assert.assertEquals(daoRegistry.getAlertConfigDAO().findAll().get(0).getEmailConfig().getFunctionIds().get(0).longValue(), func1);
    Assert.assertEquals(daoRegistry.getAlertConfigDAO().findAll().get(1).getEmailConfig().getFunctionIds().size(), 0);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testDeleteAnomalyFunctionsNullArguments() {
    AnomalyResource anomalyResource = new AnomalyResource(null, null, null);
    anomalyResource.deleteAnomalyFunctions(null);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testDeleteAnomalyFunctionsEmptyArguments() {
    AnomalyResource anomalyResource = new AnomalyResource(null, null, null);
    anomalyResource.deleteAnomalyFunctions("");
  }
}
