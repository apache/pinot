package org.apache.pinot.thirdeye.dashboard.resource;

import org.apache.pinot.thirdeye.dashboard.resources.AnomalyResource;
import org.apache.pinot.thirdeye.datalayer.bao.AlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.AnomalyFunctionManager;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.dto.AlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.AlertConfigBean;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
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

}
