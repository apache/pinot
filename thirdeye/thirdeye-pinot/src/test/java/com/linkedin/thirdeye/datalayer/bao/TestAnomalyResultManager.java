package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.DaoTestUtils;
import com.linkedin.thirdeye.datasource.DAORegistry;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;

public class TestAnomalyResultManager {

  RawAnomalyResultDTO anomalyResult;
  AnomalyFunctionDTO spec = DaoTestUtils.getTestFunctionSpec("metric", "dataset");

  private DAOTestBase testDAOProvider;
  private AnomalyFunctionManager anomalyFunctionDAO;
  private RawAnomalyResultManager rawAnomalyResultDAO;
  @BeforeClass
  void beforeClass() {
    testDAOProvider = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    anomalyFunctionDAO = daoRegistry.getAnomalyFunctionDAO();
    rawAnomalyResultDAO = daoRegistry.getRawAnomalyResultDAO();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }

  @Test
  public void testAnomalyResultCRUD() {
    anomalyFunctionDAO.save(spec);
    Assert.assertNotNull(spec);

    // create anomaly result
    anomalyResult = DaoTestUtils.getAnomalyResult();

    anomalyResult.setFunction(spec);
    rawAnomalyResultDAO.save(anomalyResult);

    RawAnomalyResultDTO resultRet = rawAnomalyResultDAO.findById(anomalyResult.getId());
    Assert.assertEquals(resultRet.getFunction(), spec);
  }

  @Test(dependsOnMethods = {"testAnomalyResultCRUD"})
  public void testResultFeedback() {
    RawAnomalyResultDTO result = rawAnomalyResultDAO.findById(anomalyResult.getId());
    Assert.assertNotNull(result);
    Assert.assertNull(result.getFeedback());

    AnomalyFeedbackDTO feedback = new AnomalyFeedbackDTO();
    feedback.setComment("this is a good find");
    feedback.setFeedbackType(AnomalyFeedbackType.ANOMALY);
    result.setFeedback(feedback);
    rawAnomalyResultDAO.save(result);

    RawAnomalyResultDTO resultRet = rawAnomalyResultDAO.findById(anomalyResult.getId());
    Assert.assertEquals(resultRet.getId(), result.getId());
    Assert.assertNotNull(resultRet.getFeedback());

    AnomalyFunctionDTO functionSpec = result.getFunction();

    rawAnomalyResultDAO.deleteById(anomalyResult.getId());
    anomalyFunctionDAO.deleteById(functionSpec.getId());
  }

}
