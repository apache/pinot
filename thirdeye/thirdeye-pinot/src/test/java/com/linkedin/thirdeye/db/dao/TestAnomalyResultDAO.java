package com.linkedin.thirdeye.db.dao;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.constant.FeedbackStatus;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;

public class TestAnomalyResultDAO extends AbstractDbTestBase {

  RawAnomalyResultDTO anomalyResult;
  AnomalyFunctionDTO spec = getTestFunctionSpec("metric", "dataset");

  @Test
  public void testAnomalyResultCRUD() {
    anomalyFunctionDAO.save(spec);
    Assert.assertNotNull(spec);

    // create anomaly result
    anomalyResult = getAnomalyResult();

    anomalyResult.setFunction(spec);
    anomalyResultDAO.save(anomalyResult);

    RawAnomalyResultDTO resultRet = anomalyResultDAO.findById(anomalyResult.getId());
    Assert.assertEquals(resultRet.getFunction(), spec);
  }

  @Test(dependsOnMethods = {"testAnomalyResultCRUD"})
  public void testResultFeedback() {
    RawAnomalyResultDTO result = anomalyResultDAO.findById(anomalyResult.getId());
    Assert.assertNotNull(result);
    Assert.assertNull(result.getFeedback());

    AnomalyFeedbackDTO feedback = new AnomalyFeedbackDTO();
    feedback.setComment("this is a good find");
    feedback.setFeedbackType(AnomalyFeedbackType.ANOMALY);
    feedback.setStatus(FeedbackStatus.NEW);
    result.setFeedback(feedback);
    anomalyResultDAO.save(result);

    RawAnomalyResultDTO resultRet = anomalyResultDAO.findById(anomalyResult.getId());
    Assert.assertEquals(resultRet.getId(), result.getId());
    Assert.assertNotNull(resultRet.getFeedback());

    AnomalyFunctionDTO functionSpec = result.getFunction();

    anomalyResultDAO.deleteById(anomalyResult.getId());
    anomalyFunctionDAO.deleteById(functionSpec.getId());
  }

}
