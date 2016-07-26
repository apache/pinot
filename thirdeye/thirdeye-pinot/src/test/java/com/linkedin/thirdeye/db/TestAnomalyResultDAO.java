package com.linkedin.thirdeye.db;

import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.constant.FeedbackStatus;
import com.linkedin.thirdeye.db.entity.AnomalyFeedback;
import com.linkedin.thirdeye.db.entity.AnomalyFunctionSpec;
import com.linkedin.thirdeye.db.entity.AnomalyResult;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestAnomalyResultDAO extends AbstractDbTestBase {

  Long anomalyResultId;

  @Test
  public void testAnomalyResultCRUD() {
    AnomalyFunctionSpec spec = TestAnomalyFunctionDAO.getTestFunctionSpec();
    anomalyFunctionDAO.save(spec);
    Assert.assertNotNull(spec);

    // create anomaly result
    AnomalyResult result = getAnomalyResult();
    anomalyResultDAO.save(result);

    // Now set the function and update
    result.setFunction(spec);
    anomalyResultDAO.update(result);

    AnomalyFunctionSpec specRet = anomalyFunctionDAO.findById(spec.getId());
    Assert.assertEquals(specRet.getAnomalies().size(), 1);

    anomalyResultId = result.getId();
  }

  @Test(dependsOnMethods = {"testAnomalyResultCRUD"})
  public void testResultFeedback() {
    AnomalyResult result = anomalyResultDAO.findById(anomalyResultId);
    Assert.assertNotNull(result);
    Assert.assertNull(result.getFeedback());

    AnomalyFeedback feedback = new AnomalyFeedback();
    feedback.setComment("this is a good find");
    feedback.setFeedbackType(AnomalyFeedbackType.ANOMALY);
    feedback.setStatus(FeedbackStatus.NEW);
    result.setFeedback(feedback);
    anomalyResultDAO.update(result);

    AnomalyResult resultRet = anomalyResultDAO.findById(anomalyResultId);
    Assert.assertEquals(resultRet.getId(), result.getId());
    Assert.assertNotNull(resultRet.getFeedback());

    anomalyResultDAO.deleteById(anomalyResultId);
  }

  static AnomalyResult getAnomalyResult() {
    AnomalyResult anomalyResult = new AnomalyResult();
    anomalyResult.setScore(1.1);
    anomalyResult.setStartTimeUtc(System.currentTimeMillis());
    anomalyResult.setEndTimeUtc(System.currentTimeMillis());
    anomalyResult.setWeight(10.1);
    anomalyResult.setDimensions("xyz dimension");
    anomalyResult.setCreationTimeUtc(System.currentTimeMillis());
    return anomalyResult;
  }
}
