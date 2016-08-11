package com.linkedin.thirdeye.db.dao;

import com.linkedin.thirdeye.anomaly.merge.AnomalyMergeConfig;
import com.linkedin.thirdeye.anomaly.merge.AnomalyMergeGenerator;
import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.constant.FeedbackStatus;
import com.linkedin.thirdeye.db.entity.AnomalyFeedback;
import com.linkedin.thirdeye.db.entity.AnomalyFunctionSpec;
import com.linkedin.thirdeye.db.entity.AnomalyMergedResult;
import com.linkedin.thirdeye.db.entity.AnomalyResult;
import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestAnomalyMergedResultDAO extends AbstractDbTestBase {
  AnomalyMergedResult mergedResult = null;
  Long anomalyResultId;
  AnomalyFunctionSpec spec = getTestFunctionSpec("metric", "dataset");

  @Test
  public void testMergedResultCRUD() {
    anomalyFunctionDAO.save(spec);
    Assert.assertNotNull(spec.getId());

    // create anomaly result
    AnomalyResult result = getAnomalyResult();
    result.setFunction(spec);
    anomalyResultDAO.save(result);

    AnomalyResult resultRet = anomalyResultDAO.findById(result.getId());
    Assert.assertEquals(resultRet.getFunction(), spec);

    anomalyResultId = result.getId();

    // Let's create merged result
    List<AnomalyResult> rawResults = new ArrayList<>();
    rawResults.add(result);

    AnomalyMergeConfig mergeConfig = new AnomalyMergeConfig();

    List<AnomalyMergedResult> mergedResults = AnomalyMergeGenerator
        .mergeAnomalies(rawResults, mergeConfig.getMergeDuration(),
            mergeConfig.getSequentialAllowedGap());
    Assert.assertEquals(mergedResults.get(0).getStartTime(),result.getStartTimeUtc());
    Assert.assertEquals(mergedResults.get(0).getEndTime(),result.getEndTimeUtc());
    Assert.assertEquals(mergedResults.get(0).getAnomalyResults().get(0), result);

    // Let's persist the merged result
    mergedResultDAO.save(mergedResults.get(0));
    mergedResult = mergedResults.get(0);
    Assert.assertNotNull(mergedResult.getId());

    // verify the merged result
    AnomalyMergedResult mergedResult1 = mergedResultDAO.findById(mergedResult.getId());
    Assert.assertEquals(mergedResult1.getAnomalyResults(), rawResults);
    Assert.assertEquals(mergedResult1.getAnomalyResults().get(0).getId(), anomalyResultId);
  }

  @Test(dependsOnMethods = {"testMergedResultCRUD"})
  public void testFeedback() {
    AnomalyMergedResult anomalyMergedResult = mergedResultDAO.findById(mergedResult.getId());
    AnomalyFeedback feedback = new AnomalyFeedback();
    feedback.setComment("this is a good find");
    feedback.setFeedbackType(AnomalyFeedbackType.ANOMALY);
    feedback.setStatus(FeedbackStatus.NEW);
    anomalyMergedResult.setFeedback(feedback);
    mergedResultDAO.save(anomalyMergedResult);

    //verify feedback
    AnomalyMergedResult mergedResult1 = mergedResultDAO.findById(mergedResult.getId());
    Assert.assertEquals(mergedResult1.getAnomalyResults().get(0).getId(), anomalyResultId);
    Assert.assertEquals(mergedResult1.getFeedback().getFeedbackType(), AnomalyFeedbackType.ANOMALY);
  }

  @Test(dependsOnMethods = {"testMergedResultCRUD"})
  public void testFindByCollectionMetricDimensions() {
    List<AnomalyMergedResult> mergedResults = mergedResultDAO
        .findByCollectionMetricDimensions(mergedResult.getCollection(), mergedResult.getMetric(),
            mergedResult.getDimensions());
    Assert.assertEquals(mergedResults.get(0), mergedResult);
  }


  @Test(dependsOnMethods = {"testMergedResultCRUD"})
  public void testFindLatestByCollectionMetricDimensions() {
    AnomalyMergedResult mergedResult1 = mergedResultDAO
        .findLatestByCollectionMetricDimensions(mergedResult.getCollection(), mergedResult.getMetric(),
            mergedResult.getDimensions());
    Assert.assertEquals(mergedResult1, mergedResult);
  }
}
