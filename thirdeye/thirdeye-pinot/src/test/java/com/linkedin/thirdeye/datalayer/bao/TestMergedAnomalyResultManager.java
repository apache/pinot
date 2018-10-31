package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.anomalydetection.context.AnomalyResult;
import com.linkedin.thirdeye.datalayer.DaoTestUtils;
import com.linkedin.thirdeye.datasource.DAORegistry;
import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.anomaly.merge.AnomalyMergeConfig;
import com.linkedin.thirdeye.anomaly.merge.AnomalyTimeBasedSummarizer;
import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;

public class TestMergedAnomalyResultManager{
  private MergedAnomalyResultDTO mergedResult = null;
  private AnomalyFunctionDTO function = DaoTestUtils.getTestFunctionSpec("metric", "dataset");

  private DAOTestBase testDAOProvider;
  private AnomalyFunctionManager anomalyFunctionDAO;
  private MergedAnomalyResultManager mergedAnomalyResultDAO;
  @BeforeClass
  void beforeClass() {
    testDAOProvider = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    anomalyFunctionDAO = daoRegistry.getAnomalyFunctionDAO();
    mergedAnomalyResultDAO = daoRegistry.getMergedAnomalyResultDAO();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }

  @Test
  public void testMergedResultCRUD() {
    long functionId = anomalyFunctionDAO.save(function);
    Assert.assertNotNull(function.getId());

    // create anomaly result
    AnomalyResult rawAnomaly = DaoTestUtils.getAnomalyResult();

    // Let's create merged result
    List<AnomalyResult> rawResults = new ArrayList<>();
    rawResults.add(rawAnomaly);

    AnomalyMergeConfig mergeConfig = new AnomalyMergeConfig();

    List<MergedAnomalyResultDTO> mergedResults = AnomalyTimeBasedSummarizer.mergeAnomalies(rawResults, mergeConfig);
    Assert.assertEquals(mergedResults.get(0).getStartTime(), rawAnomaly.getStartTime());
    Assert.assertEquals(mergedResults.get(0).getEndTime(), rawAnomaly.getEndTime());

    // Let's persist the merged result
    AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(functionId);
    mergedResults.get(0).setFunction(anomalyFunction);
    mergedResults.get(0).setCollection(anomalyFunction.getCollection());
    mergedResults.get(0).setMetric(anomalyFunction.getTopicMetric());

    mergedAnomalyResultDAO.save(mergedResults.get(0));
    mergedResult = mergedResults.get(0);
    Assert.assertNotNull(mergedResult.getId());

    // verify the merged result
    MergedAnomalyResultDTO mergedResultById = mergedAnomalyResultDAO.findById(mergedResult.getId());
    Assert.assertEquals(mergedResultById.getDimensions(), rawAnomaly.getDimensions());

    List<MergedAnomalyResultDTO> mergedResultsByMetricDimensionsTime = mergedAnomalyResultDAO
        .findByCollectionMetricDimensionsTime(mergedResult.getCollection(), mergedResult.getMetric(),
            mergedResult.getDimensions().toString(), 0, System.currentTimeMillis());

    Assert.assertEquals(mergedResultsByMetricDimensionsTime.get(0), mergedResult);
  }

  @Test(dependsOnMethods = {"testMergedResultCRUD"})
  public void testFeedback() {
    MergedAnomalyResultDTO anomalyMergedResult = mergedAnomalyResultDAO.findById(mergedResult.getId());
    AnomalyFeedbackDTO feedback = new AnomalyFeedbackDTO();
    feedback.setComment("this is a good find");
    feedback.setFeedbackType(AnomalyFeedbackType.ANOMALY);
    anomalyMergedResult.setFeedback(feedback);
    // now we need to make explicit call to anomaly update in order to update the feedback
    mergedAnomalyResultDAO.updateAnomalyFeedback(anomalyMergedResult);

    //verify feedback
    MergedAnomalyResultDTO mergedResult1 = mergedAnomalyResultDAO.findById(mergedResult.getId());
    Assert.assertEquals(mergedResult1.getFeedback().getFeedbackType(), AnomalyFeedbackType.ANOMALY);
  }
}
