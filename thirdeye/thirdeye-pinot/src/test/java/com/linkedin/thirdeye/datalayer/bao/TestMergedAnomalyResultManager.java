package com.linkedin.thirdeye.datalayer.bao;

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
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;

public class TestMergedAnomalyResultManager{
  MergedAnomalyResultDTO mergedResult = null;
  Long anomalyResultId;
  AnomalyFunctionDTO function = DaoTestUtils.getTestFunctionSpec("metric", "dataset");

  private DAOTestBase testDAOProvider;
  private AnomalyFunctionManager anomalyFunctionDAO;
  private RawAnomalyResultManager rawAnomalyResultDAO;
  private MergedAnomalyResultManager mergedAnomalyResultDAO;
  @BeforeClass
  void beforeClass() {
    testDAOProvider = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    anomalyFunctionDAO = daoRegistry.getAnomalyFunctionDAO();
    rawAnomalyResultDAO = daoRegistry.getRawAnomalyResultDAO();
    mergedAnomalyResultDAO = daoRegistry.getMergedAnomalyResultDAO();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }

  @Test
  public void testMergedResultCRUD() {
    anomalyFunctionDAO.save(function);
    Assert.assertNotNull(function.getId());

    // create anomaly result
    RawAnomalyResultDTO result = DaoTestUtils.getAnomalyResult();
    result.setFunction(function);
    rawAnomalyResultDAO.save(result);

    RawAnomalyResultDTO resultRet = rawAnomalyResultDAO.findById(result.getId());
    Assert.assertEquals(resultRet.getFunction(), function);

    anomalyResultId = result.getId();

    // Let's create merged result
    List<RawAnomalyResultDTO> rawResults = new ArrayList<>();
    rawResults.add(result);

    AnomalyMergeConfig mergeConfig = new AnomalyMergeConfig();

    List<MergedAnomalyResultDTO> mergedResults = AnomalyTimeBasedSummarizer
        .mergeAnomalies(rawResults, mergeConfig);
    Assert.assertEquals(mergedResults.get(0).getStartTime(), (long) result.getStartTime());
    Assert.assertEquals(mergedResults.get(0).getEndTime(), (long) result.getEndTime());
    Assert.assertEquals(mergedResults.get(0).getAnomalyResults().get(0), result);

    // Let's persist the merged result
    mergedResults.get(0).setDimensions(result.getDimensions());

    mergedAnomalyResultDAO.save(mergedResults.get(0));
    mergedResult = mergedResults.get(0);
    Assert.assertNotNull(mergedResult.getId());

    // verify the merged result
    MergedAnomalyResultDTO mergedResultById = mergedAnomalyResultDAO.findById(mergedResult.getId());
    Assert.assertEquals(mergedResultById.getAnomalyResults(), rawResults);
    Assert.assertEquals(mergedResultById.getAnomalyResults().get(0).getId(), anomalyResultId);
    Assert.assertEquals(mergedResultById.getDimensions(), result.getDimensions());

    List<MergedAnomalyResultDTO> mergedResultsByMetricDimensionsTime = mergedAnomalyResultDAO
        .findByCollectionMetricDimensionsTime(mergedResult.getCollection(), mergedResult.getMetric(),
            mergedResult.getDimensions().toString(), 0, System.currentTimeMillis(), true);

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
    Assert.assertEquals(mergedResult1.getAnomalyResults().get(0).getId(), anomalyResultId);
    Assert.assertEquals(mergedResult1.getFeedback().getFeedbackType(), AnomalyFeedbackType.ANOMALY);
  }
}
