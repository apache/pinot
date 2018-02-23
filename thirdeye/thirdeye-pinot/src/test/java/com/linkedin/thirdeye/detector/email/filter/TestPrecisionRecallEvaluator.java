package com.linkedin.thirdeye.detector.email.filter;

import com.linkedin.thirdeye.constant.AnomalyResultSource;
import com.linkedin.thirdeye.datalayer.DaoTestUtils;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.DAOTestBase;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import java.util.ArrayList;
import java.util.List;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.linkedin.thirdeye.constant.AnomalyFeedbackType.*;
import static org.junit.Assert.*;


public class TestPrecisionRecallEvaluator {
  private static final String TEST = "test";
  private DAOTestBase testDAOProvider;
  private MergedAnomalyResultManager mergedAnomalyDAO;
  private AnomalyFunctionManager anomalyFunctionDAO;

  @BeforeClass
  void beforeClass() {
    testDAOProvider = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    mergedAnomalyDAO = daoRegistry.getMergedAnomalyResultDAO();
    anomalyFunctionDAO = daoRegistry.getAnomalyFunctionDAO();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }

  @AfterMethod(alwaysRun = true)
  void afterMethod() {
    List<MergedAnomalyResultDTO> mergedAnomalyResultDTOS = mergedAnomalyDAO.findAll();
    for (MergedAnomalyResultDTO anomaly : mergedAnomalyResultDTOS) {
      mergedAnomalyDAO.delete(anomaly);
    }
    List<AnomalyFunctionDTO> anomalyFunction = anomalyFunctionDAO.findAll();
    for (AnomalyFunctionDTO anomalyFunctionDTO : anomalyFunction) {
      anomalyFunctionDAO.delete(anomalyFunctionDTO);
    }
  }

  @Test(dataProvider = "provideMockAnomalies")
  public void testSystemPrecisionAndRecall(List<MergedAnomalyResultDTO> anomalyResultDTOS, MergedAnomalyResultDTO notifiedTrueAnomaly,
      MergedAnomalyResultDTO notifiedFalseAnomaly, MergedAnomalyResultDTO userReportAnomaly) throws Exception {
    // test anomalies when no feedback, and no user report anomaly
    PrecisionRecallEvaluator evaluator = new PrecisionRecallEvaluator(anomalyResultDTOS);
    assertEquals(evaluator.getPrecision(), 0.0, 0.0001);
    assertTrue(Double.isNaN(evaluator.getRecall()));
    assertTrue(Double.isNaN(evaluator.getPrecisionInResponse()));

    // test data with 1 positive feedback
    anomalyResultDTOS.add(notifiedTrueAnomaly);
    evaluator.init(anomalyResultDTOS);
    assertEquals(evaluator.getPrecision(), 0.125, 0.0001);
    assertEquals(evaluator.getRecall(), 1, 0.00001);
    assertEquals(evaluator.getPrecisionInResponse(), 1.0, 0.00001);
    assertEquals(evaluator.getResponseRate(), 1.0/8, 0.00001);

    // test data with 1 positive feedback, 1 false alarm
    anomalyResultDTOS.add(notifiedFalseAnomaly);
    evaluator.init(anomalyResultDTOS);
    assertEquals(evaluator.getPrecision(), 0.1111, 0.0001);
    assertEquals(evaluator.getRecall(), 1, 0.00001);
    assertEquals(evaluator.getPrecisionInResponse(), 0.5, 0.00001);
    assertEquals(evaluator.getResponseRate(), 2.0/9, 0.00001);

    // test data with 1 positive feedback, 1 user report anomaly, 1 false alarm
    anomalyResultDTOS.add(userReportAnomaly);
    evaluator.init(anomalyResultDTOS);
    // counting user report anomalies as part of the total anomalies
    assertEquals(evaluator.getPrecision(),0.1, 0.0001);
    assertEquals(evaluator.getRecall(), 0.5, 0.00001);
    assertEquals(evaluator.getPrecisionInResponse(), 1.0/3, 0.00001);
    assertEquals(evaluator.getResponseRate(), 3.0/10, 0.00001);
  }


  @Test(dataProvider = "provideMockAnomalies")
  public void testAlertFilterProjectPrecisionAndRecall(List<MergedAnomalyResultDTO> anomalyResultDTOS, MergedAnomalyResultDTO notifiedTrueAnomaly,
      MergedAnomalyResultDTO notifiedFalseAnomaly, MergedAnomalyResultDTO userReportAnomaly) throws Exception{
    // test dummy alert filter's precision and recall
    // dummy alert filter is sending EVERYTHING out
    AlertFilter alertFilter = new DummyAlertFilter();
    anomalyResultDTOS.add(notifiedFalseAnomaly);
    anomalyResultDTOS.add(notifiedTrueAnomaly);
    anomalyResultDTOS.add(userReportAnomaly);
    PrecisionRecallEvaluator evaluator = new PrecisionRecallEvaluator(alertFilter, anomalyResultDTOS);
    assertEquals(evaluator.getPrecision(),0.2, 0.0001);
    assertEquals(evaluator.getRecall(), 1, 0.00001);
    assertEquals(evaluator.getWeightedPrecision(), 0.3076, 0.0001);
    // Here is the projected performance for alert filter (not simply based on "notified"), takes into user report anomaly as well
    assertEquals(evaluator.getPrecisionInResponse(), 0.6666,0.0001);
  }


  @DataProvider(name = "provideMockAnomalies")
  public Object[][] getMockMergedAnomalies() {
    List<MergedAnomalyResultDTO> anomalyResultDTOS = new ArrayList<>();
    int totalAnomalies = 7;
    AnomalyFeedbackDTO positiveFeedback = new AnomalyFeedbackDTO();
    AnomalyFeedbackDTO negativeFeedback = new AnomalyFeedbackDTO();
    positiveFeedback.setFeedbackType(ANOMALY);
    negativeFeedback.setFeedbackType(NOT_ANOMALY);
    AnomalyFunctionDTO anomalyFunction = DaoTestUtils.getTestFunctionSpec(TEST, TEST);
    long functionId = anomalyFunctionDAO.save(anomalyFunction);
    for (int i = 0; i < totalAnomalies; i++) {
      MergedAnomalyResultDTO anomaly = new MergedAnomalyResultDTO();
      anomaly.setFeedback(null);
      anomaly.setNotified(true);
      anomaly.setFunction(anomalyFunction);
      anomalyResultDTOS.add(anomaly);
      mergedAnomalyDAO.save(anomaly);
    }
    MergedAnomalyResultDTO notifiedTrueAnomaly = new MergedAnomalyResultDTO();
    notifiedTrueAnomaly.setNotified(true);
    notifiedTrueAnomaly.setFeedback(positiveFeedback);
    notifiedTrueAnomaly.setFunction(anomalyFunction);
    mergedAnomalyDAO.save(notifiedTrueAnomaly);
    MergedAnomalyResultDTO notifiedFalseAnomaly = new MergedAnomalyResultDTO();
    notifiedFalseAnomaly.setNotified(true);
    notifiedFalseAnomaly.setFeedback(negativeFeedback);
    notifiedFalseAnomaly.setFunction(anomalyFunction);
    mergedAnomalyDAO.save(notifiedFalseAnomaly);
    MergedAnomalyResultDTO userReportAnomaly = new MergedAnomalyResultDTO();
    userReportAnomaly.setNotified(false);
    userReportAnomaly.setFeedback(positiveFeedback);
    userReportAnomaly.setAnomalyResultSource(AnomalyResultSource.USER_LABELED_ANOMALY);
    userReportAnomaly.setFunction(anomalyFunction);
    mergedAnomalyDAO.save(userReportAnomaly);
    return new Object[][]{{anomalyResultDTOS, notifiedTrueAnomaly, notifiedFalseAnomaly, userReportAnomaly}};
  }
}