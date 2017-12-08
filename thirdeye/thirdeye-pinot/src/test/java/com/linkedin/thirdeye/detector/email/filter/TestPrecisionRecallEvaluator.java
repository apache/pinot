package com.linkedin.thirdeye.detector.email.filter;

import com.linkedin.thirdeye.constant.AnomalyResultSource;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.ArrayList;
import java.util.List;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.linkedin.thirdeye.constant.AnomalyFeedbackType.*;
import static org.junit.Assert.*;


public class TestPrecisionRecallEvaluator {

  @Test(dataProvider = "provideMockAnomalies")
  public void testSystemPrecisionAndRecall(List<MergedAnomalyResultDTO> anomalyResultDTOS, MergedAnomalyResultDTO notifiedTrueAnomaly,
      MergedAnomalyResultDTO notifiedFalseAnomaly, MergedAnomalyResultDTO userReportAnomaly) throws Exception {
    // test anomalies when no feedback, and no user report anomaly
    PrecisionRecallEvaluator evaluator = new PrecisionRecallEvaluator(anomalyResultDTOS);
    assertEquals(evaluator.getPrecision(), 0.0, 0.0001);
    assertTrue(Double.isNaN(evaluator.getRecall()));

    // test data with 1 positive feedback
    anomalyResultDTOS.add(notifiedTrueAnomaly);
    evaluator.init(anomalyResultDTOS);
    assertEquals(evaluator.getPrecision(), 0.125, 0.0001);
    assertEquals(evaluator.getRecall(), 1, 0.00001);

    // test data with 1 positive feedback, 1 false alarm
    anomalyResultDTOS.add(notifiedFalseAnomaly);
    evaluator.init(anomalyResultDTOS);
    assertEquals(evaluator.getPrecision(), 0.1111, 0.0001);
    assertEquals(evaluator.getRecall(), 1, 0.00001);

    // test data with 1 positive feedback, 1 user report anomaly, 1 false alarm
    anomalyResultDTOS.add(userReportAnomaly);
    evaluator.init(anomalyResultDTOS);
    assertEquals(evaluator.getPrecision(),0.1111, 0.0001);
    assertEquals(evaluator.getRecall(), 0.5, 0.00001);
  }


  @Test(dataProvider = "provideMockAnomalies")
  public void testAlertFilterPrecisionAndRecall(List<MergedAnomalyResultDTO> anomalyResultDTOS, MergedAnomalyResultDTO notifiedTrueAnomaly,
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
  }


  @DataProvider(name = "provideMockAnomalies")
  public Object[][] getMockMergedAnomalies() {
    List<MergedAnomalyResultDTO> anomalyResultDTOS = new ArrayList<>();
    int totalAnomalies = 7;
    AnomalyFeedbackDTO positiveFeedback = new AnomalyFeedbackDTO();
    AnomalyFeedbackDTO negativeFeedback = new AnomalyFeedbackDTO();
    positiveFeedback.setFeedbackType(ANOMALY);
    negativeFeedback.setFeedbackType(NOT_ANOMALY);
    for (int i = 0; i < totalAnomalies; i++) {
      MergedAnomalyResultDTO anomaly = new MergedAnomalyResultDTO();
      anomaly.setFeedback(null);
      anomaly.setNotified(true);
      anomalyResultDTOS.add(anomaly);
    }
    MergedAnomalyResultDTO notifiedTrueAnomaly = new MergedAnomalyResultDTO();
    notifiedTrueAnomaly.setNotified(true);
    notifiedTrueAnomaly.setFeedback(positiveFeedback);
    MergedAnomalyResultDTO notifiedFalseAnomaly = new MergedAnomalyResultDTO();
    notifiedFalseAnomaly.setNotified(true);
    notifiedFalseAnomaly.setFeedback(negativeFeedback);
    MergedAnomalyResultDTO userReportAnomaly = new MergedAnomalyResultDTO();
    userReportAnomaly.setNotified(false);
    userReportAnomaly.setFeedback(positiveFeedback);
    userReportAnomaly.setAnomalyResultSource(AnomalyResultSource.USER_LABELED_ANOMALY);
    return new Object[][]{{anomalyResultDTOS, notifiedTrueAnomaly, notifiedFalseAnomaly, userReportAnomaly}};
  }
}