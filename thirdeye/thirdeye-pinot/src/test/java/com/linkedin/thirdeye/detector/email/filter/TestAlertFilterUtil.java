package com.linkedin.thirdeye.detector.email.filter;

import com.linkedin.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

import static com.linkedin.thirdeye.constant.AnomalyFeedbackType.*;
import static org.junit.Assert.*;


public class TestAlertFilterUtil {

  @Test
  public void testAlertFitlerEvaluator() throws Exception{
    AlertFilter dummyAlertFilter = new DummyAlertFilter();
    AlertFilterUtil evaluator = new AlertFilterUtil(dummyAlertFilter);
    List<MergedAnomalyResultDTO> anomalies = getMockMergedAnomalies();
    double[] evals = evaluator.getEvalResults(anomalies);
    assertEquals(evals[AlertFilterUtil.PRECISION_INDEX], 0.1111, 0.0001);
    assertEquals(evals[AlertFilterUtil.RECALL_INDEX], 1, 0.0001);
  }

  public List<MergedAnomalyResultDTO> getMockMergedAnomaliesNullFeedbacks(){
    List<MergedAnomalyResultDTO> anomalyResultDTOS = new ArrayList<>();
    int[] ws = {1, 1, 2, 3, 4, 4, 5};
    double[] severity = {2.0, 4.0, 2.0, 3.0, 1.0, 3.0, 2.0};
    AnomalyFeedbackDTO nullfeedback = null;
    for(int i = 0; i < ws.length; i++){
      MergedAnomalyResultDTO anomaly = new MergedAnomalyResultDTO();
      anomaly.setStartTime(0l);
      anomaly.setEndTime(ws[i] * 3600 * 1000l);
      anomaly.setWeight(severity[i]);
      anomaly.setFeedback(nullfeedback);
      anomalyResultDTOS.add(anomaly);
    }
    return anomalyResultDTOS;
  }

  public List<MergedAnomalyResultDTO> getMockMergedAnomaliesSinglePositive(){
    List<MergedAnomalyResultDTO> anomalyResultDTOS = getMockMergedAnomaliesNullFeedbacks();
    AnomalyFeedbackDTO positiveFeedback = new AnomalyFeedbackDTO();
    positiveFeedback.setFeedbackType(ANOMALY);
    MergedAnomalyResultDTO anomaly = new MergedAnomalyResultDTO();
    anomaly.setStartTime(0l);
    anomaly.setEndTime(6 * 3600 * 1000l);
    anomaly.setWeight(1.0);
    anomaly.setFeedback(positiveFeedback);
    anomalyResultDTOS.add(anomaly);
    return anomalyResultDTOS;
  }

  public List<MergedAnomalyResultDTO> getMockMergedAnomalies(){
    List<MergedAnomalyResultDTO> anomalyResultDTOS = getMockMergedAnomaliesSinglePositive();
    AnomalyFeedbackDTO negativeFeedback = new AnomalyFeedbackDTO();
    negativeFeedback.setFeedbackType(NOT_ANOMALY);
    MergedAnomalyResultDTO anomaly = new MergedAnomalyResultDTO();
    anomaly.setStartTime(0l);
    anomaly.setEndTime(7 * 3600 * 1000l);
    anomaly.setWeight(3.0);
    anomaly.setFeedback(negativeFeedback);
    anomalyResultDTOS.add(anomaly);
    return anomalyResultDTOS;
  }
}