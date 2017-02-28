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
  public void testPrecisionAndRecall() throws Exception{
    AlertFilter dummyAlertFilter = new DummyAlertFilter();
    AlertFilterUtil evaluator = new AlertFilterUtil(dummyAlertFilter);
    // test data with 1 positive feedback, 1 negative feedback, other NA feedbacks
    List<MergedAnomalyResultDTO> anomalies = getMockMergedAnomalies(7,8);
    Double[] evals = evaluator.getEvalResults(anomalies);
    assertEquals(evals[AlertFilterUtil.PRECISION_INDEX], 0.1111, 0.0001);
    assertEquals(evals[AlertFilterUtil.RECALL_INDEX], 1, 0.0001);

    // test data with 1 positive feedback and others are NA feedbacks
    anomalies = getMockMergedAnomalies(6,-1);
    evals = evaluator.getEvalResults(anomalies);
    assertEquals(evals[AlertFilterUtil.PRECISION_INDEX], 0.1111, 0.0001);
    assertEquals(evals[AlertFilterUtil.RECALL_INDEX], 1, 0.0001);

    // test data with 0 positive feedback, 1 negative feedback and others are NA feedbacks
    anomalies = getMockMergedAnomalies(-1,6);
    try{
      evals = evaluator.getEvalResults(anomalies);
      fail("Should throw exception");
    } catch (Exception e) {
      assertEquals("No true labels in dataset. Check data", e.getMessage());
    }
  }


  private List<MergedAnomalyResultDTO> getMockMergedAnomalies(int posIdx, int negIdx){
    List<MergedAnomalyResultDTO> anomalyResultDTOS = new ArrayList<>();
    int[] ws = {1, 1, 2, 3, 4, 4, 5, 6 ,7};
    double[] severity = {2.0, 4.0, 2.0, 3.0, 1.0, 3.0, 2.0,1.0,3.0};
    AnomalyFeedbackDTO positiveFeedback = new AnomalyFeedbackDTO();
    AnomalyFeedbackDTO negativeFeedback = new AnomalyFeedbackDTO();
    positiveFeedback.setFeedbackType(ANOMALY);
    negativeFeedback.setFeedbackType(NOT_ANOMALY);
    for(int i = 0; i < ws.length; i++){
      MergedAnomalyResultDTO anomaly = new MergedAnomalyResultDTO();
      anomaly.setStartTime(0l);
      anomaly.setEndTime(ws[i] * 3600 * 1000l);
      anomaly.setWeight(severity[i]);
      if(i == posIdx) {
        anomaly.setFeedback(positiveFeedback);
      } else if(i == negIdx) {
        anomaly.setFeedback(negativeFeedback);
      } else {
        anomaly.setFeedback(null);
      }
      anomalyResultDTOS.add(anomaly);
    }
    return anomalyResultDTOS;
  }
}