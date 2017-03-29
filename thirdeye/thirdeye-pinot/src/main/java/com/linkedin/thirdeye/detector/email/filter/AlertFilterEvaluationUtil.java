package com.linkedin.thirdeye.detector.email.filter;

import com.linkedin.thirdeye.anomalydetection.context.AnomalyFeedback;
import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility class to evaluate alert filter
 */
public class AlertFilterEvaluationUtil {

  private double precision = -1;
  private double recall = -1;
  private int totalAnomalies = -1;
  private int totalResponses = -1;
  private int trueAnomalies = -1;
  private int falseAlarm = -1;
  private int nonActionable = -1;

  public static final String PRECISION = "precision";
  public static final String RECALL = "recall";
  public static final String TOTALANOMALIES = "totalAnomalies";
  public static final String TOTALRESPONSES = "totalResponses";
  public static final String TRUEANOMALIES = "trueAnomalies";
  public static final String FALSEALARM = "falseAlarm";
  public static final String NONACTIONABLE = "nonActionable";

  public double getPrecision(){return this.precision;}
  public double getRecall(){return this.recall;}
  public int getTotalAnomalies(){return this.totalAnomalies;}
  public int getTotalResponses(){return this.totalResponses;}
  public int getTrueAnomalies(){return this.trueAnomalies;}
  public int getFalseAlarm(){return this.falseAlarm;}
  public int getNonActionable(){return this.nonActionable;}

  private AlertFilter alertFilter;
  private static final Logger LOG = LoggerFactory.getLogger(AlertFilterEvaluationUtil.class);

  private static final List<String> propertyNames =
      Collections.unmodifiableList(new ArrayList<>(Arrays.asList(PRECISION, RECALL, TOTALANOMALIES, TOTALRESPONSES, TRUEANOMALIES, FALSEALARM, NONACTIONABLE)));

  public AlertFilterEvaluationUtil(AlertFilter alertFilter){
    this.alertFilter = alertFilter;
  }

  /**
   * Evaluate alert filter given merged anomalies and output precision and recall
   * @param mergedAnomalyResultDTOS
   * @throws Exception
   */
  public void updatePrecisionAndRecall(List<MergedAnomalyResultDTO> mergedAnomalyResultDTOS) throws Exception {
    int TP = 0;
    int FP = 0;
    int FN = 0;
    for (MergedAnomalyResultDTO anomaly: mergedAnomalyResultDTOS) {
      boolean predLabel = alertFilter.isQualified(anomaly);
      AnomalyFeedback feedback = anomaly.getFeedback();
      boolean label = !(feedback == null || feedback.getFeedbackType() == AnomalyFeedbackType.NOT_ANOMALY);
      //predicted true
      if (predLabel) {
        if (label) {
          TP++;
        } else {
          FP++;
        }
      } else if (label) {
        // else if predicted false but label is true
        FN++;
      }
    }
    if (TP + FN == 0) {
      throw new Exception("No true labels in dataset. Check data");
    }
    if (TP + FP == 0) {
      this.precision = -1;
      LOG.info("No predicted true labels. Check model input");
    } else{
      this.precision = 1.000 * TP / (TP + FP);
    }
    this.recall = 1.000 * TP / (TP + FN);
  }



  /**
   * Provide feedback summary give a list of merged anomalies
   * @param anomalies
   */
  public void updateFeedbackSummary(List<MergedAnomalyResultDTO> anomalies){
    int totalAnomalies = 0;
    int totalResponses = 0;
    int trueAnomalies = 0;
    int falseAlarm = 0;
    int nonActionable = 0;
    for(MergedAnomalyResultDTO anomaly: anomalies) {
      totalAnomalies++;
      // evaluate feedbacks
      AnomalyFeedback feedback = anomaly.getFeedback();
      if (feedback != null) {
        totalResponses++;
        AnomalyFeedbackType feedbackType = feedback.getFeedbackType();
        switch (feedbackType) {
          case ANOMALY:
            trueAnomalies++;
            break;
          case ANOMALY_NO_ACTION:
            nonActionable++;
            break;
          case NOT_ANOMALY:
            falseAlarm++;
            break;
        }
      }
    }
    this.totalAnomalies = totalAnomalies;
    this.totalResponses = totalResponses;
    this.trueAnomalies = trueAnomalies;
    this.falseAlarm = falseAlarm;
    this.nonActionable = nonActionable;
  }

  public Properties toProperties() {
    Properties evals = new Properties();
    evals.put(PRECISION, precision);
    evals.put(RECALL, recall);
    evals.put(TOTALANOMALIES, totalAnomalies);
    evals.put(TOTALRESPONSES, totalResponses);
    evals.put(TRUEANOMALIES, trueAnomalies);
    evals.put(FALSEALARM, falseAlarm);
    evals.put(NONACTIONABLE, nonActionable);
    return evals;
  }

  public static List<String> getPropertyNames() {
    return propertyNames;
  }
}
