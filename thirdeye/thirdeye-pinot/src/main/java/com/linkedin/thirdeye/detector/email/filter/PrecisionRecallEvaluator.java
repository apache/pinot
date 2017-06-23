package com.linkedin.thirdeye.detector.email.filter;

import com.linkedin.thirdeye.anomalydetection.context.AnomalyFeedback;
import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;


/**
 * Utility class to evaluate the performance of a list of merged anomalies
 */
public class PrecisionRecallEvaluator {
  private AlertFilter alertFilter;

  private int qualifiedTrueAnomaly; // Anomaly is labeled as true and is qualified
  private int qualifiedTrueAnomalyNotActionable; // Anomaly is labeled as TRUE_BUT_NOT_ACTIONABLE and is qualified
  private int qualifiedFalseAlarm;  // Anomaly is labeled as false and is qualified
  private int invalidTrueAnomaly;   // Anomaly is labeled as true but is not qualified (not sent or not detected)
  private int invalidTrueAnomalyNotActionable;   // Anomaly is labeled as TRUE_BUT_NOT_ACTIONABLE  but is not qualified (not sent or not detected)
  private int invalidAnomaly;       // Anomaly is labeled as false and is not qualified
  private int qualifiedNotLabeled;  // Anomaly is qualified, but not labeled

  public static final String PRECISION = "precision";
  public static final String WEIGHTED_PRECISION = "weightedPrecision";
  public static final String RECALL = "recall";
  public static final String RESPONSE_RATE = "responseRate";
  public static final String TOTALANOMALIES = "totalAnomalies";
  public static final String TOTALRESPONSES = "totalResponses";
  public static final String TRUEANOMALIES = "trueAnomalies";
  public static final String FALSEALARM = "falseAlarm";
  public static final String NONACTIONABLE = "nonActionable";
  public static final Double WEIGHT_OF_NULL_LABEL = 0.5; // the weight used for NA labeled data point when calculating precision

  public double getPrecision() {
    if (getTotalReports() == 0) {
      return Double.NaN;
    }
    return 1.0 * getTrueAlerts() / getTotalReports();
  }
  public double getPrecisionInResponse() {
    if (getTotalResponses() == 0) {
      return Double.NaN;
    }
    return 1.0 * getTrueAlerts() / getTotalResponses();
  }
  public double getWeightedPrecision() {
    if (getTotalReports() == 0) {
      return Double.NaN;
    }
    return 1.0 * getTrueAlerts() /
        (getTotalResponses() + WEIGHT_OF_NULL_LABEL * qualifiedNotLabeled);
  }
  public double getRecall() {
    if (getTrueAnomalies() == 0) {
      return  Double.NaN;
    }
    return 1.0 * getTrueAlerts() / (getTrueAnomalies() + getNonActionable());
  }
  public double getFalseNegativeRate() {
    if (getTrueAnomalies() == 0) {
      return  Double.NaN;
    }
    return 1.0 * getFalseNegatives() / (getTrueAnomalies() + getNonActionable());
  }
  public double getResponseRate() {
    return 1.0 * getTotalResponses() / getTotalReports();
  }
  public int getTotalAnomalies() {
    return getTotalReports() + getTotalFiltered();
  }
  public int getTotalResponses() {
    return qualifiedFalseAlarm + qualifiedTrueAnomaly + qualifiedTrueAnomalyNotActionable;
  }
  public int getTotalReports() {
    return getTotalResponses() + qualifiedNotLabeled;
  }
  public int getTotalFiltered() {
    return invalidAnomaly + invalidTrueAnomalyNotActionable + invalidTrueAnomaly;
  }
  public int getTrueAnomalies() {
    return qualifiedTrueAnomaly + invalidTrueAnomaly;
  }
  public int getFalseNegatives() {
    return invalidTrueAnomaly + invalidTrueAnomalyNotActionable;
  }
  public int getFalseAlarm() {
    return qualifiedFalseAlarm;
  }
  public int getNonActionable() {
    return qualifiedTrueAnomalyNotActionable + invalidTrueAnomalyNotActionable;
  }
  public int getTrueAlerts() {
    return qualifiedTrueAnomaly + qualifiedTrueAnomalyNotActionable;
  }
  public int getQualifiedTrueAnomaly() {
    return qualifiedTrueAnomaly;
  }
  public int getQualifiedTrueAnomalyNotActionable() {
    return qualifiedTrueAnomalyNotActionable;
  }
  public int getInvalidTrueAnomaly() {
    return invalidTrueAnomaly;
  }
  public int getInvalidTrueAnomalyNotActionable() {
    return invalidTrueAnomalyNotActionable;
  }
  public int getInvalidAnomaly() {
    return invalidAnomaly;
  }
  public int getQualifiedNotLabeled() {
    return qualifiedNotLabeled;
  }

  public PrecisionRecallEvaluator(AlertFilter alertFilter, List<MergedAnomalyResultDTO> anomalies){
    this.alertFilter = alertFilter;
    init(anomalies);
  }

  public void init(List<MergedAnomalyResultDTO> anomalies) {
    if(anomalies == null || anomalies.isEmpty()) {
      return;
    }

    this.qualifiedTrueAnomaly = 0;
    this.qualifiedTrueAnomalyNotActionable = 0;
    this.invalidTrueAnomaly = 0;
    this.invalidTrueAnomalyNotActionable = 0;
    this.qualifiedNotLabeled = 0;
    this.qualifiedFalseAlarm = 0;
    this.invalidAnomaly = 0;

    if(anomalies == null || anomalies.isEmpty()) {
      return;
    }

    for(MergedAnomalyResultDTO anomaly : anomalies) {
      AnomalyFeedback feedback = anomaly.getFeedback();
      boolean isLabeledTrueAnomaly = false;
      boolean isLabeledTrueAnomalyNotActionable = false;
      if(feedback != null && feedback.getFeedbackType() != null && feedback.getFeedbackType().equals(AnomalyFeedbackType.ANOMALY_NEW_TREND)) {
        isLabeledTrueAnomalyNotActionable = true;
      } else if (feedback != null && feedback.getFeedbackType() != null && feedback.getFeedbackType().equals(AnomalyFeedbackType.ANOMALY)) {
        isLabeledTrueAnomaly = true;
      }

      boolean isQualified = alertFilter.isQualified(anomaly);


      if(isQualified) {
        if(feedback == null || feedback.getFeedbackType() == null) {
          this.qualifiedNotLabeled++;
        } else if (isLabeledTrueAnomaly) {
          qualifiedTrueAnomaly++;
        } else if (isLabeledTrueAnomalyNotActionable) {
          qualifiedTrueAnomalyNotActionable++;
        } else {
          qualifiedFalseAlarm++;
        }
      } else {
        if(isLabeledTrueAnomaly) {
          invalidTrueAnomaly++;
        } else if (isLabeledTrueAnomalyNotActionable) {
          invalidTrueAnomalyNotActionable++;
        } else {
          invalidAnomaly++;
        }
      }
    }
  }

  /**
   * Evaluate alert filter given merged anomalies and output weigheted precision and recall
   * @param mergedAnomalyResultDTOS
   * @throws Exception
   */
  @Deprecated
  public void updateWeighedPrecisionAndRecall(List<MergedAnomalyResultDTO> mergedAnomalyResultDTOS) {
    init(mergedAnomalyResultDTOS);
  }



  /**
   * Provide feedback summary give a list of merged anomalies
   * @param anomalies
   */
  @Deprecated
  public void updateFeedbackSummary(List<MergedAnomalyResultDTO> anomalies){
    init(anomalies);
  }

  public Properties toProperties() {
    Properties evals = new Properties();
    evals.put(RESPONSE_RATE, getResponseRate());
    evals.put(PRECISION, getPrecision());
    evals.put(WEIGHTED_PRECISION, getWeightedPrecision());
    evals.put(RECALL, getRecall());
    evals.put(TOTALANOMALIES, getTotalAnomalies());
    evals.put(TOTALRESPONSES, getTotalResponses());
    evals.put(TRUEANOMALIES, getTrueAnomalies());
    evals.put(FALSEALARM, getFalseAlarm());
    evals.put(NONACTIONABLE, getNonActionable());
    return evals;
  }

  public static List<String> getPropertyNames() {
    return Collections.unmodifiableList(
        new ArrayList<>(Arrays.asList(RESPONSE_RATE, PRECISION, WEIGHTED_PRECISION, RECALL, TOTALANOMALIES,
            TOTALRESPONSES, TRUEANOMALIES, FALSEALARM, NONACTIONABLE)));
  }
}
