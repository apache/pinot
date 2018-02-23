package com.linkedin.thirdeye.detector.email.filter;

import com.linkedin.thirdeye.anomalydetection.context.AnomalyFeedback;
import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.constant.AnomalyResultSource;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.linkedin.thirdeye.detector.email.filter.UserReportUtils.*;


/**
 * Utility class to evaluate the performance of a list of merged anomalies
 * Precision and Recall Evaluator with two constructor
 * 1) Anomaly Detection System evaluation: calculate on-going performance using "notified" flag
 * 2) Alert Filter evaluation: calculate performance of alert filter on a list of anomalies
 */
public class PrecisionRecallEvaluator {

  /**
   * Using this constructor, PrecisionRecallEvaluator will be relying on anomalies' "notified" flag
   * in order to get the performance of whole anomaly detection system
   * @param anomalies The list of anomalies to be evaluated
   */
  public PrecisionRecallEvaluator(List<MergedAnomalyResultDTO> anomalies) {
    init(anomalies);
  }

  /**
   * Using this constructor, PrecisionRecallEvaluator will be evaluating performance of given alert filter
   * By comparing alert filter's "isQualified" and labels among the list of anomalies, get the performance statistics for this alert filter
   * @param alertFilter the alert filter to be evaluated
   * @param anomalies the list of anomalies as data for alert filter
   */
  public PrecisionRecallEvaluator(AlertFilter alertFilter, List<MergedAnomalyResultDTO> anomalies) {
    this.alertFilter = alertFilter;
    this.isProjected = true;
    init(anomalies);
  }

  private AlertFilter alertFilter;

  private int notifiedTrueAnomaly; // Anomaly is labeled as true and is notified
  private int notifiedTrueAnomalyNewTrend; // Anomaly is labeled as TRUE_NEW_TREND and is notified
  private int notifiedFalseAlarm;  // Anomaly is labeled as false and is notified
  private int notifiedNotLabeled;  // Anomaly is notified, but not labeled
  private int userReportTrueAnomaly; // Anomaly is user reported: true anomaly that was not sent out
  private int userReportTrueAnomalyNewTrend; // Anomaly is user reported: true anomaly new trend that was not sent out
  private boolean isProjected = false;
  // isProjected to indicate if calculating system performance or alert filter's projected performance

  public static final String PRECISION = "precision";
  public static final String WEIGHTED_PRECISION = "weightedPrecision";
  public static final String RECALL = "recall";
  public static final String RESPONSE_RATE = "responseRate";
  public static final String TOTALALERTS = "totalAlerts";
  public static final String TOTALRESPONSES = "totalResponses";
  public static final String TRUEANOMALIES = "trueAnomalies";
  public static final String FALSEALARM = "falseAlarm";
  public static final String NEWTREND = "newTrend";
  public static final String USER_REPORT = "userReportAnomaly";

  public static final Double WEIGHT_OF_NULL_LABEL = 0.5;
  // the weight used for NA labeled data point when calculating precision

  public double getPrecision() {
    if (getTotalAlerts() == 0) {
      return Double.NaN;
    }
    return 1.0 * getTrueAlerts() / getTotalAlerts();
  }

  public double getPrecisionInResponse() {
    if (getTotalResponses() == 0) {
      return Double.NaN;
    }
    return 1.0 * getTrueAlerts() / getTotalResponses();
  }

  public double getWeightedPrecision() {
    if (getTotalAlerts() == 0) {
      return Double.NaN;
    }
    return 1.0 * getTrueAlerts() / (getTotalResponses() + WEIGHT_OF_NULL_LABEL * notifiedNotLabeled);
  }

  public double getRecall() {
    if (getTrueAnomalies() == 0) {
      return Double.NaN;
    }
    return 1.0 * getTrueAlerts() / (getTrueAnomalies() + getTrueAnomalyNewTrend());
  }

  public double getFalseNegativeRate() {
    if (getTrueAnomalies() == 0) {
      return Double.NaN;
    }
    return 1.0 * getUserReportAnomaly() / (getTrueAnomalies() + getTrueAnomalyNewTrend());
  }

  public double getResponseRate() {
    return 1.0 * getTotalResponses() / getTotalAlerts();
  }

  public int getTotalResponses() {
    return notifiedFalseAlarm + notifiedTrueAnomaly + notifiedTrueAnomalyNewTrend + userReportTrueAnomaly
        + userReportTrueAnomalyNewTrend;
  }

  public int getTotalAlerts() {
    return getTotalResponses() + notifiedNotLabeled;
  }

  // number of true anomalies in global set
  public int getTrueAnomalies() {
    return notifiedTrueAnomaly + userReportTrueAnomaly;
  }

  // number of true anomalies new trend in global set
  public int getTrueAnomalyNewTrend() {
    return notifiedTrueAnomalyNewTrend + userReportTrueAnomalyNewTrend;
  }

  // number of true anomalies and true_new_trend anomalies that "NOTIFIED"
  public int getTrueAlerts() {
    return notifiedTrueAnomaly + notifiedTrueAnomalyNewTrend;
  }

  public int getUserReportAnomaly() {
    return userReportTrueAnomaly + userReportTrueAnomalyNewTrend;
  }

  public int getFalseAlarm() {
    return notifiedFalseAlarm;
  }

  public void init(List<MergedAnomalyResultDTO> anomalies) {
    if (anomalies == null || anomalies.isEmpty()) {
      return;
    }

    this.notifiedTrueAnomaly = 0;
    this.notifiedTrueAnomalyNewTrend = 0;
    this.notifiedNotLabeled = 0;
    this.notifiedFalseAlarm = 0;
    this.userReportTrueAnomaly = 0;
    this.userReportTrueAnomalyNewTrend = 0;

    if (anomalies == null || anomalies.isEmpty()) {
      return;
    }

    for (MergedAnomalyResultDTO anomaly : anomalies) {
      AnomalyFeedback feedback = anomaly.getFeedback();
      boolean isLabeledTrueAnomaly = false;
      boolean isLabeledTrueAnomalyNewTrend = false;
      if (feedback != null && feedback.getFeedbackType() != null && feedback.getFeedbackType()
          .equals(AnomalyFeedbackType.ANOMALY_NEW_TREND)) {
        isLabeledTrueAnomalyNewTrend = true;
      } else if (feedback != null && feedback.getFeedbackType() != null && feedback.getFeedbackType()
          .equals(AnomalyFeedbackType.ANOMALY)) {
        isLabeledTrueAnomaly = true;
      }

      // handle user report anomaly
      if (anomaly.getAnomalyResultSource().equals(AnomalyResultSource.USER_LABELED_ANOMALY)) {
        if (!isProjected) {
          if (isLabeledTrueAnomaly) {
            userReportTrueAnomaly++;
          } else if (isLabeledTrueAnomalyNewTrend) {
            userReportTrueAnomalyNewTrend++;
          }
        } else {
          if (isUserReportAnomalyIsQualified(this.alertFilter, anomaly)) {
            notifiedTrueAnomaly++;
          } else {
            userReportTrueAnomaly++;
          }
        }
      } else {
        // if system detected anomaly, if using projected evaluation, skip those true anomalies that are not notified
        // since these anomalies are originally unsent, but reverted the feedback based on user report
        boolean isNotified = isProjected ? alertFilter.isQualified(anomaly) : anomaly.isNotified();

        if (isNotified) {
          if (feedback == null || feedback.getFeedbackType() == null) {
            this.notifiedNotLabeled++;
          } else if (isLabeledTrueAnomaly) {
            notifiedTrueAnomaly++;
          } else if (isLabeledTrueAnomalyNewTrend) {
            notifiedTrueAnomalyNewTrend++;
          } else {
            notifiedFalseAlarm++;
          }
        }
      }
    }
  }

  public Properties toProperties() {
    Properties evals = new Properties();
    evals.put(RESPONSE_RATE, getResponseRate());
    evals.put(PRECISION, getPrecision());
    evals.put(WEIGHTED_PRECISION, getWeightedPrecision());
    evals.put(RECALL, getRecall());
    evals.put(TOTALALERTS, getTotalAlerts());
    evals.put(TOTALRESPONSES, getTotalResponses());
    evals.put(TRUEANOMALIES, getTrueAnomalies());
    evals.put(FALSEALARM, getFalseAlarm());
    evals.put(NEWTREND, getTrueAnomalyNewTrend());
    evals.put(USER_REPORT, getUserReportAnomaly());
    return evals;
  }

  public Map<String, Number> toNumberMap() {
    Map<String, Number> evals = new HashMap<>();
    evals.put(RESPONSE_RATE, getResponseRate());
    evals.put(PRECISION, getPrecisionInResponse());
    evals.put(WEIGHTED_PRECISION, getWeightedPrecision());
    evals.put(RECALL, getRecall());
    evals.put(TOTALALERTS, getTotalAlerts());
    evals.put(TOTALRESPONSES, getTotalResponses());
    evals.put(TRUEANOMALIES, getTrueAnomalies());
    evals.put(FALSEALARM, getFalseAlarm());
    evals.put(NEWTREND, getTrueAnomalyNewTrend());
    evals.put(USER_REPORT, getUserReportAnomaly());
    return evals;
  }

  public static List<String> getPropertyNames() {
    return Collections.unmodifiableList(new ArrayList<>(
        Arrays.asList(RESPONSE_RATE, PRECISION, WEIGHTED_PRECISION, RECALL, TOTALALERTS, TOTALRESPONSES, TRUEANOMALIES,
            FALSEALARM, NEWTREND, USER_REPORT)));
  }
}
