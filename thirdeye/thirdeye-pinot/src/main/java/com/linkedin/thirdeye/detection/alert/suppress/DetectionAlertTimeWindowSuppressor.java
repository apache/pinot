package com.linkedin.thirdeye.detection.alert.suppress;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.anomalydetection.context.AnomalyFeedback;
import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.detection.ConfigUtils;
import com.linkedin.thirdeye.detection.alert.DetectionAlertFilterResult;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Suppress alerts from anomalies generated during a specific time period.
 *
 * This class enables 2 ways of suppressing alerts
 * 1. Suppress all the alerts generated during the time window. No alerts will be sent.
 *    ({@link #WINDOW_START_TIME_KEY} and {@link #WINDOW_END_TIME_KEY})
 * 2. Suppress alerts in the time window based on some thresholds.
 *    ({@link #EXPECTED_CHANGE_KEY} and {@link #ACCEPTABLE_DEVIATION_KEY})
 */
public class DetectionAlertTimeWindowSuppressor extends DetectionAlertSuppressor {
  private static final Logger LOG = LoggerFactory.getLogger(DetectionAlertTimeWindowSuppressor.class);

  static final String TIME_WINDOW_SUPPRESSOR_KEY = "timeWindowSuppressor";
  static final String TIME_WINDOWS_KEY = "timeWindows";

  static final String WINDOW_START_TIME_KEY = "windowStartTime";
  static final String WINDOW_END_TIME_KEY = "windowEndTime";
  static final String IS_THRESHOLD_KEY = "isThresholdApplied";

  // The expected rise or fall of a metric during the holiday or suppression period (ex: -0.5 for 50% drop)
  static final String EXPECTED_CHANGE_KEY = "expectedChange";

  // The acceptable deviation from the dropped/risen value during the suppression period (ex: 0.1 for +/- 10%)
  static final String ACCEPTABLE_DEVIATION_KEY = "acceptableDeviation";

  public DetectionAlertTimeWindowSuppressor(DetectionAlertConfigDTO config) {
    super(config);
  }

  private boolean isAnomalySuppressedByThreshold(double anomalyWeight, Map<String, Object> suppressWindowProps) {
    double expectedDropOrSpike = (Double) suppressWindowProps.get(EXPECTED_CHANGE_KEY);
    double acceptableDeviation = (Double) suppressWindowProps.get(ACCEPTABLE_DEVIATION_KEY);
    if (anomalyWeight <= (expectedDropOrSpike + acceptableDeviation)
        && anomalyWeight >= (expectedDropOrSpike - acceptableDeviation)) {
      LOG.info("Anomaly id {} falls within the specified thresholds (anomalyWeight = {}, expectedDropOrSpike = {},"
              + " acceptableDeviation = {})", anomalyWeight, expectedDropOrSpike, acceptableDeviation);
      return true;
    }

    return false;
  }

  /**
   * Check if the anomaly needs to be suppressed. An anomaly is suppressed if the startTime
   * of the anomaly falls in the suppression time window and is within the user's expected range.
   */
  private boolean isAnomalySuppressed(MergedAnomalyResultDTO anomaly, Map<String, Object> suppressWindowProps) {
    boolean shouldSuppress = false;
    try {
      long windowStartTime = (Long) suppressWindowProps.get(WINDOW_START_TIME_KEY);
      long windowEndTime = (Long) suppressWindowProps.get(WINDOW_END_TIME_KEY);
      if (anomaly.getStartTime() >= windowStartTime && anomaly.getStartTime() < windowEndTime) {
        LOG.info("Anomaly id {} falls in the suppression time window ({}, {})", anomaly.getId(), windowStartTime, windowEndTime);
        if (suppressWindowProps.get(IS_THRESHOLD_KEY) != null && (Boolean) suppressWindowProps.get(IS_THRESHOLD_KEY)) {
          shouldSuppress = isAnomalySuppressedByThreshold(anomaly.getWeight(), suppressWindowProps);
        } else {
          shouldSuppress = true;
        }
      }
    } catch (Exception e) {
      LOG.warn("Exception while suppressing anomaly id {} with suppress window properties {}", anomaly.getId(),
          suppressWindowProps, e);
    }

    return shouldSuppress;
  }

  private void filterOutSuppressedAnomalies(final Set<MergedAnomalyResultDTO> anomalies) {
    Iterator<MergedAnomalyResultDTO> anomaliesIt = anomalies.iterator();
    MergedAnomalyResultManager anomalyMergedResultDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();

    List<Map<String, Object>> suppressWindowPropsList
        = ConfigUtils.getList(config.getAlertSuppressors().get(TIME_WINDOW_SUPPRESSOR_KEY).get(TIME_WINDOWS_KEY));

    while (anomaliesIt.hasNext()) {
      MergedAnomalyResultDTO anomaly = anomaliesIt.next();
      for (Map<String, Object> suppressWindowProps : suppressWindowPropsList) {
        if (isAnomalySuppressed(anomaly, suppressWindowProps)) {
          LOG.info("Suppressing anomaly id {} with suppress properties {}. Anomaly Details = {}", anomaly.getId(), suppressWindowProps, anomaly);
          anomaliesIt.remove();
          AnomalyFeedback feedback = anomaly.getFeedback();
          if (feedback == null) {
            feedback = new AnomalyFeedbackDTO();
          }

          // Suppressing is a way by which users admit that anomalies during this period
          // are expected. We also do not want the algorithm to readjust the baseline.
          feedback.setFeedbackType(AnomalyFeedbackType.ANOMALY);
          feedback.setComment("Suppressed anomaly. Auto-labeling as true anomaly.");

          anomaly.setFeedback(feedback);
          anomalyMergedResultDAO.updateAnomalyFeedback(anomaly);
        }
      }
    }
  }

  @Override
  public DetectionAlertFilterResult run(DetectionAlertFilterResult results) throws Exception {
    Preconditions.checkNotNull(results);
    for (Set<MergedAnomalyResultDTO> anomalies : results.getResult().values()) {
      filterOutSuppressedAnomalies(anomalies);
    }

    return results;
  }
}
