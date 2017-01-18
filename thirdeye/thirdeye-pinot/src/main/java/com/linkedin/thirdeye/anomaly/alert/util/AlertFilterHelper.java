package com.linkedin.thirdeye.anomaly.alert.util;

import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.pojo.AnomalyFunctionBean;
import com.linkedin.thirdeye.detector.email.filter.AlertFilter;
import com.linkedin.thirdeye.detector.email.filter.AlphaBetaAlertFilter;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterType;
import com.linkedin.thirdeye.detector.email.filter.DummyAlertFilter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlertFilterHelper {
  public static final String FILTER_TYPE_KEY = "type";
  private static final Logger LOG = LoggerFactory.getLogger(AlertFilterHelper.class);

  public static AlertFilter getAlertFilter(AlertFilterType filterType) {
    switch (filterType) {
      case ALPHA_BETA:
        return new AlphaBetaAlertFilter();
      case DUMMY:
      default:
        return new DummyAlertFilter();
    }
  }

  /**
   *
   * @param results
   * @return
   */
  public static List<MergedAnomalyResultDTO> applyFiltrationRule(List<MergedAnomalyResultDTO> results) {
    if (results.size() == 0) {
      return results;
    }
    // Function ID to Alert Filter
    Map<Long, AlertFilter> functionAlertFilter = new HashMap<>();

    List<MergedAnomalyResultDTO> qualifiedAnomalies = new ArrayList<>();
    for (MergedAnomalyResultDTO result : results) {
      // Lazy initiates alert filter for anomalies of the same anomaly function
      AnomalyFunctionBean anomalyFunctionSpec = result.getFunction();
      long functionId = anomalyFunctionSpec.getId();
      AlertFilter alertFilter = functionAlertFilter.get(functionId);
      if (alertFilter == null) {
        // Get filtration rule from anomaly function configuration
        alertFilter = initiateAlertFilter(anomalyFunctionSpec);
        functionAlertFilter.put(functionId, alertFilter);
        LOG.info("Using filter {} for anomaly function {} (dataset: {}, metric: {})", alertFilter,
            functionId, anomalyFunctionSpec.getCollection(), anomalyFunctionSpec.getMetric());
      }
      if (alertFilter.isQualified(result)) {
        qualifiedAnomalies.add(result);
      }
    }
    LOG.info(
        "Found [{}] anomalies qualified to alert after applying filtration rule on [{}] anomalies",
        qualifiedAnomalies.size(), results.size());
    return qualifiedAnomalies;
  }

  /**
   * Initiates an alert filter for the given anomaly function.
   *
   * @param anomalyFunctionSpec the anomaly function that contains the alert filter spec.
   *
   * @return the alert filter specified by the alert filter spec or a dummy filter if the function
   * does not have an alert filter spec or this method fails to initiates an alert filter from the
   * spec.
   */
  static AlertFilter initiateAlertFilter(AnomalyFunctionBean anomalyFunctionSpec) {
    Map<String, String> alertFilterInfo = anomalyFunctionSpec.getAlertFilter();
    if (alertFilterInfo == null) {
      alertFilterInfo = Collections.emptyMap();
    }
    AlertFilter alertFilter;
    if (alertFilterInfo.containsKey(FILTER_TYPE_KEY)) {
      AlertFilterType type = AlertFilterType.valueOf(alertFilterInfo.get(FILTER_TYPE_KEY).toUpperCase());
      alertFilter = AlertFilterHelper.getAlertFilter(type);
      alertFilter.setParameters(alertFilterInfo);
    } else {
      // Every anomaly triggers an alert by default
      alertFilter = AlertFilterHelper.getAlertFilter(AlertFilterType.DUMMY);
    }
    return alertFilter;
  }
}

