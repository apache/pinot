package com.linkedin.thirdeye.anomaly.alert.util;

import com.linkedin.thirdeye.detector.email.filter.AlertFilter;
import com.linkedin.thirdeye.detector.email.filter.AlphaBetaAlertFilter;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterType;
import com.linkedin.thirdeye.detector.email.filter.DummyAlertFilter;


public class AlertFilterHelper {
  public static final String FILTER_TYPE_KEY = "type";

  public static AlertFilter getAlertFilter(AlertFilterType filterType) {
    switch (filterType) {
      case ALPHA_BETA:
        return new AlphaBetaAlertFilter();
      case DUMMY:
      default:
        return new DummyAlertFilter();
    }
  }
}

