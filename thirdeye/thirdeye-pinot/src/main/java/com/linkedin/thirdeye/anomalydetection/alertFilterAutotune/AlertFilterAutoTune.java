package com.linkedin.thirdeye.anomalydetection.alertFilterAutotune;

import java.util.Map;


/**
 * Created by ychung on 2/9/17.
 */
public interface AlertFilterAutoTune {

  // Tune the alert filter properties using labeled anomalies
  // precision and recall need to be improved over old setting
  Map<String, String> tuneAlertFilter() throws Exception;

}
