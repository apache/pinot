package com.linkedin.thirdeye.anomalydetection.alertFilterAutotune;

import java.util.Map;


/**
 * Created by ychung on 2/9/17.
 */
public interface AlertFilterAutoTune {

  // Tune alert filter based on training data
  Map<String, String> tuneAlertFilter() throws Exception;

}
