package com.linkedin.thirdeye.anomalydetection.alertFilterAutotune;

import java.util.Map;


/**
 * The interface of AlertFilterAutoTuning.
 * Tuning based on user provided labels with machine learning methodology
 */
public interface AlertFilterAutoTune {

  // Tune alert filter based on training data and model configuration
  // Returns the tuned alert filter configuration
  Map<String, String> tuneAlertFilter() throws Exception;

}
