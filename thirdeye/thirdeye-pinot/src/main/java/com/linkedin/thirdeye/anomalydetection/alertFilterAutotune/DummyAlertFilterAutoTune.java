package com.linkedin.thirdeye.anomalydetection.alertFilterAutotune;

import java.util.Collections;
import java.util.Map;


public class DummyAlertFilterAutoTune extends BaseAlertFilterAutoTune {

  @Override
  public Map<String, String> tuneAlertFilter() throws Exception {
    // do nothing and return empty map
    return Collections.emptyMap();
  }


}
