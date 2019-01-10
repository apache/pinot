package com.linkedin.thirdeye.detection.alert.scheme;

import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import com.linkedin.thirdeye.detection.alert.DetectionAlertFilterResult;


public class RandomAlerter extends DetectionAlertScheme {
  public RandomAlerter(DetectionAlertConfigDTO config, ThirdEyeAnomalyConfiguration thirdeyeConfig,
      DetectionAlertFilterResult result) {
    super(config, result);
  }

  @Override
  public void run() {

  }
}
