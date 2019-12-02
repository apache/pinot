package org.apache.pinot.thirdeye.detection.alert.scheme;

import org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterResult;


public class AnotherRandomAlerter extends DetectionAlertScheme {
  public AnotherRandomAlerter(DetectionAlertConfigDTO subsConfig, ThirdEyeAnomalyConfiguration thirdeyeConfig,
      DetectionAlertFilterResult result) {
    super(subsConfig, result);
  }

  @Override
  public void run() {

  }
}
