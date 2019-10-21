package org.apache.pinot.thirdeye.detection.alert.scheme;

import org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterResult;
import org.apache.pinot.thirdeye.notification.formatter.ADContentFormatterContext;


public class AnotherRandomAlerter extends DetectionAlertScheme {
  public AnotherRandomAlerter(ADContentFormatterContext adContext, ThirdEyeAnomalyConfiguration thirdeyeConfig,
      DetectionAlertFilterResult result) {
    super(adContext, result);
  }

  @Override
  public void run() {

  }
}
