package org.apache.pinot.thirdeye.detection.alert.scheme;

import org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterResult;
import org.apache.pinot.thirdeye.notification.formatter.ADContentFormatterContext;


public class RandomAlerter extends DetectionAlertScheme {
  public RandomAlerter(ADContentFormatterContext adContext, ThirdEyeAnomalyConfiguration thirdeyeConfig,
      DetectionAlertFilterResult result) {
    super(adContext, result);
  }

  @Override
  public void run() {

  }
}
