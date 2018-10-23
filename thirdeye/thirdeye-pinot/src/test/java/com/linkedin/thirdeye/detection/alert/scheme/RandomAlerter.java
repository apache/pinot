package com.linkedin.thirdeye.detection.alert.scheme;

import com.linkedin.thirdeye.anomaly.task.TaskContext;
import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import com.linkedin.thirdeye.detection.alert.DetectionAlertFilterResult;


public class RandomAlerter extends DetectionAlertScheme {
  public RandomAlerter(DetectionAlertConfigDTO config, TaskContext taskContext, DetectionAlertFilterResult result) {
    super(config, result);
  }

  @Override
  public void run() {

  }
}
