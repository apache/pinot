package com.linkedin.thirdeye.anomaly;

import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClient;

public class TaskContext {

  PinotThirdEyeClient thirdEyeClient;

  public PinotThirdEyeClient getThirdEyeClient() {
    return thirdEyeClient;
  }
}
