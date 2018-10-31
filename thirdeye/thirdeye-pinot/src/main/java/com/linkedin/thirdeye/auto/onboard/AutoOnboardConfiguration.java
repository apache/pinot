package com.linkedin.thirdeye.auto.onboard;

import java.util.concurrent.TimeUnit;

import com.linkedin.thirdeye.api.TimeGranularity;

public class AutoOnboardConfiguration {

  private TimeGranularity runFrequency = new TimeGranularity(15, TimeUnit.MINUTES);

  public TimeGranularity getRunFrequency() {
    return runFrequency;
  }

  public void setRunFrequency(TimeGranularity runFrequency) {
    this.runFrequency = runFrequency;
  }


}
