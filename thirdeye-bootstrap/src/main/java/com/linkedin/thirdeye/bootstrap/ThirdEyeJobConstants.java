package com.linkedin.thirdeye.bootstrap;

public enum ThirdEyeJobConstants
{
  THIRDEYE_ROOT("thirdeye.root"),
  THIRDEYE_COLLECTION("thirdeye.collection"),
  INPUT_TIME_MIN("input.time.min"),
  INPUT_TIME_MAX("input.time.max"),
  INPUT_PATHS("input.paths");

  private final String propertyName;

  ThirdEyeJobConstants(String propertyName)
  {
    this.propertyName = propertyName;
  }

  String getPropertyName()
  {
    return propertyName;
  }
}
