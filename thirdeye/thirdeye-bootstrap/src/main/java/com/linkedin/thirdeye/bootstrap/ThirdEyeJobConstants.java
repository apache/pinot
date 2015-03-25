package com.linkedin.thirdeye.bootstrap;

public enum ThirdEyeJobConstants
{
  THIRDEYE_PHASE("thirdeye.phase"),
  THIRDEYE_ROOT("thirdeye.root"),
  THIRDEYE_COLLECTION("thirdeye.collection"),
  THIRDEYE_SERVER_URI("thirdeye.server.uri"),
  INPUT_PATHS("input.paths");

  private final String propertyName;

  ThirdEyeJobConstants(String propertyName)
  {
    this.propertyName = propertyName;
  }

  public String getPropertyName()
  {
    return propertyName;
  }
}
