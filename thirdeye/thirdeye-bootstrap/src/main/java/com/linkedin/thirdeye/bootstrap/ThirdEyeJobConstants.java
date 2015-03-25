package com.linkedin.thirdeye.bootstrap;

public enum ThirdEyeJobConstants
{
  THIRDEYE_PHASE("thirdeye.phase"),
  THIRDEYE_ROOT("thirdeye.root"),
  THIRDEYE_COLLECTION("thirdeye.collection"),
  THIRDEYE_SERVER_URI("thirdeye.server.uri"),
  THIRDEYE_STARTREE_BOOTSTRAP_PHASE2_REDUCERS("thirdeye.startree_bootstrap_phase2.reducers"),
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
