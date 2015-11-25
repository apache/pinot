package com.linkedin.thirdeye.bootstrap.partition;

import com.linkedin.thirdeye.api.StarTreeConfig;


public class PartitionPhaseConfig {

  public PartitionPhaseConfig() {
    super();
  }

  public static PartitionPhaseConfig fromStarTreeConfig(StarTreeConfig config)
  {
    return new PartitionPhaseConfig();
  }
}
