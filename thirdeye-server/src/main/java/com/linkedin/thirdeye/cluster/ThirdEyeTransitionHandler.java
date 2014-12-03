package com.linkedin.thirdeye.cluster;

import com.linkedin.thirdeye.api.StarTreeManager;
import org.apache.helix.api.TransitionHandler;
import org.apache.helix.api.id.PartitionId;

public class ThirdEyeTransitionHandler extends TransitionHandler
{
  private final PartitionId partitionId;
  private final StarTreeManager starTreeManager;

  public ThirdEyeTransitionHandler(PartitionId partitionId, StarTreeManager starTreeManager)
  {
    this.partitionId = partitionId;
    this.starTreeManager = starTreeManager;
  }
}
