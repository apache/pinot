package com.linkedin.thirdeye.cluster;

import com.linkedin.thirdeye.api.StarTreeManager;
import org.apache.helix.api.StateTransitionHandlerFactory;
import org.apache.helix.api.id.PartitionId;

public class ThirdEyeTransitionHandlerFactory extends StateTransitionHandlerFactory<ThirdEyeTransitionHandler>
{
  private final StarTreeManager starTreeManager;

  public ThirdEyeTransitionHandlerFactory(StarTreeManager starTreeManager)
  {
    this.starTreeManager = starTreeManager;
  }

  @Override
  public ThirdEyeTransitionHandler createStateTransitionHandler(PartitionId partitionId)
  {
    return new ThirdEyeTransitionHandler(partitionId, starTreeManager);
  }
}
