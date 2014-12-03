package com.linkedin.thirdeye.cluster;

import com.linkedin.thirdeye.api.StarTreeManager;
import org.apache.helix.api.StateTransitionHandlerFactory;
import org.apache.helix.api.id.PartitionId;

import java.io.File;

public class ThirdEyeTransitionHandlerFactory extends StateTransitionHandlerFactory<ThirdEyeTransitionHandler>
{
  private final StarTreeManager starTreeManager;
  private final File rootDir;

  public ThirdEyeTransitionHandlerFactory(StarTreeManager starTreeManager, File rootDir)
  {
    this.starTreeManager = starTreeManager;
    this.rootDir = rootDir;
  }

  @Override
  public ThirdEyeTransitionHandler createStateTransitionHandler(PartitionId partitionId)
  {
    return new ThirdEyeTransitionHandler(partitionId, starTreeManager, rootDir);
  }
}
