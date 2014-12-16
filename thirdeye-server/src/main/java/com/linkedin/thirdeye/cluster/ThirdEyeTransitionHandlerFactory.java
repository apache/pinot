package com.linkedin.thirdeye.cluster;

import com.linkedin.thirdeye.api.StarTreeManager;
import org.apache.helix.api.StateTransitionHandlerFactory;
import org.apache.helix.api.id.PartitionId;

import java.io.File;
import java.net.URI;

public class ThirdEyeTransitionHandlerFactory extends StateTransitionHandlerFactory<ThirdEyeTransitionHandler>
{
  private final StarTreeManager starTreeManager;
  private final URI archiveSource;
  private final File rootDir;

  public ThirdEyeTransitionHandlerFactory(StarTreeManager starTreeManager,
                                          URI archiveSource,
                                          File rootDir)
  {
    this.starTreeManager = starTreeManager;
    this.archiveSource = archiveSource;
    this.rootDir = rootDir;
  }

  @Override
  public ThirdEyeTransitionHandler createStateTransitionHandler(PartitionId partitionId)
  {
    return new ThirdEyeTransitionHandler(partitionId, starTreeManager, archiveSource, rootDir);
  }
}
