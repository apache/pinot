package com.linkedin.thirdeye.cluster;

import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.data.ThirdEyeExternalDataSource;
import org.apache.helix.api.StateTransitionHandlerFactory;
import org.apache.helix.api.id.PartitionId;

import java.io.File;

public class ThirdEyeTransitionHandlerFactory extends StateTransitionHandlerFactory<ThirdEyeTransitionHandler>
{
  private final StarTreeManager starTreeManager;
  private final ThirdEyeExternalDataSource externalDataSource;
  private final File rootDir;

  public ThirdEyeTransitionHandlerFactory(StarTreeManager starTreeManager,
                                          ThirdEyeExternalDataSource externalDataSource,
                                          File rootDir)
  {
    this.starTreeManager = starTreeManager;
    this.externalDataSource = externalDataSource;
    this.rootDir = rootDir;
  }

  @Override
  public ThirdEyeTransitionHandler createStateTransitionHandler(PartitionId partitionId)
  {
    return new ThirdEyeTransitionHandler(partitionId, starTreeManager, externalDataSource, rootDir);
  }
}
