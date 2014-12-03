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
  private final File tmpDir;

  public ThirdEyeTransitionHandlerFactory(StarTreeManager starTreeManager,
                                          ThirdEyeExternalDataSource externalDataSource,
                                          File rootDir,
                                          File tmpDir)
  {
    this.starTreeManager = starTreeManager;
    this.externalDataSource = externalDataSource;
    this.rootDir = rootDir;
    this.tmpDir = tmpDir;
  }

  @Override
  public ThirdEyeTransitionHandler createStateTransitionHandler(PartitionId partitionId)
  {
    return new ThirdEyeTransitionHandler(partitionId, starTreeManager, externalDataSource, rootDir, tmpDir);
  }
}
