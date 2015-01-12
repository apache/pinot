package com.linkedin.thirdeye.api;

import java.io.File;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public interface StarTreeRecordStoreFactory
{
  void init(File rootDir, StarTreeConfig starTreeConfig, Properties recordStoreConfig);

  StarTreeRecordStore createRecordStore(UUID nodeId);
}
