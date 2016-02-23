package com.linkedin.thirdeye.api;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

public interface StarTreeRecordStoreFactory {
  void init(File rootDir, StarTreeConfig starTreeConfig, Properties recordStoreConfig)
      throws IOException;

  StarTreeRecordStore createRecordStore(UUID nodeId) throws IOException;

  void close() throws IOException;
}
