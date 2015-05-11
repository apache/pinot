package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeRecordStore;
import com.linkedin.thirdeye.api.StarTreeRecordStoreFactory;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

public class StarTreeRecordStoreFactoryHashMapImpl implements StarTreeRecordStoreFactory {
  private StarTreeConfig starTreeConfig;
  private Properties recordStoreConfig;

  @Override
  public void init(File rootDir, StarTreeConfig starTreeConfig, Properties recordStoreConfig) throws IOException {
    this.starTreeConfig = starTreeConfig;
    this.recordStoreConfig = recordStoreConfig;
  }

  @Override
  public StarTreeRecordStore createRecordStore(UUID nodeId) throws IOException {
    return new StarTreeRecordStoreHashMapImpl(starTreeConfig);
  }
}
