package com.linkedin.thirdeye.impl.storage;

import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeRecordStore;
import com.linkedin.thirdeye.api.StarTreeRecordStoreFactory;
import com.linkedin.thirdeye.impl.StarTreeRecordStoreFactoryLogBufferImpl;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

public class StarTreeRecordStoreFactoryCompositeImpl implements StarTreeRecordStoreFactory
{
  private StarTreeRecordStoreFactory immutableFactory = new StarTreeRecordStoreFactoryFixedImpl();
  private StarTreeRecordStoreFactory mutableFactory = new StarTreeRecordStoreFactoryLogBufferImpl();

  private StarTreeConfig starTreeConfig;

  @Override
  public void init(File rootDir, StarTreeConfig starTreeConfig, Properties recordStoreConfig) throws IOException
  {
    this.starTreeConfig = starTreeConfig;
    this.immutableFactory.init(rootDir, starTreeConfig, recordStoreConfig);
    this.mutableFactory.init(rootDir, starTreeConfig, recordStoreConfig);
  }

  @Override
  public StarTreeRecordStore createRecordStore(UUID nodeId) throws IOException
  {
    StarTreeRecordStore immutableStore = immutableFactory.createRecordStore(nodeId);
    StarTreeRecordStore mutableStore = mutableFactory.createRecordStore(nodeId);
    return new StarTreeRecordStoreCompositeImpl(starTreeConfig, immutableStore, mutableStore);
  }
}
