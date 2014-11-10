package com.linkedin.thirdeye.bootstrap;

import com.linkedin.thirdeye.api.StarTreeRecordStore;
import com.linkedin.thirdeye.api.StarTreeRecordStoreFactory;

import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class StarTreeRecordStoreFactoryCircularBufferHdfsImpl implements StarTreeRecordStoreFactory
{
  @Override
  public void init(List<String> dimensionNames, List<String> metricNames, Properties config)
  {

  }

  @Override
  public StarTreeRecordStore createRecordStore(UUID nodeId)
  {
    return null;
  }
}
