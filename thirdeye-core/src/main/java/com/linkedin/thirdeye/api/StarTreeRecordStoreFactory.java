package com.linkedin.thirdeye.api;

import java.util.List;
import java.util.Properties;
import java.util.UUID;

public interface StarTreeRecordStoreFactory
{
  void init(List<String> dimensionNames, List<String> metricNames, Properties config);

  StarTreeRecordStore createRecordStore(UUID nodeId);
}
