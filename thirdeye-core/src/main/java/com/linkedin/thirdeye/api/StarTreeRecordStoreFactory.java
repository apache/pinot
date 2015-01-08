package com.linkedin.thirdeye.api;

import java.io.File;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public interface StarTreeRecordStoreFactory
{
  void init(File rootDir, List<String> dimensionNames, List<String> metricNames, List<String> metricTypes, Properties config);

  List<String> getDimensionNames();

  List<String> getMetricNames();

  List<String> getMetricTypes();
  
  Properties getConfig();

  StarTreeRecordStore createRecordStore(UUID nodeId);
}
