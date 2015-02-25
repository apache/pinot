package com.linkedin.pinot.core.realtime;

import com.linkedin.pinot.common.data.Schema;

/**
 *
 */

public class RealtimeIndexingConfig {
  Schema schema;
  
  String resourceName;
  
  String tableName;
  
  String streamProviderType;
  
  StreamProviderConfig streamProviderConfig;
  
}
