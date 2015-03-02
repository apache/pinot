package com.linkedin.pinot.core.realtime;

import java.util.Map;

import com.linkedin.pinot.common.data.Schema;


public interface StreamProviderConfig {

  public void init(Map<String, String> properties, Schema schema) throws Exception;

  public Schema getSchema();
}
