package com.linkedin.pinot.core.realtime;

import com.linkedin.pinot.common.data.Schema;

public interface StreamProviderConfig {

  public Schema getSchema();
}
