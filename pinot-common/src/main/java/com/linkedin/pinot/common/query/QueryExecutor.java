package com.linkedin.pinot.common.query;

import org.apache.commons.configuration.Configuration;

import com.linkedin.pinot.common.data.DataManager;
import com.linkedin.pinot.common.query.request.Request;
import com.linkedin.pinot.common.query.response.InstanceResponse;


public interface QueryExecutor {
  public void init(Configuration queryExecutorConfig, DataManager dataManager);

  public InstanceResponse processQuery(Request request);
}
