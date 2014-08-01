package com.linkedin.pinot.common.query;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;

import com.linkedin.pinot.common.data.DataManager;
import com.linkedin.pinot.common.query.request.Request;
import com.linkedin.pinot.common.query.response.InstanceResponse;


public interface QueryExecutor {
  public void init(Configuration queryExecutorConfig, DataManager dataManager) throws ConfigurationException;

  public void start();

  public InstanceResponse processQuery(Request request);

  public void shutDown();
}
