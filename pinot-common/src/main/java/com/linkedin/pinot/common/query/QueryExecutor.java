package com.linkedin.pinot.common.query;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;

import com.linkedin.pinot.common.data.DataManager;
import com.linkedin.pinot.common.request.InstanceRequest;
import com.linkedin.pinot.common.utils.DataTable;


public interface QueryExecutor {
  public void init(Configuration queryExecutorConfig, DataManager dataManager) throws ConfigurationException;

  public void start();

  public DataTable processQuery(InstanceRequest instanceRequest);

  public void shutDown();

  boolean isStarted();

  void updateResourceTimeOutInMs(String resource, long timeOutMs);
}
