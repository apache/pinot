package com.linkedin.pinot.core.data.manager.config;

import org.apache.commons.configuration.Configuration;


/**
 * The config used for PartitionDataManager.
 * 
 * @author xiafu
 *
 */
public class PartitionDataManagerConfig {

  private Configuration _partitionDataManagerConfiguration = null;

  public PartitionDataManagerConfig(Configuration partitionConfig) {
    _partitionDataManagerConfiguration = partitionConfig;
  }

  public Configuration getConfig() {
    return _partitionDataManagerConfiguration;
  }

  public int getPartitionId() {
    return _partitionDataManagerConfiguration.getInt("id");
  }

  public String getPartitionType() {
    return _partitionDataManagerConfiguration.getString("type");
  }

  public String getPartitionDir() {
    return _partitionDataManagerConfiguration.getString("directory");
  }
}
