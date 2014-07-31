package com.linkedin.pinot.core.data.manager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.utils.NamedThreadFactory;
import com.linkedin.pinot.core.data.manager.config.ResourceDataManagerConfig;


/**
 * ResourceDataManager will take a list of PartitionDataManager.
 * 
 * @author xiafu
 *
 */
public class ResourceDataManager {

  public static final Logger LOGGER = LoggerFactory.getLogger(ResourceDataManager.class);

  private ExecutorService _queryExecutorService = Executors.newCachedThreadPool(new NamedThreadFactory(
      "parallel-query-executor"));

  private List<PartitionDataManager> _partitionDataManagerList;
  private ResourceDataManagerConfig _resourceDataManagerConfig;
  private int[] _partitionsArray;

  private boolean _isStarted = false;

  public ResourceDataManager(ResourceDataManagerConfig resourceDataManagerConfig) {
    _resourceDataManagerConfig = resourceDataManagerConfig;
  }

  public void init() throws ConfigurationException {
    _partitionsArray = _resourceDataManagerConfig.getPartitionArray();
    _partitionDataManagerList = new ArrayList<PartitionDataManager>();
    for (int i : _partitionsArray) {
      PartitionDataManager partitionDataManager =
          PartitionProvider.getPartitionDataManager(_resourceDataManagerConfig.getPartitionConfig(i));
      _partitionDataManagerList.add(partitionDataManager);
    }
  }

  public PartitionDataManager getPartitionDataManager(int partitionId) {
    int idx = Arrays.binarySearch(_partitionsArray, partitionId);
    if (idx >= 0) {
      return _partitionDataManagerList.get(idx);
    }
    return null;
  }

  public void shutDown() {
    _queryExecutorService.shutdown();
    for (PartitionDataManager partitionDataManager : _partitionDataManagerList) {
      partitionDataManager.shutDown();
    }
    _partitionDataManagerList.clear();
    _partitionsArray = null;
    _isStarted = false;

  }

  public void start() {
    for (PartitionDataManager partitionDataManager : _partitionDataManagerList) {
      partitionDataManager.start();
    }
    _isStarted = true;
  }

  public boolean isStarted() {
    return _isStarted;
  }

  public List<PartitionDataManager> getPartitionDataManagerList() {
    return _partitionDataManagerList;
  }
}
