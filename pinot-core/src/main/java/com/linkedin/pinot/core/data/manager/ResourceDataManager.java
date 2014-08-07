package com.linkedin.pinot.core.data.manager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.NamedThreadFactory;
import com.linkedin.pinot.core.data.manager.config.ResourceDataManagerConfig;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;


/**
 * ResourceDataManager will take a list of PartitionDataManager.
 * 
 * @author xiafu
 *
 */
public class ResourceDataManager {

  public static final Logger LOGGER = LoggerFactory.getLogger(ResourceDataManager.class);

  private ExecutorService _queryExecutorService;

  private final ResourceDataManagerConfig _resourceDataManagerConfig;
  private final String _resourceName;

  private List<PartitionDataManager> _partitionDataManagerList;
  private int[] _partitionsArray;
  private ReadMode _readMode;

  private boolean _isStarted = false;

  public ResourceDataManager(String resourceName, ResourceDataManagerConfig resourceDataManagerConfig) {
    _resourceDataManagerConfig = resourceDataManagerConfig;
    _resourceName = resourceName;
  }

  public void init() throws ConfigurationException {
    LOGGER.info("Trying to initialize resource : " + _resourceName);
    _queryExecutorService =
        Executors.newCachedThreadPool(new NamedThreadFactory("parallel-query-executor-" + _resourceName));
    _partitionsArray = _resourceDataManagerConfig.getPartitionArray();
    _partitionDataManagerList = new ArrayList<PartitionDataManager>();
    _readMode = ReadMode.valueOf(_resourceDataManagerConfig.getReadMode());
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
    LOGGER.info("Trying to shutdown resource : " + _resourceName);
    if (_isStarted) {
      _queryExecutorService.shutdown();
      for (PartitionDataManager partitionDataManager : _partitionDataManagerList) {
        partitionDataManager.shutDown();
      }
      _partitionDataManagerList.clear();
      _partitionsArray = null;
      _isStarted = false;
    } else {
      LOGGER.warn("Already shutDown resource : " + _resourceName);
    }

  }

  public void start() {
    LOGGER.info("Trying to start resource : " + _resourceName);
    if (_isStarted) {
      LOGGER.warn("Already started resource : " + _resourceName);
    } else {
      for (PartitionDataManager partitionDataManager : _partitionDataManagerList) {
        partitionDataManager.start();
      }
      _isStarted = true;
    }
  }

  public boolean isStarted() {
    return _isStarted;
  }

  public List<PartitionDataManager> getPartitionDataManagerList() {
    return _partitionDataManagerList;
  }

  public void addSegment(SegmentMetadata segmentMetadata) {
    IndexSegment indexSegment = ColumnarSegmentLoader.loadSegment(segmentMetadata, _readMode);

  }
}
