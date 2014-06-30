package com.linkedin.pinot.server.partition;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.index.segment.IndexSegment;
import com.linkedin.pinot.server.conf.PartitionDataManagerConfig;
import com.linkedin.pinot.server.utils.SegmentLoader;


/**
 * An implemenation of offline parition.
 * Provide add and remove segment functionality.
 * 
 * @author xiafu
 *
 */
public class OfflinePartitionDataManager implements PartitionDataManager {

  private Map<String, TableDataManager> _tableDataManagerMap;
  private String _partitionDir;
  private PartitionDataManagerConfig _partitionDataManagerConfig;
  private Logger _logger = LoggerFactory.getLogger(OfflinePartitionDataManager.class);
  private boolean _isStarted = false;

  public OfflinePartitionDataManager() {
    _tableDataManagerMap = new HashMap<String, TableDataManager>();

  }

  @Override
  public void init(PartitionDataManagerConfig partitionDataManagerConfig) {
    _partitionDataManagerConfig = partitionDataManagerConfig;
    _partitionDir = _partitionDataManagerConfig.getPartitionDir();
    if (!new File(_partitionDir).exists()) {
      new File(_partitionDir).mkdirs();
    }
  }

  @Override
  public TableDataManager getTableDataManager(String tableName) {
    return _tableDataManagerMap.get(tableName);
  }

  @Override
  public void start() {
    bootstrapSegments();
    _isStarted = true;
  }

  private void bootstrapSegments() {
    File partitionDir = new File(_partitionDir);
    System.out.println("Bootstrap partition directory - " + partitionDir.getAbsolutePath());
    for (File segmentDir : partitionDir.listFiles()) {
      try {
        System.out.println("Bootstrap segment from directory - " + segmentDir.getAbsolutePath());
        IndexSegment indexSegment = SegmentLoader.loadIndexSegmentFromDir(segmentDir);
        addSegment(indexSegment);
      } catch (Exception e) {
        _logger.error("Unable to bootstrap segment in dir : " + segmentDir.getAbsolutePath());
      }

    }

  }

  @Override
  public void shutDown() {
    _tableDataManagerMap.clear();
    _partitionDataManagerConfig = null;
    _isStarted = false;

  }

  @Override
  public void addSegment(IndexSegment indexSegmentToAdd) {
    String tableName = indexSegmentToAdd.getSegmentMetadata().getTableName();
    System.out.println("Trying to add a new segment to table - " + tableName);
    if (!_tableDataManagerMap.containsKey(tableName)) {
      _tableDataManagerMap.put(tableName, new TableDataManager());
    }
    _tableDataManagerMap.get(tableName).addSegment(new SegmentDataManager(indexSegmentToAdd));
  }

  @Override
  public void removeSegment(String tableName, String indexSegmentToRemove) {
    _tableDataManagerMap.get(tableName).removeSegment(indexSegmentToRemove);
  }

  @Override
  public boolean isStarted() {
    return _isStarted;
  }
}
