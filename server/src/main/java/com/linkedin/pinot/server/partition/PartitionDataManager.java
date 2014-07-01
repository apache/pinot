package com.linkedin.pinot.server.partition;

import com.linkedin.pinot.index.segment.IndexSegment;
import com.linkedin.pinot.server.conf.PartitionDataManagerConfig;


/**
 * PartitionDataManager interface.
 * Provided interfaces to get TableDataManager and operate on segment level.
 * 
 * @author xiafu
 *
 */
public interface PartitionDataManager {

  public TableDataManager getTableDataManager(String tableName);

  public void start();

  public void shutDown();

  public void addSegment(IndexSegment indexSegmentToAdd);

  public void init(PartitionDataManagerConfig partitionConfig);

  public boolean isStarted();

  public void removeSegment(String tableName, String indexSegmentToRemove);
}
