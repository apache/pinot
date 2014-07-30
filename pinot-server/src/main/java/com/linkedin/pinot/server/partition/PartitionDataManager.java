package com.linkedin.pinot.server.partition;

import java.util.List;

import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.server.conf.PartitionDataManagerConfig;


/**
 * PartitionDataManager interface.
 * Provided interfaces to get TableDataManager and operate on segment level.
 * 
 * @author xiafu
 *
 */
public interface PartitionDataManager {

  /**
   * Initialize PartitionDataManager based on given config.
   * 
   * @param partitionConfig
   */
  public void init(PartitionDataManagerConfig partitionConfig);

  public void start();

  public void shutDown();

  public boolean isStarted();

  /**
   * Adding an IndexSegment into the partition.
   *  
   * @param indexSegmentToAdd
   */
  public void addSegment(IndexSegment indexSegmentToAdd);

  /**
   * Remove a segment from the partition based on segmentName.
   * @param indexSegmentToRemove
   */
  public void removeSegment(String indexSegmentToRemove);

  /**
   * 
   * @return all the segments in this partition.
   */
  public List<SegmentDataManager> getAllSegments();
}
