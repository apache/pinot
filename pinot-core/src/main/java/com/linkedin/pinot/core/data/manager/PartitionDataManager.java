package com.linkedin.pinot.core.data.manager;

import java.util.List;

import com.linkedin.pinot.core.data.manager.config.PartitionDataManagerConfig;
import com.linkedin.pinot.core.indexsegment.IndexSegment;


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
