package com.linkedin.pinot.core.realtime;

import com.linkedin.pinot.common.segment.SegmentMetadata;

/**
 * Allows one to listen to segment metadata changes
 * 
 * @author kgopalak
 *
 */
public interface SegmentMetadataListener {
  /**
   * This method is invoked in three scenarios <br/>
   * when the listener is initialized <br/>
   * when the segment metadata is changed on ZK <br/>
   * when the listener is removed <br/>
   * Dont see the need to have different methods for each type of call back, its
   * better to have the logic idempotent across the three scenarios. If needed
   * we can add the change type in the changecontext
   * 
   * @param segmentMetadata
   */
  public void onChange(SegmentMetadataChangeContext changeContext,
      SegmentMetadata segmentMetadata);

}
