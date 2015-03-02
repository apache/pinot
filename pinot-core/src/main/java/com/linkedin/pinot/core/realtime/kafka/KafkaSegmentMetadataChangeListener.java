package com.linkedin.pinot.core.realtime.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.data.manager.OfflineResourceDataManager;
import com.linkedin.pinot.core.realtime.SegmentMetadataChangeContext;
import com.linkedin.pinot.core.realtime.SegmentMetadataListener;

public class KafkaSegmentMetadataChangeListener implements SegmentMetadataListener {
  
  private static Logger LOG = LoggerFactory.getLogger(OfflineResourceDataManager.class);

  private KafkaRealtimeSegmentDataManager realtimeSegmentmanager;

  public KafkaSegmentMetadataChangeListener(KafkaRealtimeSegmentDataManager manager) {
    this.realtimeSegmentmanager = manager;
  }

  @Override
  public void onChange(SegmentMetadataChangeContext changeContext, SegmentMetadata segmentMetadata) {
    KafkaSegmentMetadata kafkaSegmentMetadata = (KafkaSegmentMetadata) segmentMetadata;
    //
    // different cases to handle here based on the current state of the
    // KafkaRealtimeSegmentDataManager and the information in segment metadata
    // case 1: manager is started/stopped, segment metadata has no end offset info. Do nothing
    // case 2: manager is started/stopped, segment metadata has end offset info. consume until the
    // end offset and stop consumption. Convert the segment into offline and reload it ( do this
    // only once).
    long startOffset = kafkaSegmentMetadata.getStartOffset();
    long endOffset = kafkaSegmentMetadata.getEndOffset();
    LOG.info("KafkaSegmentMetadataChangeListener.onChange()");
    if (endOffset > 0) {
      if (realtimeSegmentmanager.hasStarted() && !realtimeSegmentmanager.isClosed()) {
        // pause consumption, no-op if its already paused
        realtimeSegmentmanager.pause();
        long currentOffset = realtimeSegmentmanager.getCurrentOffset();
        if (currentOffset <= endOffset) {
          realtimeSegmentmanager.setEndOffset(endOffset);
          realtimeSegmentmanager.start();
          realtimeSegmentmanager.waitUntilOffset(endOffset);
          realtimeSegmentmanager.shutdown();
          realtimeSegmentmanager.convertToOffline();
        } else if (currentOffset > endOffset) {
          // clear the segment and restart consumption
          realtimeSegmentmanager.pause();
          // clear all datastructures and reset the current offset to start offset
          realtimeSegmentmanager.reset();
          realtimeSegmentmanager.setEndOffset(endOffset);
          realtimeSegmentmanager.start();
          realtimeSegmentmanager.waitUntilOffset(endOffset);
        }
      }
    }
  }
}
