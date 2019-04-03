package org.apache.pinot.core.segment.index;

import org.apache.pinot.common.segment.MutableSegmentMetadata;


public class MutableSegmentMetadataImpl extends SegmentMetadataImpl implements MutableSegmentMetadata {

  @Override
  public int getIndexingErrors() {
    return 0;
  }

  @Override
  public void setLatestIngestionTimestamp(long timestamp) {

  }

  @Override
  public void incrementIndexingErrors() {

  }
}
