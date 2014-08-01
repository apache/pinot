package com.linkedin.pinot.common.data;

import org.apache.commons.configuration.Configuration;

import com.linkedin.pinot.common.segment.SegmentMetadata;


public interface DataManager {
  void init(Configuration dataManagerConfig);

  void start();

  void addSegment(SegmentMetadata segmentMetadata);

  void removeSegment(String segmentName);

  void refreshSegment(String oldSegmentName, SegmentMetadata newSegmentMetadata);

  void shutDown();
}
