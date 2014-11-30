package com.linkedin.pinot.common.data;

import org.apache.commons.configuration.Configuration;

import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.segment.SegmentMetadataLoader;


public interface DataManager {
  void init(Configuration dataManagerConfig);

  void start();

  void addSegment(SegmentMetadata segmentMetadata) throws Exception;

  void removeSegment(String segmentName);

  void refreshSegment(String oldSegmentName, SegmentMetadata newSegmentMetadata);

  void shutDown();

  String getSegmentDataDirectory();

  String getSegmentFileDirectory();

  SegmentMetadataLoader getSegmentMetadataLoader();

  SegmentMetadata getSegmentMetadata(String resource, String segmentName);
}
