package com.linkedin.pinot.controller.helix.core.utils;

import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.StringUtil;

public class PinotHelixUtils {
  public static String constructPropertyStorePathForSegment(SegmentMetadata segmentMetadata) {
    return constructPropertyStorePathForSegment(segmentMetadata.getResourceName(), segmentMetadata.getName());
  }

  public static String constructPropertyStorePathForSegment(String resourceName, String segmentName) {
    return "/" + StringUtil.join("/", resourceName, segmentName);
  }
}
