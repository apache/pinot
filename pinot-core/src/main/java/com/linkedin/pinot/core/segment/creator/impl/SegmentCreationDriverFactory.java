package com.linkedin.pinot.core.segment.creator.impl;

import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;


/**
 * Jun 28, 2014
 *
 * @author Dhaval Patel <dpatel@linkedin.com>
 *
 */
public class SegmentCreationDriverFactory {

  public static SegmentIndexCreationDriver get(SegmentVersion version) {
    return new SegmentIndexCreationDriverImpl();
  }
}
