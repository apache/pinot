package com.linkedin.pinot.core.chunk.creator.impl;

import com.linkedin.pinot.core.chunk.creator.ChunkIndexCreationDriver;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;


/**
 * Jun 28, 2014
 *
 * @author Dhaval Patel <dpatel@linkedin.com>
 *
 */
public class SegmentCreationDriverFactory {

  public static ChunkIndexCreationDriver get(SegmentVersion version) {
    return new ChunkIndexCreationDriverImpl();
  }
}
