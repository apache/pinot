package com.linkedin.pinot.segments.creator;

import com.linkedin.pinot.raw.record.readers.RecordReader;
import com.linkedin.pinot.segments.generator.SegmentVersion;
import com.linkedin.pinot.segments.v1.creator.ColumnarSegmentCreator;

/**
 * Jun 28, 2014
 *
 * @author Dhaval Patel <dpatel@linkedin.com>
 * 
 */
public class SegmentCreatorFactory {

  public static SegmentCreator get(SegmentVersion version) {
    return new ColumnarSegmentCreator();
  }
  
  public static SegmentCreator get(SegmentVersion version, RecordReader reader) {
    return new ColumnarSegmentCreator(version, reader);
  }
}
