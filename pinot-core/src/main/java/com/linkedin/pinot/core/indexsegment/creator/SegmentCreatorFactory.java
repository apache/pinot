package com.linkedin.pinot.core.indexsegment.creator;

import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.indexsegment.columnar.creator.ColumnarSegmentCreator;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;

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
