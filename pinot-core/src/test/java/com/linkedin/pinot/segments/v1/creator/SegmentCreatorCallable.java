package com.linkedin.pinot.segments.v1.creator;

import java.io.File;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.core.data.readers.RecordReaderFactory;
import com.linkedin.pinot.core.indexsegment.columnar.creator.ColumnarSegmentCreator;
import com.linkedin.pinot.core.indexsegment.creator.SegmentCreatorFactory;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfiguration;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.time.SegmentTimeUnit;


/**
* @author Dhaval Patel<dpatel@linkedin.com>
* Aug 15, 2014
*/

public class SegmentCreatorCallable implements Callable<String> {
  private static final Logger logger = LoggerFactory.getLogger(SegmentCreatorCallable.class);

  File avro;
  File baseDir;
  String segmentName;

  public SegmentCreatorCallable(File avro, File baseSegmentDir, String segmentName) {
    this.avro = avro;
    this.baseDir = baseSegmentDir;
    this.segmentName = segmentName;
  }

  @Override
  public String call() throws Exception {
    try {
      File outDir = new File(baseDir, segmentName);
      SegmentGeneratorConfiguration config =
          SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(avro, outDir, "daysSinceEpoch",
              SegmentTimeUnit.days, "mirror", "mirror");
      ColumnarSegmentCreator creator =
          (ColumnarSegmentCreator) SegmentCreatorFactory.get(SegmentVersion.v1, RecordReaderFactory.get(config));
      creator.init(config);
      creator.buildSegment();
    } catch (Exception e) {
      logger.error(e.getMessage());
      return e.getMessage();
    }

    return "done with " + segmentName;
  }

}
