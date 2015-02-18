package com.linkedin.pinot.server.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.indexsegment.utils.AvroUtils;


public class SegmentTestUtils {

  public static SegmentGeneratorConfig getSegmentGenSpecWithSchemAndProjectedColumns(File inputAvro, File outputDir,
      String timeColumn, TimeUnit timeUnit, String clusterName, String tableName) throws FileNotFoundException,
      IOException {
    SegmentGeneratorConfig segmentGenSpec = new SegmentGeneratorConfig(AvroUtils.extractSchemaFromAvro(inputAvro));
    List<String> projectedColumns = AvroUtils.getAllColumnsInAvroFile(inputAvro);
    segmentGenSpec.setInputFilePath(inputAvro.getAbsolutePath());
    segmentGenSpec.setTimeUnitForSegment(timeUnit);
    segmentGenSpec.setInputFileFormat(FileFormat.AVRO);
    segmentGenSpec.setSegmentVersion(SegmentVersion.v1);
    segmentGenSpec.setResourceName(clusterName);
    segmentGenSpec.setTableName(tableName);
    segmentGenSpec.setIndexOutputDir(outputDir.getAbsolutePath());
    return segmentGenSpec;
  }
}
