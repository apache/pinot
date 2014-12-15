package com.linkedin.pinot.segments.v1.creator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.indexsegment.utils.AvroUtils;
import com.linkedin.pinot.core.time.SegmentTimeUnit;


public class SegmentTestUtils {

  public static SegmentGeneratorConfig getSegmentGenSpecWithSchemAndProjectedColumns(File inputAvro,
      File outputDir, String timeColumn, SegmentTimeUnit timeUnit, String clusterName, String tableName)
      throws FileNotFoundException, IOException {
    final SegmentGeneratorConfig segmentGenSpec = new SegmentGeneratorConfig();
    final List<String> projectedColumns = AvroUtils.getAllColumnsInAvroFile(inputAvro);
    segmentGenSpec.setInputFilePath(inputAvro.getAbsolutePath());
    segmentGenSpec.setProjectedColumns(projectedColumns);
    segmentGenSpec.setSchema(AvroUtils.extractSchemaFromAvro(inputAvro));
    segmentGenSpec.setTimeColumnName(timeColumn);
    segmentGenSpec.setTimeUnitForSegment(timeUnit);
    segmentGenSpec.setInputFileFormat(FileFormat.avro);
    segmentGenSpec.setSegmentVersion(SegmentVersion.v1);
    segmentGenSpec.setResourceName(clusterName);
    segmentGenSpec.setTableName(tableName);
    segmentGenSpec.setIndexOutputDir(outputDir.getAbsolutePath());
    return segmentGenSpec;
  }
}
