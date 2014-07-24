package com.linkedin.pinot.segments.v1.creator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import com.linkedin.pinot.index.time.SegmentTimeUnit;
import com.linkedin.pinot.raw.record.readers.FileFormat;
import com.linkedin.pinot.segments.generator.SegmentGeneratorConfiguration;
import com.linkedin.pinot.segments.generator.SegmentVersion;
import com.linkedin.pinot.segments.utils.AvroUtils;


public class SegmentTestUtils {

  public static SegmentGeneratorConfiguration getSegmentGenSpecWithSchemAndProjectedColumns(File inputAvro,
      File outputDir, String timeColumn, SegmentTimeUnit timeUnit, String clusterName, String tableName)
      throws FileNotFoundException, IOException {
    SegmentGeneratorConfiguration segmentGenSpec = new SegmentGeneratorConfiguration();
    List<String> projectedColumns = AvroUtils.getAllColumnsInAvroFile(inputAvro);
    segmentGenSpec.setFileName(inputAvro.getAbsolutePath());
    segmentGenSpec.setProjectedColumns(projectedColumns);
    segmentGenSpec.setSchema(AvroUtils.extractSchemaFromAvro(inputAvro));
    segmentGenSpec.setSegmentTimeUnit(timeUnit);
    segmentGenSpec.setFileFormat(FileFormat.avro);
    segmentGenSpec.setSegmentVersion(SegmentVersion.v1);
    segmentGenSpec.setResourceName(clusterName);
    segmentGenSpec.setTableName(tableName);
    segmentGenSpec.setOutputDir(outputDir.getAbsolutePath());
    return segmentGenSpec;
  }
}
