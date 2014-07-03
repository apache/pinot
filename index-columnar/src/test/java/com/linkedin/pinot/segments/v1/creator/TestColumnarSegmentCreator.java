package com.linkedin.pinot.segments.v1.creator;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.linkedin.pinot.index.data.FieldSpec;
import com.linkedin.pinot.index.data.Schema;
import com.linkedin.pinot.index.data.FieldSpec.FieldType;
import com.linkedin.pinot.raw.record.readers.FileFormat;
import com.linkedin.pinot.raw.record.readers.RecordReader;
import com.linkedin.pinot.raw.record.readers.RecordReaderFactory;
import com.linkedin.pinot.segments.generator.SegmentGeneratorConfiguration;
import com.linkedin.pinot.segments.generator.SegmentVersion;


public class TestColumnarSegmentCreator {
  private final String AVRO_DATA = "data/sample_data.avro";
  private final String JSON_DATA = "data/sample_data.json";
  private final String AVRO_MULTI_DATA = "data/sample_data_multi_value.avro";

  @Before
  public void setup() throws Exception {
    String filePath = getClass().getClassLoader().getResource(AVRO_DATA).getFile();

    List<String> projectedColumns = new ArrayList<String>();
    projectedColumns.add("dim_campaignType");
    projectedColumns.add("sort_campaignId");

    SegmentGeneratorConfiguration config = new SegmentGeneratorConfiguration();
    config.setFileFormat(FileFormat.avro);
    config.setFilePath(filePath);
    config.setProjectedColumns(projectedColumns);
    config.setSegmentVersion(SegmentVersion.v1);
    
    config.setOutputDir("/home/dpatel/data/123");
    
    Schema schema = new Schema();
    for (String column : projectedColumns) {
      System.out.println(column);
      FieldSpec spec = new FieldSpec(column, FieldType.dimension, null, true);
      schema.addSchema(column, spec);
    }
    
    ColumnarSegmentCreator cr = new ColumnarSegmentCreator(RecordReaderFactory.get(config));
    cr.init(config);
    cr.buildSegment();
  }
  
  @Test
  public void test1() {
    
  }
}
