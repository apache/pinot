package com.linkedin.pinot.index.persist;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.json.JSONObject;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.extractors.FieldExtractorFactory;
import com.linkedin.pinot.core.data.readers.AvroRecordReader;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.core.indexsegment.generator.ChunkGeneratorConfiguration;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;


public class TestAvroDataPublisher {

  private final String AVRO_DATA = "data/sample_data.avro";
  private final String JSON_DATA = "data/sample_data.json";
  private final String AVRO_MULTI_DATA = "data/sample_data_multi_value.avro";

  //  @Test
  //  public void TestReadAvro() throws Exception {
  //    Configuration fieldSpec = new PropertiesConfiguration();
  //    fieldSpec.addProperty("data.input.format", "Avro");
  //    String filePath = getClass().getClassLoader().getResource(AVRO_DATA).getFile();
  //    String jsonPath = getClass().getClassLoader().getResource(JSON_DATA).getFile();
  //    fieldSpec.addProperty("data.input.file.path", filePath);
  //    AvroDataReader avroDataPublisher = new AvroDataReader(fieldSpec);
  //    avroDataPublisher.getNextIndexableRow();
  //    int cnt = 0;
  //    for (String line : FileUtils.readLines(new File(jsonPath))) {
  //
  //      JSONObject obj = new JSONObject(line);
  //      if (avroDataPublisher.hasNext()) {
  //        GenericRow recordRow = avroDataPublisher.getNextIndexableRow();
  //
  //        for (String column : recordRow.getFieldNames()) {
  //          String valueFromJson = obj.get(column).toString();
  //          String valueFromAvro = recordRow.getValue(column).toString();
  //          if (cnt > 1) {
  //            assertEquals(valueFromJson, valueFromAvro);
  //          }
  //        }
  //      }
  //      cnt++;
  //    }
  //    assertEquals(cnt, 10000);
  //  }

  @Test
  public void TestReadPartialAvro() throws Exception {
    final String filePath = getClass().getClassLoader().getResource(AVRO_DATA).getFile();
    final String jsonPath = getClass().getClassLoader().getResource(JSON_DATA).getFile();

    final List<String> projectedColumns = new ArrayList<String>();
    projectedColumns.add("dim_campaignType");
    projectedColumns.add("sort_campaignId");

    final ChunkGeneratorConfiguration config = new ChunkGeneratorConfiguration();
    config.setInputFileFormat(FileFormat.avro);
    config.setInputFilePath(filePath);

    config.setProjectedColumns(projectedColumns);
    config.setSegmentVersion(SegmentVersion.v1);

    final Schema schema = new Schema();
    for (final String column : projectedColumns) {
      final FieldSpec spec = new FieldSpec(column, FieldType.dimension, null, true);
      schema.addSchema(column, spec);
    }

    final AvroRecordReader avroDataPublisher = new AvroRecordReader(FieldExtractorFactory.get(config), config.getInputFilePath());
    avroDataPublisher.next();
    int cnt = 0;
    for (final String line : FileUtils.readLines(new File(jsonPath))) {

      final JSONObject obj = new JSONObject(line);
      if (avroDataPublisher.hasNext()) {
        final GenericRow recordRow = avroDataPublisher.next();
        // System.out.println(recordRow);
        AssertJUnit.assertEquals(2, recordRow.getFieldNames().length);
        for (final String column : recordRow.getFieldNames()) {
          final String valueFromJson = obj.get(column).toString();
          final String valueFromAvro = recordRow.getValue(column).toString();
          if (cnt > 1) {
            AssertJUnit.assertEquals(valueFromJson, valueFromAvro);
          }
        }
      }
      cnt++;
    }
    AssertJUnit.assertEquals(cnt, 10000);
  }

  //  @Test
  //  public void TestReadMultiValueAvro() throws Exception {
  //    Configuration fieldSpec = new PropertiesConfiguration();
  //    fieldSpec.addProperty("data.input.format", "Avro");
  //    String filePath = getClass().getClassLoader().getResource(AVRO_MULTI_DATA).getFile();
  //    fieldSpec.addProperty("data.input.file.path", filePath);
  //
  //    AvroDataReader avroDataPublisher = new AvroDataReader();
  //    int cnt = 0;
  //
  //    while (avroDataPublisher.hasNext()) {
  //      GenericRow recordRow = avroDataPublisher.getNextIndexableRow();
  //      for (String column : recordRow.getFieldNames()) {
  //        String valueStringFromAvro = null;
  //        if (avroDataPublisher.getSchema().isSingleValueColumn(column)) {
  //          Object valueFromAvro = recordRow.getValue(column);
  //          valueStringFromAvro = valueFromAvro.toString();
  //        } else {
  //          Object[] valueFromAvro = (Object[]) recordRow.getValue(column);
  //          valueStringFromAvro = "[";
  //          int i = 0;
  //          for (Object valueObject : valueFromAvro) {
  //            if (i++ == 0) {
  //              valueStringFromAvro += valueObject.toString();
  //            } else {
  //              valueStringFromAvro += ", " + valueObject.toString();
  //            }
  //          }
  //          valueStringFromAvro += "]";
  //        }
  //
  //      }
  //      cnt++;
  //    }
  //    assertEquals(cnt, 28949);
  //  }
}
