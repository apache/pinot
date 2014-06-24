package com.linkedin.pinot.index.persist;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.json.JSONObject;
import org.junit.Test;

import com.linkedin.pinot.index.data.GenericRow;


public class TestAvroDataPublisher {

  private final String AVRO_DATA = "data/sample_data.avro";
  private final String JSON_DATA = "data/sample_data.json";
  private final String AVRO_MULTI_DATA = "data/sample_data_multi_value.avro";

  @Test
  public void TestReadAvro() throws Exception {
    Configuration fieldSpec = new PropertiesConfiguration();
    fieldSpec.addProperty("data.input.format", "Avro");
    String filePath = getClass().getClassLoader().getResource(AVRO_DATA).getFile();
    String jsonPath = getClass().getClassLoader().getResource(JSON_DATA).getFile();
    fieldSpec.addProperty("data.input.file.path", filePath);
    AvroDataReader avroDataPublisher = new AvroDataReader(fieldSpec);
    avroDataPublisher.getNextIndexableRow();
    int cnt = 0;
    for (String line : FileUtils.readLines(new File(jsonPath))) {

      JSONObject obj = new JSONObject(line);
      if (avroDataPublisher.hasNext()) {
        GenericRow recordRow = avroDataPublisher.getNextIndexableRow();

        for (String column : recordRow.getFieldNames()) {
          String valueFromJson = obj.get(column).toString();
          String valueFromAvro = recordRow.getValue(column).toString();
          if (cnt > 1) {
            assertEquals(valueFromJson, valueFromAvro);
          }
        }
      }
      cnt++;
    }
    assertEquals(cnt, 10000);
  }

  @Test
  public void TestReadPartialAvro() throws Exception {
    Configuration fieldSpec = new PropertiesConfiguration();
    fieldSpec.addProperty("data.input.format", "Avro");
    String filePath = getClass().getClassLoader().getResource(AVRO_DATA).getFile();
    String jsonPath = getClass().getClassLoader().getResource(JSON_DATA).getFile();
    fieldSpec.addProperty("data.input.file.path", filePath);
    List<String> projectedColumns = new ArrayList<String>();
    projectedColumns.add("dim_campaignType");
    projectedColumns.add("sort_campaignId");
    fieldSpec.addProperty("data.schema.projected.column", projectedColumns);
    fieldSpec.addProperty("data.schema.dim_campaignType.field.type", "dimension");
    fieldSpec.addProperty("data.schema.sort_campaignId.field.type", "dimension");
    AvroDataReader avroDataPublisher = new AvroDataReader(fieldSpec);
    avroDataPublisher.getNextIndexableRow();
    int cnt = 0;
    for (String line : FileUtils.readLines(new File(jsonPath))) {

      JSONObject obj = new JSONObject(line);
      if (avroDataPublisher.hasNext()) {
        GenericRow recordRow = avroDataPublisher.getNextIndexableRow();
        assertEquals(2, recordRow.getFieldNames().length);
        for (String column : recordRow.getFieldNames()) {
          String valueFromJson = obj.get(column).toString();
          String valueFromAvro = recordRow.getValue(column).toString();
          if (cnt > 1) {
            assertEquals(valueFromJson, valueFromAvro);
          }
        }
      }
      cnt++;
    }
    assertEquals(cnt, 10000);
  }

  @Test
  public void TestReadMultiValueAvro() throws Exception {
    Configuration fieldSpec = new PropertiesConfiguration();
    fieldSpec.addProperty("data.input.format", "Avro");
    String filePath = getClass().getClassLoader().getResource(AVRO_MULTI_DATA).getFile();
    fieldSpec.addProperty("data.input.file.path", filePath);
    AvroDataReader avroDataPublisher = new AvroDataReader(fieldSpec);
    int cnt = 0;

    while (avroDataPublisher.hasNext()) {
      GenericRow recordRow = avroDataPublisher.getNextIndexableRow();
      for (String column : recordRow.getFieldNames()) {
        String valueStringFromAvro = null;
        if (avroDataPublisher.getSchema().isSingleValueColumn(column)) {
          Object valueFromAvro = recordRow.getValue(column);
          valueStringFromAvro = valueFromAvro.toString();
        } else {
          Object[] valueFromAvro = (Object[]) recordRow.getValue(column);
          valueStringFromAvro = "[";
          int i = 0;
          for (Object valueObject : valueFromAvro) {
            if (i++ == 0) {
              valueStringFromAvro += valueObject.toString();
            } else {
              valueStringFromAvro += ", " + valueObject.toString();
            }
          }
          valueStringFromAvro += "]";
        }

      }
      cnt++;
    }
    assertEquals(cnt, 28949);
  }
}
