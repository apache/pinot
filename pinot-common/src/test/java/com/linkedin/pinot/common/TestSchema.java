package com.linkedin.pinot.common;

import java.io.File;
import java.io.IOException;

import org.apache.helix.ZNRecord;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.data.Schema;


public class TestSchema {

  private static Schema schema;
  private static File schemaFile;

  @BeforeClass
  public void setup() throws JsonParseException, JsonMappingException, IOException {
    schemaFile = new File(TestUtils.getFileFromResourceUrl(getClass().getClassLoader().getResource("data1.schema")));
    schema = Schema.fromFile(schemaFile);
  }

  @Test
  public void test1() throws IllegalArgumentException, IllegalAccessException, JsonParseException,
      JsonMappingException, IOException {
    ZNRecord record = Schema.toZNRecord(schema);
    Schema.fromZNRecordV2(record);
  }
}
