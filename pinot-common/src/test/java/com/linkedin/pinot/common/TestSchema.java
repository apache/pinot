/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import com.linkedin.pinot.common.data.StarTreeIndexSpec;
import org.apache.helix.ZNRecord;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONException;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.data.Schema;


public class TestSchema {

  private static Schema schema;
  private static File schemaFile;

  @BeforeClass
  public void setup() throws JsonParseException, JsonMappingException, IOException, JSONException {
    schemaFile = new File(TestUtils.getFileFromResourceUrl(getClass().getClassLoader().getResource("data1.schema")));
    schema = Schema.fromFile(schemaFile);
  }

  @Test
  public void test1() throws IllegalArgumentException, IllegalAccessException, JsonParseException,
      JsonMappingException, IOException {
    ZNRecord record = Schema.toZNRecord(schema);
    Schema.fromZNRecord(record);
  }

  @Test
  public void test2() throws Exception {
    String schemaString = new ObjectMapper().writeValueAsString(schema);
    Schema newSchema = new ObjectMapper().readValue(schemaString, Schema.class);

  }

  @Test
  public void testSchemaWithStarTree() throws Exception {
    InputStream inputStream = ClassLoader.getSystemResourceAsStream("data2.with-star-tree.schema");
    Schema schema = new ObjectMapper().readValue(inputStream, Schema.class);
    Assert.assertEquals(schema.getStarTreeIndexSpecs().size(), 1);

    StarTreeIndexSpec indexSpec = new StarTreeIndexSpec();
    indexSpec.setSplitOrder(Arrays.asList("dim2", "dim1", "dim3"));
    indexSpec.setMaxLeafRecords(10000); // the known default

    Assert.assertEquals(schema.getStarTreeIndexSpecs().get(0), indexSpec);
  }
}
