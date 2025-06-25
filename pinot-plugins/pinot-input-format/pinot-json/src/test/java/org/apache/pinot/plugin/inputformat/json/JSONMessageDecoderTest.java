/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.plugin.inputformat.json;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;


public class JSONMessageDecoderTest {

  @Test
  public void testJsonDecoderWithoutOutgoingTimeSpec()
      throws Exception {
    Schema schema = loadSchema("data/test_sample_data_schema_without_outgoing_time_spec.json");
    Map<String, DataType> sourceFields = new HashMap<>();
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      sourceFields.put(fieldSpec.getName(), fieldSpec.getDataType());
    }
    testJsonDecoder(sourceFields);
  }

  @Test
  public void testJsonDecoderWithOutgoingTimeSpec()
      throws Exception {
    Schema schema = loadSchema("data/test_sample_data_schema_with_outgoing_time_spec.json");
    Map<String, DataType> sourceFields = new HashMap<>();
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      sourceFields.put(fieldSpec.getName(), fieldSpec.getDataType());
    }
    sourceFields.remove("secondsSinceEpoch");
    sourceFields.put("time_day", DataType.INT);
    testJsonDecoder(sourceFields);
  }

  @Test
  public void testJsonDecoderNoTimeSpec()
      throws Exception {
    Schema schema = loadSchema("data/test_sample_data_schema_no_time_field.json");
    Map<String, DataType> sourceFields = new HashMap<>();
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      sourceFields.put(fieldSpec.getName(), fieldSpec.getDataType());
    }
    testJsonDecoder(sourceFields);
  }

  private Schema loadSchema(String resourcePath)
      throws Exception {
    URL resource = getClass().getClassLoader().getResource(resourcePath);
    assertNotNull(resource);
    return Schema.fromFile(new File(resource.getFile()));
  }

  private void testJsonDecoder(Map<String, DataType> sourceFields)
      throws Exception {
    URL resource = getClass().getClassLoader().getResource("data/test_sample_data.json");
    assertNotNull(resource);
    try (BufferedReader reader = new BufferedReader(new FileReader(resource.getFile()))) {
      JSONMessageDecoder decoder = new JSONMessageDecoder();
      decoder.init(Map.of(), sourceFields.keySet(), "testTopic");
      GenericRow row = new GenericRow();
      String line;
      while ((line = reader.readLine()) != null) {
        JsonNode jsonNode = JsonUtils.DEFAULT_READER.readTree(line);
        decoder.decode(line.getBytes(), row);
        for (String field : sourceFields.keySet()) {
          Object actualValue = row.getValue(field);
          JsonNode expectedValue = jsonNode.get(field);
          switch (sourceFields.get(field)) {
            case STRING:
              assertEquals(actualValue, expectedValue.asText());
              break;
            case INT:
              assertEquals(actualValue, expectedValue.asInt());
              break;
            case LONG:
              assertEquals(actualValue, expectedValue.asLong());
              break;
            case FLOAT:
              assertEquals(actualValue, (float) expectedValue.asDouble());
              break;
            case DOUBLE:
              assertEquals(actualValue, expectedValue.asDouble());
              break;
            default:
              fail("Shouldn't arrive here.");
              break;
          }
        }
      }
    }
  }
}
