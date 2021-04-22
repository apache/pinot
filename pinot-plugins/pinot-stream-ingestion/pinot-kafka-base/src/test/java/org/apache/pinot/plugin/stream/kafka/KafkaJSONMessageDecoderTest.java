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
package org.apache.pinot.plugin.stream.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.Assert;
import org.testng.annotations.Test;


public class KafkaJSONMessageDecoderTest {

  private static ObjectMapper objectMapper = new ObjectMapper();

  @Test
  public void testJsonDecoderWithoutOutgoingTimeSpec()
      throws Exception {
    Schema schema = Schema.fromFile(new File(
        getClass().getClassLoader().getResource("data/test_sample_data_schema_without_outgoing_time_spec.json")
            .getFile()));
    Map<String, FieldSpec.DataType> sourceFields = new HashMap<>();
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      sourceFields.put(fieldSpec.getName(), fieldSpec.getDataType());
    }
    testJsonDecoder(sourceFields);
  }

  @Test
  public void testJsonDecoderWithOutgoingTimeSpec()
      throws Exception {
    Schema schema = Schema.fromFile(new File(
        getClass().getClassLoader().getResource("data/test_sample_data_schema_with_outgoing_time_spec.json")
            .getFile()));
    Map<String, FieldSpec.DataType> sourceFields = new HashMap<>();
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      sourceFields.put(fieldSpec.getName(), fieldSpec.getDataType());
    }
    sourceFields.remove("secondsSinceEpoch");
    sourceFields.put("time_day", FieldSpec.DataType.INT);
    testJsonDecoder(sourceFields);
  }

  @Test
  public void testJsonDecoderNoTimeSpec()
      throws Exception {
    Schema schema = Schema.fromFile(
        new File(getClass().getClassLoader().getResource("data/test_sample_data_schema_no_time_field.json").getFile()));
    Map<String, FieldSpec.DataType> sourceFields = new HashMap<>();
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      sourceFields.put(fieldSpec.getName(), fieldSpec.getDataType());
    }
    testJsonDecoder(sourceFields);
  }

  private void testJsonDecoder(Map<String, FieldSpec.DataType> sourceFields)
      throws Exception {
    try (BufferedReader reader = new BufferedReader(
        new FileReader(getClass().getClassLoader().getResource("data/test_sample_data.json").getFile()))) {
      KafkaJSONMessageDecoder decoder = new KafkaJSONMessageDecoder();
      decoder.init(new HashMap<>(), sourceFields.keySet(), "testTopic");
      GenericRow r = new GenericRow();
      String line = reader.readLine();
      while (line != null) {
        JsonNode jsonNode = objectMapper.reader().readTree(line);
        decoder.decode(line.getBytes(), r);
        for (String field : sourceFields.keySet()) {
          Object actualValue = r.getValue(field);
          JsonNode expectedValue = jsonNode.get(field);
          switch (sourceFields.get(field)) {
            case STRING:
              Assert.assertEquals(actualValue, expectedValue.asText());
              break;
            case INT:
              Assert.assertEquals(actualValue, expectedValue.asInt());
              break;
            case LONG:
              Assert.assertEquals(actualValue, expectedValue.asLong());
              break;
            case FLOAT:
              Assert.assertEquals(actualValue, (float) expectedValue.asDouble());
              break;
            case DOUBLE:
              Assert.assertEquals(actualValue, expectedValue.asDouble());
              break;
            default:
              Assert.assertTrue(false, "Shouldn't arrive here.");
          }
        }
        line = reader.readLine();
      }
    }
  }
}
