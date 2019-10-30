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
package org.apache.pinot.core.realtime.impl.kafka;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.core.data.GenericRow;
import org.testng.Assert;
import org.testng.annotations.Test;


public class KafkaJSONMessageDecoderTest {

  @Test
  public void testJsonDecoderWithoutOutgoingTimeSpec()
      throws Exception {
    Schema schema = Schema.fromFile(new File(
        getClass().getClassLoader().getResource("data/test_sample_data_schema_without_outgoing_time_spec.json")
            .getFile()));
    BufferedReader reader = new BufferedReader(
        new FileReader(getClass().getClassLoader().getResource("data/test_sample_data.json").getFile()));
    testJsonDecoder(schema, reader);
  }

  @Test
  public void testJsonDecoderWithOutgoingTimeSpec()
      throws Exception {
    Schema schema = Schema.fromFile(new File(
        getClass().getClassLoader().getResource("data/test_sample_data_schema_with_outgoing_time_spec.json")
            .getFile()));
    BufferedReader reader = new BufferedReader(
        new FileReader(getClass().getClassLoader().getResource("data/test_sample_data.json").getFile()));
    testJsonDecoder(schema, reader);
  }

  @Test
  public void testJsonDecoderNoTimeSpec()
      throws Exception {
    Schema schema = Schema.fromFile(
        new File(getClass().getClassLoader().getResource("data/test_sample_data_schema_no_time_field.json").getFile()));
    BufferedReader reader = new BufferedReader(
        new FileReader(getClass().getClassLoader().getResource("data/test_sample_data.json").getFile()));
    testJsonDecoder(schema, reader);
  }

  private void testJsonDecoder(Schema schema, BufferedReader reader)
      throws Exception {
    KafkaJSONMessageDecoder decoder = new KafkaJSONMessageDecoder();
    decoder.init(new HashMap<>(), schema, "testTopic");
    GenericRow r = new GenericRow();
    String line = reader.readLine();
    while (line != null) {
      JsonObject jsonObject = JsonParser.parseString(line).getAsJsonObject();
      decoder.decode(line.getBytes(), r);
      for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
        String fieldSpecName = (fieldSpec.getFieldType() == FieldSpec.FieldType.TIME) ? schema.getTimeFieldSpec()
            .getIncomingTimeColumnName() : fieldSpec.getName();
        Object actualValue = r.getValue(fieldSpecName);
        JsonElement expectedValue = jsonObject.get(fieldSpecName);
        switch (fieldSpec.getDataType()) {
          case STRING:
            Assert.assertEquals(actualValue, expectedValue.getAsString());
            break;
          case INT:
            Assert.assertEquals(actualValue, expectedValue.getAsInt());
            break;
          case LONG:
            Assert.assertEquals(actualValue, expectedValue.getAsLong());
            break;
          case FLOAT:
            Assert.assertEquals(actualValue, expectedValue.getAsFloat());
            break;
          case DOUBLE:
            Assert.assertEquals(actualValue, expectedValue.getAsDouble());
            break;
          default:
            Assert.assertTrue(false, "Shouldn't arrive here.");
        }
      }
      line = reader.readLine();
    }
    reader.close();
  }
}
