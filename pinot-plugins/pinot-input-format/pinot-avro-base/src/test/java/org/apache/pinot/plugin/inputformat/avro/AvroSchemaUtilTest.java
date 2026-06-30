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
package org.apache.pinot.plugin.inputformat.avro;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.Schema;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.Assert;
import org.testng.annotations.Test;


/// Tests for the schema-shape mapping helpers in [AvroSchemaUtil]. Value-level logical-type conversion is owned by
/// `AvroRecordExtractor` and tested there.
public class AvroSchemaUtilTest {

  @Test
  public void testValueOfUuidStringLogicalType() {
    String schemaStr = "{\"type\":\"record\",\"name\":\"r\",\"fields\":[{\"name\":\"id\","
        + "\"type\":{\"type\":\"string\",\"logicalType\":\"uuid\"}}]}";
    Schema schema = new Schema.Parser().parse(schemaStr);
    Assert.assertEquals(AvroSchemaUtil.valueOf(schema.getField("id").schema()),
        FieldSpec.DataType.UUID, "STRING logicalType:uuid should map to UUID");
  }

  @Test
  public void testValueOfUuidFixed16LogicalType() {
    // FIXED(16) + logicalType:uuid — produced by Confluent fixed-uuid mode and Parquet uuid
    String schemaStr = "{\"type\":\"record\",\"name\":\"r\",\"fields\":[{\"name\":\"id\","
        + "\"type\":{\"type\":\"fixed\",\"name\":\"uuid_fixed\",\"size\":16,\"logicalType\":\"uuid\"}}]}";
    Schema schema = new Schema.Parser().parse(schemaStr);
    Assert.assertEquals(AvroSchemaUtil.valueOf(schema.getField("id").schema()),
        FieldSpec.DataType.UUID, "FIXED(16) logicalType:uuid should map to UUID");
  }

  @Test
  public void testValueOfFixed16WithoutLogicalTypeIsBytes() {
    // FIXED(16) without logicalType should stay as BYTES
    String schemaStr = "{\"type\":\"record\",\"name\":\"r\",\"fields\":[{\"name\":\"raw\","
        + "\"type\":{\"type\":\"fixed\",\"name\":\"raw16\",\"size\":16}}]}";
    Schema schema = new Schema.Parser().parse(schemaStr);
    Assert.assertEquals(AvroSchemaUtil.valueOf(schema.getField("raw").schema()),
        FieldSpec.DataType.BYTES, "FIXED(16) without logicalType:uuid should stay BYTES");
  }

  @Test
  public void testValueOfFixedWrongSizeWithUuidLogicalTypeIsBytes() {
    // FIXED of non-16 size with logicalType:uuid should not map to UUID
    String schemaStr = "{\"type\":\"record\",\"name\":\"r\",\"fields\":[{\"name\":\"id\","
        + "\"type\":{\"type\":\"fixed\",\"name\":\"uuid32\",\"size\":32,\"logicalType\":\"uuid\"}}]}";
    Schema schema = new Schema.Parser().parse(schemaStr);
    Assert.assertEquals(AvroSchemaUtil.valueOf(schema.getField("id").schema()),
        FieldSpec.DataType.BYTES, "FIXED(32) with logicalType:uuid should stay BYTES");
  }

  @Test
  public void testToAvroSchemaJsonObjectForUuid() {
    FieldSpec fieldSpec = new DimensionFieldSpec("uuidCol", FieldSpec.DataType.UUID, true);

    JsonNode jsonNode = AvroSchemaUtil.toAvroSchemaJsonObject(fieldSpec);

    Assert.assertEquals(jsonNode.get("name").asText(), "uuidCol");
    Assert.assertEquals(jsonNode.get("type").get(0).asText(), "null");
    Assert.assertEquals(jsonNode.get("type").get(1).get("type").asText(), "string");
    Assert.assertEquals(jsonNode.get("type").get(1).get("logicalType").asText(), "uuid");
  }

  @Test
  public void testToAvroSchemaJsonObjectForUuidArray() {
    FieldSpec fieldSpec = new DimensionFieldSpec("uuidCol", FieldSpec.DataType.UUID, false);

    JsonNode jsonNode = AvroSchemaUtil.toAvroSchemaJsonObject(fieldSpec);

    Assert.assertEquals(jsonNode.get("name").asText(), "uuidCol");
    Assert.assertEquals(jsonNode.get("type").get(0).asText(), "null");
    Assert.assertEquals(jsonNode.get("type").get(1).get("type").asText(), "array");
    Assert.assertEquals(jsonNode.get("type").get(1).get("items").get("type").asText(), "string");
    Assert.assertEquals(jsonNode.get("type").get(1).get("items").get("logicalType").asText(), "uuid");
  }
}
