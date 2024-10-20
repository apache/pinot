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
package org.apache.pinot.spi.data;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.data.FieldSpec.DataType.INT;
import static org.testng.Assert.assertEquals;


public class SchemaInfoTest {

  @Test
  public void testSchemaInfoSerDeserWithVirtualColumns()
      throws IOException {
    // Mock the Schema objects
    Schema schemaMock =
        new Schema.SchemaBuilder().setSchemaName("TestSchema").addDimensionField("dim1", FieldSpec.DataType.STRING)
            .addDimensionField("dim2", FieldSpec.DataType.INT).addDimensionField("dim3", FieldSpec.DataType.INT)
            .addDimensionField(CommonConstants.Segment.BuiltInVirtualColumn.DOCID, FieldSpec.DataType.INT)
            .addDimensionField(CommonConstants.Segment.BuiltInVirtualColumn.HOSTNAME, FieldSpec.DataType.STRING)
            .addDimensionField(CommonConstants.Segment.BuiltInVirtualColumn.SEGMENTNAME, FieldSpec.DataType.STRING)
            .addDateTimeField("dt1", FieldSpec.DataType.LONG, "1:HOURS:EPOCH", "1:HOURS")
            .addDateTimeField("dt2", FieldSpec.DataType.LONG, "1:HOURS:EPOCH", "1:HOURS").addMetricField("metric", INT)
            .build();
    SchemaInfo schemaInfo = new SchemaInfo(schemaMock);
    List<SchemaInfo> schemaInfoList = new ArrayList<>();
    schemaInfoList.add(schemaInfo);
    String response = JsonUtils.objectToPrettyString(schemaInfoList);
    JsonNode jsonNodeResp = JsonUtils.stringToJsonNode(response);

    // Test deserialization
    assertEquals(jsonNodeResp.get(0).get("schemaName").asText(), "TestSchema");
    assertEquals(jsonNodeResp.get(0).get("numDimensionFields").asInt(), 3);
    assertEquals(jsonNodeResp.get(0).get("numDateTimeFields").asInt(), 2);
    assertEquals(jsonNodeResp.get(0).get("numMetricFields").asInt(), 1);
    assertEquals(jsonNodeResp.get(0).get("numComplexFields").asInt(), 0);
    assertEquals(schemaInfo.getSchemaName(), "TestSchema");

    // Test column count
    assertEquals(schemaInfo.getNumDimensionFields(), 3);  // 6 - 3 virtual columns = 3
    assertEquals(schemaInfo.getNumDateTimeFields(), 2);
    assertEquals(schemaInfo.getNumMetricFields(), 1);

    // Serialize JsonNode back to SchemaInfo
    List<SchemaInfo> schemaInfoListSer = new ArrayList<>();
    schemaInfoListSer = JsonUtils.jsonNodeToObject(jsonNodeResp, new TypeReference<List<SchemaInfo>>() {
    });
    SchemaInfo schemaInfo1 = schemaInfoListSer.get(0);
    // Verify the deserialized object match
    assertEquals(schemaInfo1.getSchemaName(), "TestSchema");
    assertEquals(schemaInfo1.getNumDimensionFields(), 3);
    assertEquals(schemaInfo1.getNumDateTimeFields(), 2);
    assertEquals(schemaInfo1.getNumMetricFields(), 1);
    assertEquals(schemaInfo1.getNumComplexFields(), 0);
  }

  @Test
  public void testSchemaInfoSerDeserWithComplexAndVirtualColumns()
      throws IOException {
    // Mock the Schema objects
    Schema schemaMock =
        new Schema.SchemaBuilder().setSchemaName("TestSchema").addDimensionField("dim1", FieldSpec.DataType.STRING)
            .addDimensionField("dim2", FieldSpec.DataType.INT).addDimensionField("dim3", FieldSpec.DataType.INT)
            .addDimensionField(CommonConstants.Segment.BuiltInVirtualColumn.DOCID, FieldSpec.DataType.INT)
            .addDimensionField(CommonConstants.Segment.BuiltInVirtualColumn.HOSTNAME, FieldSpec.DataType.STRING)
            .addDimensionField(CommonConstants.Segment.BuiltInVirtualColumn.SEGMENTNAME, FieldSpec.DataType.STRING)
            .addDateTimeField("dt1", FieldSpec.DataType.LONG, "1:HOURS:EPOCH", "1:HOURS")
            .addDateTimeField("dt2", FieldSpec.DataType.LONG, "1:HOURS:EPOCH", "1:HOURS").addMetricField("metric", INT)
            .addComplex("intMap", FieldSpec.DataType.MAP,
                Map.of("key", new DimensionFieldSpec("key", FieldSpec.DataType.STRING, true), "value",
                    new DimensionFieldSpec("value", FieldSpec.DataType.INT, true)))
            .addComplex("stringMap", FieldSpec.DataType.MAP,
                Map.of("key", new DimensionFieldSpec("key", FieldSpec.DataType.STRING, true), "value",
                    new DimensionFieldSpec("value", FieldSpec.DataType.STRING, true))).build();
    SchemaInfo schemaInfo = new SchemaInfo(schemaMock);
    List<SchemaInfo> schemaInfoList = new ArrayList<>();
    schemaInfoList.add(schemaInfo);
    String response = JsonUtils.objectToPrettyString(schemaInfoList);
    JsonNode jsonNodeResp = JsonUtils.stringToJsonNode(response);

    // Test deserialization
    assertEquals(jsonNodeResp.get(0).get("schemaName").asText(), "TestSchema");
    assertEquals(jsonNodeResp.get(0).get("numDimensionFields").asInt(), 3);
    assertEquals(jsonNodeResp.get(0).get("numDateTimeFields").asInt(), 2);
    assertEquals(jsonNodeResp.get(0).get("numMetricFields").asInt(), 1);
    assertEquals(jsonNodeResp.get(0).get("numComplexFields").asInt(), 2);
    assertEquals(schemaInfo.getSchemaName(), "TestSchema");

    // Test column count
    assertEquals(schemaInfo.getNumDimensionFields(), 3);  // 6 - 3 virtual columns = 3
    assertEquals(schemaInfo.getNumDateTimeFields(), 2);
    assertEquals(schemaInfo.getNumMetricFields(), 1);

    // Serialize JsonNode back to SchemaInfo
    List<SchemaInfo> schemaInfoListSer = new ArrayList<>();
    schemaInfoListSer = JsonUtils.jsonNodeToObject(jsonNodeResp, new TypeReference<List<SchemaInfo>>() {
    });
    SchemaInfo schemaInfo1 = schemaInfoListSer.get(0);
    // Verify the deserialized object match
    assertEquals(schemaInfo1.getSchemaName(), "TestSchema");
    assertEquals(schemaInfo1.getNumDimensionFields(), 3);
    assertEquals(schemaInfo1.getNumDateTimeFields(), 2);
    assertEquals(schemaInfo1.getNumMetricFields(), 1);
    assertEquals(schemaInfo1.getNumComplexFields(), 2);
  }
}
