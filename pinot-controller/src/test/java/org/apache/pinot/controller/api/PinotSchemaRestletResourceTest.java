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
package org.apache.pinot.controller.api;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.pinot.controller.ControllerTestUtils;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class PinotSchemaRestletResourceTest {

  @BeforeClass
  public void setUp()
      throws Exception {
    ControllerTestUtils.setupClusterAndValidate();
  }

  @Test
  public void testBadContentType() {
    Schema schema = ControllerTestUtils.createDummySchema("testSchema");
    try {
      ControllerTestUtils.sendPostRequest(ControllerTestUtils.getControllerRequestURLBuilder().forSchemaCreate(),
          schema.toSingleLineJsonString());
    } catch (IOException e) {
      // TODO The Jersey API returns 400, so we need to check return code here not a string.
//      Assert.assertTrue(e.getMessage().startsWith("Server returned HTTP response code: 415"), e.getMessage());
      return;
    }
    // should not reach here
    Assert.fail("Should have caught an exception");
  }

  @Test
  public void testPostJson() {
    String schemaString = "{\n" + "  \"schemaName\" : \"transcript\",\n" + "  \"dimensionFieldSpecs\" : [ {\n"
        + "    \"name\" : \"studentID\",\n" + "    \"dataType\" : \"STRING\"\n" + "  }, {\n"
        + "    \"name\" : \"firstName\",\n" + "    \"dataType\" : \"STRING\"\n" + "  }, {\n"
        + "    \"name\" : \"lastName\",\n" + "    \"dataType\" : \"STRING\"\n" + "  }, {\n"
        + "    \"name\" : \"gender\",\n" + "    \"dataType\" : \"STRING\"\n" + "  }, {\n"
        + "    \"name\" : \"subject\",\n" + "    \"dataType\" : \"STRING\"\n" + "  } ],\n"
        + "  \"metricFieldSpecs\" : [ {\n" + "    \"name\" : \"score\",\n" + "    \"dataType\" : \"FLOAT\"\n"
        + "  } ]}";
    try {
      Map<String, String> header = new HashMap<>();
      ControllerTestUtils
          .sendPostRequest(ControllerTestUtils.getControllerRequestURLBuilder().forSchemaCreate(), schemaString,
              header);
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().startsWith("Server returned HTTP response code: 415"), e.getMessage());
    }

    try {
      Map<String, String> header = new HashMap<>();
      header.put("Content-Type", "application/json");
      final String response = ControllerTestUtils
          .sendPostRequest(ControllerTestUtils.getControllerRequestURLBuilder().forSchemaCreate(), schemaString,
              header);
      Assert.assertEquals(response, "{\"status\":\"transcript successfully added\"}");
    } catch (IOException e) {
      // should not reach here
      Assert.fail("Shouldn't have caught an exception");
    }
  }

  @Test
  public void testCreateUpdateSchema()
      throws IOException {
    String schemaName = "testSchema";
    Schema schema = ControllerTestUtils.createDummySchema(schemaName);

    // Add the schema
    String addSchemaUrl = ControllerTestUtils.getControllerRequestURLBuilder().forSchemaCreate();
    PostMethod postMethod = ControllerTestUtils.sendMultipartPostRequest(addSchemaUrl, schema.toSingleLineJsonString());
    Assert.assertEquals(postMethod.getStatusCode(), 200);

    // Add a new column
    DimensionFieldSpec newColumnFieldSpec = new DimensionFieldSpec("newColumn", DataType.STRING, true);
    schema.addField(newColumnFieldSpec);

    // Update the schema with addSchema api and override off
    postMethod =
        ControllerTestUtils.sendMultipartPostRequest(addSchemaUrl + "?override=false", schema.toSingleLineJsonString());
    Assert.assertEquals(postMethod.getStatusCode(), 409);

    // Update the schema with addSchema api and override on
    postMethod = ControllerTestUtils.sendMultipartPostRequest(addSchemaUrl, schema.toSingleLineJsonString());
    Assert.assertEquals(postMethod.getStatusCode(), 200);

    // Get the schema and verify the new column exists
    String getSchemaUrl = ControllerTestUtils.getControllerRequestURLBuilder().forSchemaGet(schemaName);
    Schema remoteSchema = Schema.fromString(ControllerTestUtils.sendGetRequest(getSchemaUrl));
    Assert.assertEquals(remoteSchema, schema);
    Assert.assertTrue(remoteSchema.hasColumn(newColumnFieldSpec.getName()));

    // Add another new column
    DimensionFieldSpec newColumnFieldSpec2 = new DimensionFieldSpec("newColumn2", DataType.STRING, true);
    schema.addField(newColumnFieldSpec2);

    // Update the schema with updateSchema api
    String updateSchemaUrl = ControllerTestUtils.getControllerRequestURLBuilder().forSchemaUpdate(schemaName);
    PutMethod putMethod = ControllerTestUtils.sendMultipartPutRequest(updateSchemaUrl, schema.toSingleLineJsonString());
    Assert.assertEquals(putMethod.getStatusCode(), 200);

    // Get the schema and verify both the new columns exist
    remoteSchema = Schema.fromString(ControllerTestUtils.sendGetRequest(getSchemaUrl));
    Assert.assertEquals(remoteSchema, schema);
    Assert.assertTrue(remoteSchema.hasColumn(newColumnFieldSpec.getName()));
    Assert.assertTrue(remoteSchema.hasColumn(newColumnFieldSpec2.getName()));

    // Change the column data type - backward-incompatible change
    newColumnFieldSpec.setDataType(DataType.INT);

    // Update the schema with addSchema api and override on
    postMethod = ControllerTestUtils.sendMultipartPostRequest(addSchemaUrl, schema.toSingleLineJsonString());
    Assert.assertEquals(postMethod.getStatusCode(), 400);

    // Update the schema with updateSchema api
    putMethod = ControllerTestUtils.sendMultipartPutRequest(updateSchemaUrl, schema.toSingleLineJsonString());
    Assert.assertEquals(putMethod.getStatusCode(), 400);

    // Change the column data type from STRING to BOOLEAN
    newColumnFieldSpec.setDataType(DataType.BOOLEAN);

    // Update the schema with addSchema api and override on
    postMethod = ControllerTestUtils.sendMultipartPostRequest(addSchemaUrl, schema.toSingleLineJsonString());
    Assert.assertEquals(postMethod.getStatusCode(), 200);

    // Change another column data type from STRING to BOOLEAN
    newColumnFieldSpec2.setDataType(DataType.BOOLEAN);

    // Update the schema with updateSchema api
    putMethod = ControllerTestUtils.sendMultipartPutRequest(updateSchemaUrl, schema.toSingleLineJsonString());
    Assert.assertEquals(putMethod.getStatusCode(), 200);

    // Get the schema and verify the data types are not changed
    remoteSchema = Schema.fromString(ControllerTestUtils.sendGetRequest(getSchemaUrl));
    Assert.assertEquals(remoteSchema.getFieldSpecFor(newColumnFieldSpec.getName()).getDataType(), DataType.STRING);
    Assert.assertEquals(remoteSchema.getFieldSpecFor(newColumnFieldSpec2.getName()).getDataType(), DataType.STRING);

    // Add a new BOOLEAN column
    DimensionFieldSpec newColumnFieldSpec3 = new DimensionFieldSpec("newColumn3", DataType.BOOLEAN, true);
    schema.addField(newColumnFieldSpec3);

    // Update the schema with updateSchema api
    putMethod = ControllerTestUtils.sendMultipartPutRequest(updateSchemaUrl, schema.toSingleLineJsonString());
    Assert.assertEquals(putMethod.getStatusCode(), 200);

    // Get the schema and verify the new column has BOOLEAN data type
    remoteSchema = Schema.fromString(ControllerTestUtils.sendGetRequest(getSchemaUrl));
    Assert.assertEquals(remoteSchema.getFieldSpecFor(newColumnFieldSpec3.getName()).getDataType(), DataType.BOOLEAN);

    // Post invalid schema string
    String invalidSchemaString = schema.toSingleLineJsonString().substring(1);
    postMethod = ControllerTestUtils.sendMultipartPostRequest(addSchemaUrl, invalidSchemaString);
    Assert.assertEquals(postMethod.getStatusCode(), 400);
    putMethod = ControllerTestUtils.sendMultipartPutRequest(updateSchemaUrl, invalidSchemaString);
    Assert.assertEquals(putMethod.getStatusCode(), 400);

    // Update schema with non-matching schema name
    String newSchemaName = "newSchemaName";
    schema.setSchemaName(newSchemaName);
    putMethod = ControllerTestUtils.sendMultipartPutRequest(updateSchemaUrl, schema.toSingleLineJsonString());
    Assert.assertEquals(putMethod.getStatusCode(), 400);

    // Update non-existing schema
    putMethod = ControllerTestUtils.sendMultipartPutRequest(
        ControllerTestUtils.getControllerRequestURLBuilder().forSchemaUpdate(newSchemaName),
        schema.toSingleLineJsonString());
    Assert.assertEquals(putMethod.getStatusCode(), 404);
  }

  @AfterClass
  public void tearDown() {
    ControllerTestUtils.cleanup();
  }
}
