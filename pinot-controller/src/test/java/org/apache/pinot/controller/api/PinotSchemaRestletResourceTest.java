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
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


public class PinotSchemaRestletResourceTest {
  private static final ControllerTest TEST_INSTANCE = ControllerTest.getInstance();

  @BeforeClass
  public void setUp()
      throws Exception {
    TEST_INSTANCE.setupSharedStateAndValidate();
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
      final String response =
          ControllerTest.sendPostRequest(TEST_INSTANCE.getControllerRequestURLBuilder().forSchemaCreate(),
              schemaString);
      assertEquals(response, "{\"unrecognizedProperties\":{},\"status\":\"transcript successfully added\"}");
    } catch (IOException e) {
      // should not reach here
      fail("Shouldn't have caught an exception: " + e.getMessage());
    }
  }

  @Test
  public void testCreateUpdateSchema()
      throws IOException {
    String schemaName = "testSchema";
    Schema schema = TEST_INSTANCE.createDummySchema(schemaName);

    // Add the schema
    String addSchemaUrl = TEST_INSTANCE.getControllerRequestURLBuilder().forSchemaCreate();
    SimpleHttpResponse resp = ControllerTest.sendMultipartPostRequest(addSchemaUrl, schema.toSingleLineJsonString());
    assertEquals(resp.getStatusCode(), 200);

    // Add a new column
    DimensionFieldSpec newColumnFieldSpec = new DimensionFieldSpec("newColumn", DataType.STRING, true);
    schema.addField(newColumnFieldSpec);

    // Update the schema with addSchema api and override off
    resp = ControllerTest.sendMultipartPostRequest(addSchemaUrl + "?override=false", schema.toSingleLineJsonString());
    assertEquals(resp.getStatusCode(), 409);

    // Update the schema with addSchema api and override on
    resp = ControllerTest.sendMultipartPostRequest(addSchemaUrl, schema.toSingleLineJsonString());
    assertEquals(resp.getStatusCode(), 200);

    // Get the schema and verify the new column exists
    String getSchemaUrl = TEST_INSTANCE.getControllerRequestURLBuilder().forSchemaGet(schemaName);
    Schema remoteSchema = Schema.fromString(ControllerTest.sendGetRequest(getSchemaUrl));
    assertEquals(remoteSchema, schema);
    assertTrue(remoteSchema.hasColumn(newColumnFieldSpec.getName()));

    // Add another new column
    DimensionFieldSpec newColumnFieldSpec2 = new DimensionFieldSpec("newColumn2", DataType.STRING, true);
    schema.addField(newColumnFieldSpec2);

    // Update the schema with updateSchema api
    String updateSchemaUrl = TEST_INSTANCE.getControllerRequestURLBuilder().forSchemaUpdate(schemaName);
    resp = ControllerTest.sendMultipartPutRequest(updateSchemaUrl, schema.toSingleLineJsonString());
    assertEquals(resp.getStatusCode(), 200);

    // Get the schema and verify both the new columns exist
    remoteSchema = Schema.fromString(ControllerTest.sendGetRequest(getSchemaUrl));
    assertEquals(remoteSchema, schema);
    assertTrue(remoteSchema.hasColumn(newColumnFieldSpec.getName()));
    assertTrue(remoteSchema.hasColumn(newColumnFieldSpec2.getName()));

    // Change the column data type - backward-incompatible change
    newColumnFieldSpec.setDataType(DataType.INT);

    // Update the schema with addSchema api and override on
    resp = ControllerTest.sendMultipartPostRequest(addSchemaUrl, schema.toSingleLineJsonString());
    assertEquals(resp.getStatusCode(), 400);

    // Update the schema with updateSchema api
    resp = ControllerTest.sendMultipartPutRequest(updateSchemaUrl, schema.toSingleLineJsonString());
    assertEquals(resp.getStatusCode(), 400);

    // Change the column data type from STRING to BOOLEAN
    newColumnFieldSpec.setDataType(DataType.BOOLEAN);

    // Update the schema with addSchema api and override on, force on
    resp = ControllerTest.sendMultipartPostRequest(addSchemaUrl + "?force=true", schema.toSingleLineJsonString());
    assertEquals(resp.getStatusCode(), 200);

    // Change another column max length from default 512 to 2000
    newColumnFieldSpec2.setMaxLength(2000);
    // Change another column default null value from default "null" to "0"
    newColumnFieldSpec2.setDefaultNullValue("0");

    // Update the schema with addSchema api and override on
    resp = ControllerTest.sendMultipartPostRequest(addSchemaUrl, schema.toSingleLineJsonString());
    assertEquals(resp.getStatusCode(), 200);

    // Get the schema and verify the default null value and max length have been changed
    remoteSchema = Schema.fromString(ControllerTest.sendGetRequest(getSchemaUrl));
    assertEquals(remoteSchema.getFieldSpecFor(newColumnFieldSpec2.getName()).getMaxLength(), 2000);
    assertEquals(remoteSchema.getFieldSpecFor(newColumnFieldSpec2.getName()).getDefaultNullValue(), "0");

    // Change another column max length from 1000
    newColumnFieldSpec2.setMaxLength(1000);
    // Change another column default null value from default "null" to "1"
    newColumnFieldSpec2.setDefaultNullValue("1");

    // Update the schema with updateSchema api and override on
    resp = ControllerTest.sendMultipartPutRequest(updateSchemaUrl, schema.toSingleLineJsonString());
    assertEquals(resp.getStatusCode(), 200);

    // Get the schema and verify the default null value and max length have been changed
    remoteSchema = Schema.fromString(ControllerTest.sendGetRequest(getSchemaUrl));
    assertEquals(remoteSchema.getFieldSpecFor(newColumnFieldSpec2.getName()).getMaxLength(), 1000);
    assertEquals(remoteSchema.getFieldSpecFor(newColumnFieldSpec2.getName()).getDefaultNullValue(), "1");

    // Add a new BOOLEAN column
    DimensionFieldSpec newColumnFieldSpec3 = new DimensionFieldSpec("newColumn3", DataType.BOOLEAN, true);
    schema.addField(newColumnFieldSpec3);

    // Update the schema with updateSchema api
    resp = ControllerTest.sendMultipartPutRequest(updateSchemaUrl, schema.toSingleLineJsonString());
    assertEquals(resp.getStatusCode(), 200);

    // Get the schema and verify the new column has BOOLEAN data type
    remoteSchema = Schema.fromString(ControllerTest.sendGetRequest(getSchemaUrl));
    assertEquals(remoteSchema.getFieldSpecFor(newColumnFieldSpec3.getName()).getDataType(), DataType.BOOLEAN);

    // Post invalid schema string
    String invalidSchemaString = schema.toSingleLineJsonString().substring(1);
    resp = ControllerTest.sendMultipartPostRequest(addSchemaUrl, invalidSchemaString);
    assertEquals(resp.getStatusCode(), 400);
    resp = ControllerTest.sendMultipartPutRequest(updateSchemaUrl, invalidSchemaString);
    assertEquals(resp.getStatusCode(), 400);

    // Update schema with null schema name
    schema.setSchemaName(null);
    resp = ControllerTest.sendMultipartPutRequest(updateSchemaUrl, schema.toSingleLineJsonString());
    assertEquals(resp.getStatusCode(), 400);

    // Update schema with non-matching schema name
    String newSchemaName = "newSchemaName";
    schema.setSchemaName(newSchemaName);
    resp = ControllerTest.sendMultipartPutRequest(updateSchemaUrl, schema.toSingleLineJsonString());
    assertEquals(resp.getStatusCode(), 400);

    // Update non-existing schema
    resp = ControllerTest.sendMultipartPutRequest(
        TEST_INSTANCE.getControllerRequestURLBuilder().forSchemaUpdate(newSchemaName), schema.toSingleLineJsonString());
    assertEquals(resp.getStatusCode(), 404);
  }

  @Test
  public void testUnrecognizedProperties()
      throws IOException {
    String schemaStringWithExtraProps =
        "{\n" + "  \"schemaName\" : \"transcript2\",\"illegalKey1\" : 1, \n" + "  \"dimensionFieldSpecs\" : [ {\n"
            + "    \"name\" : \"studentID\",\n" + "    \"dataType\" : \"STRING\"\n" + "  }, {\n"
            + "    \"name\" : \"firstName\",\n" + "    \"dataType\" : \"STRING\"\n" + "  }, {\n"
            + "    \"name\" : \"lastName\",\n" + "    \"dataType\" : \"STRING\"\n" + "  }, {\n"
            + "    \"name\" : \"gender\",\n" + "    \"dataType\" : \"STRING\"\n" + "  }, {\n"
            + "    \"name\" : \"subject\",\n" + "    \"dataType\" : \"STRING\"\n" + "  } ],\n"
            + "  \"metricFieldSpecs\" : [ {\n" + "    \"name\" : \"score\",\n" + "    \"dataType\" : \"FLOAT\"\n"
            + "  } ]}";

    String response = ControllerTest.sendPostRequest(TEST_INSTANCE.getControllerRequestURLBuilder().forSchemaValidate(),
        schemaStringWithExtraProps);
    assertTrue(response.contains("/illegalKey1\" : 1"));

    response = ControllerTest.sendPostRequest(TEST_INSTANCE.getControllerRequestURLBuilder().forSchemaCreate(),
        schemaStringWithExtraProps);
    assertEquals(response,
        "{\"unrecognizedProperties\":{\"/illegalKey1\":1},\"status\":\"transcript2 successfully added\"}");

    response =
        ControllerTest.sendPutRequest(TEST_INSTANCE.getControllerRequestURLBuilder().forSchemaUpdate("transcript2"),
            schemaStringWithExtraProps);
    assertEquals(response,
        "{\"unrecognizedProperties\":{\"/illegalKey1\":1},\"status\":\"transcript2 successfully added\"}");
  }

  @Test
  public void testUnrecognizedPropertiesFileEndpoints()
      throws IOException {
    String schemaStringWithExtraProps =
        "{\n" + "  \"schemaName\" : \"transcript2\",\"illegalKey1\" : 1, \n" + "  \"dimensionFieldSpecs\" : [ {\n"
            + "    \"name\" : \"studentID\",\n" + "    \"dataType\" : \"STRING\"\n" + "  }, {\n"
            + "    \"name\" : \"firstName\",\n" + "    \"dataType\" : \"STRING\"\n" + "  }, {\n"
            + "    \"name\" : \"lastName\",\n" + "    \"dataType\" : \"STRING\"\n" + "  }, {\n"
            + "    \"name\" : \"gender\",\n" + "    \"dataType\" : \"STRING\"\n" + "  }, {\n"
            + "    \"name\" : \"subject\",\n" + "    \"dataType\" : \"STRING\"\n" + "  } ],\n"
            + "  \"metricFieldSpecs\" : [ {\n" + "    \"name\" : \"score\",\n" + "    \"dataType\" : \"FLOAT\"\n"
            + "  } ]}";

    SimpleHttpResponse response =
        ControllerTest.sendMultipartPostRequest(TEST_INSTANCE.getControllerRequestURLBuilder().forSchemaValidate(),
            schemaStringWithExtraProps);
    assertTrue(response.getResponse().contains("/illegalKey1\" : 1"));

    response = ControllerTest.sendMultipartPostRequest(TEST_INSTANCE.getControllerRequestURLBuilder().forSchemaCreate(),
        schemaStringWithExtraProps);
    assertEquals(response.getResponse(),
        "{\"unrecognizedProperties\":{\"/illegalKey1\":1},\"status\":\"transcript2 successfully added\"}");

    response = ControllerTest.sendMultipartPutRequest(
        TEST_INSTANCE.getControllerRequestURLBuilder().forSchemaUpdate("transcript2"), schemaStringWithExtraProps);
    assertEquals(response.getResponse(),
        "{\"unrecognizedProperties\":{\"/illegalKey1\":1},\"status\":\"transcript2 successfully added\"}");
  }

  @AfterClass
  public void tearDown() {
    TEST_INSTANCE.cleanup();
  }
}
