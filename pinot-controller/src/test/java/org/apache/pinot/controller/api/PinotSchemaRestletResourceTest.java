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
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.controller.helix.ControllerTest;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class PinotSchemaRestletResourceTest extends ControllerTest {

  @BeforeClass
  public void setUp() {
    startController();
  }

  @Test
  public void testBadContentType() {
    Schema schema = createDummySchema("testSchema");
    try {
      sendPostRequest(_controllerRequestURLBuilder.forSchemaCreate(), schema.toSingleLineJsonString());
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
      sendPostRequest(_controllerRequestURLBuilder.forSchemaCreate(), schemaString, header);
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().startsWith("Server returned HTTP response code: 415"), e.getMessage());
    }

    try {
      Map<String, String> header = new HashMap<>();
      header.put("Content-Type", "application/json");
      final String response = sendPostRequest(_controllerRequestURLBuilder.forSchemaCreate(), schemaString, header);
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
    Schema schema = createDummySchema(schemaName);
    String url = _controllerRequestURLBuilder.forSchemaCreate();
    PostMethod postMethod = sendMultipartPostRequest(url, schema.toSingleLineJsonString());
    Assert.assertEquals(postMethod.getStatusCode(), 200);

    schema.addField(new DimensionFieldSpec("NewColumn", FieldSpec.DataType.STRING, true));
    postMethod = sendMultipartPostRequest(url, schema.toSingleLineJsonString());
    Assert.assertEquals(postMethod.getStatusCode(), 200);

    String schemaStr = sendGetRequest(_controllerRequestURLBuilder.forSchemaGet(schemaName));
    Schema readSchema = Schema.fromString(schemaStr);
    Schema inputSchema = Schema.fromString(schema.toSingleLineJsonString());
    Assert.assertEquals(readSchema, inputSchema);
    Assert.assertTrue(readSchema.getFieldSpecMap().containsKey("NewColumn"));

    final String yetAnotherColumn = "YetAnotherColumn";
    Assert.assertFalse(readSchema.getFieldSpecMap().containsKey(yetAnotherColumn));
    schema.addField(new DimensionFieldSpec(yetAnotherColumn, FieldSpec.DataType.STRING, true));
    PutMethod putMethod = sendMultipartPutRequest(_controllerRequestURLBuilder.forSchemaUpdate(schemaName),
        schema.toSingleLineJsonString());
    Assert.assertEquals(putMethod.getStatusCode(), 200);
    // verify some more...
    schemaStr = sendGetRequest(_controllerRequestURLBuilder.forSchemaGet(schemaName));
    readSchema = Schema.fromString(schemaStr);
    inputSchema = Schema.fromString(schema.toSingleLineJsonString());
    Assert.assertEquals(readSchema, inputSchema);
    Assert.assertTrue(readSchema.getFieldSpecMap().containsKey(yetAnotherColumn));

    // error cases
    putMethod = sendMultipartPutRequest(_controllerRequestURLBuilder.forSchemaUpdate(schemaName),
        schema.toSingleLineJsonString().substring(1));
    // invalid json
    Assert.assertEquals(putMethod.getStatusCode(), 400);

    schema.setSchemaName("differentSchemaName");
    putMethod = sendMultipartPutRequest(_controllerRequestURLBuilder.forSchemaUpdate(schemaName),
        schema.toSingleLineJsonString());
    Assert.assertEquals(putMethod.getStatusCode(), 400);
  }

  @AfterClass
  public void tearDown() {
    stopController();
  }
}
