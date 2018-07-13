/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.controller.api.resources;

import java.io.IOException;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.controller.helix.ControllerTest;


public class PinotSchemaRestletResourceTest extends ControllerTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @BeforeClass
  public void setUp() {
    startZk();
    startController();
  }

  public JSONObject createDefaultSchema() throws JSONException, JsonProcessingException {
    JSONObject schemaJson = new JSONObject();
    schemaJson.put("schemaName", "testSchema");
    JSONArray dimFieldSpec = new JSONArray();
    schemaJson.put("dimensionFieldSpecs", dimFieldSpec);
    JSONArray metricFieldSpec = new JSONArray();
    schemaJson.put("metricFieldSpecs", metricFieldSpec);

    DimensionFieldSpec df = new DimensionFieldSpec("dimA", FieldSpec.DataType.STRING, true, "");
    dimFieldSpec.put(new JSONObject(OBJECT_MAPPER.writeValueAsString(df)));
    df = new DimensionFieldSpec("dimB", FieldSpec.DataType.LONG, true, 0);
    dimFieldSpec.put(new JSONObject(OBJECT_MAPPER.writeValueAsString(df)));

    MetricFieldSpec mf = new MetricFieldSpec("metricA", FieldSpec.DataType.INT, 0);
    metricFieldSpec.put(new JSONObject(OBJECT_MAPPER.writeValueAsString(mf)));

    mf = new MetricFieldSpec("metricB", FieldSpec.DataType.DOUBLE, -1);
    metricFieldSpec.put(new JSONObject(OBJECT_MAPPER.writeValueAsString(mf)));
    return schemaJson;
  }

  @Test
  public void testBadContentType() throws JSONException, JsonProcessingException {
    Schema schema = createDummySchema("testSchema");
    try {
      sendPostRequest(_controllerRequestURLBuilder.forSchemaCreate(), schema.getJSONSchema());
    } catch (IOException e) {
      // TODO The Jersey API returns 400, so we need to check return code here not a string.
//      Assert.assertTrue(e.getMessage().startsWith("Server returned HTTP response code: 415"), e.getMessage());
      return;
    }
    // should not reach here
    Assert.fail("Should have caught an exception");
  }

  @Test
  public void testCreateUpdateSchema() throws JSONException, IOException {
    String schemaName = "testSchema";
    Schema schema = createDummySchema(schemaName);
    String url = _controllerRequestURLBuilder.forSchemaCreate();
    PostMethod postMethod = sendMultipartPostRequest(url, schema.toString());
    Assert.assertEquals(postMethod.getStatusCode(), 200);

    schema.addField(new DimensionFieldSpec("NewColumn", FieldSpec.DataType.STRING, true));
    postMethod = sendMultipartPostRequest(url, schema.toString());
    Assert.assertEquals(postMethod.getStatusCode(), 200);

    String schemaStr = sendGetRequest(_controllerRequestURLBuilder.forSchemaGet(schemaName));
    Schema readSchema = Schema.fromString(schemaStr);
    Schema inputSchema = Schema.fromString(schema.toString());
    Assert.assertEquals(readSchema, inputSchema);
    Assert.assertTrue(readSchema.getFieldSpecMap().containsKey("NewColumn"));

    final String yetAnotherColumn = "YetAnotherColumn";
    Assert.assertFalse(readSchema.getFieldSpecMap().containsKey(yetAnotherColumn));
    schema.addField(new DimensionFieldSpec(yetAnotherColumn, FieldSpec.DataType.STRING, true));
    PutMethod putMethod =
        sendMultipartPutRequest(_controllerRequestURLBuilder.forSchemaUpdate(schemaName), schema.toString());
    Assert.assertEquals(putMethod.getStatusCode(), 200);
    // verify some more...
    schemaStr = sendGetRequest(_controllerRequestURLBuilder.forSchemaGet(schemaName));
    readSchema = Schema.fromString(schemaStr);
    inputSchema = Schema.fromString(schema.toString());
    Assert.assertEquals(readSchema, inputSchema);
    Assert.assertTrue(readSchema.getFieldSpecMap().containsKey(yetAnotherColumn));

    // error cases
    putMethod = sendMultipartPutRequest(_controllerRequestURLBuilder.forSchemaUpdate(schemaName),
        schema.toString().substring(1));
    // invalid json
    Assert.assertEquals(putMethod.getStatusCode(), 400);

    schema.setSchemaName("differentSchemaName");
    putMethod = sendMultipartPutRequest(_controllerRequestURLBuilder.forSchemaUpdate(schemaName), schema.toString());
    Assert.assertEquals(putMethod.getStatusCode(), 400);
  }

  @AfterClass
  public void tearDown() {
    stopController();
    stopZk();
  }
}
