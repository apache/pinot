/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.controller.api.restlet.resources;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.ControllerRequestURLBuilder;
import com.linkedin.pinot.controller.helix.ControllerTest;
import com.linkedin.pinot.controller.helix.ControllerTestUtils;
import java.io.IOException;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class PinotSchemaRestletResourceTest extends ControllerTest {
  private static final ObjectMapper objectMapper = new ObjectMapper();
  ControllerRequestURLBuilder urlBuilder = ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL);

 @BeforeClass
  public void setUp() {
    startZk();
    ControllerConf config = ControllerTestUtils.getDefaultControllerConfiguration();
    config.setTableMinReplicas(1);
    startController(config);
  }

  public JSONObject createDefaultSchema()
      throws JSONException, JsonProcessingException {
    JSONObject schemaJson = new JSONObject();
    schemaJson.put("schemaName", "testSchema");
    JSONArray dimFieldSpec = new JSONArray();
    schemaJson.put("dimensionFieldSpecs", dimFieldSpec);
    JSONArray metricFieldSpec = new JSONArray();
    schemaJson.put("metricFieldSpecs", metricFieldSpec);
    JSONObject timeFieldSpec = new JSONObject();
    // schemaJson.put("timeFieldSpec", timeFieldSpec);

    DimensionFieldSpec df = new DimensionFieldSpec("dimA", FieldSpec.DataType.STRING, true, "");
    dimFieldSpec.put(new JSONObject(objectMapper.writeValueAsString(df)));
    df = new DimensionFieldSpec("dimB", FieldSpec.DataType.LONG, true, 0);
    dimFieldSpec.put(new JSONObject(objectMapper.writeValueAsString(df)));

    MetricFieldSpec mf = new MetricFieldSpec("metricA", FieldSpec.DataType.INT, 0);
    metricFieldSpec.put(new JSONObject(objectMapper.writeValueAsString(mf)));

    mf = new MetricFieldSpec("metricB", FieldSpec.DataType.DOUBLE, -1);
    metricFieldSpec.put(new JSONObject(objectMapper.writeValueAsString(mf)));
    return schemaJson;
  }

  @Test
  public void testBadContentType()
      throws JSONException, JsonProcessingException {
    JSONObject schema = createDefaultSchema();
    try {
      sendPostRequest(urlBuilder.forSchemaCreate(), schema.toString());
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().startsWith("Server returned HTTP response code: 415"), e.getMessage());
      return;
    }
    // should not reach here
    Assert.assertTrue(false);
  }

  @Test
  public void testCreateUpdateSchema()
      throws JSONException, IOException {
    JSONObject schema = createDefaultSchema();
    String url = urlBuilder.forSchemaCreate();
    PostMethod postMethod = sendMultipartPostRequest(url, schema.toString());
    Assert.assertEquals(postMethod.getStatusCode(), 200);

    JSONArray dimSpecs = schema.getJSONArray("dimensionFieldSpecs");
    DimensionFieldSpec df = new DimensionFieldSpec("NewColumn", FieldSpec.DataType.STRING, true);
    dimSpecs.put(new JSONObject(objectMapper.writeValueAsString(df)));

    postMethod = sendMultipartPostRequest(url, schema.toString());
    Assert.assertEquals(postMethod.getStatusCode(), 200);

    final String schemaName = schema.getString("schemaName");
    String schemaStr = sendGetRequest(urlBuilder.forSchemaGet(schemaName));
    Schema readSchema = Schema.fromString(schemaStr);
    Schema inputSchema = Schema.fromString(schema.toString());
    Assert.assertEquals(readSchema, inputSchema);
    Assert.assertTrue(readSchema.getFieldSpecMap().containsKey("NewColumn"));

    final String yetAnotherColumn = "YetAnotherColumn";
    Assert.assertFalse(readSchema.getFieldSpecMap().containsKey(yetAnotherColumn));
    df = new DimensionFieldSpec(yetAnotherColumn, FieldSpec.DataType.STRING, true);
    dimSpecs.put(new JSONObject(objectMapper.writeValueAsString(df)));
    PutMethod putMethod = sendMultipartPutRequest(urlBuilder.forSchemaUpdate(schemaName), schema.toString());
    Assert.assertEquals(putMethod.getStatusCode(), 200);
    // verify some more...
    schemaStr = sendGetRequest(urlBuilder.forSchemaGet(schemaName));
    readSchema = Schema.fromString(schemaStr);
    inputSchema = Schema.fromString(schema.toString());
    Assert.assertEquals(readSchema, inputSchema);
    Assert.assertTrue(readSchema.getFieldSpecMap().containsKey(yetAnotherColumn));

    // error cases
    putMethod = sendMultipartPutRequest(urlBuilder.forSchemaUpdate(schemaName), schema.toString().substring(1));
    // invalid json
    Assert.assertEquals(putMethod.getStatusCode(), 400);

    schema.put("schemaName", "differentSchemaName");
    putMethod = sendMultipartPutRequest(urlBuilder.forSchemaUpdate(schemaName), schema.toString());
    Assert.assertEquals(putMethod.getStatusCode(), 400);

  }

}
