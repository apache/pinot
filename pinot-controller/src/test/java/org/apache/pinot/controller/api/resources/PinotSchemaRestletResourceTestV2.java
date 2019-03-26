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
package org.apache.pinot.controller.api.resources;

import com.yammer.metrics.core.MetricsRegistry;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.api.events.DefaultMetadataEventNotifierFactory;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import static org.mockito.Mockito.mock;

public class PinotSchemaRestletResourceTestV2 extends ControllerTest {
  PinotSchemaRestletResource _pinotSchemaRestletResource;

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    startController();
    _pinotSchemaRestletResource = new PinotSchemaRestletResource();
    _pinotSchemaRestletResource._controllerMetrics = new ControllerMetrics(new MetricsRegistry());
    _pinotSchemaRestletResource._pinotHelixResourceManager = mock(PinotHelixResourceManager.class);
    _pinotSchemaRestletResource._metadataEventNotifierFactory = new DefaultMetadataEventNotifierFactory();
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    stopController();
    stopZk();
  }

  @Test
  public void testUpdateSchema()
      throws Exception {
    String schema = "test";
    String schemaConfigStr =
        "{\n" + "    \"schemaName\":\"test\",\n" + "    \"dimensionFieldSpecs\":[\n" + "        {\n"
            + "            \"name\":\"province\",\n" + "            \"dataType\":\"STRING\"\n" + "        },\n"
            + "        {\n" + "            \"name\":\"isp\",\n" + "            \"dataType\":\"STRING\"\n"
            + "        }\n" + "    ],\n" + "    \"metricFieldSpecs\":[\n" + "        {\n"
            + "            \"name\":\"total\",\n" + "            \"dataType\":\"LONG\"\n" + "        },\n"
            + "        {\n" + "            \"name\":\"succeed\",\n" + "            \"dataType\":\"DOUBLE\"\n"
            + "        }\n" + "    ],\n" + "    \"timeFieldSpec\":{\n" + "        \"incomingGranularitySpec\":{\n"
            + "            \"name\":\"fdate\",\n" + "            \"dataType\":\"LONG\",\n"
            + "            \"timeType\":\"MILLISECONDS\"\n" + "        }\n" + "    }\n" + "}";
    SuccessResponse successResponse = _pinotSchemaRestletResource.updateSchema(schema, schemaConfigStr);
    Assert.assertEquals(successResponse.getStatus(), "test successfully added");
  }

  @Test
  public void testAddSchema()
      throws Exception {
    String schemaConfigStr =
        "{\n" + "    \"schemaName\":\"test\",\n" + "    \"dimensionFieldSpecs\":[\n" + "        {\n"
            + "            \"name\":\"province\",\n" + "            \"dataType\":\"STRING\"\n" + "        },\n"
            + "        {\n" + "            \"name\":\"isp\",\n" + "            \"dataType\":\"STRING\"\n"
            + "        }\n" + "    ],\n" + "    \"metricFieldSpecs\":[\n" + "        {\n"
            + "            \"name\":\"total\",\n" + "            \"dataType\":\"LONG\"\n" + "        },\n"
            + "        {\n" + "            \"name\":\"succeed\",\n" + "            \"dataType\":\"LONG\"\n"
            + "        }\n" + "    ],\n" + "    \"timeFieldSpec\":{\n" + "        \"incomingGranularitySpec\":{\n"
            + "            \"name\":\"fdate\",\n" + "            \"dataType\":\"LONG\",\n"
            + "            \"timeType\":\"MILLISECONDS\"\n" + "        }\n" + "    }\n" + "}";
    SuccessResponse successResponse = _pinotSchemaRestletResource.addSchema(schemaConfigStr);
    Assert.assertEquals(successResponse.getStatus(), "test successfully added");
  }
}