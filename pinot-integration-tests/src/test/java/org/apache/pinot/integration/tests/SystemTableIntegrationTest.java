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
package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import org.apache.pinot.client.admin.PinotAdminClient;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Integration test to ensure system tables can be queried through the broker.
 */
public class SystemTableIntegrationTest extends BaseClusterIntegrationTestSet {

  @Override
  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    // Provide controller URL so system.tables provider can call size API.
    brokerConf.setProperty(CommonConstants.Broker.CONTROLLER_URL, getControllerBaseApiUrl());
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    startZk();
    startController();
    startBroker();
    startServer();

    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    List<File> avroFiles = unpackAvroData(_tempDir);
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(getTableName(), _tarDir);
    waitForAllDocsLoaded(600_000L);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    stopServer();
    stopBroker();
    stopController();
    stopZk();
  }

  @Test
  public void testSystemTablesSize()
      throws Exception {
    URI controllerBaseUri = URI.create(getControllerBaseApiUrl());
    URI sizeUri = controllerBaseUri.resolve("/tables/mytable/size?verbose=true&includeReplacedSegments=false");
    JsonNode controllerSize = JsonUtils.stringToJsonNode(sendGetRequest(sizeUri.toString()));
    int controllerSegments = controllerSize.path("offlineSegments").path("segments").size();
    long controllerReportedSize = controllerSize.path("reportedSizeInBytes").asLong();
    long controllerEstimatedSize = controllerSize.path("estimatedSizeInBytes").asLong();
    assertTrue(controllerSegments > 0, "controller size endpoint should report segments");
    assertTrue(controllerReportedSize > 0, "controller size endpoint should report size");
    assertTrue(controllerEstimatedSize > 0, "controller size endpoint should report estimated size");

    long controllerTotalDocs = 0;
    String controllerAddress = URI.create(getControllerBaseApiUrl()).getAuthority();
    try (PinotAdminClient adminClient = new PinotAdminClient(controllerAddress)) {
      JsonNode offlineSegments = controllerSize.path("offlineSegments").path("segments");
      Iterator<String> segmentNames = offlineSegments.fieldNames();
      while (segmentNames.hasNext()) {
        String segmentName = segmentNames.next();
        JsonNode segmentMetadata =
            adminClient.getSegmentApiClient().getSegmentMetadata("mytable_OFFLINE", segmentName);
        controllerTotalDocs += segmentMetadata.path(CommonConstants.Segment.TOTAL_DOCS).asLong();
      }
    }
    assertTrue(controllerTotalDocs > 0, "controller segment metadata should report totalDocs");

    JsonNode response = postQuery(
        "SELECT tableName,type,segments,totalDocs,reportedSizeBytes,estimatedSizeBytes FROM system.tables "
            + "WHERE tableName='mytable' AND type='OFFLINE'");
    assertNotNull(response);
    assertEquals(response.withArray("exceptions").size(), 0);
    JsonNode rows = response.get("resultTable").get("rows");
    assertEquals(rows.size(), 1, "Expected exactly one offline table row");
    JsonNode row = rows.get(0);
    assertEquals(row.get(0).asText(), "mytable");
    assertEquals(row.get(1).asText(), "OFFLINE");
    int segments = row.get(2).asInt();
    long totalDocs = row.get(3).asLong();
    long reportedSize = row.get(4).asLong();
    long estimatedSize = row.get(5).asLong();
    assertTrue(segments > 0, "segments should be > 0 but was " + segments);
    assertTrue(totalDocs > 0, "totalDocs should be > 0 but was " + totalDocs);
    assertTrue(reportedSize > 0, "reportedSizeBytes should be > 0 but was " + reportedSize);
    assertTrue(estimatedSize > 0, "estimatedSizeBytes should be > 0 but was " + estimatedSize);
    assertEquals(segments, controllerSegments, "segment count should match controller");
    assertEquals(totalDocs, controllerTotalDocs, "totalDocs should match controller segment metadata");
  }
}
