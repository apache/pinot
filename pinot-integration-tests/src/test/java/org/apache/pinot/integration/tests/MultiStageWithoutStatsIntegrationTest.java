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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.pinot.server.starter.helix.SendStatsPredicate;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.util.TestUtils;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class MultiStageWithoutStatsIntegrationTest extends BaseClusterIntegrationTestSet {
  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(getHelixClusterName())
            .build();
    startBrokers(1);
    startServers(1);

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create and upload segments. For exhaustive testing, concurrently upload multiple segments with the same name
    // and validate correctness with parallel push protection enabled.
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);
    // Create a copy of _tarDir to create multiple segments with the same name.
    File tarDir2 = new File(_tempDir, "tarDir2");
    FileUtils.copyDirectory(_tarDir, tarDir2);

    List<File> tarDirs = new ArrayList<>();
    tarDirs.add(_tarDir);
    tarDirs.add(tarDir2);
    try {
      uploadSegments(getTableName(), TableType.OFFLINE, tarDirs);
    } catch (Exception e) {
      // If enableParallelPushProtection is enabled and the same segment is uploaded concurrently, we could get one
      // of the three exception:
      //   - 409 conflict of the second call enters ProcessExistingSegment
      //   - segmentZkMetadata creation failure if both calls entered ProcessNewSegment
      //   - Failed to copy segment tar file to final location due to the same segment pushed twice concurrently
      // In such cases we upload all the segments again to ensure that the data is set up correctly.
      assertTrue(e.getMessage().contains("Another segment upload is in progress for segment") || e.getMessage()
          .contains("Failed to create ZK metadata for segment") || e.getMessage()
          .contains("java.nio.file.FileAlreadyExistsException"), e.getMessage());
      uploadSegments(getTableName(), _tarDir);
    }
    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    serverConf.setProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_SEND_STATS_MODE,
        SendStatsPredicate.Mode.NEVER.name());
    super.overrideServerConf(serverConf);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    // Brokers and servers has been stopped
    stopController();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  public boolean useMultiStageQueryEngine() {
    return true;
  }

  @Override
  protected List<FieldConfig> getFieldConfigs() {
    return Collections.singletonList(
        new FieldConfig("DivAirports", FieldConfig.EncodingType.DICTIONARY, Collections.emptyList(),
            FieldConfig.CompressionCodec.MV_ENTRY_DICT, null));
  }

  @Test
  public void testIntersection()
      throws Exception {
    @Language("sql")
    String query = "SELECT *\n"
        + "FROM (\n"
        + "    SELECT CarrierDelay\n"
        + "    FROM mytable\n"
        + "    WHERE DaysSinceEpoch > 0\n"
        + "  )\n"
        + "INTERSECT\n"
        + "(\n"
        + "  SELECT ArrDelay\n"
        + "  FROM mytable\n"
        + "  WHERE DaysSinceEpoch > 0\n"
        + ")";
    JsonNode node = postQuery(query);

    // Expected:
    // "stageStats" : {
    //    "type" : "MAILBOX_RECEIVE",
    //    "executionTimeMs" : whatever,
    //    "fanIn" : whatever,
    //    "rawMessages" : whatever,
    //    "deserializedBytes" : whatever,
    //    "upstreamWaitMs" : whatever,
    //    "children" : [ {
    //      "type" : "EMPTY_MAILBOX_SEND",
    //      "stage" : 1,
    //      "description" : "No stats available for this stage. It may have been pruned."
    //    } ]
    //  }

    JsonNode stageStats = node.get("stageStats");
    assertNotNull(stageStats, "Stage stats should not be null");

    assertEquals(stageStats.get("type").asText(), "MAILBOX_RECEIVE");

    JsonNode children = stageStats.get("children");
    assertNotNull(children, "Children should not be null");
    assertEquals(children.size(), 1);

    JsonNode child = children.get(0);
    assertEquals(child.get("type").asText(), "EMPTY_MAILBOX_SEND");
    assertEquals(child.get("stage").asInt(), 1);
    assertEquals(child.get("description").asText(), "No stats available for this stage. It may have been pruned.");
  }
}
