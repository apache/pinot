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
import java.util.Collections;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Integration tests for colocated joins with <em>multiple</em> partitions per table.
 *
 * <p>Uses {@code getNumPartitions() = 4}; each segment is built so it contains only rows from a
 * single partition (required by
 * {@link org.apache.pinot.broker.routing.segmentpartition.SegmentPartitionMetadataManager}).
 * Data is split by partition key using the same Murmur function as the table config. Inherits
 * two-table, self, and semi-join tests from {@link ColocatedJoinIntegrationTestBase} (with plan
 * assertion {@code [PARTITIONED]}). Adds third table (userFactEvents) and three-table colocated
 * join tests.
 */
public class ColocatedJoinMultiPartitionIntegrationTest extends ColocatedJoinIntegrationTestBase {

  protected static final String TABLE_EVENTS = "userFactEvents";
  private static final int NUM_PARTITIONS = 4;

  private static final String COLOCATED_RESOURCE_DIR = "colocated";

  private File _tempDir;
  private File _eventsSegmentDir;
  private File _eventsTarDir;

  @Override
  protected int getNumPartitions() {
    return NUM_PARTITIONS;
  }

  @Override
  protected File getSegmentBuildTempDir() {
    return _tempDir;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    _tempDir = new File(FileUtils.getTempDirectory(), "ColocatedJoinMultiPartition_" + System.currentTimeMillis());
    _eventsSegmentDir = new File(_tempDir, TABLE_EVENTS + "_segmentDir");
    _eventsTarDir = new File(_tempDir, TABLE_EVENTS + "_tarDir");
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _eventsSegmentDir, _eventsTarDir);

    super.setUpColocatedBase();

    // Third table for three-table colocated join (same partition config as base tables)
    SegmentPartitionConfig partitionConfig = new SegmentPartitionConfig(
        Collections.singletonMap(PARTITION_KEY, new ColumnPartitionConfig(PARTITION_FUNCTION, getNumPartitions())));
    Schema schemaEvents = loadSchema(COLOCATED_RESOURCE_DIR + "/userFactEvents_schema.json");
    addSchema(schemaEvents);
    TableConfig tableConfigEvents = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(TABLE_EVENTS)
        .setSegmentPartitionConfig(partitionConfig)
        .build();
    addTableConfig(tableConfigEvents);
    buildSegmentsForTable(COLOCATED_RESOURCE_DIR + "/userFactEvents.csv", 0,
        tableConfigEvents, schemaEvents, _eventsSegmentDir, _eventsTarDir);
    uploadSegments(TABLE_EVENTS, TableType.OFFLINE, _eventsTarDir);

    waitForDocsLoaded(TABLE_EVENTS, 3, 60_000L);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    stopServer();
    stopBroker();
    stopController();
    stopZk();
    if (_tempDir != null && _tempDir.exists()) {
      FileUtils.deleteDirectory(_tempDir);
    }
  }

  /** Three-table join: physical plan should show colocated exchange [PARTITIONED]. */
  @Test
  public void testThreeTableJoinPlanShowsPartitioned()
      throws Exception {
    String tableOpt = getTableOptPerTableHint();
    String sqlWithHint = "SELECT COUNT(*) FROM userAttributes /*+ " + tableOpt + " */ ua "
        + "JOIN userGroups /*+ " + tableOpt + " */ ug ON ua.userUUID = ug.userUUID "
        + "JOIN userFactEvents /*+ " + tableOpt + " */ ue ON ua.userUUID = ue.userUUID";
    String explainSql = "EXPLAIN IMPLEMENTATION PLAN FOR " + sqlWithHint;
    JsonNode result = postQuery(explainSql);
    assertNoExceptions(result);
    String plan = extractImplementationPlan(result);
    assertNotNull(plan, "implementation plan should be present");
    assertTrue(plan.contains(PLAN_PARTITIONED_MARKER),
        "three-table join plan should show colocated exchange [PARTITIONED]; plan: " + plan);
  }

  /** Three-table join: result correctness with colocated hint. */
  @Test
  public void testThreeTableJoinResultCorrectness()
      throws Exception {
    String tableOpt = getTableOptPerTableHint();
    String sql = "SELECT COUNT(*) FROM userAttributes /*+ " + tableOpt + " */ ua "
        + "JOIN userGroups /*+ " + tableOpt + " */ ug ON ua.userUUID = ug.userUUID "
        + "JOIN userFactEvents /*+ " + tableOpt + " */ ue ON ua.userUUID = ue.userUUID";
    JsonNode result = postQuery(sql);
    assertNoExceptions(result);
    long count = getCountFromResult(result);
    assertTrue(count >= 1, "Three-table join should return at least one row");
  }

  /** Three-table join with is_colocated_by_join_keys hint: result correctness. */
  @Test
  public void testThreeTableJoinWithJoinOptionsHintResultCorrectness()
      throws Exception {
    String tableOpt = getTableOptPerTableHint();
    String sql = "SELECT /*+ " + JOIN_OPTIONS_COLOCATED + " */ COUNT(*) FROM userAttributes /*+ " + tableOpt
        + " */ ua JOIN userGroups /*+ " + tableOpt + " */ ug ON ua.userUUID = ug.userUUID "
        + "JOIN userFactEvents /*+ " + tableOpt + " */ ue ON ua.userUUID = ue.userUUID";
    JsonNode result = postQuery(sql);
    assertNoExceptions(result);
    long count = getCountFromResult(result);
    assertTrue(count >= 1, "Three-table join with joinOptions hint should return at least one row");
  }
}
