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
import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;


/**
 * Integration tests for colocated joins using the multi-stage query engine (single partition).
 *
 * <p>Uses {@code getNumPartitions() = 1} so each segment has exactly one partition (required by
 * {@link org.apache.pinot.broker.routing.segmentpartition.SegmentPartitionMetadataManager}).
 * Validates two-table, self, left, and right joins with joinOptions/tableOptions or
 * inferPartitionHint. For multiple partitions check ColocatedJoinMultiPartitionIntegrationTest
 */
public class ColocatedJoinIntegrationTest extends ColocatedJoinIntegrationTestBase {

  /** Single partition per segment so ZK partition metadata is valid (segment must have exactly one partition). */
  private static final int NUM_PARTITIONS = 1;

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
    super.setUpColocatedBase();
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

  @Test
  public void testTwoTableJoinWithJoinOptionsAndTableOptions()
      throws Exception {
    String sql = "SELECT /*+ joinOptions(is_colocated_by_join_keys='true') */ COUNT(*) FROM "
        + "userAttributes ua JOIN userGroups ug ON ua.userUUID = ug.userUUID";
    JsonNode result = postQuery(sql);
    assertNoExceptions(result);
    assertTrue(getCountFromResult(result) >= 1, "Join should return at least one row");
  }

  @Test
  public void testTwoTableJoinWithTableOptionsOnly()
      throws Exception {
    String sql = "SELECT COUNT(*) FROM userAttributes ua JOIN userGroups ug ON ua.userUUID = ug.userUUID";
    JsonNode result = postQuery(sql);
    assertNoExceptions(result);
    assertTrue(getCountFromResult(result) >= 1, "Join should return at least one row");
  }

  @Test
  public void testTwoTableJoinWithInferPartitionHintQueryOption()
      throws Exception {
    String sql = "SELECT COUNT(*) FROM userAttributes ua JOIN userGroups ug ON ua.userUUID = ug.userUUID";
    String queryOptions = "useMultistageEngine=true; inferPartitionHint=true";
    JsonNode result = postQueryWithOptions(sql, queryOptions);
    assertNoExceptions(result);
    assertTrue(getCountFromResult(result) >= 1, "Join should return at least one row");
  }

  /** Same semantic as broker config inferPartitionHint: query option forces inference when not in SQL. */
  @Test
  public void testTwoTableJoinWithInferPartitionHintBrokerConfig()
      throws Exception {
    String sql = "SELECT COUNT(*) FROM userAttributes ua JOIN userGroups ug ON ua.userUUID = ug.userUUID";
    String queryOptions = "useMultistageEngine=true; inferPartitionHint=true";
    JsonNode result = postQueryWithOptions(sql, queryOptions);
    assertNoExceptions(result);
    assertTrue(getCountFromResult(result) >= 1);
  }

  @Test
  public void testSelfJoinWithJoinOptionsAndTableOptions()
      throws Exception {
    String sql = "SELECT /*+ joinOptions(is_colocated_by_join_keys='true') */ COUNT(*) FROM "
        + "userAttributes ua1 JOIN userAttributes ua2 ON ua1.userUUID = ua2.userUUID";
    JsonNode result = postQuery(sql);
    assertNoExceptions(result);
    assertTrue(getCountFromResult(result) >= 1, "Self-join should return at least one row");
  }

  @Test
  public void testSelfJoinWithTableOptionsOnly()
      throws Exception {
    String sql = "SELECT COUNT(*) FROM userAttributes ua1 JOIN userAttributes ua2 ON ua1.userUUID = ua2.userUUID";
    JsonNode result = postQuery(sql);
    assertNoExceptions(result);
    assertTrue(getCountFromResult(result) >= 1, "Self-join should return at least one row");
  }

  @Test
  public void testSelfJoinWithInferPartitionHintQueryOption()
      throws Exception {
    String sql = "SELECT COUNT(*) FROM userAttributes ua1 JOIN userAttributes ua2 ON ua1.userUUID = ua2.userUUID";
    JsonNode result = postQueryWithOptions(sql, "useMultistageEngine=true; inferPartitionHint=true");
    assertNoExceptions(result);
    assertTrue(getCountFromResult(result) >= 1, "Self-join should return at least one row");
  }

  @Test
  public void testLeftJoinWithJoinOptionsAndTableOptions()
      throws Exception {
    String sql = "SELECT /*+ joinOptions(is_colocated_by_join_keys='true') */ COUNT(*) FROM "
        + "userAttributes ua LEFT JOIN userGroups ug ON ua.userUUID = ug.userUUID";
    JsonNode result = postQuery(sql);
    assertNoExceptions(result);
    assertTrue(getCountFromResult(result) >= 1, "Left join should return at least one row");
  }

  @Test
  public void testLeftJoinWithTableOptionsOnly()
      throws Exception {
    String sql = "SELECT COUNT(*) FROM userAttributes ua LEFT JOIN userGroups ug ON ua.userUUID = ug.userUUID";
    JsonNode result = postQuery(sql);
    assertNoExceptions(result);
    assertTrue(getCountFromResult(result) >= 1, "Left join should return at least one row");
  }

  @Test
  public void testLeftJoinWithInferPartitionHintQueryOption()
      throws Exception {
    String sql = "SELECT COUNT(*) FROM userAttributes ua LEFT JOIN userGroups ug ON ua.userUUID = ug.userUUID";
    JsonNode result = postQueryWithOptions(sql, "useMultistageEngine=true; inferPartitionHint=true");
    assertNoExceptions(result);
    assertTrue(getCountFromResult(result) >= 1, "Left join should return at least one row");
  }

  @Test
  public void testRightJoinWithJoinOptionsAndTableOptions()
      throws Exception {
    String sql = "SELECT /*+ joinOptions(is_colocated_by_join_keys='true') */ COUNT(*) FROM "
        + "userAttributes ua RIGHT JOIN userGroups ug ON ua.userUUID = ug.userUUID";
    JsonNode result = postQuery(sql);
    assertNoExceptions(result);
    assertTrue(getCountFromResult(result) >= 1, "Right join should return at least one row");
  }

  @Test
  public void testRightJoinWithTableOptionsOnly()
      throws Exception {
    String sql = "SELECT COUNT(*) FROM userAttributes ua RIGHT JOIN userGroups ug ON ua.userUUID = ug.userUUID";
    JsonNode result = postQuery(sql);
    assertNoExceptions(result);
    assertTrue(getCountFromResult(result) >= 1, "Right join should return at least one row");
  }

  @Test
  public void testRightJoinWithInferPartitionHintQueryOption()
      throws Exception {
    String sql = "SELECT COUNT(*) FROM userAttributes ua RIGHT JOIN userGroups ug ON ua.userUUID = ug.userUUID";
    JsonNode result = postQueryWithOptions(sql, "useMultistageEngine=true; inferPartitionHint=true");
    assertNoExceptions(result);
    assertTrue(getCountFromResult(result) >= 1, "Right join should return at least one row");
  }
}
