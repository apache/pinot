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

import java.io.File;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration tests for colocated joins using the multi-stage query engine (single partition).
 *
 * <p>Uses {@code getNumPartitions() = 1} so each segment has exactly one partition (required by
 * {@link org.apache.pinot.broker.routing.segmentpartition.SegmentPartitionMetadataManager}).
 * Validates two-table, self, semi, left, and right joins with joinOptions/tableOptions or
 * inferPartitionHint query options. Broker-config coverage lives in
 * {@link ColocatedJoinBrokerConfigIntegrationTest}. For multiple partitions
 * check {@link ColocatedJoinMultiPartitionIntegrationTest}.
 */
public class ColocatedJoinIntegrationTest extends ColocatedJoinIntegrationTestBase {

  /** Single partition per segment so ZK partition metadata is valid (segment must have exactly one partition). */
  private static final int NUM_PARTITIONS = 1;

  private static final String INFER_PARTITION_HINT_OPTIONS = "useMultistageEngine=true; inferPartitionHint=true";
  private static final String SINGLE_PARTITION_PLAN_MSG =
      "single-partition join should still be recognized as colocated; plan: ";

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
    String tableOpt = getTableOptPerTableHint();
    String sql =
        "SELECT /*+ " + JOIN_OPTIONS_COLOCATED + " */ COUNT(*) FROM userAttributes /*+ " + tableOpt
            + " */ ua JOIN userGroups /*+ " + tableOpt + " */ ug ON ua.userUUID = ug.userUUID";
    runAndAssertColocatedWithPlanCheck(sql, SINGLE_PARTITION_PLAN_MSG);
  }

  @Test
  public void testTwoTableJoinWithTableOptionsOnly()
      throws Exception {
    String tableOpt = getTableOptPerTableHint();
    String sql =
        "SELECT COUNT(*) FROM userAttributes /*+ " + tableOpt + " */ ua JOIN userGroups /*+ " + tableOpt
            + " */ ug ON ua.userUUID = ug.userUUID";
    runAndAssertColocatedWithPlanCheck(sql, SINGLE_PARTITION_PLAN_MSG);
  }

  @Test
  public void testTwoTableJoinWithInferPartitionHintQueryOption()
      throws Exception {
    String sql = "SELECT COUNT(*) FROM userAttributes ua JOIN userGroups ug ON ua.userUUID = ug.userUUID";
    runAndAssertColocatedWithQueryOptions(sql, INFER_PARTITION_HINT_OPTIONS, SINGLE_PARTITION_PLAN_MSG);
  }

  @Test
  public void testSelfJoinWithJoinOptionsAndTableOptions()
      throws Exception {
    String tableOpt = getTableOptPerTableHint();
    String sql =
        "SELECT /*+ " + JOIN_OPTIONS_COLOCATED + " */ COUNT(*) FROM userAttributes /*+ " + tableOpt
            + " */ ua1 JOIN userAttributes /*+ " + tableOpt + " */ ua2 ON ua1.userUUID = ua2.userUUID";
    runAndAssertColocatedWithPlanCheck(sql, SINGLE_PARTITION_PLAN_MSG);
  }

  @Test
  public void testSelfJoinWithTableOptionsOnly()
      throws Exception {
    String tableOpt = getTableOptPerTableHint();
    String sql =
        "SELECT COUNT(*) FROM userAttributes /*+ " + tableOpt + " */ ua1 JOIN userAttributes /*+ " + tableOpt
            + " */ ua2 ON ua1.userUUID = ua2.userUUID";
    runAndAssertColocatedWithPlanCheck(sql, SINGLE_PARTITION_PLAN_MSG);
  }

  @Test
  public void testSelfJoinWithInferPartitionHintQueryOption()
      throws Exception {
    String sql =
        "SELECT COUNT(*) FROM userAttributes ua1 JOIN userAttributes ua2 ON ua1.userUUID = ua2.userUUID";
    runAndAssertColocatedWithQueryOptions(sql, INFER_PARTITION_HINT_OPTIONS, SINGLE_PARTITION_PLAN_MSG);
  }

  @Test
  public void testSemiJoinWithJoinOptionsAndTableOptions()
      throws Exception {
    String tableOpt = getTableOptPerTableHint();
    String sql =
        "SELECT /*+ " + JOIN_OPTIONS_COLOCATED + " */ ua.userUUID, COUNT(*) FROM userAttributes /*+ " + tableOpt
            + " */ ua WHERE ua.userUUID IN (SELECT ug.userUUID FROM userGroups /*+ " + tableOpt
            + " */ ug) GROUP BY ua.userUUID ORDER BY ua.userUUID";
    runAndAssertSemiJoinWithPlanCheck(sql, SINGLE_PARTITION_PLAN_MSG);
  }

  @Test
  public void testSemiJoinWithTableOptionsOnly()
      throws Exception {
    String tableOpt = getTableOptPerTableHint();
    String sql =
        "SELECT ua.userUUID, COUNT(*) FROM userAttributes /*+ " + tableOpt + " */ ua "
            + "WHERE ua.userUUID IN (SELECT ug.userUUID FROM userGroups /*+ " + tableOpt
            + " */ ug) GROUP BY ua.userUUID ORDER BY ua.userUUID";
    runAndAssertSemiJoinWithPlanCheck(sql, SINGLE_PARTITION_PLAN_MSG);
  }

  @Test
  public void testSemiJoinWithInferPartitionHintQueryOption()
      throws Exception {
    String sql =
        "SELECT ua.userUUID, COUNT(*) FROM userAttributes ua "
            + "WHERE ua.userUUID IN (SELECT ug.userUUID FROM userGroups ug) GROUP BY ua.userUUID ORDER BY ua.userUUID";
    runAndAssertSemiJoinWithQueryOptions(sql, INFER_PARTITION_HINT_OPTIONS, SINGLE_PARTITION_PLAN_MSG);
  }

  @Test
  public void testLeftJoinWithJoinOptionsAndTableOptions()
      throws Exception {
    String tableOpt = getTableOptPerTableHint();
    String sql =
        "SELECT /*+ " + JOIN_OPTIONS_COLOCATED + " */ COUNT(*) FROM userAttributes /*+ " + tableOpt
            + " */ ua LEFT JOIN userGroups /*+ " + tableOpt + " */ ug ON ua.userUUID = ug.userUUID";
    runAndAssertColocatedWithPlanCheck(sql, SINGLE_PARTITION_PLAN_MSG);
  }

  @Test
  public void testLeftJoinWithTableOptionsOnly()
      throws Exception {
    String tableOpt = getTableOptPerTableHint();
    String sql =
        "SELECT COUNT(*) FROM userAttributes /*+ " + tableOpt + " */ ua LEFT JOIN userGroups /*+ " + tableOpt
            + " */ ug ON ua.userUUID = ug.userUUID";
    runAndAssertColocatedWithPlanCheck(sql, SINGLE_PARTITION_PLAN_MSG);
  }

  @Test
  public void testLeftJoinWithInferPartitionHintQueryOption()
      throws Exception {
    String sql = "SELECT COUNT(*) FROM userAttributes ua LEFT JOIN userGroups ug ON ua.userUUID = ug.userUUID";
    runAndAssertColocatedWithQueryOptions(sql, INFER_PARTITION_HINT_OPTIONS, SINGLE_PARTITION_PLAN_MSG);
  }

  @Test
  public void testRightJoinWithJoinOptionsAndTableOptions()
      throws Exception {
    String tableOpt = getTableOptPerTableHint();
    String sql =
        "SELECT /*+ " + JOIN_OPTIONS_COLOCATED + " */ COUNT(*) FROM userAttributes /*+ " + tableOpt
            + " */ ua RIGHT JOIN userGroups /*+ " + tableOpt + " */ ug ON ua.userUUID = ug.userUUID";
    runAndAssertColocatedWithPlanCheck(sql, SINGLE_PARTITION_PLAN_MSG);
  }

  @Test
  public void testRightJoinWithTableOptionsOnly()
      throws Exception {
    String tableOpt = getTableOptPerTableHint();
    String sql =
        "SELECT COUNT(*) FROM userAttributes /*+ " + tableOpt + " */ ua RIGHT JOIN userGroups /*+ " + tableOpt
            + " */ ug ON ua.userUUID = ug.userUUID";
    runAndAssertColocatedWithPlanCheck(sql, SINGLE_PARTITION_PLAN_MSG);
  }

  @Test
  public void testRightJoinWithInferPartitionHintQueryOption()
      throws Exception {
    String sql = "SELECT COUNT(*) FROM userAttributes ua RIGHT JOIN userGroups ug ON ua.userUUID = ug.userUUID";
    runAndAssertColocatedWithQueryOptions(sql, INFER_PARTITION_HINT_OPTIONS, SINGLE_PARTITION_PLAN_MSG);
  }
}
