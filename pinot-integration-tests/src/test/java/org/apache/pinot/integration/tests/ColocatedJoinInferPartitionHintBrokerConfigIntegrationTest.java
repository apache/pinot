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
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Integration tests for colocated joins inferred from broker default configuration.
 */
public class ColocatedJoinInferPartitionHintBrokerConfigIntegrationTest extends ColocatedJoinIntegrationTestBase {
  private static final int NUM_PARTITIONS = 1;

  @Override
  protected int getNumPartitions() {
    return NUM_PARTITIONS;
  }

  @Override
  protected File getSegmentBuildTempDir() {
    return _tempDir;
  }

  @Override
  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    brokerConf.setProperty(CommonConstants.Broker.CONFIG_OF_INFER_PARTITION_HINT, "true");
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
  public void testTwoTableJoinWithInferPartitionHintBrokerConfig()
      throws Exception {
    String sql = "SELECT COUNT(*) FROM userAttributes ua JOIN userGroups ug ON ua.userUUID = ug.userUUID";
    assertPlanUsesColocatedExchange(sql,
        "broker inferPartitionHint should recognize a colocated two-table join; plan: ");
    JsonNode result = postQuery(sql);
    assertNoExceptions(result);
    assertTrue(getCountFromResult(result) >= 1, "Join should return at least one row");
  }

  @Test
  public void testSelfJoinWithInferPartitionHintBrokerConfig()
      throws Exception {
    String sql = "SELECT COUNT(*) FROM userAttributes ua1 JOIN userAttributes ua2 ON ua1.userUUID = ua2.userUUID";
    assertPlanUsesColocatedExchange(sql,
        "broker inferPartitionHint should recognize a colocated self-join; plan: ");
    JsonNode result = postQuery(sql);
    assertNoExceptions(result);
    assertTrue(getCountFromResult(result) >= 1, "Self-join should return at least one row");
  }

  @Test
  public void testSemiJoinWithInferPartitionHintBrokerConfig()
      throws Exception {
    String sql = "SELECT ua.userUUID, COUNT(*) FROM userAttributes ua "
        + "WHERE ua.userUUID IN (SELECT ug.userUUID FROM userGroups ug) GROUP BY ua.userUUID ORDER BY ua.userUUID";
    assertPlanUsesColocatedExchange(sql,
        "broker inferPartitionHint should recognize a colocated semi-join; plan: ");
    JsonNode result = postQuery(sql);
    assertNoExceptions(result);
    JsonNode rows = result.get("resultTable").get("rows");
    assertNotNull(rows, "rows");
    assertEquals(rows.size(), 3, "Semi-join should return three grouped matching join keys");
    assertEquals(rows.get(0).get(0).asText(), "user-1");
    assertEquals(rows.get(1).get(0).asText(), "user-2");
    assertEquals(rows.get(2).get(0).asText(), "user-3");
  }

  @Test
  public void testLeftJoinWithInferPartitionHintBrokerConfig()
      throws Exception {
    String sql = "SELECT COUNT(*) FROM userAttributes ua LEFT JOIN userGroups ug ON ua.userUUID = ug.userUUID";
    assertPlanUsesColocatedExchange(sql,
        "broker inferPartitionHint should recognize a colocated left join; plan: ");
    JsonNode result = postQuery(sql);
    assertNoExceptions(result);
    assertTrue(getCountFromResult(result) >= 1, "Left join should return at least one row");
  }

  @Test
  public void testRightJoinWithInferPartitionHintBrokerConfig()
      throws Exception {
    String sql = "SELECT COUNT(*) FROM userAttributes ua RIGHT JOIN userGroups ug ON ua.userUUID = ug.userUUID";
    assertPlanUsesColocatedExchange(sql,
        "broker inferPartitionHint should recognize a colocated right join; plan: ");
    JsonNode result = postQuery(sql);
    assertNoExceptions(result);
    assertTrue(getCountFromResult(result) >= 1, "Right join should return at least one row");
  }
}
