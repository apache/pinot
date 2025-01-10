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

import java.io.IOException;
import java.util.List;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.spi.data.LogicalTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


public class LogicalTableResourceTest {
  private static final ControllerTest TEST_INSTANCE = ControllerTest.getInstance();
  private static final String LOGICAL_TABLE_STRING =
      "{\n" + "  \"tableName\" : \"transcript\",\n" + "  \"includes\" : [ \n" + "    \"transcript_1\",\n"
          + "    \"transcript_2\",\n" + "    \"transcript_3\",\n" + "    \"transcript_4\"]\n" + "}";

  @BeforeClass
  public void setUp()
      throws Exception {
    TEST_INSTANCE.setupSharedStateAndValidate();
  }

  @Test
  public void testPostJson() {
    try {
      final String response =
          ControllerTest.sendPostRequest(TEST_INSTANCE.getControllerRequestURLBuilder().forLogicalTableCreate(),
              LOGICAL_TABLE_STRING);
      assertEquals(response, "{\"unrecognizedProperties\":{},\"status\":\"transcript successfully added\"}");
    } catch (IOException e) {
      fail("Shouldn't have caught an exception: " + e.getMessage());
    }
  }

  @Test
  public void testCreateUpdateLogicalTable()
      throws IOException {
    LogicalTable logicalTable = new LogicalTable();
    logicalTable.setTableName("testLogicalTable");
    logicalTable.setBrokerTenant("DEFAULT_TENANT");
    logicalTable.setPhysicalTableNames(List.of("transcript_1", "transcript_2", "transcript_3"));

    // Add the logical table
    String addLogicalTableUrl = TEST_INSTANCE.getControllerRequestURLBuilder().forLogicalTableCreate();
    String resp = ControllerTest.sendPostRequest(addLogicalTableUrl, logicalTable.toSingleLineJsonString());

    // Retry creating the same logical table
    try {
      ControllerTest.sendPostRequest(addLogicalTableUrl, logicalTable.toSingleLineJsonString());
      fail("Logical Table POST request should have failed");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Logical table: testLogicalTable already exists"));
    }

    // Get the logical table and verify the new column exists
    String getLogicalTableUrl =
        TEST_INSTANCE.getControllerRequestURLBuilder().forLogicalTableGet(logicalTable.getTableName());
    LogicalTable remoteLogicalTable = LogicalTable.fromString(ControllerTest.sendGetRequest(getLogicalTableUrl));
    assertEquals(remoteLogicalTable, logicalTable);

    logicalTable.setPhysicalTableNames(List.of("transcript_1", "transcript_2"));
    // Update the logical table with updateLogicalTable api
    String updateLogicalTableUrl =
        TEST_INSTANCE.getControllerRequestURLBuilder().forLogicalTableUpdate(logicalTable.getTableName());
    ControllerTest.sendPutRequest(updateLogicalTableUrl, logicalTable.toSingleLineJsonString());

    // Get the logical table and verify both the new columns exist
    remoteLogicalTable = LogicalTable.fromString(ControllerTest.sendGetRequest(getLogicalTableUrl));
    assertEquals(remoteLogicalTable, logicalTable);
  }

  @AfterClass
  public void tearDown() {
    TEST_INSTANCE.cleanup();
  }
}
