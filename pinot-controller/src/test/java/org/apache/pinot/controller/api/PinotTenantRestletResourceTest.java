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
package org.apache.pinot.controller.api;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class PinotTenantRestletResourceTest {
  private static final ControllerTest TEST_INSTANCE = ControllerTest.getInstance();
  private static final String TABLE_NAME = "restletTable_OFFLINE";

  @BeforeClass
  public void setUp()
      throws Exception {
    TEST_INSTANCE.setupSharedStateAndValidate();
  }

  @Test
  public void testTableListForTenant()
      throws Exception {
    // Check that no tables on tenant works
    String listTablesUrl =
        TEST_INSTANCE.getControllerRequestURLBuilder().forTablesFromTenant(TagNameUtils.DEFAULT_TENANT_NAME);
    JsonNode tableList = JsonUtils.stringToJsonNode(ControllerTest.sendGetRequest(listTablesUrl));
    assertEquals(tableList.get("tables").size(), 0);

    // Add a table
    ControllerTest.sendPostRequest(TEST_INSTANCE.getControllerRequestURLBuilder().forTableCreate(),
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build().toJsonString());

    // There should be 1 table on the tenant
    tableList = JsonUtils.stringToJsonNode(ControllerTest.sendGetRequest(listTablesUrl));
    JsonNode tables = tableList.get("tables");
    assertEquals(tables.size(), 1);

    // Check to make sure that test table exists.
    boolean found = false;
    for (int i = 0; !found && i < tables.size(); i++) {
      found = tables.get(i).asText().equals(TABLE_NAME);
    }

    assertTrue(found);
  }

  @AfterClass
  public void tearDown() {
    TEST_INSTANCE.cleanup();
  }
}
