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

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class PinotDatabaseRestletResourceTest {
  private static final String DATABASE = "db";
  private static final List<String> TABLES = Lists.newArrayList("a_REALTIME", "b_OFFLINE", "c_REALTIME", "d_OFFLINE");

  @Mock
  PinotHelixResourceManager _resourceManager;
  @InjectMocks
  PinotDatabaseRestletResource _resource;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    when(_resourceManager.getAllTables(DATABASE)).thenReturn(TABLES);
    doNothing().when(_resourceManager).deleteTable(anyString(), any(TableType.class), any());
    when(_resourceManager.deleteSchema(anyString())).thenReturn(true);
  }

  @Test
  public void successfulDatabaseDeletionDryRunTest() {
    successfulDatabaseDeletionCheck(true);
  }

  @Test
  public void successfulDatabaseDeletionTest() {
    successfulDatabaseDeletionCheck(false);
  }

  private void successfulDatabaseDeletionCheck(boolean dryRun) {
    DeleteDatabaseResponse response = _resource.deleteTablesInDatabase(DATABASE, dryRun);
    assertEquals(response.isDryRun(), dryRun);
    assertTrue(response.getFailedTables().isEmpty());
    assertEquals(response.getDeletedTables(), TABLES);
  }

  @Test
  public void partialDatabaseDeletionWithDeleteTableFailureTest() {
    int failureTableIdx = TABLES.size() / 2;
    doThrow(new RuntimeException()).when(_resourceManager)
        .deleteTable(TABLES.get(failureTableIdx), TableType.REALTIME, null);
    partialDatabaseDeletionCheck(failureTableIdx);
  }

  @Test
  public void partialDatabaseDeletionWithDeleteSchemaFailureTest() {
    int failureSchemaIdx = TABLES.size() / 2;
    doThrow(new RuntimeException()).when(_resourceManager)
        .deleteSchema(TableNameBuilder.extractRawTableName(TABLES.get(failureSchemaIdx)));
    partialDatabaseDeletionCheck(failureSchemaIdx);
  }

  private void partialDatabaseDeletionCheck(int idx) {
    DeleteDatabaseResponse response = _resource.deleteTablesInDatabase(DATABASE, false);
    List<String> resultList = new ArrayList<>(TABLES);
    String failedTable = resultList.remove(idx);
    assertFalse(response.isDryRun());
    assertEquals(response.getFailedTables().size(), 1);
    assertEquals(response.getFailedTables().get(0).getTableName(), failedTable);
    assertEquals(response.getDeletedTables(), resultList);
  }
}
