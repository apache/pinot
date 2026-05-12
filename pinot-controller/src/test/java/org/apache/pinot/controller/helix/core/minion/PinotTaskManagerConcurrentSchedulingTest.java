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
package org.apache.pinot.controller.helix.core.minion;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Unit tests for the concurrent-scheduling dispatch helpers in {@link PinotTaskManager}:
 * {@code shouldUseConcurrentPath(TaskSchedulingContext)} and
 * {@code resolveConcurrentScheduling(TableConfig)}. The tests spy on a {@link PinotTaskManager}
 * mock and inject the cluster-default flag / resource-manager collaborator via reflection,
 * avoiding a full controller boot.
 */
public class PinotTaskManagerConcurrentSchedulingTest {

  private static final String TABLE_A = "tableA_OFFLINE";
  private static final String TABLE_B = "tableB_OFFLINE";
  private static final String DATABASE = "db1";

  private PinotTaskManager newManager(boolean clusterDefault, PinotHelixResourceManager resourceManager)
      throws Exception {
    PinotTaskManager manager = Mockito.mock(PinotTaskManager.class, Mockito.CALLS_REAL_METHODS);
    FieldUtils.writeField(manager, "_clusterConcurrentSchedulingEnabled", clusterDefault, true);
    FieldUtils.writeField(manager, "_pinotHelixResourceManager", resourceManager, true);
    return manager;
  }

  private TableConfig tableConfigWith(String tableName, Boolean concurrentFlag) {
    TableTaskConfig taskConfig = new TableTaskConfig(Map.of("TestTask", Collections.emptyMap()), concurrentFlag);
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(tableName)
        .setTaskConfig(taskConfig)
        .build();
  }

  private TableConfig tableConfigWithoutTaskConfig(String tableName) {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName).build();
  }

  // ---------- resolveConcurrentScheduling ----------

  @Test
  public void testResolveConcurrentSchedulingTableTrueWinsOverClusterFalse()
      throws Exception {
    PinotTaskManager manager = newManager(false, Mockito.mock(PinotHelixResourceManager.class));
    assertTrue(manager.resolveConcurrentScheduling(tableConfigWith(TABLE_A, Boolean.TRUE)));
  }

  @Test
  public void testResolveConcurrentSchedulingTableFalseWinsOverClusterTrue()
      throws Exception {
    PinotTaskManager manager = newManager(true, Mockito.mock(PinotHelixResourceManager.class));
    assertFalse(manager.resolveConcurrentScheduling(tableConfigWith(TABLE_A, Boolean.FALSE)));
  }

  @Test
  public void testResolveConcurrentSchedulingTableNullInheritsClusterTrue()
      throws Exception {
    PinotTaskManager manager = newManager(true, Mockito.mock(PinotHelixResourceManager.class));
    assertTrue(manager.resolveConcurrentScheduling(tableConfigWith(TABLE_A, null)));
  }

  @Test
  public void testResolveConcurrentSchedulingTableNullInheritsClusterFalse()
      throws Exception {
    PinotTaskManager manager = newManager(false, Mockito.mock(PinotHelixResourceManager.class));
    assertFalse(manager.resolveConcurrentScheduling(tableConfigWith(TABLE_A, null)));
  }

  @Test
  public void testResolveConcurrentSchedulingNoTaskConfigInheritsClusterTrue()
      throws Exception {
    PinotTaskManager manager = newManager(true, Mockito.mock(PinotHelixResourceManager.class));
    assertTrue(manager.resolveConcurrentScheduling(tableConfigWithoutTaskConfig(TABLE_A)));
  }

  @Test
  public void testResolveConcurrentSchedulingNoTaskConfigInheritsClusterFalse()
      throws Exception {
    PinotTaskManager manager = newManager(false, Mockito.mock(PinotHelixResourceManager.class));
    assertFalse(manager.resolveConcurrentScheduling(tableConfigWithoutTaskConfig(TABLE_A)));
  }

  // ---------- shouldUseConcurrentPath ----------

  @Test
  public void testShouldUseConcurrentPathSingleTargetTableOptsIn()
      throws Exception {
    PinotHelixResourceManager rm = Mockito.mock(PinotHelixResourceManager.class);
    Mockito.when(rm.getTableConfig(TABLE_A)).thenReturn(tableConfigWith(TABLE_A, Boolean.TRUE));
    PinotTaskManager manager = newManager(false, rm);

    TaskSchedulingContext ctx = new TaskSchedulingContext().setTablesToSchedule(Set.of(TABLE_A));
    assertTrue(manager.shouldUseConcurrentPath(ctx));
  }

  @Test
  public void testShouldUseConcurrentPathAnyTargetTableOptsOutForcesLegacy()
      throws Exception {
    PinotHelixResourceManager rm = Mockito.mock(PinotHelixResourceManager.class);
    Mockito.when(rm.getTableConfig(TABLE_A)).thenReturn(tableConfigWith(TABLE_A, Boolean.TRUE));
    Mockito.when(rm.getTableConfig(TABLE_B)).thenReturn(tableConfigWith(TABLE_B, Boolean.FALSE));
    PinotTaskManager manager = newManager(true, rm);

    TaskSchedulingContext ctx = new TaskSchedulingContext().setTablesToSchedule(Set.of(TABLE_A, TABLE_B));
    assertFalse(manager.shouldUseConcurrentPath(ctx));
  }

  @Test
  public void testShouldUseConcurrentPathAllTargetTablesInheritClusterTrue()
      throws Exception {
    PinotHelixResourceManager rm = Mockito.mock(PinotHelixResourceManager.class);
    Mockito.when(rm.getTableConfig(TABLE_A)).thenReturn(tableConfigWith(TABLE_A, null));
    Mockito.when(rm.getTableConfig(TABLE_B)).thenReturn(tableConfigWith(TABLE_B, null));
    PinotTaskManager manager = newManager(true, rm);

    TaskSchedulingContext ctx = new TaskSchedulingContext().setTablesToSchedule(Set.of(TABLE_A, TABLE_B));
    assertTrue(manager.shouldUseConcurrentPath(ctx));
  }

  @Test
  public void testShouldUseConcurrentPathAllTargetTablesInheritClusterFalse()
      throws Exception {
    PinotHelixResourceManager rm = Mockito.mock(PinotHelixResourceManager.class);
    Mockito.when(rm.getTableConfig(TABLE_A)).thenReturn(tableConfigWith(TABLE_A, null));
    PinotTaskManager manager = newManager(false, rm);

    TaskSchedulingContext ctx = new TaskSchedulingContext().setTablesToSchedule(Set.of(TABLE_A));
    assertFalse(manager.shouldUseConcurrentPath(ctx));
  }

  @Test
  public void testShouldUseConcurrentPathMissingTableConfigIsSkipped()
      throws Exception {
    // A table whose TableConfig is not found (e.g., dropped between listing and scheduling) should
    // be treated as "not in scope" rather than forcing either path. The remaining inspected table
    // determines the decision.
    PinotHelixResourceManager rm = Mockito.mock(PinotHelixResourceManager.class);
    Mockito.when(rm.getTableConfig(TABLE_A)).thenReturn(null);
    Mockito.when(rm.getTableConfig(TABLE_B)).thenReturn(tableConfigWith(TABLE_B, Boolean.TRUE));
    PinotTaskManager manager = newManager(false, rm);

    TaskSchedulingContext ctx = new TaskSchedulingContext().setTablesToSchedule(Set.of(TABLE_A, TABLE_B));
    assertTrue(manager.shouldUseConcurrentPath(ctx));
  }

  @Test
  public void testShouldUseConcurrentPathEmptyScopeFallsBackToClusterDefaultTrue()
      throws Exception {
    // When no target tables or databases are supplied and the cluster has no tables to enumerate,
    // no table is inspected: the decision falls back to the cluster-level default.
    PinotHelixResourceManager rm = Mockito.mock(PinotHelixResourceManager.class);
    Mockito.when(rm.getAllTables()).thenReturn(Collections.emptyList());
    PinotTaskManager manager = newManager(true, rm);

    assertTrue(manager.shouldUseConcurrentPath(new TaskSchedulingContext()));
  }

  @Test
  public void testShouldUseConcurrentPathEmptyScopeFallsBackToClusterDefaultFalse()
      throws Exception {
    PinotHelixResourceManager rm = Mockito.mock(PinotHelixResourceManager.class);
    Mockito.when(rm.getAllTables()).thenReturn(Collections.emptyList());
    PinotTaskManager manager = newManager(false, rm);

    assertFalse(manager.shouldUseConcurrentPath(new TaskSchedulingContext()));
  }

  @Test
  public void testShouldUseConcurrentPathDatabaseScopeExpandsAndHonorsPerTableOptOut()
      throws Exception {
    PinotHelixResourceManager rm = Mockito.mock(PinotHelixResourceManager.class);
    Mockito.when(rm.getAllTables(DATABASE)).thenReturn(List.of(TABLE_A, TABLE_B));
    Mockito.when(rm.getTableConfig(TABLE_A)).thenReturn(tableConfigWith(TABLE_A, Boolean.TRUE));
    Mockito.when(rm.getTableConfig(TABLE_B)).thenReturn(tableConfigWith(TABLE_B, Boolean.FALSE));
    PinotTaskManager manager = newManager(true, rm);

    TaskSchedulingContext ctx = new TaskSchedulingContext().setDatabasesToSchedule(Set.of(DATABASE));
    assertFalse(manager.shouldUseConcurrentPath(ctx));
  }

  @Test
  public void testShouldUseConcurrentPathAllTablesScopeHonorsPerTableOptOut()
      throws Exception {
    // When the request doesn't name tables or databases, every table in the cluster is inspected
    // so that a single opt-out still forces the legacy path.
    PinotHelixResourceManager rm = Mockito.mock(PinotHelixResourceManager.class);
    Mockito.when(rm.getAllTables()).thenReturn(List.of(TABLE_A, TABLE_B));
    Mockito.when(rm.getTableConfig(TABLE_A)).thenReturn(tableConfigWith(TABLE_A, null));
    Mockito.when(rm.getTableConfig(TABLE_B)).thenReturn(tableConfigWith(TABLE_B, Boolean.FALSE));
    PinotTaskManager manager = newManager(true, rm);

    assertFalse(manager.shouldUseConcurrentPath(new TaskSchedulingContext()));
  }
}
