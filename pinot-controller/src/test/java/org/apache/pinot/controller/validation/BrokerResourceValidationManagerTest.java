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
package org.apache.pinot.controller.validation;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.periodictask.PeriodicTask;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Unit tests for {@link BrokerResourceValidationManager}, including that getTablesToProcess
 * returns both physical table names and logical table partition names (Issue #15751).
 */
public class BrokerResourceValidationManagerTest {

  private static final String PHYSICAL_TABLE = "myTable_OFFLINE";
  private static final String LOGICAL_TABLE_PARTITION = "my_logical_table";

  private PinotHelixResourceManager _resourceManager;
  private BrokerResourceValidationManager _validationManager;

  @BeforeMethod
  public void setUp() {
    _resourceManager = mock(PinotHelixResourceManager.class);
    when(_resourceManager.getAllTables()).thenReturn(List.of(PHYSICAL_TABLE));
    when(_resourceManager.getBrokerResourceLogicalTables()).thenReturn(List.of(LOGICAL_TABLE_PARTITION));

    ControllerConf config = new ControllerConf();
    LeadControllerManager leadControllerManager = mock(LeadControllerManager.class);
    ControllerMetrics controllerMetrics = new ControllerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    _validationManager = new BrokerResourceValidationManager(config, _resourceManager, leadControllerManager,
        controllerMetrics);
  }

  /**
   * Verifies that getTablesToProcess returns both physical tables (from getAllTables) and
   * logical table partitions (from getBrokerResourceLogicalTables) so that the
   * periodic task validates and repairs broker resource for logical tables too.
   */
  @Test
  public void testGetTablesToProcessIncludesLogicalTablePartitions() {
    List<String> tables = _validationManager.getTablesToProcess(new Properties());

    assertTrue(tables.contains(PHYSICAL_TABLE), "Should include physical table: " + PHYSICAL_TABLE);
    assertTrue(tables.contains(LOGICAL_TABLE_PARTITION),
        "Should include logical table partition: " + LOGICAL_TABLE_PARTITION);
    assertEquals(tables.size(), 2, "Should have exactly physical + logical");
  }

  @Test
  public void testGetTablesToProcessWithTableNamePropertyReturnsOnlyThatTable() {
    String singleTable = "singleTable_REALTIME";
    Properties props = new Properties();
    props.setProperty(PeriodicTask.PROPERTY_KEY_TABLE_NAME, singleTable);

    List<String> tables = _validationManager.getTablesToProcess(props);

    assertEquals(tables, List.of(singleTable), "When tableName property is set, only that table should be returned");
  }

  @Test
  public void testGetTablesToProcessWhenNoLogicalPartitions() {
    when(_resourceManager.getBrokerResourceLogicalTables()).thenReturn(Collections.emptyList());

    List<String> tables = _validationManager.getTablesToProcess(new Properties());

    assertEquals(tables, List.of(PHYSICAL_TABLE));
  }
}
