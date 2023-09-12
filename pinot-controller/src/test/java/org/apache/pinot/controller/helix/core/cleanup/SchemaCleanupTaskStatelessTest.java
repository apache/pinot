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
package org.apache.pinot.controller.helix.core.cleanup;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.MetricValueUtils;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.controller.BaseControllerStarter;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/**
 * This test can be deleted once {@link BaseControllerStarter#fixSchemaNameInTableConfig()} is deleted. Likely in 2.0.0.
 */
@Test(groups = "stateless")
public class SchemaCleanupTaskStatelessTest extends ControllerTest {
  @BeforeClass
  public void setup()
      throws Exception {
    startZk();
    startController();
    startFakeBroker();
    startFakeServer();
  }

  private void startFakeBroker()
      throws Exception {
    String brokerInstance = CommonConstants.Helix.PREFIX_OF_BROKER_INSTANCE + NetUtils.getHostAddress() + "_"
        + CommonConstants.Helix.DEFAULT_BROKER_QUERY_PORT;

    // Create server instance with the fake server state model
    HelixManager brokerHelixManager =
        HelixManagerFactory.getZKHelixManager(getHelixClusterName(), brokerInstance, InstanceType.PARTICIPANT,
            getZkUrl());
    brokerHelixManager.connect();

    // Add Helix tag to the server
    brokerHelixManager.getClusterManagmentTool().addInstanceTag(getHelixClusterName(), brokerInstance,
        TagNameUtils.getBrokerTagForTenant(TagNameUtils.DEFAULT_TENANT_NAME));
  }

  private void startFakeServer()
      throws Exception {
    String serverInstance = CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE + NetUtils.getHostAddress() + "_"
        + CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT;

    // Create server instance with the fake server state model
    HelixManager serverHelixManager = HelixManagerFactory
        .getZKHelixManager(getHelixClusterName(), serverInstance, InstanceType.PARTICIPANT, getZkUrl());
    serverHelixManager.connect();

    // Add Helix tag to the server
    serverHelixManager.getClusterManagmentTool().addInstanceTag(getHelixClusterName(), serverInstance,
        TableNameBuilder.OFFLINE.tableNameWithType(TagNameUtils.DEFAULT_TENANT_NAME));
  }

  @AfterClass
  public void teardown() {
    stopController();
    stopZk();
  }

  @Test
  public void testSchemaCleanupTask()
      throws Exception {
    PinotMetricUtils.cleanUp();
    // 1. Add a schema
    addSchema(createDummySchema("t1"));
    addSchema(createDummySchema("t2"));
    addSchema(createDummySchema("t3"));

    // 2. Add a table with the schema name reference
    addTableConfig(createDummyTableConfig("t1", "t1"));
    addTableConfig(createDummyTableConfig("t2", "t2"));
    addTableConfig(createDummyTableConfig("t3", "t3"));

    _helixResourceManager.setExistingTableConfig(createDummyTableConfig("t1", "t2"));
    _helixResourceManager.setExistingTableConfig(createDummyTableConfig("t2", "t3"));
    _helixResourceManager.setExistingTableConfig(createDummyTableConfig("t3", "t1"));

    // 3. Fix table schema
    _controllerStarter.fixSchemaNameInTableConfig();

    // 4. validate
    assertEquals(getHelixResourceManager().getAllTables().size(), 3);
    assertEquals(getHelixResourceManager().getSchemaNames().size(), 3);

    assertNull(getHelixResourceManager().getTableConfig("t1_OFFLINE").getValidationConfig().getSchemaName());
    assertNull(getHelixResourceManager().getTableConfig("t2_OFFLINE").getValidationConfig().getSchemaName());
    assertNull(getHelixResourceManager().getTableConfig("t3_OFFLINE").getValidationConfig().getSchemaName());

    assertEquals(MetricValueUtils.getGlobalGaugeValue(_controllerStarter.getControllerMetrics(),
        ControllerGauge.MISCONFIGURED_SCHEMA_TABLE_COUNT), 3);
    assertEquals(MetricValueUtils.getGlobalGaugeValue(_controllerStarter.getControllerMetrics(),
        ControllerGauge.FIXED_SCHEMA_TABLE_COUNT), 3);
    assertEquals(MetricValueUtils.getGlobalGaugeValue(_controllerStarter.getControllerMetrics(),
        ControllerGauge.TABLE_WITHOUT_SCHEMA_COUNT), 0);
    assertEquals(MetricValueUtils.getGlobalGaugeValue(_controllerStarter.getControllerMetrics(),
        ControllerGauge.FAILED_TO_COPY_SCHEMA), 0);
    assertEquals(MetricValueUtils.getGlobalGaugeValue(_controllerStarter.getControllerMetrics(),
        ControllerGauge.FAILED_TO_UPDATE_TABLE_CONFIG), 0);

    // 5. Clean up
    for (String table : getHelixResourceManager().getAllOfflineTables()) {
      getHelixResourceManager().deleteOfflineTable(table);
    }
    for (String schema : getHelixResourceManager().getSchemaNames()) {
      getHelixResourceManager().deleteSchema(schema);
    }
  }

  @Test
  public void testSchemaCleanupTaskNormalCase()
      throws Exception {
    PinotMetricUtils.cleanUp();
    // 1. Add a schema
    addSchema(createDummySchema("t1"));
    addSchema(createDummySchema("t2"));
    addSchema(createDummySchema("t3"));

    assertEquals(getHelixResourceManager().getSchemaNames().size(), 3);

    // 2. Add a table with the schema name reference
    addTableConfig(createDummyTableConfig("t1", "t1"));
    addTableConfig(createDummyTableConfig("t2", "t2"));
    addTableConfig(createDummyTableConfig("t3", "t3"));

    assertEquals(getHelixResourceManager().getAllTables().size(), 3);

    // 3. Create new schemas and update table to new schema
    addSchema(createDummySchema("t11"));
    addSchema(createDummySchema("t21"));
    addSchema(createDummySchema("t31"));
    _helixResourceManager.setExistingTableConfig(createDummyTableConfig("t1", "t11"));
    _helixResourceManager.setExistingTableConfig(createDummyTableConfig("t2", "t21"));
    _helixResourceManager.setExistingTableConfig(createDummyTableConfig("t3", "t31"));

    assertEquals(getHelixResourceManager().getAllTables().size(), 3);
    assertEquals(getHelixResourceManager().getSchemaNames().size(), 6);
    assertEquals(getHelixResourceManager().getTableConfig("t1_OFFLINE").getValidationConfig().getSchemaName(), "t11");
    assertEquals(getHelixResourceManager().getTableConfig("t2_OFFLINE").getValidationConfig().getSchemaName(), "t21");
    assertEquals(getHelixResourceManager().getTableConfig("t3_OFFLINE").getValidationConfig().getSchemaName(), "t31");

    // 4. Delete schema t1, t2, t3, so we can check if those schemas are fixed later.
    deleteSchema("t1");
    deleteSchema("t2");
    deleteSchema("t3");

    assertEquals(getHelixResourceManager().getSchemaNames().size(), 3);

    // 5. Fix table schema
    _controllerStarter.fixSchemaNameInTableConfig();

    // 6. All tables will directly set schema.
    assertEquals(getHelixResourceManager().getAllTables().size(), 3);
    assertEquals(getHelixResourceManager().getSchemaNames().size(), 6);
    assertTrue(getHelixResourceManager().getSchemaNames().contains("t1"));
    assertTrue(getHelixResourceManager().getSchemaNames().contains("t2"));
    assertTrue(getHelixResourceManager().getSchemaNames().contains("t3"));

    assertNull(getHelixResourceManager().getTableConfig("t1_OFFLINE").getValidationConfig().getSchemaName());
    assertNull(getHelixResourceManager().getTableConfig("t2_OFFLINE").getValidationConfig().getSchemaName());
    assertNull(getHelixResourceManager().getTableConfig("t3_OFFLINE").getValidationConfig().getSchemaName());

    assertEquals(MetricValueUtils.getGlobalGaugeValue(_controllerStarter.getControllerMetrics(),
        ControllerGauge.MISCONFIGURED_SCHEMA_TABLE_COUNT), 0);
    assertEquals(MetricValueUtils.getGlobalGaugeValue(_controllerStarter.getControllerMetrics(),
        ControllerGauge.FIXED_SCHEMA_TABLE_COUNT), 3);
    assertEquals(MetricValueUtils.getGlobalGaugeValue(_controllerStarter.getControllerMetrics(),
        ControllerGauge.TABLE_WITHOUT_SCHEMA_COUNT), 0);
    assertEquals(MetricValueUtils.getGlobalGaugeValue(_controllerStarter.getControllerMetrics(),
        ControllerGauge.FAILED_TO_COPY_SCHEMA), 0);
    assertEquals(MetricValueUtils.getGlobalGaugeValue(_controllerStarter.getControllerMetrics(),
        ControllerGauge.FAILED_TO_UPDATE_TABLE_CONFIG), 0);

    // 7. Clean up
    for (String table : getHelixResourceManager().getAllOfflineTables()) {
      getHelixResourceManager().deleteOfflineTable(table);
    }
    for (String schema : getHelixResourceManager().getSchemaNames()) {
      getHelixResourceManager().deleteSchema(schema);
    }
  }

  @Test
  public void testMissingSchema()
      throws Exception {
    PinotMetricUtils.cleanUp();
    // 1. Add a schema
    addSchema(createDummySchema("t1"));
    addSchema(createDummySchema("t2"));
    addSchema(createDummySchema("t3"));

    assertEquals(getHelixResourceManager().getSchemaNames().size(), 3);

    // 2. Add a table with the schema name reference
    addTableConfig(createDummyTableConfig("t1"));
    addTableConfig(createDummyTableConfig("t2"));
    addTableConfig(createDummyTableConfig("t3"));

    assertEquals(getHelixResourceManager().getAllTables().size(), 3);

    // 4. Delete schema t1, t2, t3, so we can check if those schemas are fixed later.
    deleteSchema("t1");
    deleteSchema("t2");
    deleteSchema("t3");

    assertEquals(getHelixResourceManager().getSchemaNames().size(), 0);

    // 5. Fix table schema
    _controllerStarter.fixSchemaNameInTableConfig();

    // 6. We cannot fix schema
    assertEquals(getHelixResourceManager().getAllTables().size(), 3);
    assertEquals(getHelixResourceManager().getSchemaNames().size(), 0);

    assertNull(getHelixResourceManager().getTableConfig("t1_OFFLINE").getValidationConfig().getSchemaName());
    assertNull(getHelixResourceManager().getTableConfig("t2_OFFLINE").getValidationConfig().getSchemaName());
    assertNull(getHelixResourceManager().getTableConfig("t3_OFFLINE").getValidationConfig().getSchemaName());

    assertEquals(MetricValueUtils.getGlobalGaugeValue(_controllerStarter.getControllerMetrics(),
        ControllerGauge.MISCONFIGURED_SCHEMA_TABLE_COUNT), 0);
    assertEquals(MetricValueUtils.getGlobalGaugeValue(_controllerStarter.getControllerMetrics(),
        ControllerGauge.FIXED_SCHEMA_TABLE_COUNT), 0);
    assertEquals(MetricValueUtils.getGlobalGaugeValue(_controllerStarter.getControllerMetrics(),
        ControllerGauge.TABLE_WITHOUT_SCHEMA_COUNT), 3);
    assertEquals(MetricValueUtils.getGlobalGaugeValue(_controllerStarter.getControllerMetrics(),
        ControllerGauge.FAILED_TO_COPY_SCHEMA), 0);
    assertEquals(MetricValueUtils.getGlobalGaugeValue(_controllerStarter.getControllerMetrics(),
        ControllerGauge.FAILED_TO_UPDATE_TABLE_CONFIG), 0);

    // 7. Clean up
    for (String table : getHelixResourceManager().getAllOfflineTables()) {
      getHelixResourceManager().deleteOfflineTable(table);
    }
    for (String schema : getHelixResourceManager().getSchemaNames()) {
      getHelixResourceManager().deleteSchema(schema);
    }
  }

  private TableConfig createDummyTableConfig(String table) {
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(table)
        .build();
  }

  private TableConfig createDummyTableConfig(String table, String schema) {
    TableConfig tableConfig = createDummyTableConfig(table);
    tableConfig.getValidationConfig().setSchemaName(schema);
    return tableConfig;
  }
}
