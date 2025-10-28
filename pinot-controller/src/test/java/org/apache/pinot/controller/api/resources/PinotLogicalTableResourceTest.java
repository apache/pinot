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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.table.QuotaConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.TimeBoundaryConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class PinotLogicalTableResourceTest extends ControllerTest {

  private static final String LOGICAL_TABLE_NAME = "test_logical_table";
  public static final String BROKER_TENANT = "DefaultTenant";
  public static final String NEW_BROKER_TENANT = "NewBrokerTenant";

  @BeforeClass
  public void setUpClass()
      throws Exception {
    startZk();
    startController();
    addFakeBrokerInstancesToAutoJoinHelixCluster(2, false);
    addFakeServerInstancesToAutoJoinHelixCluster(1, true);
    createBrokerTenant(BROKER_TENANT, 1);
    createBrokerTenant(NEW_BROKER_TENANT, 1);
  }

  @AfterClass
  public void tearDownClass() {
    stopFakeInstances();
    stopController();
    stopZk();
  }

  @AfterMethod
  public void tearDown() {
    // cleans up the physical tables after each testcase
    cleanup();
  }

  @DataProvider
  public Object[][] tableNamesProvider() {
    return new Object[][]{
        {"test_logical_table", List.of("test_table_1", "test_table_2"), List.of("test_table_3"), Map.of()},
        {"db.test_logical_table", List.of("db.test_table_1", "db.test_table_2"), List.of("db.test_table_3"), Map.of()},
        {
            "test_logical_table", List.of("db1.test_table_1", "db1.test_table_2"), List.of("db1.test_table_3"), Map.of(
            CommonConstants.DATABASE, "db1")
        },
    };
  }

  @Test(dataProvider = "tableNamesProvider")
  public void testCreateUpdateDeleteLogicalTables(String logicalTableName, List<String> physicalTableNames,
      List<String> physicalTablesToUpdate, Map<String, String> dbHeaders)
      throws IOException {
    Map<String, String> headers = new HashMap<>(getHeaders());
    headers.putAll(dbHeaders);
    logicalTableName = DatabaseUtils.translateTableName(logicalTableName, headers.get(CommonConstants.DATABASE));

    addDummySchema(logicalTableName);

    // verify logical table does not exist
    verifyLogicalTableDoesNotExists(logicalTableName, headers);

    // setup physical and logical tables
    List<String> physicalTableNamesWithType = createHybridTables(physicalTableNames);
    LogicalTableConfig
        logicalTableConfig = getDummyLogicalTableConfig(logicalTableName, physicalTableNamesWithType, BROKER_TENANT);

    // create logical table
    String resp = createLogicalTable(logicalTableConfig, headers);
    assertEquals(resp,
        "{\"unrecognizedProperties\":{},\"status\":\"" + logicalTableName + " logical table successfully added.\"}");

    // verify logical table
    verifyLogicalTableExists(logicalTableName, logicalTableConfig, headers);

    // update logical table and setup new physical tables
    List<String> tableNameToUpdateWithType = createHybridTables(physicalTablesToUpdate);
    tableNameToUpdateWithType.addAll(physicalTableNamesWithType);
    logicalTableConfig = getDummyLogicalTableConfig(logicalTableName, tableNameToUpdateWithType, BROKER_TENANT);

    String response = updateLogicalTable(logicalTableName, logicalTableConfig, headers);
    assertEquals(response,
        "{\"unrecognizedProperties\":{},\"status\":\"" + logicalTableName + " logical table successfully updated.\"}");

    // verify updated logical table
    verifyLogicalTableExists(logicalTableName, logicalTableConfig, headers);

    // delete logical table
    String deleteResponse = deleteLogicalTable(logicalTableName, headers);
    assertEquals(deleteResponse, "{\"status\":\"" + logicalTableName + " logical table successfully deleted.\"}");

    // verify logical table is deleted
    verifyLogicalTableDoesNotExists(logicalTableName, headers);
  }

  @Test(expectedExceptions = IOException.class,
      expectedExceptionsMessageRegExp = ".*Reason: 'quota.storage' should not be set for logical table.*")
  public void testLogicalTableQuotaConfigValidation()
      throws IOException {
    List<String> physicalTableNamesWithType = createHybridTables(List.of("test_table_1"));
    LogicalTableConfig logicalTableConfig =
        getDummyLogicalTableConfig(LOGICAL_TABLE_NAME, physicalTableNamesWithType, BROKER_TENANT);
    logicalTableConfig.setQuotaConfig(new QuotaConfig("10G", "999"));
    createLogicalTable(logicalTableConfig, getHeaders());
  }

  @Test
  public void testLogicalTableReferenceTableValidation()
      throws IOException {
    final List<String> physicalTableNamesWithType = createHybridTables(List.of("test_table_7"));

    // Test ref offline table name is null validation
    IOException aThrows = expectThrows(
        IOException.class, () -> {
          LogicalTableConfig logicalTableConfig =
              getDummyLogicalTableConfig(LOGICAL_TABLE_NAME, physicalTableNamesWithType, BROKER_TENANT);
          logicalTableConfig.setRefOfflineTableName(null);
          createLogicalTable(logicalTableConfig, getHeaders());
        }
    );
    assertTrue(aThrows.getMessage()
            .contains("Reason: 'refOfflineTableName' should not be null or empty when offline table exists"),
        aThrows.getMessage());

    // Test ref realtime table name is null validation
    aThrows = expectThrows(
        IOException.class, () -> {
          LogicalTableConfig logicalTableConfig =
              getDummyLogicalTableConfig(LOGICAL_TABLE_NAME, physicalTableNamesWithType, BROKER_TENANT);
          logicalTableConfig.setRefRealtimeTableName(null);
          createLogicalTable(logicalTableConfig, getHeaders());
        }
    );
    assertTrue(aThrows.getMessage()
            .contains("Reason: 'refRealtimeTableName' should not be null or empty when realtime table exists"),
        aThrows.getMessage());

    // Test ref offline table is present in the offline tables validation
    aThrows = expectThrows(
        IOException.class, () -> {
          LogicalTableConfig logicalTableConfig =
              getDummyLogicalTableConfig(LOGICAL_TABLE_NAME, physicalTableNamesWithType, BROKER_TENANT);
          logicalTableConfig.setRefOfflineTableName("random_table_OFFLINE");
          createLogicalTable(logicalTableConfig, getHeaders());
        }
    );
    assertTrue(aThrows.getMessage()
            .contains("Reason: 'refOfflineTableName' should be one of the provided offline tables"),
        aThrows.getMessage());

    // Test ref realtime table is present in the realtime tables validation
    aThrows = expectThrows(
        IOException.class, () -> {
          LogicalTableConfig logicalTableConfig =
              getDummyLogicalTableConfig(LOGICAL_TABLE_NAME, physicalTableNamesWithType, BROKER_TENANT);
          logicalTableConfig.setRefRealtimeTableName("random_table_REALTIME");
          createLogicalTable(logicalTableConfig, getHeaders());
        }
    );
    assertTrue(aThrows.getMessage()
            .contains("Reason: 'refRealtimeTableName' should be one of the provided realtime tables"),
        aThrows.getMessage());

    // Test ref offline table is not null but offline table does not exist
    aThrows = expectThrows(
        IOException.class, () -> {
          LogicalTableConfig logicalTableConfig =
              getDummyLogicalTableConfig(LOGICAL_TABLE_NAME, List.of("test_table_7_REALTIME"), BROKER_TENANT);
          logicalTableConfig.setRefOfflineTableName("test_table_7_OFFLINE");
          createLogicalTable(logicalTableConfig, getHeaders());
        }
    );
    assertTrue(aThrows.getMessage()
            .contains("Reason: 'refOfflineTableName' should be null or empty when offline tables do not exist"),
        aThrows.getMessage());

    // Test ref realtime table is not null but realtime table does not exist
    aThrows = expectThrows(
        IOException.class, () -> {
          LogicalTableConfig logicalTableConfig =
              getDummyLogicalTableConfig(LOGICAL_TABLE_NAME, List.of("test_table_7_OFFLINE"), BROKER_TENANT);
          logicalTableConfig.setRefRealtimeTableName("test_table_7_REALTIME");
          createLogicalTable(logicalTableConfig, getHeaders());
        }
    );
    assertTrue(aThrows.getMessage()
            .contains("Reason: 'refRealtimeTableName' should be null or empty when realtime tables do not exist"),
        aThrows.getMessage());

    // Test ref offline table name is realtime table name
    aThrows = expectThrows(
        IOException.class, () -> {
          LogicalTableConfig logicalTableConfig =
              getDummyLogicalTableConfig(LOGICAL_TABLE_NAME, physicalTableNamesWithType, BROKER_TENANT);
          logicalTableConfig.setRefOfflineTableName("test_table_7_REALTIME");
          createLogicalTable(logicalTableConfig, getHeaders());
        }
    );
    assertTrue(aThrows.getMessage().contains("Reason: 'refOfflineTableName' should be an offline table type"),
        aThrows.getMessage());

    // Test ref realtime table name is offline table name
    aThrows = expectThrows(
        IOException.class, () -> {
          LogicalTableConfig logicalTableConfig =
              getDummyLogicalTableConfig(LOGICAL_TABLE_NAME, physicalTableNamesWithType, BROKER_TENANT);
          logicalTableConfig.setRefRealtimeTableName("test_table_7_OFFLINE");
          createLogicalTable(logicalTableConfig, getHeaders());
        }
    );
    assertTrue(aThrows.getMessage().contains("Reason: 'refRealtimeTableName' should be a realtime table type"),
        aThrows.getMessage());

    // Test ref offline table is specified with a database prefix
    aThrows = expectThrows(
        IOException.class, () -> {
          LogicalTableConfig logicalTableConfig =
              getDummyLogicalTableConfig(LOGICAL_TABLE_NAME, physicalTableNamesWithType, BROKER_TENANT);
          logicalTableConfig.setRefOfflineTableName("db.test_table_7_OFFLINE");
          createLogicalTable(logicalTableConfig, getHeaders());
        }
    );
    assertTrue(
        aThrows.getMessage().contains("Reason: 'refOfflineTableName' should be one of the provided offline tables"),
        aThrows.getMessage());

    // Test ref realtime table is specified with a database prefix
    aThrows = expectThrows(
        IOException.class, () -> {
          LogicalTableConfig logicalTableConfig =
              getDummyLogicalTableConfig(LOGICAL_TABLE_NAME, physicalTableNamesWithType, BROKER_TENANT);
          logicalTableConfig.setRefRealtimeTableName("db.test_table_7_REALTIME");
          createLogicalTable(logicalTableConfig, getHeaders());
        }
    );
    assertTrue(
        aThrows.getMessage().contains("Reason: 'refRealtimeTableName' should be one of the provided realtime tables"),
        aThrows.getMessage());
  }

  @Test
  public void testLogicalTableDatabaseValidation() {
    String logicalTableName = "db1.test_logical_table";
    LogicalTableConfig logicalTableConfig =
        getDummyLogicalTableConfig(logicalTableName, List.of("test_table_1_OFFLINE", "test_table_2_REALTIME"),
            BROKER_TENANT);
    // Test add logical table with different database prefix
    String msg = expectThrows(IOException.class, () -> {
      addLogicalTableConfig(logicalTableConfig);
    }).getMessage();
    System.out.println("msg: " + msg);
    assertTrue(msg.contains(
        "Reason: 'test_table_1_OFFLINE' should have the same database name as logical table: db1 != default"), msg);

    // Test update logical table with different database prefix
    msg = expectThrows(IOException.class, () -> updateLogicalTableConfig(logicalTableConfig)).getMessage();
    System.out.println("expectThrows msg = " + msg);
    assertTrue(
        msg.contains(
            "Reason: 'test_table_1_OFFLINE' should have the same database name as logical table: db1 != default"),
        msg);
  }

  @Test(expectedExceptions = IOException.class,
      expectedExceptionsMessageRegExp = ".*Reason: 'InvalidTenant' should be one of the existing broker tenants.*")
  public void testLogicalTableBrokerTenantValidation()
      throws IOException {
    List<String> physicalTableNamesWithType = createHybridTables(List.of("test_table_3"));
    LogicalTableConfig logicalTableConfig =
        getDummyLogicalTableConfig(LOGICAL_TABLE_NAME, physicalTableNamesWithType, "InvalidTenant");

    createLogicalTable(logicalTableConfig, getHeaders());
  }

  @Test
  public void testLogicalTablePhysicalTableConfigValidation() {
    // Test empty physical table names is not allowed
    Throwable throwable = expectThrows(IOException.class, () -> {
      LogicalTableConfig tableConfig = getDummyLogicalTableConfig(LOGICAL_TABLE_NAME, List.of(), BROKER_TENANT);
      createLogicalTable(tableConfig, getHeaders());
    });
    assertTrue(throwable.getMessage().contains("Reason: 'physicalTableConfigMap' should not be null or empty"),
        throwable.getMessage());

    // Test all table names are physical table names and none is hybrid table name
    throwable = expectThrows(IOException.class, () -> {
      List<String> physicalTableNames = List.of("test_table_1");
      LogicalTableConfig logicalTableConfig =
          getDummyLogicalTableConfig(LOGICAL_TABLE_NAME, physicalTableNames, BROKER_TENANT);
      createLogicalTable(logicalTableConfig, getHeaders());
    });
    assertTrue(throwable.getMessage().contains("Reason: 'test_table_1' should be one of the existing tables"),
        throwable.getMessage());
  }

  @Test(expectedExceptions = IOException.class,
      expectedExceptionsMessageRegExp = ".*Table name: test_table already exists.*")
  public void testLogicalTableNameCannotSameAsPhysicalTableNameValidation()
      throws IOException {
    String tableName = "test_table";
    List<String> physicalTableNamesWithType = createHybridTables(List.of(tableName));
    LogicalTableConfig logicalTableConfig =
        getDummyLogicalTableConfig(tableName, physicalTableNamesWithType, BROKER_TENANT);
    createLogicalTable(logicalTableConfig, getHeaders());
  }

  @Test
  public void testLogicalTableNameSuffixValidation()
      throws IOException {
    List<String> physicalTableNamesWithType = createHybridTables(List.of("test_table_4"));

    // Test logical table name with _OFFLINE suffix validation
    Throwable throwable = expectThrows(IOException.class, () -> {
      LogicalTableConfig logicalTableConfig =
          getDummyLogicalTableConfig("test_logical_table_OFFLINE", physicalTableNamesWithType, BROKER_TENANT);
      createLogicalTable(logicalTableConfig, getHeaders());
    });
    assertTrue(throwable.getMessage().contains("Reason: 'tableName' should not end with _OFFLINE or _REALTIME"),
        throwable.getMessage());

    // Test logical table name with _REALTIME suffix validation
    throwable = expectThrows(IOException.class, () -> {
      LogicalTableConfig logicalTableConfig =
          getDummyLogicalTableConfig("test_logical_table_REALTIME", physicalTableNamesWithType, BROKER_TENANT);
      createLogicalTable(logicalTableConfig, getHeaders());
    });
    assertTrue(throwable.getMessage().contains("Reason: 'tableName' should not end with _OFFLINE or _REALTIME"),
        throwable.getMessage());
  }

  @Test(dataProvider = "tableTypeProvider")
  public void testCreateLogicalTable(TableType tableType)
      throws IOException {
    // Test logical table with only realtime table
    String tableName = "test_table";
    addDummySchema(tableName);
    TableConfig tableConfig = createDummyTableConfig(tableName, tableType);
    addTableConfig(tableConfig);
    addDummySchema(LOGICAL_TABLE_NAME);
    LogicalTableConfig logicalTableConfig =
        getDummyLogicalTableConfig(LOGICAL_TABLE_NAME, List.of(tableConfig.getTableName()), BROKER_TENANT);
    createLogicalTable(logicalTableConfig, getHeaders());
    verifyLogicalTableExists(LOGICAL_TABLE_NAME, logicalTableConfig);
  }

  @Test
  public void testLogicalTableSchemaValidation()
      throws IOException {
    final List<String> physicalTableNamesWithType = createHybridTables(List.of("test_table_6"));

    // Test logical table schema does not exist
    Throwable throwable = expectThrows(IOException.class, () -> {
      LogicalTableConfig logicalTableConfig =
          getDummyLogicalTableConfig(LOGICAL_TABLE_NAME, physicalTableNamesWithType, BROKER_TENANT);
      createLogicalTable(logicalTableConfig, getHeaders());
    });
    assertTrue(throwable.getMessage().contains("Reason: Schema with same name as logical table '" + LOGICAL_TABLE_NAME
        + "' does not exist"), throwable.getMessage());

    // Test logical table with db prefix but schema without db prefix
    throwable = expectThrows(IOException.class, () -> {
      addDummySchema(LOGICAL_TABLE_NAME);
      LogicalTableConfig logicalTableConfig =
          getDummyLogicalTableConfig("db." + LOGICAL_TABLE_NAME, createHybridTables(List.of("db.test_table_6")),
              BROKER_TENANT);
      createLogicalTable(logicalTableConfig, getHeaders());
    });
    assertTrue(throwable.getMessage()
            .contains("Reason: Schema with same name as logical table 'db." + LOGICAL_TABLE_NAME + "' does not exist"),
        throwable.getMessage());
  }

  @Test
  public void testLogicalTableTimeBoundaryConfigValidation()
      throws IOException {
    // Test logical table time boundary strategy validation
    addDummySchema(LOGICAL_TABLE_NAME);
    List<String> physicalTableNamesWithType = createHybridTables(List.of("test_table_8"));
    LogicalTableConfig logicalTableConfig =
        getDummyLogicalTableConfig(LOGICAL_TABLE_NAME, physicalTableNamesWithType, BROKER_TENANT);

    // Test logical table with no time boundary config
    logicalTableConfig.setTimeBoundaryConfig(null);
    Throwable throwable = expectThrows(IOException.class, () -> {
      createLogicalTable(logicalTableConfig, getHeaders());
    });
    assertTrue(throwable.getMessage()
            .contains("Reason: 'timeBoundaryConfig' should not be null for hybrid logical tables"),
        throwable.getMessage());

    // Test logical table with time boundary config but null strategy
    logicalTableConfig.setTimeBoundaryConfig(new TimeBoundaryConfig(null, null));
    throwable = expectThrows(IOException.class, () -> {
      createLogicalTable(logicalTableConfig, getHeaders());
    });
    assertTrue(throwable.getMessage()
            .contains("Reason: 'timeBoundaryConfig.boundaryStrategy' should not be null or empty"),
        throwable.getMessage());

    // Test logical table with time boundary config but empty strategy
    logicalTableConfig.setTimeBoundaryConfig(new TimeBoundaryConfig("", null));
    throwable = expectThrows(IOException.class, () -> {
      createLogicalTable(logicalTableConfig, getHeaders());
    });
    assertTrue(throwable.getMessage()
            .contains("Reason: 'timeBoundaryConfig.boundaryStrategy' should not be null or empty"),
        throwable.getMessage());

    // Test logical table with time boundary config but null parameters
    logicalTableConfig.setTimeBoundaryConfig(new TimeBoundaryConfig("min", null));
    throwable = expectThrows(IOException.class, () -> {
      createLogicalTable(logicalTableConfig, getHeaders());
    });
    assertTrue(throwable.getMessage()
            .contains("Reason: 'timeBoundaryConfig.parameters' should not be null or empty"),
        throwable.getMessage());

    // Test logical table with time boundary config but empty parameters
    logicalTableConfig.setTimeBoundaryConfig(new TimeBoundaryConfig("min", Map.of()));
    throwable = expectThrows(IOException.class, () -> {
      createLogicalTable(logicalTableConfig, getHeaders());
    });
    assertTrue(throwable.getMessage()
            .contains("Reason: 'timeBoundaryConfig.parameters' should not be null or empty"),
        throwable.getMessage());
  }

  @Test
  public void testLogicalTableWithSameNameNotAllowed()
      throws IOException {
    addDummySchema(LOGICAL_TABLE_NAME);
    List<String> physicalTableNamesWithType = createHybridTables(List.of("test_table_5"));

    LogicalTableConfig
        logicalTableConfig = getDummyLogicalTableConfig(LOGICAL_TABLE_NAME, physicalTableNamesWithType, BROKER_TENANT);
    createLogicalTable(logicalTableConfig, getHeaders());

    verifyLogicalTableExists(LOGICAL_TABLE_NAME, logicalTableConfig);
    try {
      // create the same logical table again
      createLogicalTable(logicalTableConfig, getHeaders());
      fail("Logical Table POST request should have failed");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Logical table: test_logical_table already exists"), e.getMessage());
    }
  }

  @DataProvider
  public Object[][] physicalTableShouldExistProvider() {
    return new Object[][]{
        {LOGICAL_TABLE_NAME, List.of("test_table_1"), "unknown_table_OFFLINE"},
        {LOGICAL_TABLE_NAME, List.of("test_table_2"), "unknown_table_REALTIME"},
        {"db." + LOGICAL_TABLE_NAME, List.of("db.test_table_1"), "db.unknown_table_OFFLINE"},
        {"db." + LOGICAL_TABLE_NAME, List.of("db.test_table_2"), "db.unknown_table_REALTIME"},
    };
  }

  @Test(dataProvider = "physicalTableShouldExistProvider")
  public void testPhysicalTableShouldExist(String logicalTableName, List<String> physicalTableNames,
      String unknownTableName)
      throws IOException {
    addDummySchema(logicalTableName);
    // setup physical tables
    List<String> physicalTableNamesWithType = createHybridTables(physicalTableNames);
    physicalTableNamesWithType.add(unknownTableName);

    // Test physical table should exist
    LogicalTableConfig
        logicalTableConfig = getDummyLogicalTableConfig(logicalTableName, physicalTableNamesWithType, BROKER_TENANT);
    try {
      createLogicalTable(logicalTableConfig, getHeaders());
      fail("Logical Table POST request should have failed");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("'" + unknownTableName + "' should be one of the existing tables"),
          e.getMessage());
    }
  }

  @Test
  public void testGetLogicalTableNames()
      throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    String response = listLogicalTableNames(getHeaders());
    assertEquals(response, objectMapper.writeValueAsString(List.of()));

    // setup physical tables and logical tables
    List<String> logicalTableNames =
        List.of("db.test_logical_table_1", "default.test_logical_table_2", "test_logical_table_3");

    for (int i = 0; i < logicalTableNames.size(); i++) {
      String logicalTableName = logicalTableNames.get(i);
      String databaseName = DatabaseUtils.extractDatabaseFromFullyQualifiedTableName(logicalTableName);
      List<String> physicalTableNamesWithType = createHybridTables(List.of(databaseName + ".test_table_" + i));
      addDummySchema(logicalTableName);
      LogicalTableConfig logicalTableConfig =
          getDummyLogicalTableConfig(logicalTableName, physicalTableNamesWithType, BROKER_TENANT);

      createLogicalTable(logicalTableConfig, getHeaders());
    }

    // verify logical table names without headers, should return tables without database prefix (or default database)
    String getLogicalTableNamesResponse = listLogicalTableNames(getHeaders());
    assertEquals(getLogicalTableNamesResponse,
        objectMapper.writeValueAsString(List.of("test_logical_table_2", "test_logical_table_3")));

    // verify logical table names with headers, should return tables with database prefix
    Map<String, String> headers = new HashMap<>(getHeaders());
    headers.put(CommonConstants.DATABASE, "db");
    getLogicalTableNamesResponse = listLogicalTableNames(headers);
    assertEquals(getLogicalTableNamesResponse,
        objectMapper.writeValueAsString(List.of("db.test_logical_table_1")));
  }

  @Test
  public void testLogicalTableDatabaseHeaderMismatchValidation() {
    Map<String, String> headers = new HashMap<>(getHeaders());
    headers.put(CommonConstants.DATABASE, "db1");
    String logicalTableName = "db2.test_logical_table";
    LogicalTableConfig logicalTableConfig = getDummyLogicalTableConfig(logicalTableName,
        List.of("test_table_1_OFFLINE", "test_table_2_REALTIME"), BROKER_TENANT);

    // Test add logical table with database header mismatch
    String msg = expectThrows(IOException.class,
        () -> createLogicalTable(logicalTableConfig, headers)).getMessage();
    assertTrue(msg.contains("Database name 'db2' from table prefix does not match database name 'db1' from header"),
        msg);

    // Test get logical table with database header mismatch
    msg = expectThrows(IOException.class,
        () -> getLogicalTable(logicalTableName, headers)).getMessage();
    assertTrue(msg.contains("Database name 'db2' from table prefix does not match database name 'db1' from header"),
        msg);

    // Test update logical table with database header mismatch
    msg = expectThrows(IOException.class,
        () -> updateLogicalTable(logicalTableName, logicalTableConfig, headers)).getMessage();
    assertTrue(msg.contains("Database name 'db2' from table prefix does not match database name 'db1' from header"),
        msg);

    // Test delete logical table with database header mismatch
    msg = expectThrows(IOException.class,
        () -> deleteLogicalTable(logicalTableName, headers)).getMessage();
    assertTrue(msg.contains("Database name 'db2' from table prefix does not match database name 'db1' from header"),
        msg);
  }

  @Test
  public void testLogicalTableUpdateBrokerTenantUpdate()
      throws Exception {
    PinotHelixResourceManager helixResourceManager = getHelixResourceManager();
    String logicalTableName = "test_logical_table";
    // Create a logical table
    addDummySchema(logicalTableName);
    List<String> physicalTables = createHybridTables(List.of("physical_table"));
    LogicalTableConfig logicalTableConfig =
        getDummyLogicalTableConfig(logicalTableName, physicalTables, BROKER_TENANT);
    String addLogicalTableResponse =
        createLogicalTable(logicalTableConfig, getHeaders());
    assertEquals(addLogicalTableResponse,
        "{\"unrecognizedProperties\":{},\"status\":\"" + logicalTableName + " logical table successfully added.\"}");
    verifyLogicalTableExists(logicalTableName, logicalTableConfig);

    // verify table broker node and broker tenant node is same
    IdealState brokerIdealStates = HelixHelper.getBrokerIdealStates(helixResourceManager.getHelixAdmin(),
        helixResourceManager.getHelixClusterName());
    Map<String, String> instanceStateMap = brokerIdealStates.getInstanceStateMap(logicalTableName);
    Set<String> brokerForTenant = helixResourceManager.getAllInstancesForBrokerTenant(BROKER_TENANT);
    assertEquals(brokerForTenant, instanceStateMap.keySet());

    //verify broker tenant node sets are different
    Set<String> allInstancesForBrokerTenant = helixResourceManager.getAllInstancesForBrokerTenant(NEW_BROKER_TENANT);
    assertNotEquals(brokerForTenant, allInstancesForBrokerTenant);

    // update logical table with new broker tenant
    logicalTableConfig.setBrokerTenant(NEW_BROKER_TENANT);
    updateLogicalTable(logicalTableName, logicalTableConfig, getHeaders());
    verifyLogicalTableExists(logicalTableName, logicalTableConfig);

    // verify the broker node set is updated in IS
    brokerIdealStates = HelixHelper.getBrokerIdealStates(helixResourceManager.getHelixAdmin(),
        helixResourceManager.getHelixClusterName());
    instanceStateMap = brokerIdealStates.getInstanceStateMap(logicalTableName);
    Set<String> brokerForNewTenant = helixResourceManager.getAllInstancesForBrokerTenant(NEW_BROKER_TENANT);
    assertEquals(brokerForNewTenant, instanceStateMap.keySet());
  }

  private String createLogicalTable(LogicalTableConfig logicalTableConfig, Map<String, String> headers)
      throws IOException {
    return executeControllerRest("POST", "/logicalTables", logicalTableConfig.toSingleLineJsonString(), null, headers);
  }

  private String updateLogicalTable(String logicalTableName, LogicalTableConfig logicalTableConfig,
      Map<String, String> headers)
      throws IOException {
    return executeControllerRest("PUT", "/logicalTables/" + logicalTableName,
        logicalTableConfig.toSingleLineJsonString(), null, headers);
  }

  private String deleteLogicalTable(String logicalTableName, Map<String, String> headers)
      throws IOException {
    return executeControllerRest("DELETE", "/logicalTables/" + logicalTableName, null, null, headers);
  }

  private String getLogicalTable(String logicalTableName, Map<String, String> headers)
      throws IOException {
    return executeControllerRest("GET", "/logicalTables/" + logicalTableName, null, null, headers);
  }

  private String listLogicalTableNames(Map<String, String> headers)
      throws IOException {
    return executeControllerRest("GET", "/logicalTables", null, null, headers);
  }

  private void verifyLogicalTableExists(String logicalTableName, LogicalTableConfig logicalTableConfig)
      throws IOException {
    verifyLogicalTableExists(logicalTableName, logicalTableConfig, getHeaders());
  }

  private void verifyLogicalTableExists(String logicalTableName, LogicalTableConfig logicalTableConfig,
      Map<String, String> headers)
      throws IOException {
    LogicalTableConfig remoteLogicalTableConfig =
        LogicalTableConfig.fromString(getLogicalTable(logicalTableName, headers));
    assertEquals(remoteLogicalTableConfig, logicalTableConfig);
  }

  private void verifyLogicalTableDoesNotExists(String logicalTableName) {
    verifyLogicalTableDoesNotExists(logicalTableName, getHeaders());
  }

  private void verifyLogicalTableDoesNotExists(String logicalTableName, Map<String, String> headers) {
    try {
      getLogicalTable(logicalTableName, headers);
      fail("Logical Table GET request should have failed");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Logical table not found"), e.getMessage());
    }
  }

  protected Map<String, String> getHeaders() {
    return Map.of();
  }
}
