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
package org.apache.pinot.spi.data;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.config.ConfigRecord;
import org.apache.pinot.spi.config.table.QueryConfig;
import org.apache.pinot.spi.config.table.QuotaConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.LogicalTableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class LogicalTableConfigSerDeTest {

  @Test
  public void testMinimalConfig()
      throws IOException {
    Map<String, PhysicalTableConfig> physicalTableConfigMap = new HashMap<>();
    physicalTableConfigMap.put("table_OFFLINE", new PhysicalTableConfig());

    LogicalTableConfig config = new LogicalTableConfigBuilder()
        .setTableName("logicalTable")
        .setBrokerTenant("defaultTenant")
        .setPhysicalTableConfigMap(physicalTableConfigMap)
        .build();

    checkMinimalConfig(config);

    // JSON round-trip
    LogicalTableConfig fromJson = JsonUtils.stringToObject(config.toSingleLineJsonString(), LogicalTableConfig.class);
    checkMinimalConfig(fromJson);

    // ZNRecord round-trip
    LogicalTableConfig fromZN = new LogicalTableConfig();
    fromZN.populateFromConfigRecord(config.toConfigRecord());
    checkMinimalConfig(fromZN);
  }

  private void checkMinimalConfig(LogicalTableConfig config) {
    assertEquals(config.getTableName(), "logicalTable");
    assertEquals(config.getBrokerTenant(), "defaultTenant");
    assertNotNull(config.getPhysicalTableConfigMap());
    assertEquals(config.getPhysicalTableConfigMap().size(), 1);
    assertTrue(config.getPhysicalTableConfigMap().containsKey("table_OFFLINE"));
    assertNull(config.getQueryConfig());
    assertNull(config.getQuotaConfig());
    assertNull(config.getRefOfflineTableName());
    assertNull(config.getRefRealtimeTableName());
    assertNull(config.getTimeBoundaryConfig());
    assertFalse(config.isHybridLogicalTable());
  }

  @Test
  public void testWithQuotaConfig()
      throws IOException {
    QuotaConfig quotaConfig = new QuotaConfig(null, "100.00");
    LogicalTableConfig config = new LogicalTableConfigBuilder()
        .setTableName("logicalTable")
        .setBrokerTenant("defaultTenant")
        .setPhysicalTableConfigMap(Map.of("table_OFFLINE", new PhysicalTableConfig()))
        .setQuotaConfig(quotaConfig)
        .build();

    checkQuotaConfig(config);

    LogicalTableConfig fromJson = JsonUtils.stringToObject(config.toSingleLineJsonString(), LogicalTableConfig.class);
    checkQuotaConfig(fromJson);

    LogicalTableConfig fromZN = new LogicalTableConfig();
    fromZN.populateFromConfigRecord(config.toConfigRecord());
    checkQuotaConfig(fromZN);
  }

  private void checkQuotaConfig(LogicalTableConfig config) {
    QuotaConfig quotaConfig = config.getQuotaConfig();
    assertNotNull(quotaConfig);
    assertEquals(quotaConfig.getMaxQueriesPerSecond(), "100.0");
    assertNull(quotaConfig.getStorage());
  }

  @Test
  public void testWithQueryConfig()
      throws IOException {
    QueryConfig queryConfig = new QueryConfig(5000L, true, false, null, null, null);
    LogicalTableConfig config = new LogicalTableConfigBuilder()
        .setTableName("logicalTable")
        .setBrokerTenant("defaultTenant")
        .setPhysicalTableConfigMap(Map.of("table_OFFLINE", new PhysicalTableConfig()))
        .setQueryConfig(queryConfig)
        .build();

    checkQueryConfig(config);

    LogicalTableConfig fromJson = JsonUtils.stringToObject(config.toSingleLineJsonString(), LogicalTableConfig.class);
    checkQueryConfig(fromJson);

    LogicalTableConfig fromZN = new LogicalTableConfig();
    fromZN.populateFromConfigRecord(config.toConfigRecord());
    checkQueryConfig(fromZN);
  }

  private void checkQueryConfig(LogicalTableConfig config) {
    QueryConfig queryConfig = config.getQueryConfig();
    assertNotNull(queryConfig);
    assertEquals(queryConfig.getTimeoutMs(), Long.valueOf(5000L));
    assertEquals(queryConfig.getDisableGroovy(), Boolean.TRUE);
  }

  @Test
  public void testWithRefTableNames()
      throws IOException {
    LogicalTableConfig config = new LogicalTableConfigBuilder()
        .setTableName("logicalTable")
        .setBrokerTenant("defaultTenant")
        .setPhysicalTableConfigMap(Map.of(
            "table_OFFLINE", new PhysicalTableConfig(),
            "table_REALTIME", new PhysicalTableConfig()))
        .setRefOfflineTableName("table_OFFLINE")
        .setRefRealtimeTableName("table_REALTIME")
        .build();

    checkRefTableNames(config);

    LogicalTableConfig fromJson = JsonUtils.stringToObject(config.toSingleLineJsonString(), LogicalTableConfig.class);
    checkRefTableNames(fromJson);

    LogicalTableConfig fromZN = new LogicalTableConfig();
    fromZN.populateFromConfigRecord(config.toConfigRecord());
    checkRefTableNames(fromZN);
  }

  private void checkRefTableNames(LogicalTableConfig config) {
    assertEquals(config.getRefOfflineTableName(), "table_OFFLINE");
    assertEquals(config.getRefRealtimeTableName(), "table_REALTIME");
    assertTrue(config.isHybridLogicalTable());
  }

  @Test
  public void testWithTimeBoundaryConfig()
      throws IOException {
    Map<String, Object> params = new HashMap<>();
    params.put("includedTables", "table_OFFLINE");
    TimeBoundaryConfig timeBoundaryConfig = new TimeBoundaryConfig("MAX", params);

    LogicalTableConfig config = new LogicalTableConfigBuilder()
        .setTableName("logicalTable")
        .setBrokerTenant("defaultTenant")
        .setPhysicalTableConfigMap(Map.of(
            "table_OFFLINE", new PhysicalTableConfig(),
            "table_REALTIME", new PhysicalTableConfig()))
        .setRefOfflineTableName("table_OFFLINE")
        .setRefRealtimeTableName("table_REALTIME")
        .setTimeBoundaryConfig(timeBoundaryConfig)
        .build();

    checkTimeBoundaryConfig(config);

    LogicalTableConfig fromJson = JsonUtils.stringToObject(config.toSingleLineJsonString(), LogicalTableConfig.class);
    checkTimeBoundaryConfig(fromJson);

    LogicalTableConfig fromZN = new LogicalTableConfig();
    fromZN.populateFromConfigRecord(config.toConfigRecord());
    checkTimeBoundaryConfig(fromZN);
  }

  private void checkTimeBoundaryConfig(LogicalTableConfig config) {
    TimeBoundaryConfig tbc = config.getTimeBoundaryConfig();
    assertNotNull(tbc);
    assertEquals(tbc.getBoundaryStrategy(), "MAX");
    assertNotNull(tbc.getParameters());
    assertEquals(tbc.getParameters().get("includedTables"), "table_OFFLINE");
  }

  @Test
  public void testWithMultiClusterPhysicalTable()
      throws IOException {
    Map<String, PhysicalTableConfig> physicalTableConfigMap = new HashMap<>();
    physicalTableConfigMap.put("table_OFFLINE", new PhysicalTableConfig());
    physicalTableConfigMap.put("remote_table_OFFLINE", new PhysicalTableConfig(true));

    LogicalTableConfig config = new LogicalTableConfigBuilder()
        .setTableName("logicalTable")
        .setBrokerTenant("defaultTenant")
        .setPhysicalTableConfigMap(physicalTableConfigMap)
        .setRefOfflineTableName("table_OFFLINE")
        .build();

    checkMultiClusterConfig(config);

    LogicalTableConfig fromJson = JsonUtils.stringToObject(config.toSingleLineJsonString(), LogicalTableConfig.class);
    checkMultiClusterConfig(fromJson);

    LogicalTableConfig fromZN = new LogicalTableConfig();
    fromZN.populateFromConfigRecord(config.toConfigRecord());
    checkMultiClusterConfig(fromZN);
  }

  private void checkMultiClusterConfig(LogicalTableConfig config) {
    assertEquals(config.getPhysicalTableConfigMap().size(), 2);
    assertFalse(config.getPhysicalTableConfigMap().get("table_OFFLINE").isMultiCluster());
    assertTrue(config.getPhysicalTableConfigMap().get("remote_table_OFFLINE").isMultiCluster());
  }

  @Test
  public void testFullConfig()
      throws IOException {
    QuotaConfig quotaConfig = new QuotaConfig(null, "200.00");
    QueryConfig queryConfig = new QueryConfig(3000L, false, true, Collections.singletonMap("func(a)", "b"), null, null);
    Map<String, Object> params = new HashMap<>();
    params.put("key", "value");
    TimeBoundaryConfig timeBoundaryConfig = new TimeBoundaryConfig("MIN", params);

    Map<String, PhysicalTableConfig> physicalTableConfigMap = new HashMap<>();
    physicalTableConfigMap.put("table_OFFLINE", new PhysicalTableConfig());
    physicalTableConfigMap.put("table_REALTIME", new PhysicalTableConfig());
    physicalTableConfigMap.put("remote_table_OFFLINE", new PhysicalTableConfig(true));

    LogicalTableConfig config = new LogicalTableConfigBuilder()
        .setTableName("myLogicalTable")
        .setBrokerTenant("myTenant")
        .setPhysicalTableConfigMap(physicalTableConfigMap)
        .setQuotaConfig(quotaConfig)
        .setQueryConfig(queryConfig)
        .setRefOfflineTableName("table_OFFLINE")
        .setRefRealtimeTableName("table_REALTIME")
        .setTimeBoundaryConfig(timeBoundaryConfig)
        .build();

    checkFullConfig(config);

    // JSON round-trip
    LogicalTableConfig fromJson = JsonUtils.stringToObject(config.toSingleLineJsonString(), LogicalTableConfig.class);
    checkFullConfig(fromJson);

    // ZNRecord round-trip
    LogicalTableConfig fromZN = new LogicalTableConfig();
    fromZN.populateFromConfigRecord(config.toConfigRecord());
    checkFullConfig(fromZN);
  }

  private void checkFullConfig(LogicalTableConfig config) {
    assertEquals(config.getTableName(), "myLogicalTable");
    assertEquals(config.getBrokerTenant(), "myTenant");
    assertTrue(config.isHybridLogicalTable());

    assertEquals(config.getPhysicalTableConfigMap().size(), 3);
    assertFalse(config.getPhysicalTableConfigMap().get("table_OFFLINE").isMultiCluster());
    assertFalse(config.getPhysicalTableConfigMap().get("table_REALTIME").isMultiCluster());
    assertTrue(config.getPhysicalTableConfigMap().get("remote_table_OFFLINE").isMultiCluster());

    assertNotNull(config.getQuotaConfig());
    assertEquals(config.getQuotaConfig().getMaxQueriesPerSecond(), "200.0");

    QueryConfig queryConfig = config.getQueryConfig();
    assertNotNull(queryConfig);
    assertEquals(queryConfig.getTimeoutMs(), Long.valueOf(3000L));
    assertEquals(queryConfig.getDisableGroovy(), Boolean.FALSE);
    assertEquals(queryConfig.getExpressionOverrideMap(), Collections.singletonMap("func(a)", "b"));

    assertEquals(config.getRefOfflineTableName(), "table_OFFLINE");
    assertEquals(config.getRefRealtimeTableName(), "table_REALTIME");

    TimeBoundaryConfig tbc = config.getTimeBoundaryConfig();
    assertNotNull(tbc);
    assertEquals(tbc.getBoundaryStrategy(), "MIN");
    assertEquals(tbc.getParameters().get("key"), "value");
  }

  @Test
  public void testConfigRecordFieldMapping()
      throws IOException {
    Map<String, PhysicalTableConfig> physicalTableConfigMap = new HashMap<>();
    physicalTableConfigMap.put("table_OFFLINE", new PhysicalTableConfig());

    LogicalTableConfig config = new LogicalTableConfigBuilder()
        .setTableName("logicalTable")
        .setBrokerTenant("testTenant")
        .setPhysicalTableConfigMap(physicalTableConfigMap)
        .setRefOfflineTableName("table_OFFLINE")
        .build();

    ConfigRecord configRecord = config.toConfigRecord();

    // Verify ConfigRecord structure
    assertEquals(configRecord.getId(), "logicalTable");
    assertEquals(configRecord.getSimpleField(LogicalTableConfig.LOGICAL_TABLE_NAME_KEY), "logicalTable");
    assertEquals(configRecord.getSimpleField(LogicalTableConfig.BROKER_TENANT_KEY), "testTenant");
    assertEquals(configRecord.getSimpleField(LogicalTableConfig.REF_OFFLINE_TABLE_NAME_KEY), "table_OFFLINE");
    assertNull(configRecord.getSimpleField(LogicalTableConfig.REF_REALTIME_TABLE_NAME_KEY));
    assertNull(configRecord.getSimpleField(LogicalTableConfig.QUERY_CONFIG_KEY));
    assertNull(configRecord.getSimpleField(LogicalTableConfig.QUOTA_CONFIG_KEY));
    assertNull(configRecord.getSimpleField(LogicalTableConfig.TIME_BOUNDARY_CONFIG_KEY));

    Map<String, String> mapField = configRecord.getMapField(LogicalTableConfig.PHYSICAL_TABLE_CONFIG_KEY);
    assertNotNull(mapField);
    assertEquals(mapField.size(), 1);
    assertTrue(mapField.containsKey("table_OFFLINE"));
  }
}
