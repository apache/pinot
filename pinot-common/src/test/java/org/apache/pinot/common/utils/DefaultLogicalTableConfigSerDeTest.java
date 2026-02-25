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
package org.apache.pinot.common.utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.spi.config.table.QueryConfig;
import org.apache.pinot.spi.config.table.QuotaConfig;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.PhysicalTableConfig;
import org.apache.pinot.spi.data.TimeBoundaryConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.LogicalTableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Tests for {@link LogicalTableConfig} JSON serialization and
 * {@link DefaultLogicalTableConfigSerDe} ZNRecord round-trip serialization.
 */
public class DefaultLogicalTableConfigSerDeTest {

  private final DefaultLogicalTableConfigSerDe _serDe = new DefaultLogicalTableConfigSerDe();

  @Test
  public void testMinimalConfig()
      throws Exception {
    Map<String, PhysicalTableConfig> physicalTableConfigMap = new HashMap<>();
    physicalTableConfigMap.put("table_OFFLINE", new PhysicalTableConfig());

    LogicalTableConfig config = new LogicalTableConfigBuilder()
        .setTableName("logicalTable")
        .setBrokerTenant("defaultTenant")
        .setPhysicalTableConfigMap(physicalTableConfigMap)
        .build();

    checkMinimalConfig(config);
    checkMinimalConfig(jsonRoundTrip(config));
    checkMinimalConfig(znRecordRoundTrip(config));
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
  public void testFullConfig()
      throws Exception {
    QuotaConfig quotaConfig = new QuotaConfig(null, "200.00");
    QueryConfig queryConfig =
        new QueryConfig(3000L, false, true, Collections.singletonMap("func(a)", "b"), null, null);
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
    checkFullConfig(jsonRoundTrip(config));
    checkFullConfig(znRecordRoundTrip(config));
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
  public void testZNRecordFieldMapping()
      throws Exception {
    Map<String, PhysicalTableConfig> physicalTableConfigMap = new HashMap<>();
    physicalTableConfigMap.put("table_OFFLINE", new PhysicalTableConfig());

    LogicalTableConfig config = new LogicalTableConfigBuilder()
        .setTableName("logicalTable")
        .setBrokerTenant("testTenant")
        .setPhysicalTableConfigMap(physicalTableConfigMap)
        .setRefOfflineTableName("table_OFFLINE")
        .build();

    ZNRecord znRecord = _serDe.toZNRecord(config);

    assertEquals(znRecord.getId(), "logicalTable");
    assertEquals(znRecord.getSimpleField(LogicalTableConfig.LOGICAL_TABLE_NAME_KEY), "logicalTable");
    assertEquals(znRecord.getSimpleField(LogicalTableConfig.BROKER_TENANT_KEY), "testTenant");
    assertEquals(znRecord.getSimpleField(LogicalTableConfig.REF_OFFLINE_TABLE_NAME_KEY), "table_OFFLINE");
    assertNull(znRecord.getSimpleField(LogicalTableConfig.REF_REALTIME_TABLE_NAME_KEY));
    assertNull(znRecord.getSimpleField(LogicalTableConfig.QUERY_CONFIG_KEY));
    assertNull(znRecord.getSimpleField(LogicalTableConfig.QUOTA_CONFIG_KEY));
    assertNull(znRecord.getSimpleField(LogicalTableConfig.TIME_BOUNDARY_CONFIG_KEY));

    Map<String, String> mapField = znRecord.getMapField(LogicalTableConfig.PHYSICAL_TABLE_CONFIG_KEY);
    assertNotNull(mapField);
    assertEquals(mapField.size(), 1);
    assertTrue(mapField.containsKey("table_OFFLINE"));
  }

  private LogicalTableConfig jsonRoundTrip(LogicalTableConfig config)
      throws Exception {
    return JsonUtils.stringToObject(config.toSingleLineJsonString(), LogicalTableConfig.class);
  }

  private LogicalTableConfig znRecordRoundTrip(LogicalTableConfig config)
      throws Exception {
    ZNRecord znRecord = _serDe.toZNRecord(config);
    return _serDe.fromZNRecord(znRecord);
  }
}
