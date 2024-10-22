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
package org.apache.pinot.broker.queryquota;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.pinot.spi.config.DatabaseConfig;
import org.apache.pinot.spi.config.table.QuotaConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.StringUtil;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class HelixExternalViewBasedQueryQuotaManagerTest {
  private ZkHelixPropertyStore<ZNRecord> _testPropertyStore;
  private HelixManager _helixManager;
  private HelixExternalViewBasedQueryQuotaManager _queryQuotaManager;
  private ZkStarter.ZookeeperInstance _zookeeperInstance;
  private static final Map<String, String> CLUSTER_CONFIG_MAP = new HashMap<>();
  private static final String APP_NAME = "app";
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String OFFLINE_TABLE_NAME = RAW_TABLE_NAME + "_OFFLINE";
  private static final String REALTIME_TABLE_NAME = RAW_TABLE_NAME + "_REALTIME";
  private static final String BROKER_INSTANCE_ID = "broker_instance_1";
  private static final long TABLE_MAX_QPS = 25;
  private static final String TABLE_MAX_QPS_STR = String.valueOf(TABLE_MAX_QPS);
  private static final long DATABASE_HIGH_QPS = 40;
  private static final String DATABASE_HIGH_QPS_STR = String.valueOf(DATABASE_HIGH_QPS);
  private static final long DATABASE_LOW_QPS = 10;
  private static final String DATABASE_LOW_QPS_STR = String.valueOf(DATABASE_LOW_QPS);

  @BeforeTest
  public void beforeTest() {
    _zookeeperInstance = ZkStarter.startLocalZkServer();
    String helixClusterName = "TestTableQueryQuotaManagerService";

    _helixManager = initHelixManager(helixClusterName);
    _testPropertyStore = _helixManager.getHelixPropertyStore();

    _queryQuotaManager =
        new HelixExternalViewBasedQueryQuotaManager(Mockito.mock(BrokerMetrics.class), BROKER_INSTANCE_ID);
    _queryQuotaManager.init(_helixManager);
  }

  private HelixManager initHelixManager(String helixClusterName) {
    return new FakeHelixManager(helixClusterName, BROKER_INSTANCE_ID, InstanceType.PARTICIPANT,
        _zookeeperInstance.getZkUrl());
  }

  public class FakeHelixManager extends ZKHelixManager {

    FakeHelixManager(String clusterName, String instanceName, InstanceType instanceType, String zkAddress) {
      super(clusterName, instanceName, instanceType, zkAddress);
      super._zkclient = new ZkClient(StringUtil.join("/", StringUtils.chomp(_zookeeperInstance.getZkUrl(), "/")),
          ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());
      _zkclient.deleteRecursively("/" + clusterName + "/PROPERTYSTORE");
      _zkclient.createPersistent("/" + clusterName + "/PROPERTYSTORE", true);
    }

    void closeZkClient() {
      _zkclient.close();
    }

    @Override
    public HelixAdmin getClusterManagmentTool() {
      return new FakeZKHelixAdmin(_zkclient);
    }
  }

  public static class FakeZKHelixAdmin extends ZKHelixAdmin {
    private final Map<String, String> _instanceConfigMap;

    public FakeZKHelixAdmin(RealmAwareZkClient zkClient) {
      super(zkClient);
      _instanceConfigMap = new HashMap<>();
    }

    @Override
    public Map<String, String> getConfig(HelixConfigScope scope, List<String> keys) {
      if (scope.getType().equals(HelixConfigScope.ConfigScopeProperty.CLUSTER)) {
        return CLUSTER_CONFIG_MAP;
      }
      return _instanceConfigMap;
    }

    @Override
    public ExternalView getResourceExternalView(String clusterName, String resourceName) {
      return generateBrokerResource(OFFLINE_TABLE_NAME);
    }
  }

  @AfterMethod
  public void afterMethod() {
    if (_helixManager instanceof FakeHelixManager) {
      _testPropertyStore.reset();
      ZKMetadataProvider.removeResourceConfigFromPropertyStore(_testPropertyStore, OFFLINE_TABLE_NAME);
      ZKMetadataProvider.removeResourceConfigFromPropertyStore(_testPropertyStore, REALTIME_TABLE_NAME);
      ZKMetadataProvider.removeDatabaseConfig(_testPropertyStore, CommonConstants.DEFAULT_DATABASE);
      ZKMetadataProvider.removeApplicationQuotas(_testPropertyStore);
      CLUSTER_CONFIG_MAP.clear();
    }
    _queryQuotaManager.cleanUpRateLimiterMap();
    _queryQuotaManager.getDatabaseRateLimiterMap().clear();
    _queryQuotaManager.getApplicationRateLimiterMap().clear();
  }

  @AfterTest
  public void afterTest() {
    if (_helixManager instanceof FakeHelixManager) {
      ((FakeHelixManager) _helixManager).closeZkClient();
    }
    ZkStarter.stopLocalZkServer(_zookeeperInstance);
  }

  @Test
  public void testOfflineTableNotnullQuota()
      throws Exception {
    ExternalView brokerResource = generateBrokerResource(OFFLINE_TABLE_NAME);
    TableConfig tableConfig = generateDefaultTableConfig(OFFLINE_TABLE_NAME);
    ZKMetadataProvider.setTableConfig(_testPropertyStore, tableConfig);
    setQps(tableConfig);
    _queryQuotaManager.initOrUpdateTableQueryQuota(tableConfig, brokerResource);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 1);

    // All the request should be passed.
    runQueries();

    _queryQuotaManager.dropTableQueryQuota(OFFLINE_TABLE_NAME);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 0);
  }

  @Test
  public void testOfflineTableNotnullQuotaWithHigherDefaultDatabaseQuota()
      throws Exception {
    ExternalView brokerResource = generateBrokerResource(OFFLINE_TABLE_NAME);
    TableConfig tableConfig = generateDefaultTableConfig(OFFLINE_TABLE_NAME);
    ZKMetadataProvider.setTableConfig(_testPropertyStore, tableConfig);
    setQps(tableConfig);
    _queryQuotaManager.initOrUpdateTableQueryQuota(tableConfig, brokerResource);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 1);
    _queryQuotaManager.createDatabaseRateLimiter(CommonConstants.DEFAULT_DATABASE);
    Assert.assertEquals(_queryQuotaManager.getDatabaseRateLimiterMap().size(), 1);

    setDefaultDatabaseQps("40");
    // qps withing table and default database qps quota
    runQueries(25, false);
    // qps exceeding table qps quota but withing default database quota
    runQueries(40, true);

    _queryQuotaManager.dropTableQueryQuota(OFFLINE_TABLE_NAME);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 0);
  }

  @Test
  public void testOfflineTableNotnullQuotaWithLowerDefaultDatabaseQuota()
      throws Exception {
    ExternalView brokerResource = generateBrokerResource(OFFLINE_TABLE_NAME);
    TableConfig tableConfig = generateDefaultTableConfig(OFFLINE_TABLE_NAME);
    ZKMetadataProvider.setTableConfig(_testPropertyStore, tableConfig);
    setQps(tableConfig);
    _queryQuotaManager.initOrUpdateTableQueryQuota(tableConfig, brokerResource);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 1);
    _queryQuotaManager.createDatabaseRateLimiter(CommonConstants.DEFAULT_DATABASE);
    Assert.assertEquals(_queryQuotaManager.getDatabaseRateLimiterMap().size(), 1);

    setDefaultDatabaseQps(DATABASE_LOW_QPS_STR);
    // qps withing table and default database qps quota
    runQueries(DATABASE_LOW_QPS, false);
    // qps withing table qps quota but exceeding default database quota
    runQueries(TABLE_MAX_QPS, true);

    _queryQuotaManager.dropTableQueryQuota(OFFLINE_TABLE_NAME);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 0);
  }

  @Test
  public void testOfflineTableNotnullQuotaWithHigherDatabaseQuota()
      throws Exception {
    ExternalView brokerResource = generateBrokerResource(OFFLINE_TABLE_NAME);
    TableConfig tableConfig = generateDefaultTableConfig(OFFLINE_TABLE_NAME);
    ZKMetadataProvider.setTableConfig(_testPropertyStore, tableConfig);
    setQps(tableConfig);
    _queryQuotaManager.initOrUpdateTableQueryQuota(tableConfig, brokerResource);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 1);

    DatabaseConfig databaseConfig = generateDefaultDatabaseConfig();
    setHigherDatabaseQps(databaseConfig);
    // qps withing table and database qps quota
    runQueries(TABLE_MAX_QPS, false);
    // qps exceeding table qps quota but within database quota
    runQueries(DATABASE_HIGH_QPS, true);

    _queryQuotaManager.dropTableQueryQuota(OFFLINE_TABLE_NAME);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 0);
  }

  @Test
  public void testOfflineTableNotnullQuotaWithLowerDatabaseQuota()
      throws Exception {
    ExternalView brokerResource = generateBrokerResource(OFFLINE_TABLE_NAME);
    TableConfig tableConfig = generateDefaultTableConfig(OFFLINE_TABLE_NAME);
    ZKMetadataProvider.setTableConfig(_testPropertyStore, tableConfig);
    setQps(tableConfig);
    _queryQuotaManager.initOrUpdateTableQueryQuota(tableConfig, brokerResource);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 1);

    DatabaseConfig databaseConfig = generateDefaultDatabaseConfig();
    setLowerDatabaseQps(databaseConfig);
    // qps withing table and database qps quota
    runQueries(DATABASE_LOW_QPS, false);
    // qps within table qps quota but exceeding database quota
    runQueries(TABLE_MAX_QPS, true);

    _queryQuotaManager.dropTableQueryQuota(OFFLINE_TABLE_NAME);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 0);
  }

  @Test
  public void testWhenNoTableOrDatabaseOrApplicationQuotasSetQueriesRunWild()
      throws InterruptedException {
    ExternalView brokerResource = generateBrokerResource(OFFLINE_TABLE_NAME);
    TableConfig tableConfig = generateDefaultTableConfig(OFFLINE_TABLE_NAME);
    ZKMetadataProvider.setTableConfig(_testPropertyStore, tableConfig);
    _queryQuotaManager.initOrUpdateTableQueryQuota(tableConfig, brokerResource);
    _queryQuotaManager.createDatabaseRateLimiter(CommonConstants.DEFAULT_DATABASE);
    _queryQuotaManager.createApplicationRateLimiter(APP_NAME);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 1);
    Assert.assertEquals(_queryQuotaManager.getDatabaseRateLimiterMap().size(), 1);
    Assert.assertEquals(_queryQuotaManager.getApplicationRateLimiterMap().size(), 1);

    setDefaultDatabaseQps("-1");
    setDefaultApplicationQps("-1");

    runQueries(25, false);
    runQueries(40, false);
    runQueries(100, false);

    _queryQuotaManager.dropTableQueryQuota(OFFLINE_TABLE_NAME);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 0);
  }

  @Test
  public void testWhenOnlySpecificAppQuotaIsSetItAffectsQueriesWithAppOption()
      throws InterruptedException {
    ExternalView brokerResource = generateBrokerResource(OFFLINE_TABLE_NAME);
    TableConfig tableConfig = generateDefaultTableConfig(OFFLINE_TABLE_NAME);
    ZKMetadataProvider.setTableConfig(_testPropertyStore, tableConfig);
    _queryQuotaManager.initOrUpdateTableQueryQuota(tableConfig, brokerResource);
    _queryQuotaManager.createDatabaseRateLimiter(CommonConstants.DEFAULT_DATABASE);

    ZKMetadataProvider.setApplicationQpsQuota(_testPropertyStore, APP_NAME, 50d);
    _queryQuotaManager.createApplicationRateLimiter(APP_NAME);

    setDefaultDatabaseQps("-1");
    setDefaultApplicationQps("-1");

    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 1);
    Assert.assertEquals(_queryQuotaManager.getDatabaseRateLimiterMap().size(), 1);
    Assert.assertEquals(_queryQuotaManager.getApplicationRateLimiterMap().size(), 1);

    runQueries(50, false);
    runQueries(100, true);

    _queryQuotaManager.dropTableQueryQuota(OFFLINE_TABLE_NAME);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 0);
  }

  @Test
  public void testWhenOnlyDefaultAppQuotaIsSetItAffectsAllApplications()
      throws InterruptedException {
    ExternalView brokerResource = generateBrokerResource(OFFLINE_TABLE_NAME);
    TableConfig tableConfig = generateDefaultTableConfig(OFFLINE_TABLE_NAME);
    ZKMetadataProvider.setTableConfig(_testPropertyStore, tableConfig);
    _queryQuotaManager.initOrUpdateTableQueryQuota(tableConfig, brokerResource);
    _queryQuotaManager.createDatabaseRateLimiter(CommonConstants.DEFAULT_DATABASE);

    setDefaultDatabaseQps("-1");
    setDefaultApplicationQps("50");

    ZKMetadataProvider.setApplicationQpsQuota(_testPropertyStore, "someApp", 100d);
    _queryQuotaManager.createApplicationRateLimiter("someApp");

    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 1);
    Assert.assertEquals(_queryQuotaManager.getDatabaseRateLimiterMap().size(), 1);
    Assert.assertEquals(_queryQuotaManager.getApplicationRateLimiterMap().size(), 1);

    runQueries(100, true, APP_NAME);
    runQueries(100, true, "otherApp");
    runQueries(100, false, "someApp");
    runQueries(201, true, "someApp");

    Assert.assertEquals(_queryQuotaManager.getApplicationRateLimiterMap().size(), 3);
    _queryQuotaManager.dropTableQueryQuota(OFFLINE_TABLE_NAME);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 0);
  }

  @Test
  public void tesCreateAndUpdateAppRateLimiterChangesRateLimiterMap() {
    Map<String, Double> apps = new HashMap<>();
    apps.put("app1", null);
    apps.put("app2", 1d);
    apps.put("app3", 2d);

    apps.entrySet().stream().forEach(e -> {
      ZKMetadataProvider.setApplicationQpsQuota(_testPropertyStore, e.getKey(), e.getValue());
    });
    apps.entrySet().forEach(app -> _queryQuotaManager.createApplicationRateLimiter(app.getKey()));
    Map<String, QueryQuotaEntity> appQuotaMap = _queryQuotaManager.getApplicationRateLimiterMap();

    Assert.assertNull(appQuotaMap.get("app1").getRateLimiter());
    Assert.assertEquals(appQuotaMap.get("app2").getRateLimiter().getRate(), 1);
    Assert.assertEquals(appQuotaMap.get("app3").getRateLimiter().getRate(), 2);

    ZKMetadataProvider.setApplicationQpsQuota(_testPropertyStore, "app1", 1d);
    ZKMetadataProvider.setApplicationQpsQuota(_testPropertyStore, "app2", 2d);

    apps.entrySet().forEach(e -> _queryQuotaManager.updateApplicationRateLimiter(e.getKey()));

    Assert.assertEquals(appQuotaMap.get("app1").getRateLimiter().getRate(), 1);
    Assert.assertEquals(appQuotaMap.get("app2").getRateLimiter().getRate(), 2);
    Assert.assertEquals(appQuotaMap.get("app3").getRateLimiter().getRate(), 2);
  }

  @Test
  public void testCreateOrUpdateDatabaseRateLimiter() {
    List<String> dbList = new ArrayList<>(2);
    dbList.add("db1");
    dbList.add("db2");
    dbList.add("db3");
    DatabaseConfig db1 = new DatabaseConfig(dbList.get(0), new QuotaConfig(null, null));
    DatabaseConfig db2 = new DatabaseConfig(dbList.get(1), new QuotaConfig(null, "1"));
    DatabaseConfig db3 = new DatabaseConfig(dbList.get(2), new QuotaConfig(null, "2"));

    ZKMetadataProvider.setDatabaseConfig(_testPropertyStore, db1);
    ZKMetadataProvider.setDatabaseConfig(_testPropertyStore, db2);
    ZKMetadataProvider.setDatabaseConfig(_testPropertyStore, db3);

    dbList.forEach(db -> _queryQuotaManager.createDatabaseRateLimiter(db));
    Map<String, QueryQuotaEntity> dbQuotaMap = _queryQuotaManager.getDatabaseRateLimiterMap();
    Assert.assertNull(dbQuotaMap.get(dbList.get(0)).getRateLimiter());
    Assert.assertEquals(dbQuotaMap.get(dbList.get(1)).getRateLimiter().getRate(), 1);
    Assert.assertEquals(dbQuotaMap.get(dbList.get(2)).getRateLimiter().getRate(), 2);

    db1.setQuotaConfig(new QuotaConfig(null, "1"));
    db2.setQuotaConfig(new QuotaConfig(null, "2"));
    ZKMetadataProvider.setDatabaseConfig(_testPropertyStore, db1);
    ZKMetadataProvider.setDatabaseConfig(_testPropertyStore, db2);
    dbList.forEach(db -> _queryQuotaManager.updateDatabaseRateLimiter(db));

    Assert.assertEquals(dbQuotaMap.get(dbList.get(0)).getRateLimiter().getRate(), 1);
    Assert.assertEquals(dbQuotaMap.get(dbList.get(1)).getRateLimiter().getRate(), 2);
    Assert.assertEquals(dbQuotaMap.get(dbList.get(2)).getRateLimiter().getRate(), 2);
  }

  @Test
  public void testOfflineTableWithNullQuotaAndNoRealtimeTableConfig() {
    ExternalView brokerResource = generateBrokerResource(OFFLINE_TABLE_NAME);
    TableConfig tableConfig = generateDefaultTableConfig(OFFLINE_TABLE_NAME);
    _queryQuotaManager.initOrUpdateTableQueryQuota(tableConfig, brokerResource);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 1);
    Assert.assertEquals(_queryQuotaManager.getTableQueryQuota(OFFLINE_TABLE_NAME), 0);
  }

  @Test
  public void testOfflineTableWithNullQuotaButWithRealtimeTableConfigNullQpsConfig()
      throws Exception {
    QuotaConfig quotaConfig = new QuotaConfig("6G", null);
    TableConfig realtimeTableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setQuotaConfig(quotaConfig)
            .setRetentionTimeUnit("DAYS").setRetentionTimeValue("1").setSegmentPushType("APPEND")
            .setBrokerTenant("testBroker").setServerTenant("testServer").build();
    ZKMetadataProvider.setTableConfig(_testPropertyStore, realtimeTableConfig);

    ExternalView brokerResource = generateBrokerResource(OFFLINE_TABLE_NAME);
    TableConfig tableConfig = generateDefaultTableConfig(OFFLINE_TABLE_NAME);
    _queryQuotaManager.initOrUpdateTableQueryQuota(tableConfig, brokerResource);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 1);
    Assert.assertEquals(_queryQuotaManager.getTableQueryQuota(OFFLINE_TABLE_NAME), 0);

    // Nothing happened since it doesn't have qps quota.
    _queryQuotaManager.dropTableQueryQuota(OFFLINE_TABLE_NAME);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 0);
  }

  @Test
  public void testOfflineTableWithNullQuotaButWithRealtimeTableConfigNotNullQpsConfig()
      throws Exception {
    QuotaConfig quotaConfig = new QuotaConfig("6G", TABLE_MAX_QPS_STR);
    TableConfig realtimeTableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setQuotaConfig(quotaConfig)
            .setRetentionTimeUnit("DAYS").setRetentionTimeValue("1").setSegmentPushType("APPEND")
            .setBrokerTenant("testBroker").setServerTenant("testServer").build();
    ZKMetadataProvider.setTableConfig(_testPropertyStore, realtimeTableConfig);

    ExternalView brokerResource = generateBrokerResource(REALTIME_TABLE_NAME);
    TableConfig tableConfig = generateDefaultTableConfig(OFFLINE_TABLE_NAME);
    _queryQuotaManager.initOrUpdateTableQueryQuota(tableConfig, brokerResource);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 1);
    Assert.assertEquals(_queryQuotaManager.getTableQueryQuota(OFFLINE_TABLE_NAME), 0);

    // Drop the offline table won't have any affect since it is table type specific.
    _queryQuotaManager.dropTableQueryQuota(OFFLINE_TABLE_NAME);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 0);
  }

  @Test
  public void testBothTableHaveQpsQuotaConfig()
      throws Exception {
    ExternalView brokerResource = generateBrokerResource(OFFLINE_TABLE_NAME);
    brokerResource.setState(REALTIME_TABLE_NAME, BROKER_INSTANCE_ID, "ONLINE");
    brokerResource.setState(REALTIME_TABLE_NAME, "broker_instance_2", "OFFLINE");

    QuotaConfig quotaConfig = new QuotaConfig("6G", TABLE_MAX_QPS_STR);
    TableConfig realtimeTableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setQuotaConfig(quotaConfig)
            .setRetentionTimeUnit("DAYS").setRetentionTimeValue("1").setSegmentPushType("APPEND")
            .setBrokerTenant("testBroker").setServerTenant("testServer").build();
    TableConfig offlineTableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setQuotaConfig(quotaConfig)
            .setRetentionTimeUnit("DAYS").setRetentionTimeValue("1").setSegmentPushType("APPEND")
            .setBrokerTenant("testBroker").setServerTenant("testServer").build();

    ZKMetadataProvider.setTableConfig(_testPropertyStore, realtimeTableConfig);
    ZKMetadataProvider.setTableConfig(_testPropertyStore, offlineTableConfig);

    // Since each table has 2 online brokers, per broker rate becomes 100.0 / 2 = 50.0
    _queryQuotaManager.initOrUpdateTableQueryQuota(offlineTableConfig, brokerResource);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 1);
    _queryQuotaManager.initOrUpdateTableQueryQuota(realtimeTableConfig, brokerResource);
    // The hash map now contains 2 entries for both of the tables.
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 2);

    // Rate limiter generates 1 token every 10 milliseconds, have to make it sleep for a while.
    runQueries();

    _queryQuotaManager.dropTableQueryQuota(OFFLINE_TABLE_NAME);
    // Since real-time table still has the qps quota, the size of the hash map becomes 1.
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 1);

    _queryQuotaManager.dropTableQueryQuota(REALTIME_TABLE_NAME);
    // Since the only 1 table which has qps quota has been dropped, the size of the hash map becomes 0.
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 0);
  }

  @Test
  public void testRealtimeTableNotnullQuota()
      throws Exception {
    ExternalView brokerResource = generateBrokerResource(REALTIME_TABLE_NAME);
    TableConfig tableConfig = generateDefaultTableConfig(REALTIME_TABLE_NAME);
    ZKMetadataProvider.setTableConfig(_testPropertyStore, tableConfig);
    setQps(tableConfig);
    _queryQuotaManager.initOrUpdateTableQueryQuota(tableConfig, brokerResource);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 1);

    runQueries();

    _queryQuotaManager.dropTableQueryQuota(REALTIME_TABLE_NAME);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 0);
  }

  @Test
  public void testRealtimeTableNotnullQuotaWhileTableConfigGetsDeleted()
      throws Exception {
    ExternalView brokerResource = generateBrokerResource(REALTIME_TABLE_NAME);
    TableConfig tableConfig = generateDefaultTableConfig(REALTIME_TABLE_NAME);
    ZKMetadataProvider.setTableConfig(_testPropertyStore, tableConfig);
    setQps(tableConfig);
    _queryQuotaManager.initOrUpdateTableQueryQuota(tableConfig, brokerResource);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 1);

    runQueries();

    ZKMetadataProvider.removeResourceConfigFromPropertyStore(_testPropertyStore, REALTIME_TABLE_NAME);
    _queryQuotaManager.processQueryRateLimitingExternalViewChange(brokerResource);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 0);
  }

  @Test
  public void testRealtimeTableWithNullQuotaAndNoOfflineTableConfig()
      throws Exception {
    ExternalView brokerResource = generateBrokerResource(REALTIME_TABLE_NAME);
    TableConfig tableConfig = generateDefaultTableConfig(REALTIME_TABLE_NAME);
    _queryQuotaManager.initOrUpdateTableQueryQuota(tableConfig, brokerResource);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 1);
    Assert.assertEquals(_queryQuotaManager.getTableQueryQuota(REALTIME_TABLE_NAME), 0);
  }

  @Test
  public void testRealtimeTableWithNullQuotaButWithOfflineTableConfigNullQpsConfig()
      throws Exception {
    QuotaConfig quotaConfig = new QuotaConfig("6G", null);
    TableConfig offlineTableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setQuotaConfig(quotaConfig)
            .setRetentionTimeUnit("DAYS").setRetentionTimeValue("1").setSegmentPushType("APPEND")
            .setBrokerTenant("testBroker").setServerTenant("testServer").build();
    ZKMetadataProvider.setTableConfig(_testPropertyStore, offlineTableConfig);

    ExternalView brokerResource = generateBrokerResource(REALTIME_TABLE_NAME);
    TableConfig tableConfig = generateDefaultTableConfig(REALTIME_TABLE_NAME);
    _queryQuotaManager.initOrUpdateTableQueryQuota(tableConfig, brokerResource);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 1);
    Assert.assertEquals(_queryQuotaManager.getTableQueryQuota(REALTIME_TABLE_NAME), 0);
  }

  @Test
  public void testRealtimeTableWithNullQuotaButWithOfflineTableConfigNotNullQpsConfig() {
    QuotaConfig quotaConfig = new QuotaConfig("6G", TABLE_MAX_QPS_STR);
    TableConfig offlineTableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setQuotaConfig(quotaConfig)
            .setRetentionTimeUnit("DAYS").setRetentionTimeValue("1").setSegmentPushType("APPEND")
            .setBrokerTenant("testBroker").setServerTenant("testServer").build();
    ZKMetadataProvider.setTableConfig(_testPropertyStore, offlineTableConfig);

    ExternalView brokerResource = generateBrokerResource(OFFLINE_TABLE_NAME);
    TableConfig tableConfig = generateDefaultTableConfig(REALTIME_TABLE_NAME);
    _queryQuotaManager.initOrUpdateTableQueryQuota(tableConfig, brokerResource);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 1);
    Assert.assertEquals(_queryQuotaManager.getTableQueryQuota(REALTIME_TABLE_NAME), 0);
  }

  @Test
  public void testNoBrokerResource()
      throws Exception {
    TableConfig tableConfig = generateDefaultTableConfig(OFFLINE_TABLE_NAME);
    setQps(tableConfig);
    _queryQuotaManager.initOrUpdateTableQueryQuota(tableConfig, null);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 0);
    Assert.assertEquals(_queryQuotaManager.getTableQueryQuota(REALTIME_TABLE_NAME), 0);
  }

  @Test
  public void testNoBrokerServiceOnBrokerResource()
      throws Exception {
    ExternalView brokerResource = new ExternalView(CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    TableConfig tableConfig = generateDefaultTableConfig(OFFLINE_TABLE_NAME);
    ZKMetadataProvider.setTableConfig(_testPropertyStore, tableConfig);
    setQps(tableConfig);
    _queryQuotaManager.initOrUpdateTableQueryQuota(tableConfig, brokerResource);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 1);
  }

  @Test
  public void testNoOnlineBrokerServiceOnBrokerResource()
      throws Exception {
    ExternalView brokerResource = new ExternalView(CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    brokerResource.setState(OFFLINE_TABLE_NAME, "broker_instance_2", "OFFLINE");
    TableConfig tableConfig = generateDefaultTableConfig(OFFLINE_TABLE_NAME);
    ZKMetadataProvider.setTableConfig(_testPropertyStore, tableConfig);
    setQps(tableConfig);
    _queryQuotaManager.initOrUpdateTableQueryQuota(tableConfig, brokerResource);

    // For the 1st version we don't check the number of online brokers.
    // Thus the expected size now is 1. It'll be 0 when we bring dynamic rate back.
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 1);
  }

  private TableConfig generateDefaultTableConfig(String tableName) {
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    TableConfigBuilder builder = new TableConfigBuilder(tableType);
    builder.setTableName(tableName);
    return builder.build();
  }

  private DatabaseConfig generateDefaultDatabaseConfig() {
    return new DatabaseConfig(CommonConstants.DEFAULT_DATABASE, null);
  }

  private void setLowerDatabaseQps(DatabaseConfig databaseConfig) {
    setDatabaseQps(databaseConfig, DATABASE_LOW_QPS_STR);
  }

  private void setHigherDatabaseQps(DatabaseConfig databaseConfig) {
    setDatabaseQps(databaseConfig, DATABASE_HIGH_QPS_STR);
  }

  private void setDefaultDatabaseQps(String maxQps) {
    ZKMetadataProvider.removeDatabaseConfig(_testPropertyStore, CommonConstants.DEFAULT_DATABASE);
    CLUSTER_CONFIG_MAP.put(CommonConstants.Helix.DATABASE_MAX_QUERIES_PER_SECOND, maxQps);
    _queryQuotaManager.processQueryRateLimitingClusterConfigChange();
  }

  private void setDefaultApplicationQps(String maxQps) {
    CLUSTER_CONFIG_MAP.put(CommonConstants.Helix.APPLICATION_MAX_QUERIES_PER_SECOND, maxQps);
    _queryQuotaManager.processApplicationQueryRateLimitingClusterConfigChange();
  }

  private void setDatabaseQps(DatabaseConfig databaseConfig, String maxQps) {
    QuotaConfig quotaConfig = new QuotaConfig(null, maxQps);
    databaseConfig.setQuotaConfig(quotaConfig);
    ZKMetadataProvider.setDatabaseConfig(_testPropertyStore, databaseConfig);
    _queryQuotaManager.createDatabaseRateLimiter(CommonConstants.DEFAULT_DATABASE);
  }

  private void setApplicationQps(String appName, Double maxQps) {
    ZKMetadataProvider.setApplicationQpsQuota(_testPropertyStore, appName, maxQps);
    _queryQuotaManager.createApplicationRateLimiter(appName);
  }

  private void setQps(TableConfig tableConfig) {
    QuotaConfig quotaConfig = new QuotaConfig(null, TABLE_MAX_QPS_STR);
    tableConfig.setQuotaConfig(quotaConfig);
  }

  private void setQps(TableConfig tableConfig, String value) {
    QuotaConfig quotaConfig = new QuotaConfig(null, value);
    tableConfig.setQuotaConfig(quotaConfig);
  }

  private static ExternalView generateBrokerResource(String tableName) {
    ExternalView brokerResource = new ExternalView(CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    brokerResource.setState(tableName, BROKER_INSTANCE_ID, "ONLINE");
    brokerResource.setState(tableName, "broker_instance_2", "OFFLINE");
    return brokerResource;
  }

  private void runQueries()
      throws InterruptedException {
    runQueries(TABLE_MAX_QPS, false);
    // increase the qps and some of the queries should be throttled.
    // keep in mind that permits are 'regenerated' on every call based on how much time elapsed since last one
    // that means for 25 QPS we get new permit every 40 ms or 0.5 every 20 ms
    // if we start with 25 permits at time t1 then if we want to exceed the qps in the next second  we've to do more
    // double requests, because 25 will regenerate
    runQueries(TABLE_MAX_QPS * 2 + 1, true);
  }

  private void runQueries(double qps, boolean shouldFail)
      throws InterruptedException {
    runQueries(qps, shouldFail, APP_NAME);
  }

  // try to keep the qps below 50 to ensure that the time lost between 2 query runs on top of the sleepMillis
  // is not comparable to sleepMillis, else the actual qps would end being lot lower than required qps
  private void runQueries(double qps, boolean shouldFail, String appName)
      throws InterruptedException {
    int failCount = 0;
    long sleepMillis = (long) (1000 / qps);
    for (int i = 0; i < qps; i++) {
      if (!_queryQuotaManager.acquireApplication(appName)) {
        failCount++;
      }
      if (!_queryQuotaManager.acquireDatabase(CommonConstants.DEFAULT_DATABASE)) {
        failCount++;
      }
      if (!_queryQuotaManager.acquire(RAW_TABLE_NAME)) {
        failCount++;
      }
      Thread.sleep(sleepMillis);
    }

    if (shouldFail) {
      Assert.assertTrue(failCount != 0, "Expected failure with qps: " + qps + " and app :" + appName);
    } else {
      Assert.assertTrue(failCount == 0, "Expected no failure with qps: " + qps + " and app :" + appName);
    }
  }
}
