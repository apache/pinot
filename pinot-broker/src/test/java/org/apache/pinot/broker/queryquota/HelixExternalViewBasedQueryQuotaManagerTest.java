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

import org.apache.commons.lang.StringUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.ExternalView;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.config.QuotaConfig;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.common.utils.ZkStarter;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.apache.pinot.common.utils.CommonConstants.Helix.BROKER_RESOURCE_INSTANCE;
import static org.apache.pinot.common.utils.CommonConstants.Helix.TableType;


public class HelixExternalViewBasedQueryQuotaManagerTest {
  private ZkHelixPropertyStore<ZNRecord> _testPropertyStore;
  private HelixManager _helixManager;
  private HelixExternalViewBasedQueryQuotaManager _queryQuotaManager;
  private ZkStarter.ZookeeperInstance _zookeeperInstance;
  private static String RAW_TABLE_NAME = "testTable";
  private static String OFFLINE_TABLE_NAME = RAW_TABLE_NAME + "_OFFLINE";
  private static String REALTIME_TABLE_NAME = RAW_TABLE_NAME + "_REALTIME";
  private static final String BROKER_INSTANCE_ID = "broker_instance_1";

  @BeforeTest
  public void beforeTest() {
    _zookeeperInstance = ZkStarter.startLocalZkServer();
    String helixClusterName = "TestTableQueryQuotaManagerService";

    _helixManager = initHelixManager(helixClusterName);
    _testPropertyStore = _helixManager.getHelixPropertyStore();

    _queryQuotaManager = new HelixExternalViewBasedQueryQuotaManager();
    _queryQuotaManager.init(_helixManager);
  }

  private HelixManager initHelixManager(String helixClusterName) {
    return new FakeHelixManager(helixClusterName, BROKER_INSTANCE_ID, InstanceType.PARTICIPANT,
        ZkStarter.DEFAULT_ZK_STR);
  }

  public class FakeHelixManager extends ZKHelixManager {
    private ZkHelixPropertyStore<ZNRecord> _propertyStore;

    FakeHelixManager(String clusterName, String instanceName, InstanceType instanceType, String zkAddress) {
      super(clusterName, instanceName, instanceType, zkAddress);
      super._zkclient = new ZkClient(StringUtil.join("/", StringUtils.chomp(ZkStarter.DEFAULT_ZK_STR, "/")),
          ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());
      _zkclient.deleteRecursively("/" + clusterName + "/PROPERTYSTORE");
      _zkclient.createPersistent("/" + clusterName + "/PROPERTYSTORE", true);
      setPropertyStore(clusterName);
    }

    void setPropertyStore(String clusterName) {
      _propertyStore =
          new ZkHelixPropertyStore<>(new ZkBaseDataAccessor<ZNRecord>(_zkclient), "/" + clusterName + "/PROPERTYSTORE",
              null);
    }

    void closeZkClient() {
      _zkclient.close();
    }
  }

  @AfterMethod
  public void afterMethod() {
    if (_helixManager instanceof FakeHelixManager) {
      _testPropertyStore.reset();
      ZKMetadataProvider.removeResourceConfigFromPropertyStore(_testPropertyStore, OFFLINE_TABLE_NAME);
      ZKMetadataProvider.removeResourceConfigFromPropertyStore(_testPropertyStore, REALTIME_TABLE_NAME);
    }
    _queryQuotaManager.cleanUpRateLimiterMap();
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
    setQps(tableConfig);
    _queryQuotaManager.initTableQueryQuota(tableConfig, brokerResource);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 1);

    // All the request should be passed.
    runQueries(70, 10);

    _queryQuotaManager.dropTableQueryQuota(OFFLINE_TABLE_NAME);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 0);
  }

  @Test
  public void testOfflineTableWithNullQuotaAndNoRealtimeTableConfig()
      throws Exception {
    ExternalView brokerResource = generateBrokerResource(OFFLINE_TABLE_NAME);
    TableConfig tableConfig = generateDefaultTableConfig(OFFLINE_TABLE_NAME);
    _queryQuotaManager.initTableQueryQuota(tableConfig, brokerResource);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 0);
  }

  @Test
  public void testOfflineTableWithNullQuotaButWithRealtimeTableConfigNullQpsConfig()
      throws Exception {
    QuotaConfig quotaConfig = new QuotaConfig("6G", null);
    TableConfig realtimeTableConfig =
        new TableConfig.Builder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setQuotaConfig(quotaConfig)
            .setRetentionTimeUnit("DAYS").setRetentionTimeValue("1").setSegmentPushType("APPEND")
            .setBrokerTenant("testBroker").setServerTenant("testServer").build();
    ZKMetadataProvider
        .setRealtimeTableConfig(_testPropertyStore, REALTIME_TABLE_NAME, realtimeTableConfig.toZNRecord());

    ExternalView brokerResource = generateBrokerResource(OFFLINE_TABLE_NAME);
    TableConfig tableConfig = generateDefaultTableConfig(OFFLINE_TABLE_NAME);
    _queryQuotaManager.initTableQueryQuota(tableConfig, brokerResource);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 0);

    // Nothing happened since it doesn't have qps quota.
    _queryQuotaManager.dropTableQueryQuota(OFFLINE_TABLE_NAME);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 0);
  }

  @Test
  public void testOfflineTableWithNullQuotaButWithRealtimeTableConfigNotNullQpsConfig()
      throws Exception {
    QuotaConfig quotaConfig = new QuotaConfig("6G", "100.00");
    TableConfig realtimeTableConfig =
        new TableConfig.Builder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setQuotaConfig(quotaConfig)
            .setRetentionTimeUnit("DAYS").setRetentionTimeValue("1").setSegmentPushType("APPEND")
            .setBrokerTenant("testBroker").setServerTenant("testServer").build();
    ZKMetadataProvider
        .setRealtimeTableConfig(_testPropertyStore, REALTIME_TABLE_NAME, realtimeTableConfig.toZNRecord());

    ExternalView brokerResource = generateBrokerResource(REALTIME_TABLE_NAME);
    TableConfig tableConfig = generateDefaultTableConfig(OFFLINE_TABLE_NAME);
    _queryQuotaManager.initTableQueryQuota(tableConfig, brokerResource);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 0);

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

    QuotaConfig quotaConfig = new QuotaConfig("6G", "100.00");
    TableConfig realtimeTableConfig =
        new TableConfig.Builder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setQuotaConfig(quotaConfig)
            .setRetentionTimeUnit("DAYS").setRetentionTimeValue("1").setSegmentPushType("APPEND")
            .setBrokerTenant("testBroker").setServerTenant("testServer").build();
    TableConfig offlineTableConfig =
        new TableConfig.Builder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setQuotaConfig(quotaConfig)
            .setRetentionTimeUnit("DAYS").setRetentionTimeValue("1").setSegmentPushType("APPEND")
            .setBrokerTenant("testBroker").setServerTenant("testServer").build();

    ZKMetadataProvider
        .setRealtimeTableConfig(_testPropertyStore, REALTIME_TABLE_NAME, realtimeTableConfig.toZNRecord());
    ZKMetadataProvider.setOfflineTableConfig(_testPropertyStore, OFFLINE_TABLE_NAME, offlineTableConfig.toZNRecord());

    // Since each table has 2 online brokers, per broker rate becomes 100.0 / 2 = 50.0
    _queryQuotaManager.initTableQueryQuota(offlineTableConfig, brokerResource);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 1);
    _queryQuotaManager.initTableQueryQuota(realtimeTableConfig, brokerResource);
    // The hash map now contains 2 entries for both of the tables.
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 2);

    // Rate limiter generates 1 token every 10 milliseconds, have to make it sleep for a while.
    runQueries(70, 10L);

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
    setQps(tableConfig);
    _queryQuotaManager.initTableQueryQuota(tableConfig, brokerResource);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 1);

    runQueries(70, 10L);

    _queryQuotaManager.dropTableQueryQuota(REALTIME_TABLE_NAME);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 0);
  }

  @Test
  public void testRealtimeTableWithNullQuotaAndNoOfflineTableConfig()
      throws Exception {
    ExternalView brokerResource = generateBrokerResource(REALTIME_TABLE_NAME);
    TableConfig tableConfig = generateDefaultTableConfig(REALTIME_TABLE_NAME);
    _queryQuotaManager.initTableQueryQuota(tableConfig, brokerResource);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 0);
  }

  @Test
  public void testRealtimeTableWithNullQuotaButWithOfflineTableConfigNullQpsConfig()
      throws Exception {
    QuotaConfig quotaConfig = new QuotaConfig("6G", null);
    TableConfig offlineTableConfig =
        new TableConfig.Builder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setQuotaConfig(quotaConfig)
            .setRetentionTimeUnit("DAYS").setRetentionTimeValue("1").setSegmentPushType("APPEND")
            .setBrokerTenant("testBroker").setServerTenant("testServer").build();
    ZKMetadataProvider.setOfflineTableConfig(_testPropertyStore, OFFLINE_TABLE_NAME, offlineTableConfig.toZNRecord());

    ExternalView brokerResource = generateBrokerResource(REALTIME_TABLE_NAME);
    TableConfig tableConfig = generateDefaultTableConfig(REALTIME_TABLE_NAME);
    _queryQuotaManager.initTableQueryQuota(tableConfig, brokerResource);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 0);
  }

  @Test
  public void testRealtimeTableWithNullQuotaButWithOfflineTableConfigNotNullQpsConfig()
      throws Exception {
    QuotaConfig quotaConfig = new QuotaConfig("6G", "100.00");
    TableConfig offlineTableConfig =
        new TableConfig.Builder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setQuotaConfig(quotaConfig)
            .setRetentionTimeUnit("DAYS").setRetentionTimeValue("1").setSegmentPushType("APPEND")
            .setBrokerTenant("testBroker").setServerTenant("testServer").build();
    ZKMetadataProvider.setOfflineTableConfig(_testPropertyStore, OFFLINE_TABLE_NAME, offlineTableConfig.toZNRecord());

    ExternalView brokerResource = generateBrokerResource(OFFLINE_TABLE_NAME);
    TableConfig tableConfig = generateDefaultTableConfig(REALTIME_TABLE_NAME);
    _queryQuotaManager.initTableQueryQuota(tableConfig, brokerResource);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 0);
  }

  @Test
  public void testInvalidQpsQuota()
      throws Exception {
    ExternalView brokerResource = generateBrokerResource(OFFLINE_TABLE_NAME);
    TableConfig tableConfig = generateDefaultTableConfig(OFFLINE_TABLE_NAME);
    // Set invalid qps quota
    QuotaConfig quotaConfig = new QuotaConfig(null, "InvalidQpsQuota");
    tableConfig.setQuotaConfig(quotaConfig);
    _queryQuotaManager.initTableQueryQuota(tableConfig, brokerResource);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 0);
  }

  @Test
  public void testInvalidNegativeQpsQuota()
      throws Exception {
    ExternalView brokerResource = generateBrokerResource(OFFLINE_TABLE_NAME);
    TableConfig tableConfig = generateDefaultTableConfig(OFFLINE_TABLE_NAME);
    // Set invalid negative qps quota
    QuotaConfig quotaConfig = new QuotaConfig(null, "-1.0");
    tableConfig.setQuotaConfig(quotaConfig);
    _queryQuotaManager.initTableQueryQuota(tableConfig, brokerResource);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 0);
  }

  @Test
  public void testNoBrokerResource()
      throws Exception {
    TableConfig tableConfig = generateDefaultTableConfig(OFFLINE_TABLE_NAME);
    setQps(tableConfig);
    _queryQuotaManager.initTableQueryQuota(tableConfig, null);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 0);
  }

  @Test
  public void testNoBrokerServiceOnBrokerResource()
      throws Exception {
    ExternalView brokerResource = new ExternalView(BROKER_RESOURCE_INSTANCE);
    TableConfig tableConfig = generateDefaultTableConfig(OFFLINE_TABLE_NAME);
    setQps(tableConfig);
    _queryQuotaManager.initTableQueryQuota(tableConfig, brokerResource);
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 1);
  }

  @Test
  public void testNoOnlineBrokerServiceOnBrokerResource()
      throws Exception {
    ExternalView brokerResource = new ExternalView(BROKER_RESOURCE_INSTANCE);
    brokerResource.setState(OFFLINE_TABLE_NAME, "broker_instance_2", "OFFLINE");
    TableConfig tableConfig = generateDefaultTableConfig(OFFLINE_TABLE_NAME);
    setQps(tableConfig);
    _queryQuotaManager.initTableQueryQuota(tableConfig, brokerResource);

    // For the 1st version we don't check the number of online brokers.
    // Thus the expected size now is 1. It'll be 0 when we bring dynamic rate back.
    Assert.assertEquals(_queryQuotaManager.getRateLimiterMapSize(), 1);
  }

  private TableConfig generateDefaultTableConfig(String tableName) {
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    TableConfig.Builder builder = new TableConfig.Builder(tableType);
    builder.setTableName(tableName);
    return builder.build();
  }

  private void setQps(TableConfig tableConfig) {
    QuotaConfig quotaConfig = new QuotaConfig(null, "100.00");
    tableConfig.setQuotaConfig(quotaConfig);
  }

  private ExternalView generateBrokerResource(String tableName) {
    ExternalView brokerResource = new ExternalView(BROKER_RESOURCE_INSTANCE);
    brokerResource.setState(tableName, BROKER_INSTANCE_ID, "ONLINE");
    brokerResource.setState(tableName, "broker_instance_2", "OFFLINE");
    return brokerResource;
  }

  private void runQueries(int numOfTimesToRun, long millis)
      throws InterruptedException {
    int count = 0;
    for (int i = 0; i < numOfTimesToRun; i++) {
      Assert.assertTrue(_queryQuotaManager.acquire(RAW_TABLE_NAME));
      count++;
      Thread.sleep(millis);
    }
    Assert.assertEquals(count, numOfTimesToRun);

    //Reduce the time of sleeping and some of the queries should be throttled.
    count = 0;
    millis /= 2;
    for (int i = 0; i < numOfTimesToRun; i++) {
      if (!_queryQuotaManager.acquire(RAW_TABLE_NAME)) {
        count++;
      }
      Thread.sleep(millis);
    }
    Assert.assertTrue(count > 0 && count < numOfTimesToRun);
  }
}
