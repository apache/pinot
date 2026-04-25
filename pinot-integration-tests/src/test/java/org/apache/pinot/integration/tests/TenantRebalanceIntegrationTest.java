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
package org.apache.pinot.integration.tests;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.pinot.common.restlet.resources.TenantRebalanceResult;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.rebalance.tenant.TenantRebalanceConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.InstanceTypeUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class TenantRebalanceIntegrationTest extends BaseHybridClusterIntegrationTest {
  private static final String SHARED_TABLE_NAME_PREFIX = "tenant_rebalance";
  private static final String SHARED_KAFKA_TOPIC_PREFIX = "tenant-rebalance";
  private static final String SHARED_TENANT_NAME_PREFIX = "TenantRebalanceTenant";

  private final String _sharedResourceSuffix = Long.toUnsignedString(RANDOM.nextLong(), Character.MAX_RADIX);
  private final Map<String, String> _originalClusterConfigValues = new LinkedHashMap<>();
  private final Map<String, List<String>> _originalInstanceTags = new LinkedHashMap<>();
  private boolean _clusterConfigOverrideApplied;

  @Override
  protected boolean shouldStartSharedKafka() {
    return true;
  }

  @Override
  protected int getSharedNumBrokers() {
    return 1;
  }

  @Override
  protected int getSharedNumServers() {
    return NUM_SERVERS;
  }

  @Override
  protected boolean shouldStartSharedMinion() {
    return false;
  }

  @Override
  protected String getTableName() {
    return isSharedRichClusterEnabled() ? SHARED_TABLE_NAME_PREFIX + "_" + _sharedResourceSuffix
        : super.getTableName();
  }

  @Override
  protected String getKafkaTopic() {
    return isSharedRichClusterEnabled() ? SHARED_KAFKA_TOPIC_PREFIX + "-" + _sharedResourceSuffix
        : super.getKafkaTopic();
  }

  @Override
  protected String getBrokerTenant() {
    return isSharedRichClusterEnabled() ? SHARED_TENANT_NAME_PREFIX + "_" + _sharedResourceSuffix
        : super.getBrokerTenant();
  }

  @Override
  protected String getServerTenant() {
    return isSharedRichClusterEnabled() ? SHARED_TENANT_NAME_PREFIX + "_" + _sharedResourceSuffix
        : super.getServerTenant();
  }

  @Override
  protected void overrideControllerConf(Map<String, Object> properties) {
    properties.put(ControllerConf.CLUSTER_TENANT_ISOLATION_ENABLE, false);
  }

  @Override
  protected void overrideBrokerConf(PinotConfiguration configuration) {
    if (!isSharedRichClusterEnabled()) {
      configuration.setProperty(CommonConstants.Broker.CONFIG_OF_BROKER_INSTANCE_TAGS,
          TagNameUtils.getBrokerTagForTenant(getBrokerTenant()));
    }
  }

  @BeforeClass(alwaysRun = true)
  @Override
  public void setUp()
      throws Exception {
    super.setUp();
  }

  @Override
  protected void startHybridCluster()
      throws Exception {
    startZk();
    startController();
    applyClusterConfigOverrides();
    startBroker();
    startServers(NUM_SERVERS);
    snapshotSharedInstanceTags();
    createOrUpdateSharedBrokerTenant();
    startKafka();
    createOrUpdateServerTenant();
  }

  @AfterClass(alwaysRun = true)
  @Override
  public void tearDown()
      throws Exception {
    Exception exception = null;
    exception = runCleanup(exception, this::cleanTableAndSchema);
    exception = runCleanup(exception, this::deleteKafkaTopicIfPresent);
    exception = runCleanup(exception, this::restoreClusterConfigOverrides);
    exception = runCleanup(exception, this::closeH2Connection);
    exception = runCleanup(exception, this::closePinotConnections);
    exception = runCleanup(exception, this::restoreSharedInstanceTags);
    if (!isSharedRichClusterEnabled()) {
      exception = runCleanup(exception, this::stopServer);
      exception = runCleanup(exception, this::stopBroker);
      exception = runCleanup(exception, this::stopController);
      exception = runCleanup(exception, this::stopKafka);
      exception = runCleanup(exception, this::stopZk);
    }
    exception = runCleanup(exception, this::cleanupHybridCluster);
    if (exception != null) {
      throw exception;
    }
  }

  private TenantRebalanceResult rebalanceTenant(TenantRebalanceConfig config, @Nullable Map<String, String> queryParams)
      throws Exception {
    return getOrCreateAdminClient().getTenantClient()
        .rebalanceTenantWithConfigObject(config.getTenantName(), JsonUtils.objectToString(config), queryParams);
  }

  private TenantRebalanceResult rebalanceTenant(TenantRebalanceConfig config)
      throws Exception {
    return rebalanceTenant(config, null);
  }

  @Test
  public void testParallelWhitelistBlacklistCompatibility()
      throws Exception {
    // Prepare a TenantRebalanceConfig with parallelWhitelist and parallelBlacklist (old usage)
    TenantRebalanceConfig config = new TenantRebalanceConfig();
    config.setTenantName(getServerTenant());
    config.setDryRun(true);
    // Add a table to parallelWhitelist and another to parallelBlacklist
    String table1 = getTableName() + "_OFFLINE";
    String table2 = getTableName() + "_REALTIME";
    config.getParallelWhitelist().add(table1);
    config.getParallelBlacklist().add(table2);

    // Call the rebalance endpoint with the config
    TenantRebalanceResult result = rebalanceTenant(config);

    // Assert that the result contains the expected tables and statuses
    assertNotNull(result);
    assertTrue(result.getRebalanceTableResults().containsKey(table1));
    assertTrue(result.getRebalanceTableResults().containsKey(table2));
  }

  @Test
  public void testIncludeTablesQueryParamOverridesBody()
      throws Exception {
    TenantRebalanceConfig config = new TenantRebalanceConfig();
    config.setTenantName(getServerTenant());
    config.setDryRun(true);
    String table1 = getTableName() + "_OFFLINE";
    String table2 = getTableName() + "_REALTIME";
    // Set both tables in the body
    config.getIncludeTables().add(table1);
    config.getIncludeTables().add(table2);

    // Pass only table1 in the query param, should override the body
    TenantRebalanceResult result =
        rebalanceTenant(config, Map.of("includeTables", table1));
    assertNotNull(result);
    assertTrue(result.getRebalanceTableResults().containsKey(table1));
    assertFalse(result.getRebalanceTableResults().containsKey(table2), "Query param should override body");
  }

  @Test
  public void testIncludeTablesBodyUsedIfNoQueryParam()
      throws Exception {
    TenantRebalanceConfig config = new TenantRebalanceConfig();
    config.setTenantName(getServerTenant());
    config.setDryRun(true);
    String table1 = getTableName() + "_OFFLINE";
    String table2 = getTableName() + "_REALTIME";
    // Set only table2 in the body
    config.getIncludeTables().add(table2);

    // No query param, should use body
    TenantRebalanceResult result = rebalanceTenant(config);
    assertNotNull(result);
    assertTrue(result.getRebalanceTableResults().containsKey(table2));
    assertFalse(result.getRebalanceTableResults().containsKey(table1), "Should only use body if no query param");
  }

  @Test
  public void testIncludeExcludeTablesQueryParamMultipleTables()
      throws Exception {
    TenantRebalanceConfig config = new TenantRebalanceConfig();
    config.setTenantName(getServerTenant());
    config.setDryRun(true);
    String table1 = getTableName() + "_OFFLINE";
    String table2 = getTableName() + "_REALTIME";

    // Pass includeTables and excludeTables as comma separated list with spaces and quotes
    Map<String, String> queryParams = Map.of("includeTables", " " + table1 + " , \"" + table2 + "\" , ",
        "excludeTables", " " + table2 + ", ");
    TenantRebalanceResult result = rebalanceTenant(config, queryParams);
    assertNotNull(result);
    // Should only include table1, table2 are excluded
    assertTrue(result.getRebalanceTableResults().containsKey(table1));
    assertFalse(result.getRebalanceTableResults().containsKey(table2), "excludeTables should remove table2");
  }

  private void createOrUpdateSharedBrokerTenant()
      throws Exception {
    if (!isSharedRichClusterEnabled()) {
      return;
    }
    try {
      createBrokerTenant(getBrokerTenant(), getSharedNumBrokers());
    } catch (Exception e) {
      updateBrokerTenant(getBrokerTenant(), getSharedNumBrokers());
    }
  }

  private void createOrUpdateServerTenant()
      throws Exception {
    try {
      createServerTenant(getServerTenant(), NUM_SERVERS_OFFLINE, NUM_SERVERS_REALTIME);
    } catch (Exception e) {
      if (!isSharedRichClusterEnabled()) {
        throw e;
      }
      updateServerTenant(getServerTenant(), NUM_SERVERS_OFFLINE, NUM_SERVERS_REALTIME);
    }
  }

  private void applyClusterConfigOverrides() {
    Map<String, String> configOverrides = new LinkedHashMap<>();
    configOverrides.put(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM, Integer.toString(10));
    configOverrides.put(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM,
        Integer.toString(6));
    configOverrides.put(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_DOWNLOAD_PARALLELISM, Integer.toString(12));

    HelixConfigScope scope = getClusterConfigScope();
    try {
      _clusterConfigOverrideApplied = true;
      configOverrides.forEach((key, value) -> {
        _originalClusterConfigValues.put(key, _helixManager.getConfigAccessor().get(scope, key));
        _helixManager.getConfigAccessor().set(scope, key, value);
      });
    } catch (RuntimeException e) {
      restoreClusterConfigOverrides();
      throw e;
    }
  }

  private void restoreClusterConfigOverrides() {
    if ((!_clusterConfigOverrideApplied && _originalClusterConfigValues.isEmpty()) || _helixManager == null) {
      return;
    }

    HelixConfigScope scope = getClusterConfigScope();
    _originalClusterConfigValues.forEach((key, originalValue) -> {
      if (originalValue == null) {
        _helixManager.getConfigAccessor().remove(scope, key);
      } else {
        _helixManager.getConfigAccessor().set(scope, key, originalValue);
      }
    });
    _originalClusterConfigValues.clear();
    _clusterConfigOverrideApplied = false;
  }

  private HelixConfigScope getClusterConfigScope() {
    return new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(getHelixClusterName())
        .build();
  }

  private void snapshotSharedInstanceTags() {
    if (!isSharedRichClusterEnabled() || _helixAdmin == null) {
      return;
    }

    _originalInstanceTags.clear();
    for (String instanceName : _helixAdmin.getInstancesInCluster(getHelixClusterName())) {
      InstanceConfig instanceConfig = _helixAdmin.getInstanceConfig(getHelixClusterName(), instanceName);
      _originalInstanceTags.put(instanceName, new ArrayList<>(instanceConfig.getTags()));
    }
  }

  private void restoreSharedInstanceTags() {
    if (!isSharedRichClusterEnabled() || _originalInstanceTags.isEmpty() || _helixResourceManager == null) {
      return;
    }

    _originalInstanceTags.forEach((instanceName, tags) -> {
      if (_helixAdmin.getInstancesInCluster(getHelixClusterName()).contains(instanceName)) {
        _helixResourceManager.updateInstanceTags(instanceName, String.join(",", tags),
            InstanceTypeUtils.isBroker(instanceName));
      }
    });
    _originalInstanceTags.clear();
  }

  private void cleanTableAndSchema()
      throws Exception {
    if (_helixResourceManager == null) {
      return;
    }

    String tableName = getTableName();
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    if (_helixResourceManager.getTableConfig(offlineTableName) != null
        || _helixResourceManager.hasOfflineTable(tableName)) {
      dropOfflineTable(tableName);
      waitForTableDataManagerRemoved(offlineTableName);
      waitForEVToDisappear(offlineTableName);
    }

    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    if (_helixResourceManager.getTableConfig(realtimeTableName) != null
        || _helixResourceManager.hasRealtimeTable(tableName)) {
      dropRealtimeTable(tableName);
      waitForTableDataManagerRemoved(realtimeTableName);
      waitForEVToDisappear(realtimeTableName);
    }

    if (_helixResourceManager.getSchema(tableName) != null) {
      deleteSchema(tableName);
    }
  }

  private void deleteKafkaTopicIfPresent() {
    if (isKafkaTopicPresent()) {
      deleteKafkaTopic(getKafkaTopic());
    }
  }

  private boolean isKafkaTopicPresent() {
    if (_kafkaStarters == null || _kafkaStarters.isEmpty()) {
      return false;
    }
    Properties adminProps = new Properties();
    adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaBrokerList());
    adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
    adminProps.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000");
    try (AdminClient adminClient = AdminClient.create(adminProps)) {
      return adminClient.listTopics().names().get(5, TimeUnit.SECONDS).contains(getKafkaTopic());
    } catch (Exception e) {
      return false;
    }
  }

  private void closeH2Connection()
      throws Exception {
    if (_h2Connection != null) {
      _h2Connection.close();
      _h2Connection = null;
    }
    _queryGenerator = null;
  }

  private void closePinotConnections() {
    if (_pinotConnection != null) {
      _pinotConnection.close();
      _pinotConnection = null;
    }
    if (_pinotConnectionV2 != null) {
      _pinotConnectionV2.close();
      _pinotConnectionV2 = null;
    }
  }

  private Exception runCleanup(Exception firstException, Cleanup cleanup) {
    try {
      cleanup.run();
    } catch (Exception e) {
      if (firstException == null) {
        return e;
      }
      firstException.addSuppressed(e);
    }
    return firstException;
  }

  private interface Cleanup {
    void run()
        throws Exception;
  }
}
