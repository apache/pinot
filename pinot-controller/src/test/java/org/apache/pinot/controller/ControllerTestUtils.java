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
package org.apache.pinot.controller;

import com.google.common.base.Preconditions;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.multipart.MultipartRequestEntity;
import org.apache.commons.httpclient.methods.multipart.Part;
import org.apache.commons.httpclient.methods.multipart.StringPart;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.Message;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.controller.helix.ControllerRequestURLBuilder;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.tenant.Tenant;
import org.apache.pinot.spi.config.tenant.TenantRole;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

import static org.apache.pinot.common.utils.CommonConstants.Helix.Instance.*;
import static org.apache.pinot.common.utils.CommonConstants.Helix.*;
import static org.apache.pinot.common.utils.CommonConstants.Server.*;
import static org.testng.Assert.*;


/**
 * Base class for controller tests.
 */
public abstract class ControllerTestUtils {
  public static final String LOCAL_HOST = "localhost";
  protected static final int DEFAULT_CONTROLLER_PORT = 18998;
  protected static final String DEFAULT_DATA_DIR = new File(FileUtils.getTempDirectoryPath(),
      "test-controller-" + System.currentTimeMillis()).getAbsolutePath();
  public static final String BROKER_INSTANCE_ID_PREFIX = "Broker_localhost_";
  public static final String SERVER_INSTANCE_ID_PREFIX = "Server_localhost_";

  // NUM_BROKER_INSTANCES and NUM_SERVER_INSTANCES must be a multiple of MIN_NUM_REPLICAS.
  public static final int MIN_NUM_REPLICAS = 2;
  public static final int NUM_BROKER_INSTANCES = 4;
  public static final int NUM_SERVER_INSTANCES = 4;

  public static final String TENANT_NAME = "testTenant";

  public static Tenant _brokerTenant;

  public static void createBrokerTenant(Tenant _brokerTenant) {
    _brokerTenant = _brokerTenant;
    getHelixResourceManager().createBrokerTenant(_brokerTenant);
  }

  public static Tenant geBrokerTenant() {
    return _brokerTenant;
  }

  public static Tenant _serverTenant;

  public static void createServerTenant(Tenant _serverTenant) {
    _serverTenant = _serverTenant;
    getHelixResourceManager().createServerTenant(_serverTenant);
  }

  public static Tenant getServerTenant() {
    return _serverTenant;
  }


  protected static final List<HelixManager> _fakeInstanceHelixManagers = new ArrayList<>();
  protected static int _controllerPort;
  protected static String _controllerBaseApiUrl;

  protected static ControllerRequestURLBuilder _controllerRequestURLBuilder;
  protected static String _controllerDataDir;
  protected static ControllerStarter _controllerStarter;
  protected static PinotHelixResourceManager _helixResourceManager;
  protected static HelixManager _helixManager;
  protected static HelixAdmin _helixAdmin;
  protected static HelixDataAccessor _helixDataAccessor;

  protected static ZkHelixPropertyStore<ZNRecord> _propertyStore;

  private static ZkStarter.ZookeeperInstance _zookeeperInstance;

  public static String getHelixClusterName() {
    return ControllerTestUtils.class.getSimpleName();
  }

  protected static void startZk() {
    _zookeeperInstance = ZkStarter.startLocalZkServer();
  }

  protected static void startZk(int port) {
    _zookeeperInstance = ZkStarter.startLocalZkServer(port);
  }

  protected static void stopZk() {
    try {
      ZkStarter.stopLocalZkServer(_zookeeperInstance);
    } catch (Exception e) {
      // Swallow exceptions
    }
  }

  public static Map<String, Object> getDefaultControllerConfiguration() {
    Map<String, Object> properties = new HashMap<>();

    properties.put(ControllerConf.CONTROLLER_HOST, LOCAL_HOST);
    properties.put(ControllerConf.CONTROLLER_PORT, DEFAULT_CONTROLLER_PORT);
    properties.put(ControllerConf.DATA_DIR, DEFAULT_DATA_DIR);
    properties.put(ControllerConf.ZK_STR, ZkStarter.DEFAULT_ZK_STR);
    properties.put(ControllerConf.HELIX_CLUSTER_NAME, getHelixClusterName());

    return properties;
  }

  public static void startController() {
    startController(getDefaultControllerConfiguration());
  }

  public static void startController(Map<String, Object> properties) {
    Preconditions.checkState(_controllerStarter == null);

    ControllerConf config = new ControllerConf(properties);

    _controllerPort = Integer.valueOf(config.getControllerPort());
    _controllerBaseApiUrl = "http://localhost:" + _controllerPort;
    _controllerRequestURLBuilder = ControllerRequestURLBuilder.baseUrl(_controllerBaseApiUrl);
    _controllerDataDir = config.getDataDir();

    _controllerStarter = getControllerStarter(config);
    _controllerStarter.start();
    _helixResourceManager = _controllerStarter.getHelixResourceManager();
    _helixManager = _controllerStarter.getHelixControllerManager();
    _helixDataAccessor = _helixManager.getHelixDataAccessor();
    ConfigAccessor configAccessor = _helixManager.getConfigAccessor();
    // HelixResourceManager is null in Helix only mode, while HelixManager is null in Pinot only mode.
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(getHelixClusterName())
            .build();
    switch (_controllerStarter.getControllerMode()) {
      case DUAL:
      case PINOT_ONLY:
        _helixAdmin = _helixResourceManager.getHelixAdmin();
        _propertyStore = _helixResourceManager.getPropertyStore();

        // TODO: Enable periodic rebalance per 10 seconds as a temporary work-around for the Helix issue:
        //       https://github.com/apache/helix/issues/331. Remove this after Helix fixing the issue.
        configAccessor.set(scope, ClusterConfig.ClusterConfigProperty.REBALANCE_TIMER_PERIOD.name(), "10000");
        break;
      case HELIX_ONLY:
        _helixAdmin = _helixManager.getClusterManagmentTool();
        _propertyStore = _helixManager.getHelixPropertyStore();
        break;
    }
    //enable case insensitive pql for test cases.
    configAccessor.set(scope, CommonConstants.Helix.ENABLE_CASE_INSENSITIVE_KEY, Boolean.toString(true));
    //Set hyperloglog log2m value to 12.
    configAccessor.set(scope, CommonConstants.Helix.DEFAULT_HYPERLOGLOG_LOG2M_KEY, Integer.toString(12));
  }

  protected static ControllerStarter getControllerStarter(ControllerConf config) {
    return new ControllerStarter(config);
  }

  public static void stopController() {
    _controllerStarter.stop();
    _controllerStarter = null;
    FileUtils.deleteQuietly(new File(_controllerDataDir));
  }

  public static int getFakeBrokerInstanceCount() {
    return getHelixAdmin().getInstancesInClusterWithTag(getHelixClusterName(), "DefaultTenant_BROKER").size() +
        getHelixAdmin().getInstancesInClusterWithTag(getHelixClusterName(), UNTAGGED_BROKER_INSTANCE).size();
  }

  public static int getFakeBrokerInstanceCount(boolean isSingleTenant) {
    return isSingleTenant ?
        getHelixAdmin().getInstancesInClusterWithTag(getHelixClusterName(), "DefaultTenant_BROKER").size():
        getHelixAdmin().getInstancesInClusterWithTag(getHelixClusterName(), UNTAGGED_BROKER_INSTANCE).size();
  }

  public static void addFakeBrokerInstancesToAutoJoinHelixCluster(int numInstances, boolean isSingleTenant)
      throws Exception {
    for (int i = 0; i < numInstances; i++) {
      addFakeBrokerInstanceToAutoJoinHelixCluster(BROKER_INSTANCE_ID_PREFIX + i, isSingleTenant);
    }
  }

  public static void addMaxFakeBrokerInstancesToAutoJoinHelixCluster(int maxCount, boolean isSingleTenant)
      throws Exception {

    // get current instance count
    int currentCount = getFakeBrokerInstanceCount();

    // Add more instances if current count is less than max instance count.
    if (currentCount < maxCount) {
      for (int i = currentCount; i < maxCount; i++) {
        addFakeBrokerInstanceToAutoJoinHelixCluster(BROKER_INSTANCE_ID_PREFIX + i, isSingleTenant);
      }
    }
  }

  public static void addFakeBrokerInstanceToAutoJoinHelixCluster(String instanceId, boolean isSingleTenant)
      throws Exception {
    HelixManager helixManager = HelixManagerFactory
        .getZKHelixManager(getHelixClusterName(), instanceId, InstanceType.PARTICIPANT, ZkStarter.DEFAULT_ZK_STR);
    helixManager.getStateMachineEngine()
        .registerStateModelFactory(FakeBrokerResourceOnlineOfflineStateModelFactory.STATE_MODEL_DEF,
            FakeBrokerResourceOnlineOfflineStateModelFactory.FACTORY_INSTANCE);
    helixManager.connect();
    HelixAdmin helixAdmin = helixManager.getClusterManagmentTool();
    if (isSingleTenant) {
      helixAdmin.addInstanceTag(getHelixClusterName(), instanceId, TagNameUtils.getBrokerTagForTenant(null));
    } else {
      helixAdmin.addInstanceTag(getHelixClusterName(), instanceId, UNTAGGED_BROKER_INSTANCE);
    }
    _fakeInstanceHelixManagers.add(helixManager);
  }

  public static class FakeBrokerResourceOnlineOfflineStateModelFactory extends StateModelFactory<StateModel> {
    private static final String STATE_MODEL_DEF = "BrokerResourceOnlineOfflineStateModel";
    private static final FakeBrokerResourceOnlineOfflineStateModelFactory FACTORY_INSTANCE =
        new FakeBrokerResourceOnlineOfflineStateModelFactory();
    private static final FakeBrokerResourceOnlineOfflineStateModel STATE_MODEL_INSTANCE =
        new FakeBrokerResourceOnlineOfflineStateModel();

    private FakeBrokerResourceOnlineOfflineStateModelFactory() {
    }

    @Override
    public StateModel createNewStateModel(String resourceName, String partitionName) {
      return STATE_MODEL_INSTANCE;
    }

    @SuppressWarnings("unused")
    @StateModelInfo(states = "{'OFFLINE', 'ONLINE', 'DROPPED'}", initialState = "OFFLINE")
    public static class FakeBrokerResourceOnlineOfflineStateModel extends StateModel {
      private static final Logger LOGGER = LoggerFactory.getLogger(FakeBrokerResourceOnlineOfflineStateModel.class);

      private FakeBrokerResourceOnlineOfflineStateModel() {
      }

      @Transition(from = "OFFLINE", to = "ONLINE")
      public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
        LOGGER.debug("onBecomeOnlineFromOffline(): {}", message);
      }

      @Transition(from = "OFFLINE", to = "DROPPED")
      public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
        LOGGER.debug("onBecomeDroppedFromOffline(): {}", message);
      }

      @Transition(from = "ONLINE", to = "OFFLINE")
      public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
        LOGGER.debug("onBecomeOfflineFromOnline(): {}", message);
      }

      @Transition(from = "ONLINE", to = "DROPPED")
      public void onBecomeDroppedFromOnline(Message message, NotificationContext context) {
        LOGGER.debug("onBecomeDroppedFromOnline(): {}", message);
      }

      @Transition(from = "ERROR", to = "OFFLINE")
      public void onBecomeOfflineFromError(Message message, NotificationContext context) {
        LOGGER.debug("onBecomeOfflineFromError(): {}", message);
      }
    }
  }

  public static int getFakeServerInstanceCount() {
    return getHelixAdmin().getInstancesInClusterWithTag(getHelixClusterName(), "DefaultTenant_OFFLINE").size() +
        getHelixAdmin().getInstancesInClusterWithTag(getHelixClusterName(), UNTAGGED_SERVER_INSTANCE).size();
  }

  public static int getFakeServerInstanceCount(boolean isSingleTenant) {
    return isSingleTenant ?
        getHelixAdmin().getInstancesInClusterWithTag(getHelixClusterName(), "DefaultTenant_OFFLINE").size():
        getHelixAdmin().getInstancesInClusterWithTag(getHelixClusterName(), UNTAGGED_SERVER_INSTANCE).size();
  }

  public static void addFakeServerInstancesToAutoJoinHelixCluster(int numInstances, boolean isSingleTenant)
      throws Exception {
    addFakeServerInstancesToAutoJoinHelixCluster(numInstances, isSingleTenant, DEFAULT_ADMIN_API_PORT);
  }

  public static void addFakeServerInstancesToAutoJoinHelixCluster(int numInstances, boolean isSingleTenant,
      int baseAdminPort)
      throws Exception {
    for (int i = 0; i < numInstances; i++) {
      addFakeServerInstanceToAutoJoinHelixCluster(SERVER_INSTANCE_ID_PREFIX + i, isSingleTenant, baseAdminPort + i);
    }
  }

  public static void addFakeServerInstanceToAutoJoinHelixCluster(String instanceId, boolean isSingleTenant)
      throws Exception {
    addFakeServerInstanceToAutoJoinHelixCluster(instanceId, isSingleTenant, DEFAULT_ADMIN_API_PORT);
  }

  public static void addMaxFakeServerInstancesToAutoJoinHelixCluster(int maxCount, boolean isSingleTenant)
      throws Exception {
    addMaxFakeServerInstancesToAutoJoinHelixCluster(maxCount, isSingleTenant, DEFAULT_ADMIN_API_PORT);
  }

  public static void addMaxFakeServerInstancesToAutoJoinHelixCluster(int maxCount, boolean isSingleTenant, int baseAdminPort)
      throws Exception {

    // get current instance count
    int currentCount = getFakeServerInstanceCount();

    // Add more instances if current count is less than max instance count.
    if (currentCount < maxCount) {
      for (int i = currentCount; i < maxCount; i++) {
        addFakeServerInstanceToAutoJoinHelixCluster(SERVER_INSTANCE_ID_PREFIX + i, isSingleTenant, baseAdminPort + i);
      }
    }
  }

  protected static void addFakeServerInstanceToAutoJoinHelixCluster(String instanceId, boolean isSingleTenant, int adminPort)
      throws Exception {
    HelixManager helixManager = HelixManagerFactory
        .getZKHelixManager(getHelixClusterName(), instanceId, InstanceType.PARTICIPANT, ZkStarter.DEFAULT_ZK_STR);
    helixManager.getStateMachineEngine()
        .registerStateModelFactory(FakeSegmentOnlineOfflineStateModelFactory.STATE_MODEL_DEF,
            FakeSegmentOnlineOfflineStateModelFactory.FACTORY_INSTANCE);
    helixManager.connect();
    HelixAdmin helixAdmin = helixManager.getClusterManagmentTool();
    if (isSingleTenant) {
      helixAdmin.addInstanceTag(getHelixClusterName(), instanceId, TagNameUtils.getOfflineTagForTenant(null));
      helixAdmin.addInstanceTag(getHelixClusterName(), instanceId, TagNameUtils.getRealtimeTagForTenant(null));
    } else {
      helixAdmin.addInstanceTag(getHelixClusterName(), instanceId, UNTAGGED_SERVER_INSTANCE);
    }
    HelixConfigScope configScope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.PARTICIPANT, getHelixClusterName())
            .forParticipant(instanceId).build();
    helixAdmin.setConfig(configScope, Collections.singletonMap(ADMIN_PORT_KEY, Integer.toString(adminPort)));
    _fakeInstanceHelixManagers.add(helixManager);
  }

  public static class FakeSegmentOnlineOfflineStateModelFactory extends StateModelFactory<StateModel> {
    private static final String STATE_MODEL_DEF = "SegmentOnlineOfflineStateModel";
    private static final FakeSegmentOnlineOfflineStateModelFactory FACTORY_INSTANCE =
        new FakeSegmentOnlineOfflineStateModelFactory();
    private static final FakeSegmentOnlineOfflineStateModel STATE_MODEL_INSTANCE =
        new FakeSegmentOnlineOfflineStateModel();

    private FakeSegmentOnlineOfflineStateModelFactory() {
    }

    @Override
    public StateModel createNewStateModel(String resourceName, String partitionName) {
      return STATE_MODEL_INSTANCE;
    }

    @SuppressWarnings("unused")
    @StateModelInfo(states = "{'OFFLINE', 'ONLINE', 'CONSUMING', 'DROPPED'}", initialState = "OFFLINE")
    public static class FakeSegmentOnlineOfflineStateModel extends StateModel {
      private static final Logger LOGGER = LoggerFactory.getLogger(FakeSegmentOnlineOfflineStateModel.class);

      private FakeSegmentOnlineOfflineStateModel() {
      }

      @Transition(from = "OFFLINE", to = "ONLINE")
      public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
        LOGGER.debug("onBecomeOnlineFromOffline(): {}", message);
      }

      @Transition(from = "OFFLINE", to = "CONSUMING")
      public void onBecomeConsumingFromOffline(Message message, NotificationContext context) {
        LOGGER.debug("onBecomeConsumingFromOffline(): {}", message);
      }

      @Transition(from = "OFFLINE", to = "DROPPED")
      public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
        LOGGER.debug("onBecomeDroppedFromOffline(): {}", message);
      }

      @Transition(from = "ONLINE", to = "OFFLINE")
      public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
        LOGGER.debug("onBecomeOfflineFromOnline(): {}", message);
      }

      @Transition(from = "ONLINE", to = "DROPPED")
      public void onBecomeDroppedFromOnline(Message message, NotificationContext context) {
        LOGGER.debug("onBecomeDroppedFromOnline(): {}", message);
      }

      @Transition(from = "CONSUMING", to = "OFFLINE")
      public void onBecomeOfflineFromConsuming(Message message, NotificationContext context) {
        LOGGER.debug("onBecomeOfflineFromConsuming(): {}", message);
      }

      @Transition(from = "CONSUMING", to = "ONLINE")
      public void onBecomeOnlineFromConsuming(Message message, NotificationContext context) {
        LOGGER.debug("onBecomeOnlineFromConsuming(): {}", message);
      }

      @Transition(from = "CONSUMING", to = "DROPPED")
      public void onBecomeDroppedFromConsuming(Message message, NotificationContext context) {
        LOGGER.debug("onBecomeDroppedFromConsuming(): {}", message);
      }

      @Transition(from = "ERROR", to = "OFFLINE")
      public void onBecomeOfflineFromError(Message message, NotificationContext context) {
        LOGGER.debug("onBecomeOfflineFromError(): {}", message);
      }
    }
  }

  public static void stopFakeInstances() {
    for (HelixManager helixManager : _fakeInstanceHelixManagers) {
      helixManager.disconnect();
    }
    _fakeInstanceHelixManagers.clear();
  }

  public static void stopFakeInstance(String instanceId) {
    for (HelixManager helixManager : _fakeInstanceHelixManagers) {
      if (helixManager.getInstanceName().equalsIgnoreCase(instanceId)) {
        helixManager.disconnect();
        _fakeInstanceHelixManagers.remove(helixManager);
        return;
      }
    }
  }

  public static Schema createDummySchema(String tableName) {
    Schema schema = new Schema();
    schema.setSchemaName(tableName);
    schema.addField(new DimensionFieldSpec("dimA", FieldSpec.DataType.STRING, true, ""));
    schema.addField(new DimensionFieldSpec("dimB", FieldSpec.DataType.STRING, true, 0));
    schema.addField(new MetricFieldSpec("metricA", FieldSpec.DataType.INT, 0));
    schema.addField(new MetricFieldSpec("metricB", FieldSpec.DataType.DOUBLE, -1));
    schema.addField(new DateTimeFieldSpec("timeColumn", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:DAYS"));
    return schema;
  }

  public static void addDummySchema(String tableName)
      throws IOException {
    addSchema(createDummySchema(tableName));
  }

  /**
   * Add a schema to the controller.
   */
  protected static void addSchema(Schema schema)
      throws IOException {
    String url = _controllerRequestURLBuilder.forSchemaCreate();
    PostMethod postMethod = sendMultipartPostRequest(url, schema.toSingleLineJsonString());
    assertEquals(postMethod.getStatusCode(), 200);
  }

  protected static Schema getSchema(String schemaName) {
    Schema schema = _helixResourceManager.getSchema(schemaName);
    assertNotNull(schema);
    return schema;
  }

  protected static void addTableConfig(TableConfig tableConfig)
      throws IOException {
    sendPostRequest(_controllerRequestURLBuilder.forTableCreate(), tableConfig.toJsonString());
  }

  protected static void updateTableConfig(TableConfig tableConfig)
      throws IOException {
    sendPutRequest(_controllerRequestURLBuilder.forUpdateTableConfig(tableConfig.getTableName()),
        tableConfig.toJsonString());
  }

  protected static TableConfig getOfflineTableConfig(String tableName) {
    TableConfig offlineTableConfig = _helixResourceManager.getOfflineTableConfig(tableName);
    Assert.assertNotNull(offlineTableConfig);
    return offlineTableConfig;
  }

  protected static TableConfig getRealtimeTableConfig(String tableName) {
    TableConfig realtimeTableConfig = _helixResourceManager.getRealtimeTableConfig(tableName);
    Assert.assertNotNull(realtimeTableConfig);
    return realtimeTableConfig;
  }

  protected static void dropOfflineTable(String tableName)
      throws IOException {
    sendDeleteRequest(
        _controllerRequestURLBuilder.forTableDelete(TableNameBuilder.OFFLINE.tableNameWithType(tableName)));
  }

  protected static void dropRealtimeTable(String tableName)
      throws IOException {
    sendDeleteRequest(
        _controllerRequestURLBuilder.forTableDelete(TableNameBuilder.REALTIME.tableNameWithType(tableName)));
  }

  protected static void reloadOfflineTable(String tableName)
      throws IOException {
    sendPostRequest(_controllerRequestURLBuilder.forTableReload(tableName, TableType.OFFLINE.name()), null);
  }

  protected static void reloadRealtimeTable(String tableName)
      throws IOException {
    sendPostRequest(_controllerRequestURLBuilder.forTableReload(tableName, TableType.REALTIME.name()), null);
  }

  protected static String getBrokerTenantRequestPayload(String tenantName, int numBrokers) {
    return new Tenant(TenantRole.BROKER, tenantName, numBrokers, 0, 0).toJsonString();
  }

  public static void createBrokerTenant(String tenantName, int numBrokers)
      throws IOException {
    sendPostRequest(_controllerRequestURLBuilder.forTenantCreate(),
        getBrokerTenantRequestPayload(tenantName, numBrokers));
  }

  public static void updateBrokerTenant(String tenantName, int numBrokers)
      throws IOException {
    sendPutRequest(_controllerRequestURLBuilder.forTenantCreate(),
        getBrokerTenantRequestPayload(tenantName, numBrokers));
  }

  protected static String getServerTenantRequestPayload(String tenantName, int numOfflineServers, int numRealtimeServers) {
    return new Tenant(TenantRole.SERVER, tenantName, numOfflineServers + numRealtimeServers, numOfflineServers,
        numRealtimeServers).toJsonString();
  }

  public static void createServerTenant(String tenantName, int numOfflineServers, int numRealtimeServers)
      throws IOException {
    sendPostRequest(_controllerRequestURLBuilder.forTenantCreate(),
        getServerTenantRequestPayload(tenantName, numOfflineServers, numRealtimeServers));
  }

  public static void updateServerTenant(String tenantName, int numOfflineServers, int numRealtimeServers)
      throws IOException {
    sendPutRequest(_controllerRequestURLBuilder.forTenantCreate(),
        getServerTenantRequestPayload(tenantName, numOfflineServers, numRealtimeServers));
  }

  public static void enableResourceConfigForLeadControllerResource(boolean enable) {
    ConfigAccessor configAccessor = _helixManager.getConfigAccessor();
    ResourceConfig resourceConfig =
        configAccessor.getResourceConfig(getHelixClusterName(), LEAD_CONTROLLER_RESOURCE_NAME);
    if (Boolean.parseBoolean(resourceConfig.getSimpleConfig(LEAD_CONTROLLER_RESOURCE_ENABLED_KEY)) != enable) {
      resourceConfig.putSimpleConfig(LEAD_CONTROLLER_RESOURCE_ENABLED_KEY, Boolean.toString(enable));
      configAccessor.setResourceConfig(getHelixClusterName(), LEAD_CONTROLLER_RESOURCE_NAME, resourceConfig);
    }
  }

  public static String sendGetRequest(String urlString)
      throws IOException {
    return constructResponse(new URL(urlString).openStream());
  }

  public static String sendGetRequestRaw(String urlString)
      throws IOException {
    return IOUtils.toString(new URL(urlString).openStream());
  }

  public static String sendPostRequest(String urlString, String payload)
      throws IOException {
    return sendPostRequest(urlString, payload, Collections.EMPTY_MAP);
  }

  public static String sendPostRequest(String urlString, String payload, Map<String, String> headers)
      throws IOException {
    HttpURLConnection httpConnection = (HttpURLConnection) new URL(urlString).openConnection();
    httpConnection.setRequestMethod("POST");
    if (headers != null) {
      for (String key : headers.keySet()) {
        httpConnection.setRequestProperty(key, headers.get(key));
      }
    }

    if (payload != null && !payload.isEmpty()) {
      httpConnection.setDoOutput(true);
      try (BufferedWriter writer = new BufferedWriter(
          new OutputStreamWriter(httpConnection.getOutputStream(), StandardCharsets.UTF_8))) {
        writer.write(payload, 0, payload.length());
        writer.flush();
      }
    }

    return constructResponse(httpConnection.getInputStream());
  }

  public static String sendPutRequest(String urlString, String payload)
      throws IOException {
    HttpURLConnection httpConnection = (HttpURLConnection) new URL(urlString).openConnection();
    httpConnection.setDoOutput(true);
    httpConnection.setRequestMethod("PUT");

    try (BufferedWriter writer = new BufferedWriter(
        new OutputStreamWriter(httpConnection.getOutputStream(), StandardCharsets.UTF_8))) {
      writer.write(payload);
      writer.flush();
    }

    return constructResponse(httpConnection.getInputStream());
  }

  public static String sendPutRequest(String urlString)
      throws IOException {
    HttpURLConnection httpConnection = (HttpURLConnection) new URL(urlString).openConnection();
    httpConnection.setDoOutput(true);
    httpConnection.setRequestMethod("PUT");
    return constructResponse(httpConnection.getInputStream());
  }

  public static String sendDeleteRequest(String urlString)
      throws IOException {
    HttpURLConnection httpConnection = (HttpURLConnection) new URL(urlString).openConnection();
    httpConnection.setRequestMethod("DELETE");
    httpConnection.connect();

    return constructResponse(httpConnection.getInputStream());
  }

  private static String constructResponse(InputStream inputStream)
      throws IOException {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
      StringBuilder responseBuilder = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        responseBuilder.append(line);
      }
      return responseBuilder.toString();
    }
  }

  public static PostMethod sendMultipartPostRequest(String url, String body)
      throws IOException {
    HttpClient httpClient = new HttpClient();
    PostMethod postMethod = new PostMethod(url);
    // our handlers ignore key...so we can put anything here
    Part[] parts = {new StringPart("body", body)};
    postMethod.setRequestEntity(new MultipartRequestEntity(parts, postMethod.getParams()));
    httpClient.executeMethod(postMethod);
    return postMethod;
  }

  public static PutMethod sendMultipartPutRequest(String url, String body)
      throws IOException {
    HttpClient httpClient = new HttpClient();
    PutMethod putMethod = new PutMethod(url);
    // our handlers ignore key...so we can put anything here
    Part[] parts = {new StringPart("body", body)};
    putMethod.setRequestEntity(new MultipartRequestEntity(parts, putMethod.getParams()));
    httpClient.executeMethod(putMethod);
    return putMethod;
  }

  public static ControllerRequestURLBuilder getControllerRequestURLBuilder() {
    return _controllerRequestURLBuilder;
  }

  public static HelixAdmin getHelixAdmin() {
    return _helixAdmin;
  }

  public static PinotHelixResourceManager getHelixResourceManager() {
    return _helixResourceManager;
  }

  public static String getControllerBaseApiUrl() {
    return _controllerBaseApiUrl;
  }

  public static HelixManager getHelixManager() {
    return _helixManager;
  }

  public static ZkHelixPropertyStore<ZNRecord> getPropertyStore() {
    return _propertyStore;
  }

  public static int getControllerPort() {
    return _controllerPort;
  }

  public static ControllerStarter getControllerStarter() {
    return _controllerStarter;
  }
}
