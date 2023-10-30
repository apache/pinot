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
package org.apache.pinot.controller.helix;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.Message;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.http.client.entity.EntityBuilder;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.controller.BaseControllerStarter;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.ControllerStarter;
import org.apache.pinot.controller.api.access.AllowAllAccessFactory;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.spi.utils.builder.ControllerRequestURLBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE;
import static org.apache.pinot.spi.utils.CommonConstants.Server.DEFAULT_ADMIN_API_PORT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


public class ControllerTest {
  public static final String LOCAL_HOST = "localhost";
  public static final int DEFAULT_CONTROLLER_PORT = 18998;
  public static final String DEFAULT_DATA_DIR = new File(FileUtils.getTempDirectoryPath(),
      "test-controller-data-dir" + System.currentTimeMillis()).getAbsolutePath();
  public static final String DEFAULT_LOCAL_TEMP_DIR = new File(FileUtils.getTempDirectoryPath(),
      "test-controller-local-temp-dir" + System.currentTimeMillis()).getAbsolutePath();
  public static final String BROKER_INSTANCE_ID_PREFIX = "Broker_localhost_";
  public static final String SERVER_INSTANCE_ID_PREFIX = "Server_localhost_";
  public static final String MINION_INSTANCE_ID_PREFIX = "Minion_localhost_";

  // Default ControllerTest instance settings
  public static final int DEFAULT_MIN_NUM_REPLICAS = 2;
  public static final int DEFAULT_NUM_BROKER_INSTANCES = 3;
  // NOTE: To add HLC realtime table, number of Server instances must be multiple of replicas
  public static final int DEFAULT_NUM_SERVER_INSTANCES = 4;

  public static final long TIMEOUT_MS = 10_000L;

  /**
   * default static instance used to access all wrapped static instances.
   */
  public static final ControllerTest DEFAULT_INSTANCE = new ControllerTest();

  protected final String _clusterName = getClass().getSimpleName();

  protected static HttpClient _httpClient = null;

  private int _controllerPort;
  private String _controllerBaseApiUrl;
  protected ControllerConf _controllerConfig;
  protected ControllerRequestURLBuilder _controllerRequestURLBuilder;

  protected ControllerRequestClient _controllerRequestClient = null;

  protected final List<HelixManager> _fakeInstanceHelixManagers = new ArrayList<>();
  protected String _controllerDataDir;

  protected BaseControllerStarter _controllerStarter;
  protected PinotHelixResourceManager _helixResourceManager;
  protected HelixManager _helixManager;
  protected HelixAdmin _helixAdmin;
  protected HelixDataAccessor _helixDataAccessor;
  protected ZkHelixPropertyStore<ZNRecord> _propertyStore;

  private ZkStarter.ZookeeperInstance _zookeeperInstance;

  /**
   * Acquire the {@link ControllerTest} default instance that can be shared across different test cases.
   *
   * @return the default instance.
   */
  public static ControllerTest getInstance() {
    return DEFAULT_INSTANCE;
  }

  public String getHelixClusterName() {
    return _clusterName;
  }

  /**
   * HttpClient is lazy evaluated, static object, only instantiate when first use.
   *
   * <p>This is because {@code ControllerTest} has HTTP utils that depends on the TLSUtils to install the security
   * context first before the HttpClient can be initialized. However, because we have static usages of the HTTPClient,
   * it is not possible to create normal member variable, thus the workaround.
   */
  public static HttpClient getHttpClient() {
    if (_httpClient == null) {
      _httpClient = HttpClient.getInstance();
    }
    return _httpClient;
  }

  /**
   * ControllerRequestClient is lazy evaluated, static object, only instantiate when first use.
   *
   * <p>This is because {@code ControllerTest} has HTTP utils that depends on the TLSUtils to install the security
   * context first before the ControllerRequestClient can be initialized. However, because we have static usages of the
   * ControllerRequestClient, it is not possible to create normal member variable, thus the workaround.
   */
  public ControllerRequestClient getControllerRequestClient() {
    if (_controllerRequestClient == null) {
      _controllerRequestClient = new ControllerRequestClient(_controllerRequestURLBuilder, getHttpClient());
    }
    return _controllerRequestClient;
  }

  public void startDefaultTestZk() {
    if (_zookeeperInstance == null) {
      _zookeeperInstance = ZkStarter.startLocalZkServer(ZkStarter.DEFAULT_ZK_TEST_PORT);
    }
  }

  public void startZk() {
    if (_zookeeperInstance == null) {
      _zookeeperInstance = ZkStarter.startLocalZkServer(NetUtils.findOpenPort(20000 + RandomUtils.nextInt(10000)));
    }
  }

  public void startZk(int port) {
    if (_zookeeperInstance == null) {
      _zookeeperInstance = ZkStarter.startLocalZkServer(port);
    }
  }

  public void stopZk() {
    try {
      if (_zookeeperInstance != null) {
        ZkStarter.stopLocalZkServer(_zookeeperInstance);
        _zookeeperInstance = null;
      }
    } catch (Exception e) {
      // Swallow exceptions
    }
  }

  public String getZkUrl() {
    return _zookeeperInstance.getZkUrl();
  }

  public Map<String, Object> getDefaultControllerConfiguration() {
    Map<String, Object> properties = new HashMap<>();

    properties.put(ControllerConf.CONTROLLER_HOST, LOCAL_HOST);
    properties.put(ControllerConf.CONTROLLER_PORT,
        NetUtils.findOpenPort(DEFAULT_CONTROLLER_PORT + RandomUtils.nextInt(10000)));
    properties.put(ControllerConf.DATA_DIR, DEFAULT_DATA_DIR);
    properties.put(ControllerConf.LOCAL_TEMP_DIR, DEFAULT_LOCAL_TEMP_DIR);
    properties.put(ControllerConf.ZK_STR, getZkUrl());
    properties.put(ControllerConf.HELIX_CLUSTER_NAME, getHelixClusterName());
    // Enable groovy on the controller
    properties.put(ControllerConf.DISABLE_GROOVY, false);
    overrideControllerConf(properties);
    return properties;
  }

  protected void overrideControllerConf(Map<String, Object> properties) {
    // do nothing, to be overridden by tests if they need something specific
  }

  public void startController()
      throws Exception {
    startController(getDefaultControllerConfiguration());
  }

  public void startController(Map<String, Object> properties)
      throws Exception {
    Preconditions.checkState(_controllerStarter == null);

    _controllerConfig = new ControllerConf(properties);

    String controllerScheme = "http";
    if (StringUtils.isNotBlank(_controllerConfig.getControllerVipProtocol())) {
      controllerScheme = _controllerConfig.getControllerVipProtocol();
    }

    _controllerPort = DEFAULT_CONTROLLER_PORT;
    if (StringUtils.isNotBlank(_controllerConfig.getControllerPort())) {
      _controllerPort = Integer.parseInt(_controllerConfig.getControllerPort());
    } else if (StringUtils.isNotBlank(_controllerConfig.getControllerVipPort())) {
      _controllerPort = Integer.parseInt(_controllerConfig.getControllerVipPort());
    }

    _controllerBaseApiUrl = controllerScheme + "://localhost:" + _controllerPort;
    _controllerRequestURLBuilder = ControllerRequestURLBuilder.baseUrl(_controllerBaseApiUrl);
    _controllerDataDir = _controllerConfig.getDataDir();

    _controllerStarter = getControllerStarter();
    _controllerStarter.init(new PinotConfiguration(properties));
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
        //       https://github.com/apache/helix/issues/331 and https://github.com/apache/helix/issues/2309.
        //       Remove this after Helix fixing the issue.
        configAccessor.set(scope, ClusterConfig.ClusterConfigProperty.REBALANCE_TIMER_PERIOD.name(), "10000");
        break;
      case HELIX_ONLY:
        _helixAdmin = _helixManager.getClusterManagmentTool();
        _propertyStore = _helixManager.getHelixPropertyStore();
        break;
      default:
        break;
    }
    // Enable case-insensitive for test cases.
    configAccessor.set(scope, Helix.ENABLE_CASE_INSENSITIVE_KEY, Boolean.toString(true));
    // Set hyperloglog log2m value to 12.
    configAccessor.set(scope, Helix.DEFAULT_HYPERLOGLOG_LOG2M_KEY, Integer.toString(12));
  }

  public void stopController() {
    Preconditions.checkState(_controllerStarter != null);
    _controllerStarter.stop();
    _controllerStarter = null;
    _controllerRequestClient = null;
    FileUtils.deleteQuietly(new File(_controllerDataDir));
  }

  public int getFakeBrokerInstanceCount() {
    return _helixAdmin.getInstancesInClusterWithTag(getHelixClusterName(), "DefaultTenant_BROKER").size()
        + _helixAdmin.getInstancesInClusterWithTag(getHelixClusterName(), UNTAGGED_BROKER_INSTANCE).size();
  }

  public void addFakeBrokerInstancesToAutoJoinHelixCluster(int numInstances, boolean isSingleTenant)
      throws Exception {
    for (int i = 0; i < numInstances; i++) {
      addFakeBrokerInstanceToAutoJoinHelixCluster(BROKER_INSTANCE_ID_PREFIX + i, isSingleTenant);
    }
  }

  /**
   * Adds fake broker instances until total number of broker instances equals maxCount.
   */
  public void addFakeBrokerInstanceToAutoJoinHelixCluster(String instanceId, boolean isSingleTenant)
      throws Exception {
    HelixManager helixManager =
        HelixManagerFactory.getZKHelixManager(getHelixClusterName(), instanceId, InstanceType.PARTICIPANT, getZkUrl());
    helixManager.getStateMachineEngine()
        .registerStateModelFactory(FakeBrokerResourceOnlineOfflineStateModelFactory.STATE_MODEL_DEF,
            new FakeBrokerResourceOnlineOfflineStateModelFactory());
    helixManager.connect();
    HelixAdmin helixAdmin = helixManager.getClusterManagmentTool();
    if (isSingleTenant) {
      helixAdmin.addInstanceTag(getHelixClusterName(), instanceId, TagNameUtils.getBrokerTagForTenant(null));
    } else {
      helixAdmin.addInstanceTag(getHelixClusterName(), instanceId, Helix.UNTAGGED_BROKER_INSTANCE);
    }
    _fakeInstanceHelixManagers.add(helixManager);
  }

  public void addMoreFakeBrokerInstancesToAutoJoinHelixCluster(int maxCount, boolean isSingleTenant)
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

  public static class FakeBrokerResourceOnlineOfflineStateModelFactory extends StateModelFactory<StateModel> {
    private static final String STATE_MODEL_DEF = "BrokerResourceOnlineOfflineStateModel";

    private FakeBrokerResourceOnlineOfflineStateModelFactory() {
    }

    @Override
    public StateModel createNewStateModel(String resourceName, String partitionName) {
      return new FakeBrokerResourceOnlineOfflineStateModel();
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

  public void addFakeServerInstancesToAutoJoinHelixCluster(int numInstances, boolean isSingleTenant)
      throws Exception {
    addFakeServerInstancesToAutoJoinHelixCluster(numInstances, isSingleTenant, Server.DEFAULT_ADMIN_API_PORT);
  }

  public void addFakeServerInstancesToAutoJoinHelixCluster(int numInstances, boolean isSingleTenant, int baseAdminPort)
      throws Exception {
    for (int i = 0; i < numInstances; i++) {
      addFakeServerInstanceToAutoJoinHelixCluster(SERVER_INSTANCE_ID_PREFIX + i, isSingleTenant,
          NetUtils.findOpenPort(baseAdminPort + i + RandomUtils.nextInt(10000)));
    }
  }

  public int getFakeServerInstanceCount() {
    return _helixAdmin.getInstancesInClusterWithTag(getHelixClusterName(), "DefaultTenant_OFFLINE").size()
        + _helixAdmin.getInstancesInClusterWithTag(getHelixClusterName(), UNTAGGED_SERVER_INSTANCE).size();
  }

  public void addFakeServerInstanceToAutoJoinHelixCluster(String instanceId, boolean isSingleTenant)
      throws Exception {
    addFakeServerInstanceToAutoJoinHelixCluster(instanceId, isSingleTenant,
        NetUtils.findOpenPort(Server.DEFAULT_ADMIN_API_PORT + RandomUtils.nextInt(10000)));
  }

  public void addFakeServerInstanceToAutoJoinHelixCluster(String instanceId, boolean isSingleTenant, int adminPort)
      throws Exception {
    HelixManager helixManager =
        HelixManagerFactory.getZKHelixManager(getHelixClusterName(), instanceId, InstanceType.PARTICIPANT, getZkUrl());
    helixManager.getStateMachineEngine()
        .registerStateModelFactory(FakeSegmentOnlineOfflineStateModelFactory.STATE_MODEL_DEF,
            new FakeSegmentOnlineOfflineStateModelFactory());
    helixManager.connect();
    HelixAdmin helixAdmin = helixManager.getClusterManagmentTool();
    if (isSingleTenant) {
      helixAdmin.addInstanceTag(getHelixClusterName(), instanceId, TagNameUtils.getOfflineTagForTenant(null));
      helixAdmin.addInstanceTag(getHelixClusterName(), instanceId, TagNameUtils.getRealtimeTagForTenant(null));
    } else {
      helixAdmin.addInstanceTag(getHelixClusterName(), instanceId, Helix.UNTAGGED_SERVER_INSTANCE);
    }
    HelixConfigScope configScope = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.PARTICIPANT,
        getHelixClusterName()).forParticipant(instanceId).build();
    helixAdmin.setConfig(configScope,
        Collections.singletonMap(Helix.Instance.ADMIN_PORT_KEY, Integer.toString(adminPort)));
    _fakeInstanceHelixManagers.add(helixManager);
  }

  /**
   * Add fake server instances until total number of server instances reaches maxCount
   */
  public void addMoreFakeServerInstancesToAutoJoinHelixCluster(int maxCount, boolean isSingleTenant)
      throws Exception {
    addMoreFakeServerInstancesToAutoJoinHelixCluster(maxCount, isSingleTenant,
        NetUtils.findOpenPort(DEFAULT_ADMIN_API_PORT + RandomUtils.nextInt(10000)));
  }

  /**
   * Add fake server instances until total number of server instances reaches maxCount
   */
  public void addMoreFakeServerInstancesToAutoJoinHelixCluster(int maxCount, boolean isSingleTenant, int baseAdminPort)
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

  public static class FakeSegmentOnlineOfflineStateModelFactory extends StateModelFactory<StateModel> {
    private static final String STATE_MODEL_DEF = "SegmentOnlineOfflineStateModel";

    private FakeSegmentOnlineOfflineStateModelFactory() {
    }

    @Override
    public StateModel createNewStateModel(String resourceName, String partitionName) {
      return new FakeSegmentOnlineOfflineStateModel();
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

  public void addFakeMinionInstancesToAutoJoinHelixCluster(int numInstances)
      throws Exception {
    for (int i = 0; i < numInstances; i++) {
      addFakeMinionInstanceToAutoJoinHelixCluster(MINION_INSTANCE_ID_PREFIX + i);
    }
  }

  public void addFakeMinionInstanceToAutoJoinHelixCluster(String instanceId)
      throws Exception {
    HelixManager helixManager =
        HelixManagerFactory.getZKHelixManager(getHelixClusterName(), instanceId, InstanceType.PARTICIPANT, getZkUrl());
    helixManager.getStateMachineEngine()
        .registerStateModelFactory(FakeMinionResourceOnlineOfflineStateModelFactory.STATE_MODEL_DEF,
            new FakeMinionResourceOnlineOfflineStateModelFactory());
    helixManager.connect();
    HelixAdmin helixAdmin = helixManager.getClusterManagmentTool();
    helixAdmin.addInstanceTag(getHelixClusterName(), instanceId, Helix.UNTAGGED_MINION_INSTANCE);
    _fakeInstanceHelixManagers.add(helixManager);
  }

  public static class FakeMinionResourceOnlineOfflineStateModelFactory extends StateModelFactory<StateModel> {
    private static final String STATE_MODEL_DEF = "MinionResourceOnlineOfflineStateModel";

    private FakeMinionResourceOnlineOfflineStateModelFactory() {
    }

    @Override
    public StateModel createNewStateModel(String resourceName, String partitionName) {
      return new FakeMinionResourceOnlineOfflineStateModel();
    }

    @SuppressWarnings("unused")
    @StateModelInfo(states = "{'OFFLINE', 'ONLINE', 'DROPPED'}", initialState = "OFFLINE")
    public static class FakeMinionResourceOnlineOfflineStateModel extends StateModel {
      private static final Logger LOGGER = LoggerFactory.getLogger(FakeMinionResourceOnlineOfflineStateModel.class);

      private FakeMinionResourceOnlineOfflineStateModel() {
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

  public void stopFakeInstances() {
    for (HelixManager helixManager : _fakeInstanceHelixManagers) {
      helixManager.disconnect();
    }
    _fakeInstanceHelixManagers.clear();
  }

  public void stopFakeInstance(String instanceId) {
    for (HelixManager helixManager : _fakeInstanceHelixManagers) {
      if (helixManager.getInstanceName().equalsIgnoreCase(instanceId)) {
        helixManager.disconnect();
        _fakeInstanceHelixManagers.remove(helixManager);
        return;
      }
    }
  }

  public void stopAndDropFakeInstance(String instanceId) {
    stopFakeInstance(instanceId);
    _helixResourceManager.dropInstance(instanceId);
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

  public static Schema createDummySchemaWithPrimaryKey(String tableName) {
    Schema schema = createDummySchema(tableName);
    schema.setPrimaryKeyColumns(Collections.singletonList("dimA"));
    return schema;
  }

  public void addDummySchema(String tableName)
      throws IOException {
    addSchema(createDummySchema(tableName));
  }

  /**
   * Add a schema to the controller.
   */
  public void addSchema(Schema schema)
      throws IOException {
    getControllerRequestClient().addSchema(schema);
  }

  public void updateSchema(Schema schema)
      throws IOException {
    getControllerRequestClient().updateSchema(schema);
  }

  public Schema getSchema(String schemaName) {
    Schema schema = _helixResourceManager.getSchema(schemaName);
    assertNotNull(schema);
    return schema;
  }

  public void deleteSchema(String schemaName)
      throws IOException {
    getControllerRequestClient().deleteSchema(schemaName);
  }

  public void addTableConfig(TableConfig tableConfig)
      throws IOException {
    getControllerRequestClient().addTableConfig(tableConfig);
  }

  public void updateTableConfig(TableConfig tableConfig)
      throws IOException {
    getControllerRequestClient().updateTableConfig(tableConfig);
  }

  public TableConfig getOfflineTableConfig(String tableName) {
    TableConfig offlineTableConfig = _helixResourceManager.getOfflineTableConfig(tableName);
    assertNotNull(offlineTableConfig);
    return offlineTableConfig;
  }

  public TableConfig getRealtimeTableConfig(String tableName) {
    TableConfig realtimeTableConfig = _helixResourceManager.getRealtimeTableConfig(tableName);
    assertNotNull(realtimeTableConfig);
    return realtimeTableConfig;
  }

  public void dropOfflineTable(String tableName)
      throws IOException {
    getControllerRequestClient().deleteTable(TableNameBuilder.OFFLINE.tableNameWithType(tableName));
  }

  public void dropRealtimeTable(String tableName)
      throws IOException {
    getControllerRequestClient().deleteTable(TableNameBuilder.REALTIME.tableNameWithType(tableName));
  }

  public void waitForEVToAppear(String tableNameWithType) {
    TestUtils.waitForCondition(aVoid -> _helixResourceManager.getTableExternalView(tableNameWithType) != null, 60_000L,
        "Failed to create the external view for table: " + tableNameWithType);
  }

  public void waitForEVToDisappear(String tableNameWithType) {
    TestUtils.waitForCondition(aVoid -> _helixResourceManager.getTableExternalView(tableNameWithType) == null, 60_000L,
        "Failed to clean up the external view for table: " + tableNameWithType);
  }

  public List<String> listSegments(String tableName)
      throws IOException {
    return listSegments(tableName, null, false);
  }

  public List<String> listSegments(String tableName, @Nullable String tableType, boolean excludeReplacedSegments)
      throws IOException {
    return getControllerRequestClient().listSegments(tableName, tableType, excludeReplacedSegments);
  }

  public void dropSegment(String tableName, String segmentName)
      throws IOException {
    getControllerRequestClient().deleteSegment(tableName, segmentName);
  }

  public void dropAllSegments(String tableName, TableType tableType)
      throws IOException {
    getControllerRequestClient().deleteSegments(tableName, tableType);
  }

  public long getTableSize(String tableName)
      throws IOException {
    return getControllerRequestClient().getTableSize(tableName);
  }

  public String reloadOfflineTable(String tableName)
      throws IOException {
    return reloadOfflineTable(tableName, false);
  }

  public String reloadOfflineTable(String tableName, boolean forceDownload)
      throws IOException {
    return getControllerRequestClient().reloadTable(tableName, TableType.OFFLINE, forceDownload);
  }

  public void reloadOfflineSegment(String tableName, String segmentName, boolean forceDownload)
      throws IOException {
    getControllerRequestClient().reloadSegment(tableName, segmentName, forceDownload);
  }

  public String reloadRealtimeTable(String tableName)
      throws IOException {
    return getControllerRequestClient().reloadTable(tableName, TableType.REALTIME, false);
  }

  public void createBrokerTenant(String tenantName, int numBrokers)
      throws IOException {
    getControllerRequestClient().createBrokerTenant(tenantName, numBrokers);
  }

  public void updateBrokerTenant(String tenantName, int numBrokers)
      throws IOException {
    getControllerRequestClient().updateBrokerTenant(tenantName, numBrokers);
  }

  public void deleteBrokerTenant(String tenantName)
      throws IOException {
    getControllerRequestClient().deleteBrokerTenant(tenantName);
  }

  public void createServerTenant(String tenantName, int numOfflineServers, int numRealtimeServers)
      throws IOException {
    getControllerRequestClient().createServerTenant(tenantName, numOfflineServers, numRealtimeServers);
  }

  public void updateServerTenant(String tenantName, int numOfflineServers, int numRealtimeServers)
      throws IOException {
    getControllerRequestClient().updateServerTenant(tenantName, numOfflineServers, numRealtimeServers);
  }

  public void enableResourceConfigForLeadControllerResource(boolean enable) {
    ConfigAccessor configAccessor = _helixManager.getConfigAccessor();
    ResourceConfig resourceConfig =
        configAccessor.getResourceConfig(getHelixClusterName(), Helix.LEAD_CONTROLLER_RESOURCE_NAME);
    if (Boolean.parseBoolean(resourceConfig.getSimpleConfig(Helix.LEAD_CONTROLLER_RESOURCE_ENABLED_KEY)) != enable) {
      resourceConfig.putSimpleConfig(Helix.LEAD_CONTROLLER_RESOURCE_ENABLED_KEY, Boolean.toString(enable));
      configAccessor.setResourceConfig(getHelixClusterName(), Helix.LEAD_CONTROLLER_RESOURCE_NAME, resourceConfig);
    }
  }

  public static String sendGetRequest(String urlString)
      throws IOException {
    return sendGetRequest(urlString, null);
  }

  public static String sendGetRequest(String urlString, Map<String, String> headers)
      throws IOException {
    try {
      SimpleHttpResponse resp =
          HttpClient.wrapAndThrowHttpException(getHttpClient().sendGetRequest(new URL(urlString).toURI(), headers));
      return constructResponse(resp);
    } catch (URISyntaxException | HttpErrorStatusException e) {
      throw new IOException(e);
    }
  }

  public static String sendGetRequestRaw(String urlString)
      throws IOException {
    return IOUtils.toString(new URL(urlString).openStream());
  }

  public static String sendPostRequest(String urlString)
      throws IOException {
    return sendPostRequest(urlString, null);
  }

  public static String sendPostRequest(String urlString, String payload)
      throws IOException {
    return sendPostRequest(urlString, payload, Collections.emptyMap());
  }

  public static String sendPostRequest(String urlString, String payload, Map<String, String> headers)
      throws IOException {
    try {
      SimpleHttpResponse resp = HttpClient.wrapAndThrowHttpException(
          getHttpClient().sendJsonPostRequest(new URL(urlString).toURI(), payload, headers));
      return constructResponse(resp);
    } catch (URISyntaxException | HttpErrorStatusException e) {
      throw new IOException(e);
    }
  }

  public static String sendPostRequestRaw(String urlString, String payload, Map<String, String> headers)
      throws IOException {
    try {
      EntityBuilder builder = EntityBuilder.create();
      builder.setText(payload);
      SimpleHttpResponse resp = HttpClient.wrapAndThrowHttpException(
          getHttpClient().sendPostRequest(new URL(urlString).toURI(), builder.build(), headers));
      return constructResponse(resp);
    } catch (URISyntaxException | HttpErrorStatusException e) {
      throw new IOException(e);
    }
  }

  public static String sendPutRequest(String urlString)
      throws IOException {
    return sendPutRequest(urlString, null);
  }

  public static String sendPutRequest(String urlString, String payload)
      throws IOException {
    return sendPutRequest(urlString, payload, Collections.emptyMap());
  }

  public static String sendPutRequest(String urlString, String payload, Map<String, String> headers)
      throws IOException {
    try {
      SimpleHttpResponse resp = HttpClient.wrapAndThrowHttpException(
          getHttpClient().sendJsonPutRequest(new URL(urlString).toURI(), payload, headers));
      return constructResponse(resp);
    } catch (URISyntaxException | HttpErrorStatusException e) {
      throw new IOException(e);
    }
  }

  public static String sendDeleteRequest(String urlString)
      throws IOException {
    return sendDeleteRequest(urlString, Collections.emptyMap());
  }

  public static String sendDeleteRequest(String urlString, Map<String, String> headers)
      throws IOException {
    try {
      SimpleHttpResponse resp =
          HttpClient.wrapAndThrowHttpException(getHttpClient().sendDeleteRequest(new URL(urlString).toURI(), headers));
      return constructResponse(resp);
    } catch (URISyntaxException | HttpErrorStatusException e) {
      throw new IOException(e);
    }
  }

  private static String constructResponse(SimpleHttpResponse resp) {
    return resp.getResponse();
  }

  public static SimpleHttpResponse sendMultipartPostRequest(String url, String body)
      throws IOException {
    return sendMultipartPostRequest(url, body, Collections.emptyMap());
  }

  public static SimpleHttpResponse sendMultipartPostRequest(String url, String body, Map<String, String> headers)
      throws IOException {
    return getHttpClient().sendMultipartPostRequest(url, body, headers);
  }

  public static SimpleHttpResponse sendMultipartPutRequest(String url, String body)
      throws IOException {
    return sendMultipartPutRequest(url, body, null);
  }

  public static SimpleHttpResponse sendMultipartPutRequest(String url, String body, Map<String, String> headers)
      throws IOException {
    return getHttpClient().sendMultipartPutRequest(url, body, headers);
  }

  /**
   * @return Number of instances used by all the broker tenants
   */
  public int getTaggedBrokerCount() {
    int count = 0;
    Set<String> brokerTenants = _helixResourceManager.getAllBrokerTenantNames();
    for (String tenant : brokerTenants) {
      count += _helixResourceManager.getAllInstancesForBrokerTenant(tenant).size();
    }

    return count;
  }

  /**
   * @return Number of instances used by all the server tenants
   */
  public int getTaggedServerCount() {
    int count = 0;
    Set<String> serverTenants = _helixResourceManager.getAllServerTenantNames();
    for (String tenant : serverTenants) {
      count += _helixResourceManager.getAllInstancesForServerTenant(tenant).size();
    }

    return count;
  }

  public ControllerRequestURLBuilder getControllerRequestURLBuilder() {
    return _controllerRequestURLBuilder;
  }

  public HelixAdmin getHelixAdmin() {
    return _helixAdmin;
  }

  public PinotHelixResourceManager getHelixResourceManager() {
    return _helixResourceManager;
  }

  public String getControllerBaseApiUrl() {
    return _controllerBaseApiUrl;
  }

  public HelixManager getHelixManager() {
    return _helixManager;
  }

  public ZkHelixPropertyStore<ZNRecord> getPropertyStore() {
    return _propertyStore;
  }

  public int getControllerPort() {
    return _controllerPort;
  }

  public BaseControllerStarter getControllerStarter() {
    return _controllerStarter == null ? new ControllerStarter() : _controllerStarter;
  }

  public ControllerConf getControllerConfig() {
    return _controllerConfig;
  }

  /**
   * Do not override this method as the configuration is shared across all default TestNG group.
   */
  public final Map<String, Object> getSharedControllerConfiguration() {
    Map<String, Object> properties = getDefaultControllerConfiguration();

    // TODO: move these test specific configs into respective test classes.
    properties.put(ControllerConf.ACCESS_CONTROL_FACTORY_CLASS, AllowAllAccessFactory.class.getName());

    // Used in PinotTableRestletResourceTest
    properties.put(ControllerConf.TABLE_MIN_REPLICAS, DEFAULT_MIN_NUM_REPLICAS);

    // Used in PinotControllerAppConfigsTest to test obfuscation
    properties.put("controller.segment.fetcher.auth.token", "*personal*");
    properties.put("controller.admin.access.control.principals.user.password", "*personal*");

    return properties;
  }

  /**
   * Initialize shared state for the TestNG default test group.
   */
  public void startSharedTestSetup()
      throws Exception {
    startZk();
    startController(getSharedControllerConfiguration());

    addMoreFakeBrokerInstancesToAutoJoinHelixCluster(DEFAULT_NUM_BROKER_INSTANCES, true);
    addMoreFakeServerInstancesToAutoJoinHelixCluster(DEFAULT_NUM_SERVER_INSTANCES, true);
  }

  /**
   * Cleanup shared state used in the TestNG default test group.
   */
  public void stopSharedTestSetup() {
    cleanup();

    stopFakeInstances();
    stopController();
    stopZk();
  }

  /**
   * Checks if the number of online instances for a given resource matches the expected num of instances or not.
   */
  public void checkNumOnlineInstancesFromExternalView(String resourceName, int expectedNumOnlineInstances)
      throws InterruptedException {
    long endTime = System.currentTimeMillis() + TIMEOUT_MS;
    while (System.currentTimeMillis() < endTime) {
      ExternalView resourceExternalView = DEFAULT_INSTANCE.getHelixAdmin()
          .getResourceExternalView(DEFAULT_INSTANCE.getHelixClusterName(), resourceName);
      Set<String> instanceSet = HelixHelper.getOnlineInstanceFromExternalView(resourceExternalView);
      if (instanceSet.size() == expectedNumOnlineInstances) {
        return;
      }
      Thread.sleep(100L);
    }
    fail("Failed to reach " + expectedNumOnlineInstances + " online instances for resource: " + resourceName);
  }

  /**
   * Make sure shared state is setup and valid before each test case class is run.
   */
  public void setupSharedStateAndValidate()
      throws Exception {
    if (_zookeeperInstance == null || _helixResourceManager == null) {
      // this is expected to happen only when running a single test case outside of testNG group, i.e when test
      // cases are run one at a time within IntelliJ or through maven command line. When running under a testNG
      // group, state will have already been setup by @BeforeGroups method in ControllerTestSetup.
      startSharedTestSetup();
    }

    // In a single tenant cluster, only the default tenant should exist
    assertEquals(_helixResourceManager.getAllBrokerTenantNames(),
        Collections.singleton(TagNameUtils.DEFAULT_TENANT_NAME));
    assertEquals(_helixResourceManager.getAllInstancesForBrokerTenant(TagNameUtils.DEFAULT_TENANT_NAME).size(),
        DEFAULT_NUM_BROKER_INSTANCES);
    assertEquals(_helixResourceManager.getAllServerTenantNames(),
        Collections.singleton(TagNameUtils.DEFAULT_TENANT_NAME));
    assertEquals(_helixResourceManager.getAllInstancesForServerTenant(TagNameUtils.DEFAULT_TENANT_NAME).size(),
        DEFAULT_NUM_SERVER_INSTANCES);

    // No pre-existing tables
    assertTrue(CollectionUtils.isEmpty(getHelixResourceManager().getAllTables()));
    // No pre-existing schemas
    assertTrue(CollectionUtils.isEmpty(getHelixResourceManager().getSchemaNames()));
  }

  /**
   * Clean shared state after a test case class has completed running. Additional cleanup may be needed depending upon
   * test functionality.
   */
  public void cleanup() {
    // Delete all tables
    List<String> tables = _helixResourceManager.getAllTables();
    for (String tableNameWithType : tables) {
      if (TableNameBuilder.isOfflineTableResource(tableNameWithType)) {
        _helixResourceManager.deleteOfflineTable(tableNameWithType);
      } else {
        _helixResourceManager.deleteRealtimeTable(tableNameWithType);
      }
    }

    // Wait for all external views to disappear
    Set<String> tablesWithEV = new HashSet<>(tables);
    TestUtils.waitForCondition(aVoid -> {
      tablesWithEV.removeIf(t -> _helixResourceManager.getTableExternalView(t) == null);
      return tablesWithEV.isEmpty();
    }, 60_000L, "Failed to clean up all the external views");

    // Delete all schemas.
    List<String> schemaNames = _helixResourceManager.getSchemaNames();
    if (CollectionUtils.isNotEmpty(schemaNames)) {
      for (String schemaName : schemaNames) {
        getHelixResourceManager().deleteSchema(schemaName);
      }
    }
  }
}
