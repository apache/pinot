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
import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hc.client5.http.entity.EntityBuilder;
import org.apache.helix.AccessOption;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.HelixPropertyFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.CloudConfig;
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
import org.apache.pinot.client.PinotClientException;
import org.apache.pinot.client.admin.PinotAdminClient;
import org.apache.pinot.client.admin.PinotAdminException;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.restlet.resources.PauseStatusDetails;
import org.apache.pinot.common.restlet.resources.TableSegmentsReloadCheckResponse;
import org.apache.pinot.common.restlet.resources.TableView;
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
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.controller.helix.core.minion.TaskSchedulingContext;
import org.apache.pinot.controller.helix.core.rebalance.TableRebalanceManager;
import org.apache.pinot.controller.util.TableSizeReader;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.apache.pinot.spi.config.table.QueryConfig;
import org.apache.pinot.spi.config.table.QuotaConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.PhysicalTableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeBoundaryConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.ControllerRequestURLBuilder;
import org.apache.pinot.spi.utils.builder.LogicalTableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.DataProvider;

import static org.apache.pinot.spi.utils.CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE;
import static org.testng.Assert.*;


public class ControllerTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(ControllerTest.class);

  public static final String LOCAL_HOST = "localhost";
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
  public static final int DEFAULT_NUM_MINION_INSTANCES = 2;

  public static final long TIMEOUT_MS = 10_000L;

  /**
   * default static instance used to access all wrapped static instances.
   */
  public static final ControllerTest DEFAULT_INSTANCE = new ControllerTest();

  protected static HttpClient _httpClient;

  protected final String _clusterName = getClass().getSimpleName();
  protected final List<HelixManager> _fakeInstanceHelixManagers = new ArrayList<>();

  private ZkStarter.ZookeeperInstance _zookeeperInstance;

  // The following fields need to be reset when stopping the controller.
  protected BaseControllerStarter _controllerStarter;
  protected int _controllerPort;
  private PinotAdminClient _pinotAdminClient;

  // The following fields are always set when controller is started. No need to reset them when stopping the controller.
  protected ControllerConf _controllerConfig;
  protected String _controllerBaseApiUrl;
  protected ControllerRequestURLBuilder _controllerRequestURLBuilder;
  protected String _controllerDataDir;
  protected PinotHelixResourceManager _helixResourceManager;
  protected HelixManager _helixManager;
  protected HelixDataAccessor _helixDataAccessor;
  protected HelixAdmin _helixAdmin;
  protected ZkHelixPropertyStore<ZNRecord> _propertyStore;
  protected TableRebalanceManager _tableRebalanceManager;
  protected TableSizeReader _tableSizeReader;

  /**
   * Acquire the {@link ControllerTest} default instance that can be shared across different test cases.
   *
   * @return the default instance.
   */
  public static ControllerTest getInstance() {
    return DEFAULT_INSTANCE;
  }

  public List<String> createHybridTables(List<String> tableNames)
      throws IOException {
    List<String> tableNamesWithType = new ArrayList<>();
    for (String tableName : tableNames) {
      addDummySchema(tableName);
      TableConfig offlineTable = createDummyTableConfig(tableName, TableType.OFFLINE);
      TableConfig realtimeTable = createDummyTableConfig(tableName, TableType.REALTIME);
      addTableConfig(offlineTable);
      addTableConfig(realtimeTable);
      tableNamesWithType.add(offlineTable.getTableName());
      tableNamesWithType.add(realtimeTable.getTableName());
    }
    return tableNamesWithType;
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

  /// Retrieves the headers to be used for the [PinotAdminClient].
  ///
  /// This method returns an empty map, indicating that no custom headers
  /// are set by default for the [PinotAdminClient].
  ///
  /// @return A map of headers (key-value pairs) to be used for the [PinotAdminClient].
  protected Map<String, String> getAdminClientHeaders() {
    return Map.of();
  }

  /**
   * Optionally provide an SSL context for controller admin transport and HTTP utilities.
   */
  @Nullable
  protected SSLContext getControllerTransportSslContext() {
    return null;
  }

  // Each fork JVM owns a disjoint slice of the sub-ephemeral range [20000, 32000), kept below the kernel
  // ephemeral range (32768+ on Linux) so a reserved port cannot be stolen by an outbound connection
  // before the service binds it. Ownership is arbitrated by the OS, not by hashing or a lock file: a
  // fork claims a slice by binding that slice's reservation port and holding the socket open for the
  // JVM's whole life (SLICE_RESERVATION, never closed). The kernel grants exactly one binder, so the
  // claim is a true mutex with no claim-time race, and it is released automatically when the JVM exits
  // -- no stale state to clean up, and correct across modules and across separate `mvn -T` invocations
  // on the same host. Within its slice a fork is the sole allocator, so nextFreePort()'s bind probe only
  // ever races unrelated external processes, never a sibling fork. If every slice is already owned (more
  // concurrent forks than slices) the fork falls back to the whole range and leans on the probe alone.
  private static final int PORT_RANGE_BEGIN = 20000;
  private static final int PORT_RANGE_END = 32000;
  private static final int PORTS_PER_FORK = 250;
  private static final int PORT_SLICE_COUNT = (PORT_RANGE_END - PORT_RANGE_BEGIN) / PORTS_PER_FORK;
  // Times a server start retries on a fresh port to ride out the unavoidable probe-to-bind race.
  private static final int PORT_BIND_RETRIES = 3;
  // Held open for the JVM's whole life; the field exists only to keep the reservation socket from being
  // closed or garbage-collected, so it is intentionally never read. Null when no slice was free.
  @SuppressWarnings("unused")
  private static final ServerSocket SLICE_RESERVATION;
  private static final int FORK_PORT_BASE;
  private static final int FORK_PORT_SPAN;
  private static final AtomicInteger PORT_CURSOR;

  static {
    int pid = (int) ProcessHandle.current().pid();
    ServerSocket reservation = null;
    int base = PORT_RANGE_BEGIN;
    int span = PORT_RANGE_END - PORT_RANGE_BEGIN;
    // Probe slices in a pid-rotated order so forks starting together tend to claim different slices
    // first instead of all contending for slice 0.
    int start = Math.floorMod(pid, PORT_SLICE_COUNT);
    for (int i = 0; i < PORT_SLICE_COUNT; i++) {
      int sliceBase = PORT_RANGE_BEGIN + ((start + i) % PORT_SLICE_COUNT) * PORTS_PER_FORK;
      // The slice's last port is its reservation port; keep it out of the allocatable span below.
      ServerSocket socket = tryReserve(sliceBase + PORTS_PER_FORK - 1);
      if (socket != null) {
        reservation = socket;
        base = sliceBase;
        span = PORTS_PER_FORK - 1;
        break;
      }
    }
    SLICE_RESERVATION = reservation;
    FORK_PORT_BASE = base;
    FORK_PORT_SPAN = span;
    // With a reserved slice this fork is the sole allocator, so a plain cursor is fine. In the
    // no-slice-free fallback every fork shares the whole range, so seed the cursor by pid to spread
    // their starting points instead of all probing from PORT_RANGE_BEGIN.
    PORT_CURSOR = new AtomicInteger(reservation != null ? 0 : Math.floorMod(pid, span));
  }

  /**
   * Binds {@code port} with {@code SO_REUSEADDR} disabled and returns the still-open socket on success,
   * or {@code null} (closing any partially-opened socket) if the port is taken. Used to claim a port
   * slice for the JVM's lifetime; the caller keeps the returned socket open.
   */
  private static ServerSocket tryReserve(int port) {
    ServerSocket socket = null;
    try {
      socket = new ServerSocket();
      socket.setReuseAddress(false);
      socket.bind(new InetSocketAddress(port));
      return socket;
    } catch (IOException e) {
      if (socket != null) {
        try {
          socket.close();
        } catch (IOException ignored) {
          // Nothing more we can do; the fd will be reclaimed on GC.
        }
      }
      return null;
    }
  }

  /**
   * Returns a free port that is safe to bind later in the test. Ports come from this fork's exclusively
   * owned slice of a sub-ephemeral range (see {@link #SLICE_RESERVATION}), so a reserved port is never
   * handed out by a concurrent fork; a no-{@code SO_REUSEADDR} bind probe additionally skips ports held
   * by unrelated processes. A port can still be taken between the probe and the eventual bind, so callers
   * that start a server should retry on a fresh port -- see {@link #startZkOnFreePort} and the controller
   * start path.
   */
  protected static int nextFreePort() {
    for (int attempt = 0; attempt < FORK_PORT_SPAN; attempt++) {
      int port = FORK_PORT_BASE + Math.floorMod(PORT_CURSOR.getAndIncrement(), FORK_PORT_SPAN);
      try (ServerSocket socket = new ServerSocket()) {
        // No SO_REUSEADDR: a port that only passes the probe thanks to address reuse may still be
        // unbindable by a server that does not set the option.
        socket.setReuseAddress(false);
        socket.bind(new InetSocketAddress(port));
        return port;
      } catch (IOException e) {
        // Port is held by an unrelated process; try the next one.
      }
    }
    throw new RuntimeException("Cannot allocate a free port for test in fork range [" + FORK_PORT_BASE + ", "
        + (FORK_PORT_BASE + FORK_PORT_SPAN) + ")");
  }

  public void startZk() {
    if (_zookeeperInstance == null) {
      runWithHelixMock(() -> _zookeeperInstance = startZkOnFreePort());
    }
  }

  /**
   * Starts a local ZooKeeper on a {@link #nextFreePort() free port}, retrying on a fresh port only when
   * the chosen one was taken between the probe and ZK actually binding it. Any other failure is a real
   * error and is rethrown immediately rather than masked behind retries.
   */
  private static ZkStarter.ZookeeperInstance startZkOnFreePort() {
    for (int attempt = 0; ; attempt++) {
      int port = nextFreePort();
      try {
        return ZkStarter.startLocalZkServer(port);
      } catch (RuntimeException e) {
        if (attempt >= PORT_BIND_RETRIES - 1 || !isPortBindFailure(e)) {
          throw e;
        }
      }
    }
  }

  /**
   * Returns true if {@code t} or one of its causes is an address-already-in-use bind failure, i.e. the
   * port was stolen between {@link #nextFreePort()} probing it and the server binding it.
   */
  private static boolean isPortBindFailure(Throwable t) {
    for (Throwable c = t; c != null; c = c.getCause()) {
      if (c instanceof BindException) {
        return true;
      }
      String message = c.getMessage();
      if (message != null && message.contains("Address already in use")) {
        return true;
      }
    }
    return false;
  }

  public void startZk(int port) {
    if (_zookeeperInstance == null) {
      runWithHelixMock(() -> _zookeeperInstance = ZkStarter.startLocalZkServer(port));
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
    properties.put(ControllerConf.ZK_STR, getZkUrl());
    properties.put(ControllerConf.HELIX_CLUSTER_NAME, getHelixClusterName());
    properties.put(ControllerConf.CONTROLLER_HOST, LOCAL_HOST);
    int controllerPort = nextFreePort();
    properties.put(ControllerConf.CONTROLLER_PORT, controllerPort);
    if (_controllerPort == 0) {
      _controllerPort = controllerPort;
    }
    properties.put(ControllerConf.DATA_DIR, DEFAULT_DATA_DIR);
    properties.put(ControllerConf.LOCAL_TEMP_DIR, DEFAULT_LOCAL_TEMP_DIR);
    // Enable groovy on the controller
    properties.put(ControllerConf.DISABLE_GROOVY, false);
    properties.put(ControllerConf.CONSOLE_SWAGGER_ENABLE, false);
    properties.put(CommonConstants.CONFIG_OF_TIMEZONE, "UTC");
    properties.put(ControllerConf.CLUSTER_TENANT_ISOLATION_ENABLE, true);
    overrideControllerConf(properties);
    return properties;
  }

  /**
   * Can be overridden to add more properties.
   */
  protected void overrideControllerConf(Map<String, Object> properties) {
  }

  /**
   * Can be overridden to use a different implementation.
   */
  public BaseControllerStarter createControllerStarter() {
    return new ControllerStarter();
  }

  public void startController()
      throws Exception {
    startController(getDefaultControllerConfiguration());
  }

  public void startControllerWithSwagger()
      throws Exception {
    Map<String, Object> config = getDefaultControllerConfiguration();
    config.put(ControllerConf.CONSOLE_SWAGGER_ENABLE, true);
    startController(config);
  }

  public void startController(Map<String, Object> properties)
      throws Exception {
    runWithHelixMock(() -> {
      assertNull(_controllerStarter, "Controller is already started");
      assertTrue(_controllerPort > 0, "Controller port is not assigned");
      startControllerWithBindRetry(properties);
      _controllerConfig = _controllerStarter.getConfig();
      _controllerBaseApiUrl = _controllerConfig.generateVipUrl();
      _controllerRequestURLBuilder = ControllerRequestURLBuilder.baseUrl(_controllerBaseApiUrl);
      _controllerDataDir = _controllerConfig.getDataDir();
      _helixResourceManager = _controllerStarter.getHelixResourceManager();
      _helixManager = _controllerStarter.getHelixControllerManager();
      _tableRebalanceManager = _controllerStarter.getTableRebalanceManager();
      _tableSizeReader = _controllerStarter.getTableSizeReader();
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
      assertEquals(System.getProperty("user.timezone"), "UTC");
    });
  }

  /**
   * Creates and starts the controller, retrying on a fresh port if the HTTP server port was taken
   * between {@link #nextFreePort()} and the bind. Only {@code start()} is retried; an {@code init()}
   * failure is not port-related and propagates immediately. A failed starter is stopped before the
   * retry, and {@code _controllerStarter} is left assigned only on success.
   */
  private void startControllerWithBindRetry(Map<String, Object> properties)
      throws Exception {
    for (int attempt = 0; ; attempt++) {
      BaseControllerStarter starter = createControllerStarter();
      starter.init(new PinotConfiguration(properties));
      try {
        starter.start();
        _controllerStarter = starter;
        return;
      } catch (Exception e) {
        try {
          starter.stop();
        } catch (Exception ignored) {
          // Best effort: the starter failed to come up, so teardown may be partial.
        }
        // Only retry a lost probe-to-bind race on a fresh port; rethrow any other failure immediately
        // so it is not masked or its latency tripled by pointless retries.
        if (attempt >= PORT_BIND_RETRIES - 1 || !isPortBindFailure(e)) {
          throw e;
        }
        int newPort = nextFreePort();
        properties.put(ControllerConf.CONTROLLER_PORT, newPort);
        _controllerPort = newPort;
      }
    }
  }

  public void stopController() {
    assertNotNull(_controllerStarter, "Controller hasn't been started");
    _controllerStarter.stop();
    _controllerStarter = null;
    _controllerPort = 0;
    _controllerRequestURLBuilder = null;
    if (_pinotAdminClient != null) {
      try {
        _pinotAdminClient.close();
      } catch (Exception e) {
        // ignore
      }
      _pinotAdminClient = null;
    }
    FileUtils.deleteQuietly(new File(_controllerDataDir));
  }

  public void restartController()
      throws Exception {
    assertNotNull(_controllerStarter, "Controller hasn't been started");
    _controllerStarter.stop();
    _controllerStarter = null;
    startController(_controllerConfig.toMap());
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

  public static LogicalTableConfig getDummyLogicalTableConfig(String tableName, List<String> physicalTableNames,
      String brokerTenant) {
    Map<String, PhysicalTableConfig> physicalTableConfigMap = new HashMap<>();
    for (String physicalTableName : physicalTableNames) {
      physicalTableConfigMap.put(physicalTableName, new PhysicalTableConfig());
    }
    String offlineTableName =
        physicalTableNames.stream().filter(TableNameBuilder::isOfflineTableResource).findFirst().orElse(null);
    String realtimeTableName =
        physicalTableNames.stream().filter(TableNameBuilder::isRealtimeTableResource).findFirst().orElse(null);
    LogicalTableConfigBuilder builder = new LogicalTableConfigBuilder()
        .setTableName(tableName)
        .setBrokerTenant(brokerTenant)
        .setRefOfflineTableName(offlineTableName)
        .setRefRealtimeTableName(realtimeTableName)
        .setQuotaConfig(new QuotaConfig(null, "99999"))
        .setQueryConfig(new QueryConfig(1L, true, false, null, 1L, 1L))
        .setTimeBoundaryConfig(new TimeBoundaryConfig("min", Map.of("includedTables", physicalTableNames)))
        .setPhysicalTableConfigMap(physicalTableConfigMap);
    return builder.build();
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
    for (int i = 0; i < numInstances; i++) {
      addFakeServerInstanceToAutoJoinHelixCluster(SERVER_INSTANCE_ID_PREFIX + i, isSingleTenant);
    }
  }

  public void addFakeServerInstanceToAutoJoinHelixCluster(String instanceId, boolean isSingleTenant)
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
    int adminPort = nextFreePort();
    helixAdmin.setConfig(configScope, Map.of(Helix.Instance.ADMIN_PORT_KEY, Integer.toString(adminPort)));
    _fakeInstanceHelixManagers.add(helixManager);
  }

  public void addFakeServerInstanceToAutoJoinHelixClusterWithEmptyTag(String instanceId, boolean isSingleTenant)
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
    }
    HelixConfigScope configScope = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.PARTICIPANT,
        getHelixClusterName()).forParticipant(instanceId).build();
    int adminPort = nextFreePort();
    helixAdmin.setConfig(configScope, Map.of(Helix.Instance.ADMIN_PORT_KEY, Integer.toString(adminPort)));
    _fakeInstanceHelixManagers.add(helixManager);
  }

  /** Add fake server instances until total number of server instances reaches maxCount */
  public void addMoreFakeServerInstancesToAutoJoinHelixCluster(int maxCount, boolean isSingleTenant)
      throws Exception {
    // get current instance count
    int currentCount = getFakeServerInstanceCount();

    // Add more instances if current count is less than max instance count.
    for (int i = currentCount; i < maxCount; i++) {
      addFakeServerInstanceToAutoJoinHelixCluster(SERVER_INSTANCE_ID_PREFIX + i, isSingleTenant);
    }
  }

  public int getFakeServerInstanceCount() {
    return _helixAdmin.getInstancesInClusterWithTag(getHelixClusterName(), "DefaultTenant_OFFLINE").size()
        + _helixAdmin.getInstancesInClusterWithTag(getHelixClusterName(), UNTAGGED_SERVER_INSTANCE).size();
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
    TestUtils.waitForCondition(aVoid -> _helixResourceManager.dropInstance(instanceId).isSuccessful(), 60_000L,
        "Failed to drop fake instance: " + instanceId);
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

  public ControllerRequestURLBuilder getControllerRequestURLBuilder() {
    return _controllerRequestURLBuilder;
  }

  public PinotAdminClient getOrCreateAdminClient()
      throws IOException {
    if (_pinotAdminClient != null) {
      return _pinotAdminClient;
    }
    try {
      String baseApiUrl = _controllerBaseApiUrl;
      if (baseApiUrl == null && _controllerRequestURLBuilder != null) {
        // Some tests customize the controller request builder without starting their own controller instance.
        baseApiUrl = _controllerRequestURLBuilder.getBaseUrl();
      }
      if (baseApiUrl == null && this != DEFAULT_INSTANCE && DEFAULT_INSTANCE._controllerBaseApiUrl != null) {
        baseApiUrl = DEFAULT_INSTANCE._controllerBaseApiUrl;
      }
      if (baseApiUrl == null) {
        throw new IOException("Controller base API URL is not initialized");
      }
      URI uri = URI.create(baseApiUrl);
      String controllerAddress = uri.getHost() + ":" + uri.getPort();
      java.util.Properties properties = new java.util.Properties();
      if (uri.getScheme() != null) {
        properties.setProperty(org.apache.pinot.client.admin.PinotAdminTransport.ADMIN_TRANSPORT_SCHEME,
            uri.getScheme());
      }
      SSLContext sslContext = getControllerTransportSslContext();
      if (sslContext != null) {
        org.apache.pinot.common.utils.tls.TlsUtils.setSslContext(sslContext);
      }
      _pinotAdminClient = new PinotAdminClient(controllerAddress, properties, getAdminClientHeaders(),
          sslContext);
      return _pinotAdminClient;
    } catch (PinotClientException e) {
      throw new IOException(e);
    }
  }

  /**
   * Exposes the admin client for callers that cannot access protected helpers.
   */
  public PinotAdminClient getAdminClient()
      throws IOException {
    return getOrCreateAdminClient();
  }

  public static TableConfig createDummyTableConfig(String tableName, TableType tableType) {
    TableConfigBuilder builder = new TableConfigBuilder(tableType);
    if (tableType == TableType.REALTIME) {
      builder.setStreamConfigs(FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs().getStreamConfigsMap());
    }
    return builder.setTableName(tableName)
        .setTimeColumnName("timeColumn")
        .setTimeType("DAYS")
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("5")
        .build();
  }

  public static Schema createDummySchemaWithPrimaryKey(String tableName) {
    Schema schema = createDummySchema(tableName);
    schema.setPrimaryKeyColumns(List.of("dimA"));
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
    try {
      getOrCreateAdminClient().getSchemaClient().createSchema(schema.toSingleLineJsonString());
    } catch (PinotAdminException e) {
      throw new IOException(e);
    }
  }

  public void updateSchema(Schema schema)
      throws IOException {
    try {
      getOrCreateAdminClient().getSchemaClient().updateSchema(schema.getSchemaName(),
          schema.toSingleLineJsonString());
    } catch (PinotAdminException e) {
      throw new IOException(e);
    }
  }

  public void forceUpdateSchema(Schema schema)
      throws IOException {
    try {
      getOrCreateAdminClient().getSchemaClient().updateSchema(schema.getSchemaName(),
          schema.toSingleLineJsonString(), false, true);
    } catch (PinotAdminException e) {
      throw new IOException(e);
    }
  }

  public Schema getSchema(String schemaName) {
    Schema schema = _helixResourceManager.getSchema(schemaName);
    assertNotNull(schema);
    return schema;
  }

  public void deleteSchema(String schemaName)
      throws IOException {
    try {
      getOrCreateAdminClient().getSchemaClient().deleteSchema(schemaName);
    } catch (PinotAdminException e) {
      throw new IOException(e);
    }
  }

  public void addTableConfig(TableConfig tableConfig)
      throws IOException {
    try {
      getOrCreateAdminClient().getTableClient().createTable(tableConfig.toJsonString(), null);
    } catch (PinotAdminException e) {
      throw new IOException(e);
    }
  }

  public void addLogicalTableConfig(LogicalTableConfig logicalTableConfig)
      throws IOException {
    try {
      getOrCreateAdminClient().getLogicalTableClient().createLogicalTable(logicalTableConfig.toJsonString());
    } catch (PinotAdminException e) {
      e.printStackTrace();
      throw new IOException(e);
    } catch (RuntimeException e) {
      e.printStackTrace();
      throw new IOException(e);
    }
  }

  public void updateTableConfig(TableConfig tableConfig)
      throws IOException {
    try {
      getOrCreateAdminClient().getTableClient().updateTableConfig(tableConfig.getTableName(),
          tableConfig.toJsonString());
    } catch (PinotAdminException e) {
      throw new IOException(e);
    }
  }

  public void updateLogicalTableConfig(LogicalTableConfig logicalTableConfig)
      throws IOException {
    try {
      getOrCreateAdminClient().getLogicalTableClient()
          .updateLogicalTable(logicalTableConfig.getTableName(), logicalTableConfig.toJsonString());
    } catch (PinotAdminException e) {
      throw new IOException(e);
    } catch (RuntimeException e) {
      throw new IOException(e);
    }
  }

  public void toggleTableState(String tableName, TableType type, boolean enable)
      throws IOException {
    try {
      getOrCreateAdminClient().getTableClient().setTableState(tableName, type.toString().toLowerCase(), enable);
    } catch (PinotAdminException e) {
      throw new IOException(e);
    }
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
    try {
      getOrCreateAdminClient().getTableClient()
          .deleteTable(TableNameBuilder.OFFLINE.tableNameWithType(tableName));
    } catch (PinotAdminException e) {
      throw new IOException(e);
    }
  }

  public void dropOfflineTable(String tableName, String retentionPeriod)
      throws IOException {
    try {
      getOrCreateAdminClient().getTableClient()
          .deleteTable(TableNameBuilder.OFFLINE.tableNameWithType(tableName), null, retentionPeriod, null);
    } catch (PinotAdminException e) {
      throw new IOException(e);
    }
  }

  public void dropRealtimeTable(String tableName)
      throws IOException {
    try {
      getOrCreateAdminClient().getTableClient()
          .deleteTable(TableNameBuilder.REALTIME.tableNameWithType(tableName));
    } catch (PinotAdminException e) {
      throw new IOException(e);
    }
  }

  public void dropLogicalTable(String logicalTableName)
      throws IOException {
    try {
      getOrCreateAdminClient().getLogicalTableClient().deleteLogicalTable(logicalTableName);
    } catch (PinotAdminException e) {
      throw new IOException(e);
    }
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
    try {
      return getOrCreateAdminClient().getSegmentClient()
          .listSegments(tableName, tableType, excludeReplacedSegments);
    } catch (PinotAdminException e) {
      throw new IOException(e);
    }
  }

  public void dropSegment(String tableName, String segmentName)
      throws IOException {
    try {
      getOrCreateAdminClient().getSegmentClient().deleteSegment(tableName, segmentName, null);
    } catch (PinotAdminException e) {
      throw new IOException(e);
    }
  }

  public void dropAllSegments(String tableName, TableType tableType)
      throws IOException {
    try {
      getOrCreateAdminClient().getSegmentClient().deleteMultipleSegments(
          TableNameBuilder.forType(tableType).tableNameWithType(tableName), null, null);
    } catch (PinotAdminException e) {
      throw new IOException(e);
    }
  }

  public long getTableSize(String tableName)
      throws IOException {
    try {
      return getOrCreateAdminClient().getTableClient().getReportedTableSizeInBytes(tableName);
    } catch (PinotAdminException e) {
      throw new IOException(e);
    }
  }

  public Map<String, List<String>> getTableServersToSegmentsMap(String tableName, TableType tableType)
      throws IOException {
    try {
      return getOrCreateAdminClient().getSegmentClient()
          .getServerToSegmentsMapAsMap(tableName, tableType != null ? tableType.name() : null);
    } catch (PinotAdminException e) {
      throw new IOException(e);
    }
  }

  protected String getInstancePartitionsResponse(String tableName, @Nullable String instancePartitionsType)
      throws IOException {
    try {
      return getOrCreateAdminClient().getTableClient().getInstancePartitions(tableName, instancePartitionsType);
    } catch (PinotAdminException e) {
      throw new IOException(e);
    } catch (RuntimeException e) {
      throw wrapRuntimeException(e);
    }
  }

  protected String assignInstances(String tableName, @Nullable InstancePartitionsType instancePartitionsType,
      boolean dryRun)
      throws IOException {
    try {
      return getOrCreateAdminClient().getTableClient()
          .assignInstances(tableName, instancePartitionsType != null ? instancePartitionsType.toString() : null,
              dryRun);
    } catch (PinotAdminException e) {
      throw new IOException(e);
    } catch (RuntimeException e) {
      throw wrapRuntimeException(e);
    }
  }

  protected void deleteInstancePartitions(String tableName, @Nullable String instancePartitionsType)
      throws IOException {
    try {
      getOrCreateAdminClient().getTableClient().deleteInstancePartitions(tableName, instancePartitionsType);
    } catch (PinotAdminException e) {
      throw new IOException(e);
    } catch (RuntimeException e) {
      throw wrapRuntimeException(e);
    }
  }

  protected String replaceInstanceInPartitions(String tableName,
      @Nullable InstancePartitionsType instancePartitionsType, String oldInstanceId, String newInstanceId)
      throws IOException {
    try {
      return getOrCreateAdminClient().getTableClient()
          .replaceInstance(tableName, instancePartitionsType != null ? instancePartitionsType.toString() : null,
              oldInstanceId, newInstanceId);
    } catch (PinotAdminException e) {
      throw new IOException(e);
    } catch (RuntimeException e) {
      throw wrapRuntimeException(e);
    }
  }

  protected String updateInstancePartitions(String tableName, String instancePartitionsJson)
      throws IOException {
    try {
      return getOrCreateAdminClient().getTableClient().updateInstancePartitions(tableName, instancePartitionsJson);
    } catch (PinotAdminException e) {
      throw new IOException(e);
    } catch (RuntimeException e) {
      throw wrapRuntimeException(e);
    }
  }

  protected IOException wrapRuntimeException(RuntimeException e) {
    Throwable t = e;
    while (t.getCause() != null) {
      t = t.getCause();
    }
    String message = t.getMessage();
    if (message == null || message.isEmpty()) {
      message = e.toString();
    }
    return new IOException(message, e);
  }

  public String reloadOfflineTable(String tableName)
      throws IOException {
    return reloadOfflineTable(tableName, false);
  }

  public String reloadOfflineTable(String tableName, boolean forceDownload)
      throws IOException {
    try {
      return getOrCreateAdminClient().getSegmentClient()
          .reloadTable(tableName, TableType.OFFLINE.name(), forceDownload);
    } catch (PinotAdminException e) {
      throw new IOException(e);
    }
  }

  public TableSegmentsReloadCheckResponse checkIfReloadIsNeeded(String tableNameWithType, Boolean verbose)
      throws IOException {
    try {
      return getOrCreateAdminClient().getSegmentClient()
          .checkIfReloadIsNeeded(tableNameWithType, verbose != null && verbose);
    } catch (PinotAdminException e) {
      throw new IOException(e);
    }
  }

  public String reloadOfflineSegment(String tableName, String segmentName, boolean forceDownload)
      throws IOException {
    try {
      return getOrCreateAdminClient().getSegmentClient().reloadSegment(tableName, segmentName, forceDownload);
    } catch (PinotAdminException e) {
      throw new IOException(e);
    }
  }

  public String reloadRealtimeTable(String tableName)
      throws IOException {
    try {
      return getOrCreateAdminClient().getSegmentClient().reloadTable(tableName, TableType.REALTIME.name(), false);
    } catch (PinotAdminException e) {
      throw new IOException(e);
    }
  }

  public void createBrokerTenant(String tenantName, int numBrokers)
      throws IOException {
    try {
      String tenantJson = new org.apache.pinot.spi.config.tenant.Tenant(
          org.apache.pinot.spi.config.tenant.TenantRole.BROKER, tenantName, numBrokers, 0, 0).toJsonString();
      getOrCreateAdminClient().getTenantClient().createTenant(tenantJson);
    } catch (PinotAdminException e) {
      throw new IOException(e);
    }
  }

  public void updateBrokerTenant(String tenantName, int numBrokers)
      throws IOException {
    try {
      String tenantJson = new org.apache.pinot.spi.config.tenant.Tenant(
          org.apache.pinot.spi.config.tenant.TenantRole.BROKER, tenantName, numBrokers, 0, 0).toJsonString();
      getOrCreateAdminClient().getTenantClient().updateTenant(tenantJson);
    } catch (PinotAdminException e) {
      throw new IOException(e);
    }
  }

  public void deleteBrokerTenant(String tenantName)
      throws IOException {
    try {
      getOrCreateAdminClient().getTenantClient().deleteTenant(tenantName, "BROKER");
    } catch (PinotAdminException e) {
      throw new IOException(e);
    }
  }

  public void createServerTenant(String tenantName, int numOfflineServers, int numRealtimeServers)
      throws IOException {
    try {
      String tenantJson = new org.apache.pinot.spi.config.tenant.Tenant(
          org.apache.pinot.spi.config.tenant.TenantRole.SERVER, tenantName,
          numOfflineServers + numRealtimeServers, numOfflineServers, numRealtimeServers).toJsonString();
      getOrCreateAdminClient().getTenantClient().createTenant(tenantJson);
    } catch (PinotAdminException e) {
      throw new IOException(e);
    }
  }

  public void updateServerTenant(String tenantName, int numOfflineServers, int numRealtimeServers)
      throws IOException {
    try {
      String tenantJson = new org.apache.pinot.spi.config.tenant.Tenant(
          org.apache.pinot.spi.config.tenant.TenantRole.SERVER, tenantName,
          numOfflineServers + numRealtimeServers, numOfflineServers, numRealtimeServers).toJsonString();
      getOrCreateAdminClient().getTenantClient().updateTenant(tenantJson);
    } catch (PinotAdminException e) {
      throw new IOException(e);
    }
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

  public void runRealtimeSegmentValidationTask(String tableName)
      throws IOException {
    runPeriodicTask("RealtimeSegmentValidationManager", tableName, TableType.REALTIME);
  }

  public void runPeriodicTask(String taskName, String tableName, TableType tableType)
      throws IOException {
    try {
      getOrCreateAdminClient().getClusterClient()
          .runPeriodicTask(taskName, tableName, tableType != null ? tableType.name() : null);
    } catch (PinotAdminException e) {
      throw new IOException(e);
    }
  }

  public void updateClusterConfig(Map<String, String> clusterConfig)
      throws IOException {
    try {
      String payload = JsonUtils.objectToString(clusterConfig);
      getOrCreateAdminClient().getClusterClient().updateClusterConfig(payload);
    } catch (PinotAdminException e) {
      throw new IOException(e);
    }
  }

  public void deleteClusterConfig(String clusterConfig)
      throws IOException {
    try {
      getOrCreateAdminClient().getClusterClient().deleteClusterConfig(clusterConfig);
    } catch (PinotAdminException e) {
      throw new IOException(e);
    }
  }

  /**
   * Trigger a task on a table and wait for completion
   */
  protected String triggerMinionTask(String taskType, String tableNameWithType) {
    PinotTaskManager taskManager = _controllerStarter.getTaskManager();

    TaskSchedulingContext context = new TaskSchedulingContext()
        .setTasksToSchedule(Set.of(taskType))
        .setTablesToSchedule(Set.of(tableNameWithType));

    List<String> taskIds = taskManager.scheduleTasks(context)
        .get(taskType)
        .getScheduledTaskNames();

    assert taskIds != null;
    LOGGER.info("Scheduled {} for table {} with id: {}", taskType, tableNameWithType, taskIds);
    assertEquals(taskIds.size(), 1,
        String.format("Task %s not scheduled as expected for table %s. Expected 1 task, but got: %s",
            taskType, tableNameWithType, taskIds.size()));
    return taskIds.get(0);
  }

  public void pauseTable(String tableName)
      throws IOException {
    try {
      getOrCreateAdminClient().getTableClient().pauseConsumption(tableName);
    } catch (PinotAdminException e) {
      throw new IOException(e);
    }
    TestUtils.waitForCondition((aVoid) -> {
      try {
        PauseStatusDetails pauseStatusDetails =
            getOrCreateAdminClient().getTableClient().getPauseStatusDetails(tableName);
        if (pauseStatusDetails.getConsumingSegments().isEmpty()) {
          return true;
        }
        LOGGER.warn("Table not yet paused. Response " + pauseStatusDetails);
        return false;
      } catch (IOException | PinotAdminException e) {
        throw new RuntimeException(e);
      }
    }, 2000, 60_000L, "Failed to pause table: " + tableName);
  }

  public void resumeTable(String tableName)
      throws IOException {
    resumeTable(tableName, "lastConsumed");
  }

  public void resumeTable(String tableName, String offsetCriteria)
      throws IOException {
    try {
      getOrCreateAdminClient().getTableClient().resumeConsumption(tableName, offsetCriteria);
    } catch (PinotAdminException e) {
      throw new IOException(e);
    }
    TestUtils.waitForCondition((aVoid) -> {
      try {
        PauseStatusDetails pauseStatusDetails =
            getOrCreateAdminClient().getTableClient().getPauseStatusDetails(tableName);
        // Its possible no segment is in consuming state, so check pause flag
        if (!pauseStatusDetails.getPauseFlag()) {
          return true;
        }
        LOGGER.warn("Pause flag is not yet set to false. Response " + pauseStatusDetails);
        return false;
      } catch (IOException | PinotAdminException e) {
        throw new RuntimeException(e);
      }
    }, 2000, 60_000L, "Failed to resume table: " + tableName);
  }

  public void waitForNumSegmentsInDesiredStateInEV(String tableName, String desiredState,
      int desiredNumConsumingSegments, TableType type) {
    TestUtils.waitForCondition((aVoid) -> {
          try {
            AtomicInteger numConsumingSegments = new AtomicInteger(0);
            TableView tableView = getExternalView(tableName, type);
            Map<String, Map<String, String>> viewForType =
                type.equals(TableType.OFFLINE) ? tableView._offline : tableView._realtime;
            viewForType.values().forEach((v) -> {
              numConsumingSegments.addAndGet((int) v.values().stream().filter((v1) -> v1.equals(desiredState)).count());
            });
            return numConsumingSegments.get() == desiredNumConsumingSegments;
          } catch (IOException e) {
            return false;
          }
        }, 5000, 60_000L,
        "Failed to wait for " + desiredNumConsumingSegments + " consuming segments for table: " + tableName
    );
  }

  public TableView getExternalView(String tableName, TableType type)
      throws IOException {
    try {
      return getOrCreateAdminClient().getTableClient()
          .getExternalViewObject(tableName + "_" + type);
    } catch (PinotAdminException e) {
      throw new IOException(e);
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
    return IOUtils.toString(new URL(urlString).openStream(), StandardCharsets.UTF_8);
  }

  /**
   * Sends a GET request to the specified URL and returns the status code along with the stringified response.
   * @param urlString the URL to send the GET request
   * @param headers the headers to include in the GET request
   * @return a Pair containing the status code and the stringified response
   */
  public static Pair<Integer, String> sendGetRequestWithStatusCode(String urlString, Map<String, String> headers)
      throws IOException {
    try {
      SimpleHttpResponse resp =
          getHttpClient().sendGetRequest(new URL(urlString).toURI(), headers);
      return Pair.of(resp.getStatusCode(), constructResponse(resp));
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
  }

  public static String sendPostRequest(String urlString)
      throws IOException {
    return sendPostRequest(urlString, null);
  }

  public static String sendPostRequest(String urlString, String payload)
      throws IOException {
    return sendPostRequest(urlString, payload, Map.of());
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

  /**
   * Sends a POST request to the specified URL with the given payload and returns the status code along with the
   * stringified response.
   * @param urlString the URL to send the POST request to
   * @param payload the payload to send in the POST request
   * @return a Pair containing the status code and the stringified response
   */
  public static Pair<Integer, String> postRequestWithStatusCode(String urlString, String payload)
      throws IOException {
    try {
      SimpleHttpResponse resp =
          getHttpClient().sendJsonPostRequest(new URL(urlString).toURI(), payload, Map.of());
      return Pair.of(resp.getStatusCode(), constructResponse(resp));
    } catch (URISyntaxException e) {
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
    return sendPutRequest(urlString, payload, Map.of());
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
    return sendDeleteRequest(urlString, Map.of());
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
    return sendMultipartPostRequest(url, body, Map.of());
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

  public HelixAdmin getHelixAdmin() {
    return _helixAdmin;
  }

  public BaseControllerStarter getControllerStarter() {
    return _controllerStarter;
  }

  public PinotHelixResourceManager getHelixResourceManager() {
    return _helixResourceManager;
  }

  public String getControllerBaseApiUrl() {
    return _controllerBaseApiUrl;
  }

  protected String controllerUrl(String path) {
    String relativePath = path.startsWith("/") ? path : "/" + path;
    return _controllerBaseApiUrl + relativePath;
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
    addFakeMinionInstancesToAutoJoinHelixCluster(DEFAULT_NUM_MINION_INSTANCES);
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
      // this is expected to happen only when running a single test case outside testNG group, i.e. when test
      // cases are run one at a time within IntelliJ or through maven command line. When running under a testNG
      // group, state will have already been setup by @BeforeGroups method in ControllerTestSetup.
      startSharedTestSetup();
    } else {
      // Ensure the shared cluster starts clean between test classes.
      List<String> existingTables = getHelixResourceManager().getAllTables();
      List<String> existingSchemas = getHelixResourceManager().getSchemaNames();
      if (!existingTables.isEmpty() || !existingSchemas.isEmpty()) {
        cleanup();
      }
    }

    // Always clean tables/schemas from any previous test class to guarantee isolation.
    cleanup();

    // Ensure expected fake instances are present before validation.
    int currentBrokers =
        _helixResourceManager.getAllInstancesForBrokerTenant(TagNameUtils.DEFAULT_TENANT_NAME).size();
    if (currentBrokers < DEFAULT_NUM_BROKER_INSTANCES) {
      addMoreFakeBrokerInstancesToAutoJoinHelixCluster(DEFAULT_NUM_BROKER_INSTANCES - currentBrokers, true);
    }
    int currentServers =
        _helixResourceManager.getAllInstancesForServerTenant(TagNameUtils.DEFAULT_TENANT_NAME).size();
    if (currentServers < DEFAULT_NUM_SERVER_INSTANCES) {
      addMoreFakeServerInstancesToAutoJoinHelixCluster(DEFAULT_NUM_SERVER_INSTANCES - currentServers, true);
    }

    // In a single tenant cluster, only the default tenant should exist
    assertEquals(_helixResourceManager.getAllBrokerTenantNames(),
        Set.of(TagNameUtils.DEFAULT_TENANT_NAME));
    assertEquals(_helixResourceManager.getAllInstancesForBrokerTenant(TagNameUtils.DEFAULT_TENANT_NAME).size(),
        DEFAULT_NUM_BROKER_INSTANCES);
    assertEquals(_helixResourceManager.getAllServerTenantNames(),
        Set.of(TagNameUtils.DEFAULT_TENANT_NAME));
    assertEquals(_helixResourceManager.getAllInstancesForServerTenant(TagNameUtils.DEFAULT_TENANT_NAME).size(),
        DEFAULT_NUM_SERVER_INSTANCES);

    // No pre-existing tables
    assertTrue(CollectionUtils.isEmpty(getHelixResourceManager().getAllTables()));
    // No pre-existing schemas
    assertTrue(CollectionUtils.isEmpty(getHelixResourceManager().getSchemaNames()));
  }

  @DataProvider
  public Object[][] tableTypeProvider() {
    return new Object[][]{
        {TableType.OFFLINE},
        {TableType.REALTIME}
    };
  }

  /**
   * Clean shared state after a test case class has completed running. Additional cleanup may be needed depending upon
   * test functionality.
   */
  public void cleanup() {
    // Delete logical tables
    List<String> logicalTables = _helixResourceManager.getAllLogicalTableNames();
    for (String logicalTableName : logicalTables) {
      _helixResourceManager.deleteLogicalTableConfig(logicalTableName);
    }

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
    try {
      TestUtils.waitForCondition(aVoid -> {
        tablesWithEV.removeIf(t -> _helixResourceManager.getTableExternalView(t) == null);
        return tablesWithEV.isEmpty();
      }, 60_000L, "Failed to clean up all the external views");
    } catch (AssertionError e) {
      LOGGER.warn("Remaining external views not cleaned up for tables: {}", tablesWithEV);
      throw e;
    }

    // Delete all schemas.
    List<String> schemaNames = _helixResourceManager.getAllSchemaNames();
    if (CollectionUtils.isNotEmpty(schemaNames)) {
      for (String schemaName : schemaNames) {
        getHelixResourceManager().deleteSchema(schemaName);
      }
    }

    // Ensure cluster is purely empty before returning
    try {
      TestUtils.waitForCondition(aVoid -> {
        boolean noTables = CollectionUtils.isEmpty(_helixResourceManager.getAllTables());
        boolean noLogicalTables = CollectionUtils.isEmpty(_helixResourceManager.getAllLogicalTableNames());
        boolean noSchemas = CollectionUtils.isEmpty(_helixResourceManager.getAllSchemaNames());

        // Helix: ensure no table IdealState or ExternalView remains
        boolean noTableResourcesInIdealState = _helixDataAccessor.getChildNames(_helixDataAccessor.keyBuilder()
            .idealStates()).stream().noneMatch(TableNameBuilder::isTableResource);
        boolean noTableResourcesInExternalView = _helixDataAccessor.getChildNames(_helixDataAccessor.keyBuilder()
            .externalViews()).stream().noneMatch(TableNameBuilder::isTableResource);

        // Property store: ensure no segments or table-config nodes remain
        boolean noSegmentsNodes = CollectionUtils.isEmpty(_propertyStore.getChildNames(
            "/SEGMENTS", AccessOption.PERSISTENT));
        boolean noTableConfigNodes = CollectionUtils.isEmpty(_propertyStore.getChildNames(
            "/CONFIGS/TABLE", AccessOption.PERSISTENT));

        return noTables && noLogicalTables && noSchemas
            && noTableResourcesInIdealState && noTableResourcesInExternalView
            && noSegmentsNodes && noTableConfigNodes;
      }, 60_000L, "Failed to fully clean up cluster state");
    } catch (AssertionError e) {
      // Log detailed remaining resources to aid debugging
      List<String> remainingTables = _helixResourceManager.getAllTables();
      List<String> remainingLogicalTables = _helixResourceManager.getAllLogicalTableNames();
      List<String> remainingSchemas = _helixResourceManager.getAllSchemaNames();

      List<String> remainingIdealStateResources = _helixDataAccessor
          .getChildNames(_helixDataAccessor.keyBuilder().idealStates())
          .stream().filter(TableNameBuilder::isTableResource).collect(Collectors.toList());
      List<String> remainingExternalViewResources = _helixDataAccessor
          .getChildNames(_helixDataAccessor.keyBuilder().externalViews())
          .stream().filter(TableNameBuilder::isTableResource).collect(Collectors.toList());

      List<String> remainingSegmentNodes = _propertyStore.getChildNames(
          "/SEGMENTS", AccessOption.PERSISTENT);
      List<String> remainingTableConfigNodes = _propertyStore.getChildNames(
          "/CONFIGS/TABLE", AccessOption.PERSISTENT);

      LOGGER.warn(
          "Cluster cleanup incomplete. Remaining - tables: {}, logicalTables: {}, schemas: {}, idealStateResources: "
              + "{}, externalViewResources: {}, segmentNodes: {}, tableConfigNodes: {}",
          remainingTables, remainingLogicalTables, remainingSchemas, remainingIdealStateResources,
          remainingExternalViewResources, remainingSegmentNodes, remainingTableConfigNodes);
      throw e;
    }
  }

  @FunctionalInterface
  public interface ExceptionalRunnable {
    void run()
        throws Exception;
  }

  protected void runWithHelixMock(ExceptionalRunnable r) {
    try (MockedStatic<HelixPropertyFactory> mock = Mockito.mockStatic(HelixPropertyFactory.class)) {

      // mock helix method to disable slow, but useless, getCloudConfig() call
      Mockito.when(HelixPropertyFactory.getCloudConfig(Mockito.anyString(), Mockito.anyString()))
          .then((i) -> new CloudConfig());

      mock.when(HelixPropertyFactory::getInstance).thenCallRealMethod();

      r.run();
    } catch (Exception e) {
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      } else {
        throw new RuntimeException(e);
      }
    }
  }
}
