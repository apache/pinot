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

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import java.io.File;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import kafka.server.KafkaServerStartable;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.config.TagNameUtils;
import org.apache.pinot.common.config.TagOverrideConfig;
import org.apache.pinot.common.config.TenantConfig;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.common.utils.retry.RetryPolicies;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.core.indexsegment.generator.SegmentVersion;
import org.apache.pinot.core.realtime.impl.kafka.KafkaStarterUtils;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.ITestContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterGroups;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeGroups;
import org.testng.annotations.Test;


/**
 * Integration tests for all {@link org.apache.pinot.controller.helix.core.periodictask.ControllerPeriodicTask}
 * The intention of these tests is not to test functionality of daemons,
 * but simply to check that they run as expected and process the tables when the controller starts.
 *
 * Cluster setup/teardown is common across all tests in the @BeforeClass method {@link ControllerPeriodicTasksIntegrationTests::setup}.
 * This includes:
 * zk, controller, 1 broker, 3 offline servers, 3 realtime servers, kafka with avro loaded, offline table with segments from avro
 *
 * There will be a separate beforeTask(), testTask() and afterTask() for each ControllerPeriodicTask test, grouped by task name.
 * See group = "segmentStatusChecker" for example.
 * The tables needed for the test will be created in beforeTask(), and dropped in afterTask()
 *
 * The groups run sequentially in the order: segmentStatusChecker -> realtimeSegmentRelocation -> brokerResourceValidationManager -> ....
 */
public class ControllerPeriodicTasksIntegrationTests extends BaseClusterIntegrationTestSet {

  private static final String TENANT_NAME = "TestTenant";
  private static final String DEFAULT_TABLE_NAME = "mytable";

  private static final int PERIODIC_TASK_INITIAL_DELAY_SECONDS = 60;
  private static final int PERIODIC_TASK_FREQ_SECONDS = 5;
  private static final String PERIODIC_TASK_FREQ = "5s";

  private String _currentTableName;
  private List<File> _avroFiles;

  /**
   * Setup the cluster for the tests
   * @throws Exception
   */
  @BeforeClass
  public void setUp() throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    startZk();
    startKafka();

    // Set initial delay of 60 seconds for periodic tasks, to allow time for tables setup.
    // Run at 5 seconds freq in order to keep them running, in case first run happens before table setup
    ControllerConf controllerConf = getDefaultControllerConfiguration();
    controllerConf.setTenantIsolationEnabled(false);
    controllerConf.setStatusCheckerInitialDelayInSeconds(PERIODIC_TASK_INITIAL_DELAY_SECONDS);
    controllerConf.setStatusCheckerFrequencyInSeconds(PERIODIC_TASK_FREQ_SECONDS);
    controllerConf.setRealtimeSegmentRelocationInitialDelayInSeconds(PERIODIC_TASK_INITIAL_DELAY_SECONDS);
    controllerConf.setRealtimeSegmentRelocatorFrequency(PERIODIC_TASK_FREQ);
    controllerConf.setBrokerResourceValidationInitialDelayInSeconds(PERIODIC_TASK_FREQ_SECONDS);
    controllerConf.setBrokerResourceValidationFrequencyInSeconds(PERIODIC_TASK_FREQ_SECONDS);

    startController(controllerConf);
    startBroker();
    startServers(6);

    // Create tenants
    createBrokerTenant(TENANT_NAME, 1);
    createServerTenant(TENANT_NAME, 3, 3);

    // unpack avro into _tempDir
    _avroFiles = unpackAvroData(_tempDir);

    // setup a default offline table, shared across all tests. Each test can create additional tables and destroy them
    setupOfflineTableAndSegments(DEFAULT_TABLE_NAME, _avroFiles);

    // push avro into kafka, each test can create the realtime table and destroy it
    ExecutorService executor = Executors.newCachedThreadPool();
    pushAvroIntoKafka(_avroFiles, getKafkaTopic(), executor);
    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.MINUTES);
  }

  /**
   * Setup offline table, but no segments
   */
  private void setupOfflineTable(String table) throws Exception {
    _realtimeTableConfig = null;
    addOfflineTable(table, null, null, TENANT_NAME, TENANT_NAME, null, SegmentVersion.v1, null, null, null);
    completeTableConfiguration();
  }

  /**
   * Setup offline table, with segments from avro
   */
  private void setupOfflineTableAndSegments(String table, List<File> avroFiles) throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_segmentDir, _tarDir);
    setTableName(table);
    _realtimeTableConfig = null;

    File schemaFile = getSchemaFile();
    Schema schema = Schema.fromFile(schemaFile);
    String schemaName = schema.getSchemaName();
    addSchema(schemaFile, schemaName);

    String timeColumnName = schema.getTimeColumnName();
    Assert.assertNotNull(timeColumnName);
    TimeUnit outgoingTimeUnit = schema.getOutgoingTimeUnit();
    Assert.assertNotNull(outgoingTimeUnit);
    String timeType = outgoingTimeUnit.toString();

    addOfflineTable(table, timeColumnName, timeType, TENANT_NAME, TENANT_NAME, null, SegmentVersion.v1, null, null, null);
    completeTableConfiguration();

    ExecutorService executor = Executors.newCachedThreadPool();
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, 0, _segmentDir, _tarDir, table, false,
        null, null, null, executor);
    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.MINUTES);
    uploadSegments(_tarDir);
    waitForAllDocsLoaded(600_000L);
  }

  /**
   * Setup realtime table for given tablename and topic
   */
  private void setupRealtimeTable(String table, String  topic, File avroFile) throws Exception {
    _offlineTableConfig = null;
    File schemaFile = getSchemaFile();
    Schema schema = Schema.fromFile(schemaFile);
    String schemaName = schema.getSchemaName();
    addSchema(schemaFile, schemaName);

    String timeColumnName = schema.getTimeColumnName();
    Assert.assertNotNull(timeColumnName);
    TimeUnit outgoingTimeUnit = schema.getOutgoingTimeUnit();
    Assert.assertNotNull(outgoingTimeUnit);
    String timeType = outgoingTimeUnit.toString();

    addRealtimeTable(table, useLlc(), KafkaStarterUtils.DEFAULT_KAFKA_BROKER, KafkaStarterUtils.DEFAULT_ZK_STR, topic,
        getRealtimeSegmentFlushSize(), avroFile, timeColumnName, timeType, schemaName, TENANT_NAME, TENANT_NAME,
        getLoadMode(), getSortedColumn(), getInvertedIndexColumns(), getBloomFilterIndexColumns(), getRawIndexColumns(),
        getTaskConfig(), getStreamConsumerFactoryClassName());
    completeTableConfiguration();
  }

  @Override
  public String getTableName() {
    return _currentTableName;
  }

  private void setTableName(String tableName) {
    _currentTableName = tableName;
  }

  /**
   * Group - segmentStatusChecker - Integration tests for {@link org.apache.pinot.controller.helix.SegmentStatusChecker}
   * @throws Exception
   */
  @BeforeGroups(groups = "segmentStatusChecker")
  public void beforeTestSegmentStatusCheckerTest(ITestContext context) throws Exception {
    String emptyTable = "table1_OFFLINE";
    String disabledOfflineTable = "table2_OFFLINE";
    String basicOfflineTable = getDefaultOfflineTableName();
    String errorOfflineTable = "table4_OFFLINE";
    String basicRealtimeTable = getDefaultRealtimeTableName();
    int numTables = 5;

    context.setAttribute("emptyTable", emptyTable);
    context.setAttribute("disabledOfflineTable", disabledOfflineTable);
    context.setAttribute("basicOfflineTable", basicOfflineTable);
    context.setAttribute("errorOfflineTable", errorOfflineTable);
    context.setAttribute("basicRealtimeTable", basicRealtimeTable);
    context.setAttribute("numTables", numTables);

    // empty table
    setupOfflineTable(emptyTable);

    // table with disabled ideal state
    setupOfflineTable(disabledOfflineTable);
    _helixAdmin.enableResource(_clusterName, disabledOfflineTable, false);

    // some segments offline
    setupOfflineTableAndSegments(errorOfflineTable, _avroFiles);
    HelixHelper.updateIdealState(_helixManager, errorOfflineTable, new Function<IdealState, IdealState>() {
      @Nullable
      @Override
      public IdealState apply(@Nullable IdealState input) {
        List<String> segmentNames = Lists.newArrayList(input.getPartitionSet());
        Collections.sort(segmentNames);

        Map<String, String> instanceStateMap1 = input.getInstanceStateMap(segmentNames.get(0));
        for (String instance : instanceStateMap1.keySet()) {
          instanceStateMap1.put(instance, "OFFLINE");
          break;
        }
        return input;
      }
    }, RetryPolicies.fixedDelayRetryPolicy(2, 10));

    // setup default realtime table
    setupRealtimeTable(basicRealtimeTable, getKafkaTopic(), _avroFiles.get(0));
  }

  /**
   * After 1 run of SegmentStatusChecker the controllerMetrics will be set for each table
   * Validate that we are seeing the expected numbers
   */
  @Test(groups = "segmentStatusChecker")
  public void testSegmentStatusChecker(ITestContext context) throws Exception {
    String emptyTable = (String) context.getAttribute("emptyTable");
    String disabledOfflineTable = (String) context.getAttribute("disabledOfflineTable");
    String basicOfflineTable = (String) context.getAttribute("basicOfflineTable");
    String errorOfflineTable = (String) context.getAttribute("errorOfflineTable");
    String basicRealtimeTable = (String) context.getAttribute("basicRealtimeTable");
    int numTables = (int) context.getAttribute("numTables");

    ControllerMetrics controllerMetrics = _controllerStarter.getControllerMetrics();

    TestUtils.waitForCondition(input ->
        controllerMetrics.getValueOfGlobalGauge(ControllerGauge.PERIODIC_TASK_NUM_TABLES_PROCESSED,
            "SegmentStatusChecker") >= numTables, 240_000, "Timed out waiting for SegmentStatusChecker");

    // empty table - table1_OFFLINE
    // num replicas set from ideal state
    checkSegmentStatusCheckerMetrics(controllerMetrics, emptyTable, null, 3, 100, 0, 100);

    // disabled table - table2_OFFLINE
    // reset to defaults
    checkSegmentStatusCheckerMetrics(controllerMetrics, disabledOfflineTable, null, Long.MIN_VALUE, Long.MIN_VALUE,
        Long.MIN_VALUE, Long.MIN_VALUE);

    // happy path table - mytable_OFFLINE
    IdealState idealState = _helixResourceManager.getTableIdealState(basicOfflineTable);
    checkSegmentStatusCheckerMetrics(controllerMetrics, basicOfflineTable, idealState, 3, 100, 0, 100);

    // offline segments - table4_OFFLINE
    // 2 replicas available out of 3, percent 66
    idealState = _helixResourceManager.getTableIdealState(errorOfflineTable);
    checkSegmentStatusCheckerMetrics(controllerMetrics, errorOfflineTable, idealState, 2, 66, 0, 100);

    // happy path table - mytable_REALTIME
    idealState = _helixResourceManager.getTableIdealState(basicRealtimeTable);
    checkSegmentStatusCheckerMetrics(controllerMetrics, basicRealtimeTable, idealState, 1, 100, 0, 100);

    // Total metrics
    Assert.assertEquals(controllerMetrics.getValueOfGlobalGauge(ControllerGauge.OFFLINE_TABLE_COUNT), 4);
    Assert.assertEquals(controllerMetrics.getValueOfGlobalGauge(ControllerGauge.REALTIME_TABLE_COUNT), 1);
    Assert.assertEquals(controllerMetrics.getValueOfGlobalGauge(ControllerGauge.DISABLED_TABLE_COUNT), 1);
  }

  private void checkSegmentStatusCheckerMetrics(ControllerMetrics controllerMetrics, String tableName,
      IdealState idealState, long numReplicas, long percentReplicas, long segmentsInErrorState,
      long percentSegmentsAvailable) {
    if (idealState != null) {
      Assert.assertEquals(controllerMetrics.getValueOfTableGauge(tableName, ControllerGauge.IDEALSTATE_ZNODE_SIZE),
          idealState.toString().length());
      Assert.assertEquals(controllerMetrics.getValueOfTableGauge(tableName, ControllerGauge.SEGMENT_COUNT),
          (long) (idealState.getPartitionSet().size()));
    }
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(tableName, ControllerGauge.NUMBER_OF_REPLICAS),
        numReplicas);
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(tableName, ControllerGauge.PERCENT_OF_REPLICAS),
        percentReplicas);
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(tableName, ControllerGauge.SEGMENTS_IN_ERROR_STATE),
        segmentsInErrorState);
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(tableName, ControllerGauge.PERCENT_SEGMENTS_AVAILABLE),
        percentSegmentsAvailable);
  }

  @AfterGroups(groups = "segmentStatusChecker")
  public void afterTestSegmentStatusChecker(ITestContext context) throws Exception {
    String emptyTable = (String) context.getAttribute("emptyTable");
    String disabledOfflineTable = (String) context.getAttribute("disabledOfflineTable");
    String errorOfflineTable = (String) context.getAttribute("errorOfflineTable");
    String basicRealtimeTable = (String) context.getAttribute("basicRealtimeTable");

    dropOfflineTable(emptyTable);
    dropOfflineTable(disabledOfflineTable);
    dropOfflineTable(errorOfflineTable);
    dropOfflineTable(basicRealtimeTable);
  }

  /**
   * Group - realtimeSegmentRelocator - Integration tests for {@link org.apache.pinot.controller.helix.core.relocation.RealtimeSegmentRelocator}
   * @param context
   * @throws Exception
   */
  @BeforeGroups(groups = "realtimeSegmentRelocator", dependsOnGroups = "segmentStatusChecker")
  public void beforeRealtimeSegmentRelocatorTest(ITestContext context) throws Exception {
    String relocationTable = getDefaultRealtimeTableName();
    context.setAttribute("relocationTable", relocationTable);

    // setup default realtime table
    setupRealtimeTable(relocationTable, getKafkaTopic(), _avroFiles.get(0));

    // add tag override for relocation
    TenantConfig tenantConfig = new TenantConfig();
    tenantConfig.setServer(TENANT_NAME);
    tenantConfig.setBroker(TENANT_NAME);
    TagOverrideConfig tagOverrideConfig = new TagOverrideConfig();
    tagOverrideConfig.setRealtimeConsuming(TENANT_NAME + "_REALTIME");
    tagOverrideConfig.setRealtimeCompleted(TENANT_NAME + "_OFFLINE");
    tenantConfig.setTagOverrideConfig(tagOverrideConfig);
    updateRealtimeTableTenant(TableNameBuilder.extractRawTableName(relocationTable), tenantConfig);
  }

  @Test(groups = "realtimeSegmentRelocator", dependsOnGroups = "segmentStatusChecker")
  public void testRealtimeSegmentRelocator(ITestContext context) throws Exception {

    String relocationTable = (String) context.getAttribute("relocationTable");

    ControllerMetrics controllerMetrics = _controllerStarter.getControllerMetrics();

    long taskRunCount = controllerMetrics.getMeteredTableValue("RealtimeSegmentRelocator",
        ControllerMeter.CONTROLLER_PERIODIC_TASK_RUN).count();
    TestUtils.waitForCondition(input ->
        controllerMetrics.getMeteredTableValue("RealtimeSegmentRelocator", ControllerMeter.CONTROLLER_PERIODIC_TASK_RUN)
            .count() > taskRunCount, 60_000, "Timed out waiting for RealtimeSegmentRelocation to run");

    Assert.assertTrue(controllerMetrics.getValueOfGlobalGauge(ControllerGauge.PERIODIC_TASK_NUM_TABLES_PROCESSED,
        "RealtimeSegmentRelocator") > 0);

    // check servers for ONLINE segment and CONSUMING segments are disjoint sets
    Set<String> consuming = new HashSet<>();
    Set<String> completed = new HashSet<>();
    IdealState tableIdealState = _helixResourceManager.getTableIdealState(relocationTable);
    for (String partition : tableIdealState.getPartitionSet()) {
      Map<String, String> instanceStateMap = tableIdealState.getInstanceStateMap(partition);
      if (instanceStateMap.containsValue("CONSUMING")) {
        consuming.addAll(instanceStateMap.keySet());
      }
      if (instanceStateMap.containsValue("ONLINE")) {
        completed.addAll(instanceStateMap.keySet());
      }
    }

    Assert.assertTrue(Collections.disjoint(consuming, completed));
  }

  @AfterGroups(groups = "realtimeSegmentRelocator", dependsOnGroups = "segmentStatusChecker")
  public void afterRealtimeSegmentRelocatorTest(ITestContext context) throws Exception {
    String relocationTable = (String) context.getAttribute("relocationTable");
    dropRealtimeTable(relocationTable);
  }

  @BeforeGroups(groups = "brokerResourceValidationManager", dependsOnGroups = "realtimeSegmentRelocator")
  public void beforeBrokerResourceValidationManagerTest(ITestContext context)
      throws Exception {
    String table1 = "testTable";
    String table2 = "testTable2";
    context.setAttribute("testTableOne", table1);
    context.setAttribute("testTableTwo", table2);
    setupOfflineTable(table1);
  }

  @Test(groups = "brokerResourceValidationManager", dependsOnGroups = "realtimeSegmentRelocator")
  public void testBrokerResourceValidationManager(ITestContext context)
      throws Exception {
    // Check that the first table we added doesn't need to be rebuilt(case where ideal state brokers and brokers in broker resource are the same.
    String table1 = (String) context.getAttribute("testTableOne");
    String table2 = (String) context.getAttribute("testTableTwo");
    TableConfig tableConfigOne = new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE).setTableName(table1).build();
    String partitionNameOne = tableConfigOne.getTableName();

    // Ensure that the broker resource is not rebuilt.
    TestUtils.waitForCondition(input -> {
      IdealState idealState = HelixHelper.getBrokerIdealStates(_helixAdmin, getHelixClusterName());
      return idealState.getInstanceSet(partitionNameOne)
          .equals(_helixResourceManager.getAllInstancesForBrokerTenant(TENANT_NAME));
    }, 60_000L, "Timeout when waiting for broker resource to be rebuilt");

    // Add another table that needs to be rebuilt
    TableConfig offlineTableConfigTwo =
        new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE).setTableName(table2)
            .setBrokerTenant(TENANT_NAME).setServerTenant(TENANT_NAME).build();
    _helixResourceManager.addTable(offlineTableConfigTwo);
    String partitionNameTwo = offlineTableConfigTwo.getTableName();

    // Add a new broker manually such that the ideal state is not updated and ensure that rebuild broker resource is called
    final String brokerId = "Broker_localhost_2";
    InstanceConfig instanceConfig = new InstanceConfig(brokerId);
    instanceConfig.setInstanceEnabled(true);
    instanceConfig.setHostName("Broker_localhost");
    instanceConfig.setPort("2");
    _helixAdmin.addInstance(getHelixClusterName(), instanceConfig);
    _helixAdmin.addInstanceTag(getHelixClusterName(), instanceConfig.getInstanceName(),
        TagNameUtils.getBrokerTagForTenant(TENANT_NAME));

    // Count the number of times we check on ideal state change, which is made by rebuild broker resource method.
    AtomicInteger count = new AtomicInteger();
    TestUtils.waitForCondition(input -> {
      count.getAndIncrement();
      IdealState idealState = HelixHelper.getBrokerIdealStates(_helixAdmin, getHelixClusterName());
      return idealState.getInstanceSet(partitionNameTwo)
          .equals(_helixResourceManager.getAllInstancesForBrokerTenant(TENANT_NAME));
    }, 60_000L, "Timeout when waiting for broker resource to be rebuilt");

    // At least the broker resource won't be changed immediately.
    Assert.assertTrue(count.get() > 1);

    // Drop the instance so that broker resource doesn't match the current one.
    _helixAdmin.dropInstance(getHelixClusterName(), instanceConfig);
    count.set(0);
    TestUtils.waitForCondition(input -> {
      count.getAndIncrement();
      IdealState idealState = HelixHelper.getBrokerIdealStates(_helixAdmin, getHelixClusterName());
      return idealState.getInstanceSet(partitionNameTwo)
          .equals(_helixResourceManager.getAllInstancesForBrokerTenant(TENANT_NAME));
    }, 60_000L, "Timeout when waiting for broker resource to be rebuilt");

    // At least the broker resource won't be changed immediately.
    Assert.assertTrue(count.get() > 1);
  }

  @AfterGroups(groups = "brokerResourceValidationManager", dependsOnGroups = "realtimeSegmentRelocator")
  public void afterBrokerResourceValidationManagerTest(ITestContext context)
      throws Exception {
    String table1 = (String) context.getAttribute("testTableOne");
    String table2 = (String) context.getAttribute("testTableTwo");
    dropOfflineTable(table1);
    dropOfflineTable(table2);
  }

  // TODO: tests for other ControllerPeriodicTasks (RetentionManager, OfflineSegmentIntervalChecker, RealtimeSegmentValidationManager)

  @Override
  protected boolean isUsingNewConfigFormat() {
    return true;
  }

  @Override
  protected boolean useLlc() {
    return true;
  }

  private String getDefaultOfflineTableName() {
    return DEFAULT_TABLE_NAME + "_OFFLINE";
  }

  private String getDefaultRealtimeTableName() {
    return DEFAULT_TABLE_NAME + "_REALTIME";
  }

  /**
   * Tear down the cluster after tests
   * @throws Exception
   */
  @AfterClass
  public void tearDown() throws Exception {
    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }
}
