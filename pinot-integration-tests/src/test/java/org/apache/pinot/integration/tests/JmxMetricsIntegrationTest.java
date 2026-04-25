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

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.commons.io.FileUtils;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Tests that verify JMX metrics emitted by various Pinot components.
 */
public class JmxMetricsIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final Logger LOGGER = LoggerFactory.getLogger(JmxMetricsIntegrationTest.class);

  private static final int NUM_BROKERS = 1;
  private static final int NUM_SERVERS = 1;

  private static final MBeanServer MBEAN_SERVER = ManagementFactory.getPlatformMBeanServer();
  private static final String PINOT_JMX_METRICS_DOMAIN = "\"org.apache.pinot.common.metrics\"";
  private static final String BROKER_METRICS_TYPE = "\"BrokerMetrics\"";
  private static final String SERVER_METRICS_TYPE = "\"ServerMetrics\"";
  private static final String SHARED_TABLE_NAME = "jmx_metrics";

  private File _classTempDir;
  private File _classSegmentDir;
  private File _classTarDir;
  private String _originalMaxServerQueryThreads;
  private boolean _clusterConfigOverrideApplied;

  @BeforeClass
  public void setUp() throws Exception {
    _classTempDir = getClassTempDir();
    _classSegmentDir = new File(_classTempDir, "segmentDir");
    _classTarDir = new File(_classTempDir, "tarDir");
    TestUtils.ensureDirectoriesExistAndEmpty(_classTempDir, _classSegmentDir, _classTarDir);

    // Start the Pinot cluster
    startZk();
    startController();

    // Enable query throttling
    applyClusterConfigOverride();

    startBrokers(NUM_BROKERS);
    startServers(NUM_SERVERS);

    cleanTableAndSchema();

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_classTempDir);

    // Create and upload segments
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _classSegmentDir,
        _classTarDir);
    uploadSegments(getTableName(), _classTarDir);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
    Exception exception = null;
    exception = runCleanup(exception, this::cleanTableAndSchema);
    exception = runCleanup(exception, this::closePinotConnections);
    exception = runCleanup(exception, this::restoreClusterConfigOverride);
    exception = runCleanup(exception, this::stopServer);
    exception = runCleanup(exception, this::stopBroker);
    exception = runCleanup(exception, this::stopControllerIfStarted);
    exception = runCleanup(exception, this::stopZk);
    exception = runCleanup(exception, this::cleanTempDirectory);
    if (exception != null) {
      throw exception;
    }
  }

  @Test
  public void testMultiStageMigrationMetric() throws Exception {
    if (!isMultiStageMigrationMetricEnabled()) {
      throw new SkipException("Broker was not started with multi-stage migration metric enabled");
    }

    ObjectName multiStageMigrationMetric = new ObjectName(PINOT_JMX_METRICS_DOMAIN,
        new Hashtable<>(Map.of("type", BROKER_METRICS_TYPE,
            "name", "\"pinot.broker.singleStageQueriesInvalidMultiStage\"")));

    ObjectName queriesGlobalMetric = new ObjectName(PINOT_JMX_METRICS_DOMAIN,
        new Hashtable<>(Map.of("type", BROKER_METRICS_TYPE,
            "name", "\"pinot.broker.queriesGlobal\"")));

    // Some queries are run during setup to ensure that all the docs are loaded
    long initialQueryCount = getMBeanLongAttribute(queriesGlobalMetric, "Count");
    assertTrue(initialQueryCount > 0L);
    long initialMigrationMetricCount = getMBeanLongAttribute(multiStageMigrationMetric, "Count");

    String tableName = getTableName();
    postQuery("SELECT COUNT(*) FROM " + tableName);

    // Run some queries that are known to not work as is with the multi-stage query engine

    // MV column requires ARRAY_TO_MV wrapper to be used in filter predicates
    JsonNode response = postQuery("SELECT COUNT(*) FROM " + tableName + " WHERE DivAirports = 'JFK'");
    assertFalse(response.get("resultTable").get("rows").isEmpty());

    // Unsupported function
    response = postQuery("SELECT AirlineID, count(*) FROM " + tableName + " WHERE IN_SUBQUERY(airlineID, 'SELECT "
        + "ID_SET(AirlineID) FROM " + tableName + " WHERE Carrier = ''AA''') = 1 GROUP BY AirlineID;");
    assertFalse(response.get("resultTable").get("rows").isEmpty());

    // Repeated columns in an ORDER BY query
    response = postQuery("SELECT AirTime, AirTime FROM " + tableName + " ORDER BY AirTime");
    assertFalse(response.get("resultTable").get("rows").isEmpty());

    assertEquals(getMBeanLongAttribute(queriesGlobalMetric, "Count"), initialQueryCount + 5L);

    AtomicLong multiStageMigrationMetricValue = new AtomicLong();
    long expectedMigrationMetricCount = initialMigrationMetricCount + 3L;
    TestUtils.waitForCondition((aVoid) -> {
      try {
        multiStageMigrationMetricValue.set(getMBeanLongAttribute(multiStageMigrationMetric, "Count"));
        return multiStageMigrationMetricValue.get() == expectedMigrationMetricCount;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 5000, "Expected value of MBean 'pinot.broker.singleStageQueriesInvalidMultiStage' to be: "
        + expectedMigrationMetricCount + "; actual value: " + multiStageMigrationMetricValue.get());

    assertEquals(getMBeanLongAttribute(multiStageMigrationMetric, "Count"), expectedMigrationMetricCount);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testNumGroupsLimitMetrics(boolean useMultiStageEngine) throws Exception {
    ObjectName aggregateTimesNumGroupsLimitReachedMetric = new ObjectName(PINOT_JMX_METRICS_DOMAIN,
        new Hashtable<>(Map.of("type", SERVER_METRICS_TYPE,
            "name", "\"pinot.server.aggregateTimesNumGroupsLimitReached\"")));

    ObjectName aggregateTimesNumGroupsWarningLimitReachedMetric = new ObjectName(PINOT_JMX_METRICS_DOMAIN,
        new Hashtable<>(Map.of("type", SERVER_METRICS_TYPE,
            "name", "\"pinot.server.aggregateTimesNumGroupsWarningLimitReached\"")));

    postQuery("SET useMultiStageEngine=" + useMultiStageEngine + ";SET numGroupsLimit=100;"
        + "SELECT DestState, Dest, count(*) FROM " + getTableName() + " GROUP BY DestState, Dest");
    assertTrue(getMBeanLongAttribute(aggregateTimesNumGroupsLimitReachedMetric, "Count") > 0L);
    assertTrue(getMBeanLongAttribute(aggregateTimesNumGroupsWarningLimitReachedMetric, "Count") > 0L);
  }

  @Test
  public void testEstimatedMseServerThreadsBrokerMetric() throws Exception {
    ObjectName estimatedMseServerThreadsMetric = new ObjectName(PINOT_JMX_METRICS_DOMAIN,
        new Hashtable<>(Map.of("type", BROKER_METRICS_TYPE,
            "name", "\"pinot.broker.estimatedMseServerThreads\"")));

    ExecutorService executorService = Executors.newSingleThreadExecutor();

    try {
      executorService.submit(() -> {
        try {
          postQuery("SET useMultiStageEngine=true; SET timeoutMs=15000; "
              + "SELECT sleep(ActualElapsedTime+5000) FROM " + getTableName()
              + " WHERE ActualElapsedTime > 0 limit 1");
        } catch (Exception e) {
          LOGGER.warn("Caught exception while running query", e);
        }
      });

      TestUtils.waitForCondition((aVoid) -> {
        try {
          return getMBeanLongAttribute(estimatedMseServerThreadsMetric, "Value") > 0L;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }, 5000, "Expected value of MBean 'pinot.broker.estimatedMseServerThreads' to be non-zero");
    } finally {
      executorService.shutdown();
      if (!executorService.awaitTermination(15, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
      }
    }
  }

  @Override
  protected String getTableName() {
    return isSharedRichClusterEnabled() ? SHARED_TABLE_NAME : super.getTableName();
  }

  @Override
  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    brokerConf.setProperty(CommonConstants.Broker.CONFIG_OF_BROKER_ENABLE_MULTISTAGE_MIGRATION_METRIC, "true");
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    serverConf.setProperty(CommonConstants.Server.CONFIG_OF_QUERY_EXECUTOR_NUM_GROUPS_WARN_LIMIT, 1);
  }

  private File getClassTempDir() {
    return isSharedRichClusterEnabled() ? new File(_tempDir, "testData") : _tempDir;
  }

  private void cleanTableAndSchema()
      throws Exception {
    if (_helixResourceManager == null) {
      return;
    }

    String tableName = getTableName();
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    if (_helixResourceManager.getAllTables().contains(offlineTableName) || _helixResourceManager.hasOfflineTable(
        tableName)) {
      dropOfflineTable(tableName);
      waitForTableDataManagerRemoved(offlineTableName);
      waitForEVToDisappear(offlineTableName);
    }
    if (_helixResourceManager.getSchema(tableName) != null) {
      deleteSchema(tableName);
    }
  }

  private void applyClusterConfigOverride() {
    HelixConfigScope scope = getClusterConfigScope();
    ConfigAccessor configAccessor = _helixManager.getConfigAccessor();
    _originalMaxServerQueryThreads =
        configAccessor.get(scope, CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_MAX_SERVER_QUERY_THREADS);
    try {
      configAccessor.set(scope, CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_MAX_SERVER_QUERY_THREADS, "30");
      _clusterConfigOverrideApplied = true;
    } catch (RuntimeException e) {
      restoreClusterConfig(configAccessor, scope,
          CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_MAX_SERVER_QUERY_THREADS,
          _originalMaxServerQueryThreads);
      throw e;
    }
  }

  private void restoreClusterConfigOverride() {
    if (!_clusterConfigOverrideApplied || _helixManager == null) {
      return;
    }

    restoreClusterConfig(_helixManager.getConfigAccessor(), getClusterConfigScope(),
        CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_MAX_SERVER_QUERY_THREADS, _originalMaxServerQueryThreads);
    _clusterConfigOverrideApplied = false;
  }

  private void restoreClusterConfig(ConfigAccessor configAccessor, HelixConfigScope scope, String key,
      String originalValue) {
    if (originalValue == null) {
      configAccessor.remove(scope, key);
    } else {
      configAccessor.set(scope, key, originalValue);
    }
  }

  private HelixConfigScope getClusterConfigScope() {
    return new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER)
        .forCluster(getHelixClusterName())
        .build();
  }

  private boolean isMultiStageMigrationMetricEnabled() {
    return !_brokerStarters.isEmpty() && _brokerStarters.get(0).getConfig()
        .getProperty(CommonConstants.Broker.CONFIG_OF_BROKER_ENABLE_MULTISTAGE_MIGRATION_METRIC,
            CommonConstants.Broker.DEFAULT_ENABLE_MULTISTAGE_MIGRATION_METRIC);
  }

  private long getMBeanLongAttribute(ObjectName objectName, String attribute)
      throws Exception {
    return ((Number) MBEAN_SERVER.getAttribute(objectName, attribute)).longValue();
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

  private void stopControllerIfStarted() {
    if (_controllerStarter != null) {
      stopController();
    }
  }

  private void cleanTempDirectory()
      throws Exception {
    if (_classTempDir != null) {
      FileUtils.deleteDirectory(_classTempDir);
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
