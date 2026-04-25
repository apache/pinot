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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.pinot.client.ResultSetGroup;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Hybrid cluster integration test that uses one of the DateTimeFieldSpec as primary time column
 */
public class DateTimeFieldSpecHybridClusterIntegrationTest extends HybridClusterIntegrationTest {
  private static final String SCHEMA_WITH_DATETIME_FIELDSPEC_NAME =
      "On_Time_On_Time_Performance_2014_100k_subset_nonulls_datetimefieldspecs.schema";
  private static final String SHARED_TABLE_NAME = "datetime_fieldspec_hybrid";
  private static final String SHARED_KAFKA_TOPIC = "datetime-fieldspec-hybrid";
  private static final int NUM_OFFLINE_SEGMENTS = 8;
  private static final int NUM_REALTIME_SEGMENTS = 6;

  private File _classTempDir;
  private File _classSegmentDir;
  private File _classTarDir;

  @Override
  protected String getTableName() {
    return isSharedRichClusterEnabled() ? SHARED_TABLE_NAME : super.getTableName();
  }

  @Override
  protected String getKafkaTopic() {
    return isSharedRichClusterEnabled() ? SHARED_KAFKA_TOPIC : super.getKafkaTopic();
  }

  @Override
  protected String getSchemaFileName() {
    return SCHEMA_WITH_DATETIME_FIELDSPEC_NAME;
  }

  @BeforeClass(alwaysRun = true)
  public void setUp()
      throws Exception {
    _classTempDir = getClassTempDir();
    _classSegmentDir = new File(_classTempDir, "segmentDir");
    _classTarDir = new File(_classTempDir, "tarDir");
    TestUtils.ensureDirectoriesExistAndEmpty(_classTempDir, _classSegmentDir, _classTarDir);

    startHybridClusterForThisTest();
    cleanUpTablesAndSchema();
    resetKafkaTopic();

    List<File> avroFiles = getAllAvroFiles(_classTempDir);
    List<File> offlineAvroFiles = getOfflineAvroFiles(avroFiles, NUM_OFFLINE_SEGMENTS);
    List<File> realtimeAvroFiles = getRealtimeAvroFiles(avroFiles, NUM_REALTIME_SEGMENTS);

    Schema schema = createSchema();
    addSchema(schema);
    TableConfig offlineTableConfig = createOfflineTableConfig();
    addTableConfig(offlineTableConfig);
    addTableConfig(createRealtimeTableConfig(realtimeAvroFiles.get(0)));

    ClusterIntegrationTestUtils.buildSegmentsFromAvro(offlineAvroFiles, offlineTableConfig, schema, 0, _classSegmentDir,
        _classTarDir);
    uploadSegments(getTableName(), _classTarDir);

    pushAvroIntoKafka(realtimeAvroFiles);

    setUpH2Connection(avroFiles);
    setUpQueryGenerator(avroFiles);

    waitForAllDocsLoaded(600_000L);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
    Exception exception = null;
    exception = runCleanup(exception, this::cleanUpTablesAndSchema);
    exception = runCleanup(exception, this::deleteKafkaTopicIfPresent);
    exception = runCleanup(exception, this::closeH2Connection);
    exception = runCleanup(exception, this::closePinotConnections);
    exception = runCleanup(exception, this::stopServer);
    exception = runCleanup(exception, this::stopBroker);
    exception = runCleanup(exception, this::stopController);
    exception = runCleanup(exception, this::stopKafka);
    exception = runCleanup(exception, this::stopZk);
    exception = runCleanup(exception, this::cleanTempDirectory);
    if (exception != null) {
      throw exception;
    }
  }

  @Test
  @Override
  public void testUpdateBrokerResource()
      throws Exception {
    if (isSharedRichClusterEnabled()) {
      throw new SkipException("Skipping broker add/drop coverage in shared rich cluster mode");
    }
    super.testUpdateBrokerResource();
  }

  @Test
  @Override
  public void testInstanceShutdown()
      throws Exception {
    if (isSharedRichClusterEnabled()) {
      throw new SkipException("Skipping instance shutdown coverage in shared rich cluster mode");
    }
    super.testInstanceShutdown();
  }

  @Test
  @Override
  public void testBrokerResponseMetadata()
      throws Exception {
    if (!isSharedRichClusterEnabled()) {
      super.testBrokerResponseMetadata();
      return;
    }

    String[] queries = new String[]{
        "SELECT count(*) FROM " + getTableName(),
        "SELECT count(*) FROM " + getTableName() + " where non_existing_column='non_existing_value'",
        "SELECT count(*) FROM " + getTableName() + "_foo"
    };
    String[] statNames = new String[]{
        "totalDocs", "numServersQueried", "numServersResponded", "numSegmentsQueried", "numSegmentsProcessed",
        "numSegmentsMatched", "numDocsScanned", "totalDocs", "timeUsedMs", "numEntriesScannedInFilter",
        "numEntriesScannedPostFilter"
    };

    for (String query : queries) {
      JsonNode response = postQuery(query);
      for (String statName : statNames) {
        Assert.assertTrue(response.has(statName), "Response does not contain stat: " + statName);
      }
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  @Override
  public void testVirtualColumnQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    if (!isSharedRichClusterEnabled()) {
      super.testVirtualColumnQueries(useMultiStageQueryEngine);
      return;
    }

    ResultSetGroup resultSetGroup = getPinotConnection().execute("select * from " + getTableName());
    for (int i = 0; i < resultSetGroup.getResultSet(0).getColumnCount(); i++) {
      Assert.assertFalse(resultSetGroup.getResultSet(0).getColumnName(i).startsWith("$"));
    }
    getPinotConnection().execute("select $docId, $segmentName, $hostName, $partitionId from " + getTableName());
    getPinotConnection().execute(
        "select $docId, $segmentName, $hostName, $partitionId from " + getTableName() + " where $docId < 5 limit 50");
    getPinotConnection().execute(
        "select $docId, $segmentName, $hostName, $partitionId from " + getTableName() + " where $docId = 5 limit 50");
    getPinotConnection().execute(
        "select $docId, $segmentName, $hostName, $partitionId from " + getTableName()
            + " where $docId > 19998 limit 50");
  }

  @Override
  protected void testQuery(String query)
      throws Exception {
    if (isSharedRichClusterEnabled()) {
      String sharedQuery = rewriteDefaultTableName(query);
      super.testQuery(sharedQuery, sharedQuery);
      return;
    }
    super.testQuery(query);
  }

  @Override
  protected void testQuery(String pinotQuery, String h2Query)
      throws Exception {
    if (isSharedRichClusterEnabled()) {
      super.testQuery(rewriteDefaultTableName(pinotQuery), rewriteDefaultTableName(h2Query));
      return;
    }
    super.testQuery(pinotQuery, h2Query);
  }

  private void startHybridClusterForThisTest()
      throws Exception {
    startZk();
    startController();
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(getHelixClusterName())
            .build();
    _helixManager.getConfigAccessor()
        .set(scope, CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM, Integer.toString(10));
    _helixManager.getConfigAccessor()
        .set(scope, CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM, Integer.toString(6));
    _helixManager.getConfigAccessor()
        .set(scope, CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_DOWNLOAD_PARALLELISM, Integer.toString(12));
    startBroker();
    startServers(NUM_SERVERS);
    startKafka();
    createOrUpdateServerTenant();
  }

  private void createOrUpdateServerTenant()
      throws IOException {
    try {
      createServerTenant(getServerTenant(), NUM_SERVERS_OFFLINE, NUM_SERVERS_REALTIME);
    } catch (IOException e) {
      if (!isSharedRichClusterEnabled()) {
        throw e;
      }
      updateServerTenant(getServerTenant(), NUM_SERVERS_OFFLINE, NUM_SERVERS_REALTIME);
    }
  }

  private List<File> getAllAvroFiles(File outputDir)
      throws Exception {
    int numSegments = unpackAvroData(outputDir).size();
    List<File> avroFiles = new ArrayList<>(numSegments);
    for (int i = 1; i <= numSegments; i++) {
      avroFiles.add(new File(outputDir, "On_Time_On_Time_Performance_2014_" + i + ".avro"));
    }
    return avroFiles;
  }

  private File getClassTempDir() {
    return isSharedRichClusterEnabled() ? new File(_tempDir, "testData") : _tempDir;
  }

  private String rewriteDefaultTableName(String query) {
    return query.replace(DEFAULT_TABLE_NAME, getTableName());
  }

  private void resetKafkaTopic() {
    deleteKafkaTopicIfPresent();
    createKafkaTopic(getKafkaTopic());
  }

  private void cleanUpTablesAndSchema()
      throws Exception {
    if (_helixResourceManager == null) {
      return;
    }

    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(getTableName());
    if (_helixResourceManager.getTableConfig(realtimeTableName) != null
        || _helixResourceManager.hasRealtimeTable(getTableName())) {
      dropRealtimeTable(getTableName());
      waitForTableDataManagerRemoved(realtimeTableName);
      waitForEVToDisappear(realtimeTableName);
    }

    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
    if (_helixResourceManager.getTableConfig(offlineTableName) != null
        || _helixResourceManager.hasOfflineTable(getTableName())) {
      dropOfflineTable(getTableName());
      waitForTableDataManagerRemoved(offlineTableName);
      waitForEVToDisappear(offlineTableName);
    }

    if (_helixResourceManager.getSchema(getTableName()) != null) {
      deleteSchema(getTableName());
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
