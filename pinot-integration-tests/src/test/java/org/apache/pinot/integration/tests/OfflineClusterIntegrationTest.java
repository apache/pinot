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
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.http.message.BasicNameValuePair;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.pinot.client.PinotConnection;
import org.apache.pinot.client.PinotDriver;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.response.server.TableIndexMetadataResponse;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.core.operator.query.NonScanBasedAggregationOperator;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.FieldConfig.CompressionCodec;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.QueryConfig;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.InstanceTypeUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.intellij.lang.annotations.Language;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.common.function.scalar.StringFunctions.*;
import static org.apache.pinot.controller.helix.core.PinotHelixResourceManager.EXTERNAL_VIEW_CHECK_INTERVAL_MS;
import static org.apache.pinot.controller.helix.core.PinotHelixResourceManager.EXTERNAL_VIEW_ONLINE_SEGMENTS_MAX_WAIT_MS;
import static org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey.SKIP_INDEXES;
import static org.testng.Assert.*;


/**
 * Integration test that converts Avro data for 12 segments and runs queries against it.
 */
public class OfflineClusterIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final int NUM_BROKERS = 1;
  private static final int NUM_SERVERS = 1;
  private static final int NUM_SEGMENTS = 12;
  private static final String SEGMENT_UPLOAD_TEST_TABLE = "segmentUploadTestTable";

  // For table config refresh test, make an expensive query to ensure the query won't finish in 5ms
  private static final String TEST_TIMEOUT_QUERY =
      "SELECT DISTINCTCOUNT(AirlineID) FROM mytable GROUP BY Carrier LIMIT 10000";

  // For inverted index triggering test
  private static final List<String> UPDATED_INVERTED_INDEX_COLUMNS =
      List.of("FlightNum", "Origin", "Quarter", "DivActualElapsedTime");
  private static final String TEST_UPDATED_INVERTED_INDEX_QUERY =
      "SELECT COUNT(*) FROM mytable WHERE DivActualElapsedTime = 305";

  // For range index triggering test
  private static final List<String> UPDATED_RANGE_INDEX_COLUMNS = List.of("DivActualElapsedTime");
  private static final String TEST_UPDATED_RANGE_INDEX_QUERY =
      "SELECT COUNT(*) FROM mytable WHERE DivActualElapsedTime > 305";

  // For bloom filter triggering test
  private static final List<String> UPDATED_BLOOM_FILTER_COLUMNS = List.of("Carrier");
  private static final String TEST_UPDATED_BLOOM_FILTER_QUERY = "SELECT COUNT(*) FROM mytable WHERE Carrier = 'CA'";

  // For star-tree triggering test
  private static final StarTreeIndexConfig STAR_TREE_INDEX_CONFIG_1 =
      new StarTreeIndexConfig(List.of("Carrier"), null, List.of(AggregationFunctionColumnPair.COUNT_STAR_NAME), null,
          100);
  private static final String TEST_STAR_TREE_QUERY_1 = "SELECT COUNT(*) FROM mytable WHERE Carrier = 'UA'";
  private static final String TEST_STAR_TREE_QUERY_1_FILTER_INVERT =
      "SELECT COUNT(*) FILTER (WHERE Carrier = 'UA') FROM mytable";
  private static final StarTreeIndexConfig STAR_TREE_INDEX_CONFIG_2 =
      new StarTreeIndexConfig(List.of("DestState"), null, List.of(AggregationFunctionColumnPair.COUNT_STAR_NAME), null,
          100);
  private static final String TEST_STAR_TREE_QUERY_2 = "SELECT COUNT(*) FROM mytable WHERE DestState = 'CA'";
  private static final StarTreeIndexConfig STAR_TREE_INDEX_CONFIG_3 =
      new StarTreeIndexConfig(List.of("Carrier", "NewInt", "DerivedAirTime"), null,
          List.of(AggregationFunctionColumnPair.COUNT_STAR_NAME), null, 1);
  private static final String TEST_STAR_TREE_QUERY_3 =
      "SELECT COUNT(*) FROM mytable WHERE Carrier = 'UA' AND NewInt = 1 AND DerivedAirTime >= 2";
  private static final String TEST_STAR_TREE_QUERY_3_REFERENCE =
      "SELECT COUNT(*) FROM mytable WHERE Carrier = 'UA' AND AirTime >= 120";
  private static final int EXPECTED_NUM_DOCS_SCANNED_STAR_TREE_QUERY_3 = 87;

  // For default columns test
  private static final String TEST_EXTRA_COLUMNS_QUERY = "SELECT COUNT(*) FROM mytable WHERE NewAddedIntMetric = 1";
  private static final String TEST_REGULAR_COLUMNS_QUERY = "SELECT COUNT(*) FROM mytable WHERE AirlineID > 0";
  private static final String SELECT_STAR_QUERY = "SELECT * FROM mytable";

  private static final String DISK_SIZE_IN_BYTES_KEY = "diskSizeInBytes";
  private static final String NUM_SEGMENTS_KEY = "numSegments";
  private static final String NUM_ROWS_KEY = "numRows";
  private static final String COLUMN_LENGTH_MAP_KEY = "columnLengthMap";
  private static final String COLUMN_CARDINALITY_MAP_KEY = "columnCardinalityMap";
  private static final String MAX_NUM_MULTI_VALUES_MAP_KEY = "maxNumMultiValuesMap";
  private static final int NUM_ROWS = 115545;

  private final List<ServiceStatus.ServiceStatusCallback> _serviceStatusCallbacks =
      new ArrayList<>(getNumBrokers() + getNumServers());

  private TableConfig _tableConfig;
  private Schema _schema;

  // Store the table size. Table size is platform dependent because of the native library used by the ChunkCompressor.
  // Once this value is set, assert that table size always gets back to this value after removing the added indices.
  private long _tableSize;

  protected int getNumBrokers() {
    return NUM_BROKERS;
  }

  protected int getNumServers() {
    return NUM_SERVERS;
  }

  /// Create inverted index when generating the segment to ensure the index ordering won't change when re-assembling
  /// them, so that the table size is consistent.
  @Override
  protected boolean isCreateInvertedIndexDuringSegmentGeneration() {
    return true;
  }

  @Override
  protected List<FieldConfig> getFieldConfigs() {
    List<FieldConfig> fieldConfigs = new ArrayList<>();
    fieldConfigs.add(
        new FieldConfig("DivAirports", FieldConfig.EncodingType.DICTIONARY, List.of(), CompressionCodec.MV_ENTRY_DICT,
            null));
    return fieldConfigs;
  }

  @Override
  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    super.overrideBrokerConf(brokerConf);
    brokerConf.setProperty(CommonConstants.Broker.CONFIG_OF_BROKER_ENABLE_QUERY_CANCELLATION, "true");
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    super.overrideServerConf(serverConf);
    serverConf.setProperty(CommonConstants.Server.CONFIG_OF_ENABLE_QUERY_CANCELLATION, "true");
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(getHelixClusterName())
            .build();
    // Set hyperloglog log2m value to 12.
    _helixManager.getConfigAccessor()
        .set(scope, CommonConstants.Helix.DEFAULT_HYPERLOGLOG_LOG2M_KEY, Integer.toString(12));
    // Set max segment preprocess parallelism to 8 to test that all segments can be processed
    _helixManager.getConfigAccessor()
        .set(scope, CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM, Integer.toString(8));
    // Set max segment startree preprocess parallelism to 6 to test that all segments can be processed
    _helixManager.getConfigAccessor()
        .set(scope, CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM, Integer.toString(6));
    // Set max segment download parallelism to 8 to test that all segments can be processed
    _helixManager.getConfigAccessor()
        .set(scope, CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_DOWNLOAD_PARALLELISM, Integer.toString(8));
    startBrokers();
    startServers();

    // Create and upload the schema and table config
    _schema = createSchema();
    addSchema(_schema);
    _tableConfig = createOfflineTableConfig();
    addTableConfig(_tableConfig);

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create and upload segments. For exhaustive testing, concurrently upload multiple segments with the same name
    // and validate correctness with parallel push protection enabled.
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, _tableConfig, _schema, 0, _segmentDir, _tarDir);
    // Create a copy of _tarDir to create multiple segments with the same name.
    File tarDir2 = new File(_tempDir, "tarDir2");
    FileUtils.copyDirectory(_tarDir, tarDir2);

    List<File> tarDirs = new ArrayList<>();
    tarDirs.add(_tarDir);
    tarDirs.add(tarDir2);
    try {
      uploadSegments(getTableName(), TableType.OFFLINE, tarDirs);
    } catch (Exception e) {
      // If enableParallelPushProtection is enabled and the same segment is uploaded concurrently, we could get one
      // of the three exception:
      //   - 409 conflict of the second call enters ProcessExistingSegment
      //   - segmentZkMetadata creation failure if both calls entered ProcessNewSegment
      //   - Failed to copy segment tar file to final location due to the same segment pushed twice concurrently
      // In such cases we upload all the segments again to ensure that the data is set up correctly.
      assertTrue(e.getMessage().contains("Another segment upload is in progress for segment") || e.getMessage()
          .contains("Failed to create ZK metadata for segment") || e.getMessage()
          .contains("java.nio.file.FileAlreadyExistsException"), e.getMessage());
      uploadSegments(getTableName(), _tarDir);
    }

    // Set up the H2 connection
    setUpH2Connection(avroFiles);

    // Initialize the query generator
    setUpQueryGenerator(avroFiles);

    // Set up service status callbacks
    // NOTE: put this step after creating the table and uploading all segments so that brokers and servers can find the
    // resources to monitor
    registerCallbackHandlers();

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);

    _tableSize = getTableSize(getTableName());
  }

  protected void startBrokers()
      throws Exception {
    startBrokers(getNumBrokers());
  }

  protected void startServers()
      throws Exception {
    startServers(getNumServers());
  }

  private void registerCallbackHandlers() {
    List<String> instances = _helixAdmin.getInstancesInCluster(getHelixClusterName());
    instances.removeIf(
        instanceId -> !InstanceTypeUtils.isBroker(instanceId) && !InstanceTypeUtils.isServer(instanceId));
    List<String> resourcesInCluster = _helixAdmin.getResourcesInCluster(getHelixClusterName());
    resourcesInCluster.removeIf(resource -> (!TableNameBuilder.isTableResource(resource)
        && !CommonConstants.Helix.BROKER_RESOURCE_INSTANCE.equals(resource)));
    for (String instance : instances) {
      List<String> resourcesToMonitor = new ArrayList<>();
      for (String resourceName : resourcesInCluster) {
        IdealState idealState = _helixAdmin.getResourceIdealState(getHelixClusterName(), resourceName);
        for (String partitionName : idealState.getPartitionSet()) {
          if (idealState.getInstanceSet(partitionName).contains(instance)) {
            resourcesToMonitor.add(resourceName);
            break;
          }
        }
      }
      _serviceStatusCallbacks.add(new ServiceStatus.MultipleCallbackServiceStatusCallback(ImmutableList.of(
          new ServiceStatus.IdealStateAndCurrentStateMatchServiceStatusCallback(_helixManager, getHelixClusterName(),
              instance, resourcesToMonitor, 100.0),
          new ServiceStatus.IdealStateAndExternalViewMatchServiceStatusCallback(_helixManager, getHelixClusterName(),
              instance, resourcesToMonitor, 100.0))));
    }
  }

  /// Ensure table is at the same state after the test.
  @AfterMethod
  public void checkTableSetup()
      throws IOException {
    assertEquals(getOfflineTableConfig(), _tableConfig);
    assertEquals(getSchema(getTableName()), _schema);
    assertEquals(getTableSize(getTableName()), _tableSize);
  }

  private void reloadAllSegments(String testQuery, boolean forceDownload, long numTotalDocs)
      throws IOException {
    // Try to refresh all the segments again with force download from the controller URI.
    String reloadJob = reloadTableAndValidateResponse(getTableName(), TableType.OFFLINE, forceDownload);
    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode queryResponse = postQuery(testQuery);
        if (!queryResponse.get("exceptions").isEmpty()) {
          return false;
        }
        // Total docs should not change during reload
        assertEquals(queryResponse.get("totalDocs").asLong(), numTotalDocs);
        return isReloadJobCompleted(reloadJob);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 600_000L, "Failed to reload table with force download");
  }

  private void testQueryError(@Language("sql") String query, QueryErrorCode errorCode)
      throws Exception {
    assertQuery(query).firstException().hasErrorCode(errorCode);
  }

  @Test
  public void testInstancesStarted() {
    assertEquals(_serviceStatusCallbacks.size(), getNumBrokers() + getNumServers());
    for (ServiceStatus.ServiceStatusCallback serviceStatusCallback : _serviceStatusCallbacks) {
      assertEquals(serviceStatusCallback.getServiceStatus(), ServiceStatus.Status.GOOD);
    }
  }

  @Test
  public void testInvalidTableConfig() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("badTable").build();
    ObjectNode tableConfigJson = (ObjectNode) tableConfig.toJsonNode();
    // Remove a mandatory field
    tableConfigJson.remove(TableConfig.VALIDATION_CONFIG_KEY);
    try {
      sendPostRequest(_controllerRequestURLBuilder.forTableCreate(), tableConfigJson.toString());
      fail();
    } catch (IOException e) {
      // Should get response code 400 (BAD_REQUEST)
      assertTrue(e.getMessage().contains("Got error status code: 400"));
    }
  }

  @Test
  public void testRefreshTableConfigAndQueryTimeout()
      throws Exception {
    // Set timeout as 5ms so that query will timeout
    TableConfig tableConfig = createOfflineTableConfig();
    tableConfig.setQueryConfig(new QueryConfig(5L, null, null, null, null, null));
    updateTableConfig(tableConfig);

    // Wait for at most 1 minute for broker to receive and process the table config refresh message
    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode queryResponse = postQuery(TEST_TIMEOUT_QUERY);
        JsonNode exceptions = queryResponse.get("exceptions");
        if (exceptions.isEmpty()) {
          return false;
        }
        int errorCode = exceptions.get(0).get("errorCode").asInt();
        if (errorCode == QueryErrorCode.BROKER_TIMEOUT.getId()) {
          // Timed out on broker side
          return true;
        }
        if (errorCode == QueryErrorCode.SERVER_NOT_RESPONDING.getId()) {
          // Timed out on server side
          int numServersQueried = queryResponse.get("numServersQueried").asInt();
          int numServersResponded = queryResponse.get("numServersResponded").asInt();
          int numDocsScanned = queryResponse.get("numDocsScanned").asInt();
          return numServersQueried == getNumServers() && numServersResponded == 0 && numDocsScanned == 0;
        }
        return false;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 60_000L, "Failed to refresh table config");

    // Remove timeout so that query will finish
    updateTableConfig(_tableConfig);

    // Wait for at most 1 minute for broker to receive and process the table config refresh message
    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode queryResponse = postQuery(TEST_TIMEOUT_QUERY);
        JsonNode exceptions = queryResponse.get("exceptions");
        if (!exceptions.isEmpty()) {
          return false;
        }
        int numServersQueried = queryResponse.get("numServersQueried").asInt();
        int numServersResponded = queryResponse.get("numServersResponded").asInt();
        int numDocsScanned = queryResponse.get("numDocsScanned").asInt();
        return numServersQueried == getNumServers() && numServersResponded == getNumServers()
            && numDocsScanned == getCountStarResult();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 60_000L, "Failed to refresh table config");
  }

  @Test
  public void testUploadSameSegments()
      throws Exception {
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
    SegmentZKMetadata segmentZKMetadata = _helixResourceManager.getSegmentsZKMetadata(offlineTableName).get(0);
    String segmentName = segmentZKMetadata.getSegmentName();
    long crc = segmentZKMetadata.getCrc();
    // Creation time is when the segment gets created
    long creationTime = segmentZKMetadata.getCreationTime();
    // Push time is when the segment gets first pushed (new segment)
    long pushTime = segmentZKMetadata.getPushTime();
    // Refresh time is when the segment gets refreshed (existing segment)
    long refreshTime = segmentZKMetadata.getRefreshTime();

    uploadSegments(offlineTableName, _tarDir);
    for (SegmentZKMetadata segmentZKMetadataAfterUpload : _helixResourceManager.getSegmentsZKMetadata(
        offlineTableName)) {
      // Only check one segment
      if (segmentZKMetadataAfterUpload.getSegmentName().equals(segmentName)) {
        assertEquals(segmentZKMetadataAfterUpload.getCrc(), crc);
        assertEquals(segmentZKMetadataAfterUpload.getCreationTime(), creationTime);
        assertEquals(segmentZKMetadataAfterUpload.getPushTime(), pushTime);
        // Refresh time should change
        assertTrue(segmentZKMetadataAfterUpload.getRefreshTime() > refreshTime);
        return;
      }
    }
  }

  @Test
  public void testUploadSegmentRefreshOnly()
      throws Exception {
    Schema schema = createSchema();
    schema.setSchemaName(SEGMENT_UPLOAD_TEST_TABLE);
    addSchema(schema);
    TableConfig segmentUploadTestTableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(SEGMENT_UPLOAD_TEST_TABLE)
            .setTimeColumnName(getTimeColumnName()).setSortedColumn(getSortedColumn())
            .setInvertedIndexColumns(getInvertedIndexColumns()).setNoDictionaryColumns(getNoDictionaryColumns())
            .setRangeIndexColumns(getRangeIndexColumns()).setBloomFilterColumns(getBloomFilterColumns())
            .setFieldConfigList(getFieldConfigs()).setNumReplicas(getNumReplicas())
            .setSegmentVersion(getSegmentVersion()).setLoadMode(getLoadMode()).setTaskConfig(getTaskConfig())
            .setBrokerTenant(getBrokerTenant()).setServerTenant(getServerTenant())
            .setIngestionConfig(getIngestionConfig()).setNullHandlingEnabled(getNullHandlingEnabled()).build();
    addTableConfig(segmentUploadTestTableConfig);
    String offlineTableName = segmentUploadTestTableConfig.getTableName();
    File[] segmentTarFiles = _tarDir.listFiles();
    assertNotNull(segmentTarFiles);
    int numSegments = segmentTarFiles.length;
    assertTrue(numSegments > 0);
    List<Header> headers = new ArrayList<>();
    headers.add(new BasicHeader(FileUploadDownloadClient.CustomHeaders.REFRESH_ONLY, "true"));
    List<NameValuePair> parameters = new ArrayList<>();
    NameValuePair tableNameParameter = new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.TABLE_NAME,
        TableNameBuilder.extractRawTableName(offlineTableName));
    parameters.add(tableNameParameter);

    URI uploadSegmentHttpURI = URI.create(getControllerRequestURLBuilder().forSegmentUpload());
    try (FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient()) {
      // Refresh non-existing segment
      File segmentTarFile = segmentTarFiles[0];
      try {
        fileUploadDownloadClient.uploadSegment(uploadSegmentHttpURI, segmentTarFile.getName(), segmentTarFile, headers,
            parameters, HttpClient.DEFAULT_SOCKET_TIMEOUT_MS);
        fail();
      } catch (HttpErrorStatusException e) {
        assertEquals(e.getStatusCode(), HttpStatus.SC_GONE);
        assertTrue(_helixResourceManager.getSegmentsZKMetadata(SEGMENT_UPLOAD_TEST_TABLE).isEmpty());
      }

      // Upload segment
      SimpleHttpResponse response =
          fileUploadDownloadClient.uploadSegment(uploadSegmentHttpURI, segmentTarFile.getName(), segmentTarFile, null,
              parameters, HttpClient.DEFAULT_SOCKET_TIMEOUT_MS);
      assertEquals(response.getStatusCode(), HttpStatus.SC_OK);
      List<SegmentZKMetadata> segmentsZKMetadata = _helixResourceManager.getSegmentsZKMetadata(offlineTableName);
      assertEquals(segmentsZKMetadata.size(), 1);

      // Refresh existing segment
      response = fileUploadDownloadClient.uploadSegment(uploadSegmentHttpURI, segmentTarFile.getName(), segmentTarFile,
          headers, parameters, HttpClient.DEFAULT_SOCKET_TIMEOUT_MS);
      assertEquals(response.getStatusCode(), HttpStatus.SC_OK);
      segmentsZKMetadata = _helixResourceManager.getSegmentsZKMetadata(offlineTableName);
      assertEquals(segmentsZKMetadata.size(), 1);
      assertNotEquals(segmentsZKMetadata.get(0).getRefreshTime(), Long.MIN_VALUE);
    }
    waitForNumOfSegmentsBecomeOnline(offlineTableName, 1);
    dropOfflineTable(SEGMENT_UPLOAD_TEST_TABLE);
    deleteSchema(SEGMENT_UPLOAD_TEST_TABLE);
    waitForEVToDisappear(offlineTableName);
  }

  private void waitForNumOfSegmentsBecomeOnline(String tableNameWithType, int numSegments)
      throws InterruptedException, TimeoutException {
    long endTimeMs = System.currentTimeMillis() + EXTERNAL_VIEW_ONLINE_SEGMENTS_MAX_WAIT_MS;
    do {
      Set<String> onlineSegments = _helixResourceManager.getOnlineSegmentsFromExternalView(tableNameWithType);
      if (onlineSegments.size() == numSegments) {
        return;
      }
      Thread.sleep(EXTERNAL_VIEW_CHECK_INTERVAL_MS);
    } while (System.currentTimeMillis() < endTimeMs);
    throw new TimeoutException("Time out while waiting segments become ONLINE. (tableNameWithType = "
        + tableNameWithType + ")");
  }

  @Test
  public void testInvertedIndexTriggering()
      throws Exception {
    long numTotalDocs = getCountStarResult();

    // Without index on DivActualElapsedTime, all docs are scanned at filtering stage.
    assertEquals(postQuery(TEST_UPDATED_INVERTED_INDEX_QUERY).get("numEntriesScannedInFilter").asLong(), numTotalDocs);

    addInvertedIndex();
    String rawTableName = getTableName();
    long tableSizeWithNewIndex = getTableSize(rawTableName);

    // Update table config to remove the new inverted index, and check if the new inverted index is removed
    updateTableConfig(_tableConfig);
    reloadAllSegments(TEST_UPDATED_INVERTED_INDEX_QUERY, false, numTotalDocs);
    assertEquals(postQuery(TEST_UPDATED_INVERTED_INDEX_QUERY).get("numEntriesScannedInFilter").asLong(), numTotalDocs);
    assertEquals(getTableSize(rawTableName), _tableSize);

    // Add the inverted index back to test index removal via force download.
    addInvertedIndex();
    assertEquals(getTableSize(rawTableName), tableSizeWithNewIndex);

    // Update table config to remove the new inverted index.
    updateTableConfig(_tableConfig);

    // Force to download a single segment, and disk usage should drop a bit.
    SegmentZKMetadata segmentZKMetadata =
        _helixResourceManager.getSegmentsZKMetadata(TableNameBuilder.OFFLINE.tableNameWithType(rawTableName)).get(0);
    String segmentName = segmentZKMetadata.getSegmentName();
    reloadOfflineSegment(rawTableName, segmentName, true);
    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode queryResponse = postQuery(TEST_UPDATED_INVERTED_INDEX_QUERY);
        // Total docs should not change during reload
        assertEquals(queryResponse.get("totalDocs").asLong(), numTotalDocs);
        // If the segment got reloaded, the query should scan its docs.
        return queryResponse.get("numEntriesScannedInFilter").asLong() > 0;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 600_000L, "Failed to clean up obsolete index in segment");
    // As query behavior changed, the segment reload must have been done. The new table size should be like below,
    // with only one segment being reloaded with force download and dropping the inverted index.
    long tableSizeAfterReloadSegment = getTableSize(rawTableName);
    assertTrue(tableSizeAfterReloadSegment > _tableSize && tableSizeAfterReloadSegment < tableSizeWithNewIndex,
        "Table size: " + tableSizeAfterReloadSegment + " should be between " + _tableSize + " and "
            + tableSizeWithNewIndex + " after dropping inverted index from segment: " + segmentName);

    // Add inverted index back to check if reloading whole table with force download works.
    // Note that because we have force downloaded a segment above, it's important to reset the table state by adding
    // the inverted index back before check if reloading whole table with force download works. Otherwise, the query's
    // numEntriesScannedInFilter can become numTotalDocs sooner than expected, while the segment (reloaded by the test
    // above) is being reloaded again, causing the table size smaller than expected.
    //
    // As to why the table size could be smaller than expected, it's because when reloading a segment with force
    // download, the original segment dir is deleted and then replaced with the newly downloaded segment, leaving a
    // small chance of race condition between getting table size check and replacing the segment dir, i.e. flaky test.
    addInvertedIndex();
    assertEquals(getTableSize(rawTableName), tableSizeWithNewIndex);

    // Force to download the whole table and use the original table config, so the disk usage should get back to
    // initial value.
    updateTableConfig(_tableConfig);
    reloadAllSegments(TEST_UPDATED_INVERTED_INDEX_QUERY, true, numTotalDocs);
    assertEquals(postQuery(TEST_UPDATED_INVERTED_INDEX_QUERY).get("numEntriesScannedInFilter").asLong(), numTotalDocs);
  }

  private void addInvertedIndex(boolean shouldReload)
      throws Exception {
    // Update table config to add inverted index on DivActualElapsedTime column, and
    // reload the table to get config change into effect and add the inverted index.
    TableConfig tableConfig = createOfflineTableConfig();
    tableConfig.getIndexingConfig().setInvertedIndexColumns(UPDATED_INVERTED_INDEX_COLUMNS);
    updateTableConfig(tableConfig);

    // It takes a while to reload multiple segments, thus we retry the query for some time.
    // After all segments are reloaded, the inverted index is added on DivActualElapsedTime.
    // It's expected to have numEntriesScannedInFilter equal to 0, i.e. no docs is scanned
    // at filtering stage when inverted index can answer the predicate directly.
    if (shouldReload) {
      reloadAllSegments(TEST_UPDATED_INVERTED_INDEX_QUERY, false, getCountStarResult());
      assertEquals(postQuery(TEST_UPDATED_INVERTED_INDEX_QUERY).get("numEntriesScannedInFilter").asLong(), 0L);
    }
  }

  private void addInvertedIndex()
      throws Exception {
    addInvertedIndex(true);
  }

  private void addRangeIndex(boolean shouldReload)
      throws Exception {
    // Update table config to add Range index on DivActualElapsedTime column, and
    // reload the table to get config change into effect and add the Range index.
    TableConfig tableConfig = createOfflineTableConfig();
    tableConfig.getIndexingConfig().setRangeIndexColumns(UPDATED_RANGE_INDEX_COLUMNS);
    updateTableConfig(tableConfig);

    // It takes a while to reload multiple segments, thus we retry the query for some time.
    // After all segments are reloaded, the range index is added on DivActualElapsedTime.
    // It's expected to have numEntriesScannedInFilter equal to 0, i.e. no docs is scanned
    // at filtering stage when inverted index can answer the predicate directly.
    if (shouldReload) {
      reloadAllSegments(TEST_UPDATED_RANGE_INDEX_QUERY, false, getCountStarResult());
      assertEquals(postQuery(TEST_UPDATED_RANGE_INDEX_QUERY).get("numEntriesScannedInFilter").asLong(), 0L);
    }
  }

  private void addRangeIndex()
      throws Exception {
    addRangeIndex(true);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testTimeFunc(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String sqlQuery = "SELECT toDateTime(now(), 'yyyy-MM-dd z'), toDateTime(ago('PT1H'), 'yyyy-MM-dd z') FROM mytable";
    JsonNode response = postQuery(sqlQuery);
    String todayStr = response.get("resultTable").get("rows").get(0).get(0).asText();
    String expectedTodayStr =
        Instant.now().atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd z"));
    assertEquals(todayStr, expectedTodayStr);

    String oneHourAgoTodayStr = response.get("resultTable").get("rows").get(0).get(1).asText();
    String expectedOneHourAgoTodayStr = Instant.now().minus(Duration.parse("PT1H")).atZone(ZoneId.of("UTC"))
        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd z"));
    assertEquals(oneHourAgoTodayStr, expectedOneHourAgoTodayStr);
  }

  @Test
  public void testMaxServerResponseSizeQueryOption()
      throws Exception {
    String queryWithOption = "SET maxServerResponseSizeBytes=1000; " + SELECT_STAR_QUERY;
    JsonNode response = postQuery(queryWithOption);
    JsonNode exceptions = response.get("exceptions");
    assertFalse(exceptions.isEmpty());
    int errorCode = exceptions.get(0).get("errorCode").asInt();
    assertEquals(errorCode, QueryErrorCode.QUERY_CANCELLATION.getId());
  }

  @Test
  public void testMaxQueryResponseSizeQueryOption()
      throws Exception {
    String queryWithOption = "SET maxQueryResponseSizeBytes=1000; " + SELECT_STAR_QUERY;
    JsonNode response = postQuery(queryWithOption);
    JsonNode exceptions = response.get("exceptions");
    assertFalse(exceptions.isEmpty());
    int errorCode = exceptions.get(0).get("errorCode").asInt();
    assertEquals(errorCode, QueryErrorCode.QUERY_CANCELLATION.getId());
  }

  @Test
  public void testMaxQueryResponseSizeTableConfig()
      throws Exception {
    TableConfig tableConfig = createOfflineTableConfig();
    tableConfig.setQueryConfig(new QueryConfig(null, false, null, null, 1000L, null));
    updateTableConfig(tableConfig);

    TestUtils.waitForCondition(aVoid -> {
      try {
        // Server should return exception after the table config is picked up
        JsonNode response = postQuery(SELECT_STAR_QUERY);
        JsonNode exceptions = response.get("exceptions");
        return !exceptions.isEmpty()
            && exceptions.get(0).get("errorCode").asInt() == QueryErrorCode.QUERY_CANCELLATION.getId();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 60_000L, "Failed to execute query");

    updateTableConfig(_tableConfig);

    TestUtils.waitForCondition(aVoid -> {
      try {
        // Server should not return exception after the table config is picked up
        JsonNode response = postQuery(SELECT_STAR_QUERY);
        return response.get("exceptions").isEmpty();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 60_000L, "Failed to execute query");
  }

  @Test
  public void testMaxServerResponseSizeTableConfig()
      throws Exception {
    TableConfig tableConfig = createOfflineTableConfig();
    tableConfig.setQueryConfig(new QueryConfig(null, false, null, null, null, 1000L));
    updateTableConfig(tableConfig);

    TestUtils.waitForCondition(aVoid -> {
      try {
        // Server should return exception after the table config is picked up
        JsonNode response = postQuery(SELECT_STAR_QUERY);
        JsonNode exceptions = response.get("exceptions");
        return !exceptions.isEmpty()
            && exceptions.get(0).get("errorCode").asInt() == QueryErrorCode.QUERY_CANCELLATION.getId();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 60_000L, "Failed to execute query");

    updateTableConfig(_tableConfig);

    TestUtils.waitForCondition(aVoid -> {
      try {
        // Server should not return exception after the table config is picked up
        JsonNode response = postQuery(SELECT_STAR_QUERY);
        return response.get("exceptions").isEmpty();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 60_000L, "Failed to execute query");
  }

  @Test
  public void testMaxResponseSizeTableConfigOrdering()
      throws Exception {
    TableConfig tableConfig = createOfflineTableConfig();
    tableConfig.setQueryConfig(new QueryConfig(null, false, null, null, 1000000L, 1000L));
    updateTableConfig(tableConfig);

    TestUtils.waitForCondition(aVoid -> {
      try {
        // Server should return exception after the table config is picked up
        JsonNode response = postQuery(SELECT_STAR_QUERY);
        JsonNode exceptions = response.get("exceptions");
        return !exceptions.isEmpty()
            && exceptions.get(0).get("errorCode").asInt() == QueryErrorCode.QUERY_CANCELLATION.getId();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 60_000L, "Failed to execute query");

    updateTableConfig(_tableConfig);

    TestUtils.waitForCondition(aVoid -> {
      try {
        // Server should not return exception after the table config is picked up
        JsonNode response = postQuery(SELECT_STAR_QUERY);
        return response.get("exceptions").isEmpty();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 60_000L, "Failed to execute query");
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testRegexpReplace(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    // Correctness tests of regexpReplace.

    // Test replace all.
    String sqlQuery = "SELECT regexpReplace('CA', 'C', 'TEST')";
    JsonNode response = postQuery(sqlQuery);
    String result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "TESTA");

    sqlQuery = "SELECT regexpReplace('foobarbaz', 'b', 'X')";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "fooXarXaz");

    sqlQuery = "SELECT regexpReplace('foobarbaz', 'b', 'XY')";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "fooXYarXYaz");

    sqlQuery = "SELECT regexpReplace('Argentina', '(.)', '$1 ')";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "A r g e n t i n a ");

    sqlQuery = "SELECT regexpReplace('Pinot is  blazing  fast', '( ){2,}', ' ')";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "Pinot is blazing fast");

    sqlQuery = "SELECT regexpReplace('healthy, wealthy, and wise','\\w+thy', 'something')";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "something, something, and wise");

    sqlQuery = "SELECT regexpReplace('11234567898','(\\d)(\\d{3})(\\d{3})(\\d{4})', '$1-($2) $3-$4')";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "1-(123) 456-7898");

    // Test replace starting at index.

    sqlQuery = "SELECT regexpReplace('healthy, wealthy, stealthy and wise','\\w+thy', 'something', 4)";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "healthy, something, something and wise");

    sqlQuery = "SELECT regexpReplace('healthy, wealthy, stealthy and wise','\\w+thy', 'something', 1)";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "hsomething, something, something and wise");

    // Test occurence
    sqlQuery = "SELECT regexpReplace('healthy, wealthy, stealthy and wise','\\w+thy', 'something', 0, 2)";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "healthy, wealthy, something and wise");

    sqlQuery = "SELECT regexpReplace('healthy, wealthy, stealthy and wise','\\w+thy', 'something', 0, 0)";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "something, wealthy, stealthy and wise");

    // Test flags
    sqlQuery = "SELECT regexpReplace('healthy, wealthy, stealthy and wise','\\w+tHy', 'something', 0, 0, 'i')";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "something, wealthy, stealthy and wise");

    // Negative test. Pattern match not found.
    sqlQuery = "SELECT regexpReplace('healthy, wealthy, stealthy and wise','\\w+tHy', 'something')";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "healthy, wealthy, stealthy and wise");

    // Negative test. Pattern match not found.
    sqlQuery = "SELECT regexpReplace('healthy, wealthy, stealthy and wise','\\w+tHy', 'something', 3, 21, 'i')";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "healthy, wealthy, stealthy and wise");

    // Negative test - incorrect flag
    sqlQuery = "SELECT regexpReplace('healthy, wealthy, stealthy and wise','\\w+tHy', 'something', 3, 12, 'xyz')";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "healthy, wealthy, stealthy and wise");

    // Test in select clause with column values
    sqlQuery = "SELECT regexpReplace(DestCityName, ' ', '', 0, -1, 'i') from mytable where OriginState = 'CA'";
    response = postQuery(sqlQuery);
    JsonNode rows = response.get("resultTable").get("rows");
    for (int i = 0; i < rows.size(); i++) {
      JsonNode row = rows.get(i);
      assertFalse(row.get(0).asText().contains(" "));
    }

    // Test in where clause
    sqlQuery = "SELECT count(*) from mytable where regexpReplace(OriginState, '[VC]A', 'TEST') = 'TEST'";
    response = postQuery(sqlQuery);
    int count1 = response.get("resultTable").get("rows").get(0).get(0).asInt();
    sqlQuery = "SELECT count(*) from mytable where OriginState='CA' or OriginState='VA'";
    response = postQuery(sqlQuery);
    int count2 = response.get("resultTable").get("rows").get(0).get(0).asInt();
    assertEquals(count1, count2);

    // Test nested transform
    sqlQuery =
        "SELECT count(*) from mytable where contains(regexpReplace(OriginState, '(C)(A)', '$1TEST$2'), 'CTESTA')";
    response = postQuery(sqlQuery);
    count1 = response.get("resultTable").get("rows").get(0).get(0).asInt();
    sqlQuery = "SELECT count(*) from mytable where OriginState='CA'";
    response = postQuery(sqlQuery);
    count2 = response.get("resultTable").get("rows").get(0).get(0).asInt();
    assertEquals(count1, count2);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testRegexpReplaceVar(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    // Correctness tests of regexpReplace.

    // Test replace all.
    String sqlQuery = "SELECT regexpReplaceVar('CA', 'C', 'TEST')";
    JsonNode response = postQuery(sqlQuery);
    String result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "TESTA");

    sqlQuery = "SELECT regexpReplaceVar('foobarbaz', 'b', 'X')";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "fooXarXaz");

    sqlQuery = "SELECT regexpReplaceVar('foobarbaz', 'b', 'XY')";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "fooXYarXYaz");

    sqlQuery = "SELECT regexpReplaceVar('Argentina', '(.)', '$1 ')";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "A r g e n t i n a ");

    sqlQuery = "SELECT regexpReplaceVar('Pinot is  blazing  fast', '( ){2,}', ' ')";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "Pinot is blazing fast");

    sqlQuery = "SELECT regexpReplaceVar('healthy, wealthy, and wise','\\w+thy', 'something')";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "something, something, and wise");

    sqlQuery = "SELECT regexpReplaceVar('11234567898','(\\d)(\\d{3})(\\d{3})(\\d{4})', '$1-($2) $3-$4')";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "1-(123) 456-7898");

    // Test replace starting at index.

    sqlQuery = "SELECT regexpReplaceVar('healthy, wealthy, stealthy and wise','\\w+thy', 'something', 4)";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "healthy, something, something and wise");

    sqlQuery = "SELECT regexpReplaceVar('healthy, wealthy, stealthy and wise','\\w+thy', 'something', 1)";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "hsomething, something, something and wise");

    // Test occurence
    sqlQuery = "SELECT regexpReplaceVar('healthy, wealthy, stealthy and wise','\\w+thy', 'something', 0, 2)";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "healthy, wealthy, something and wise");

    sqlQuery = "SELECT regexpReplaceVar('healthy, wealthy, stealthy and wise','\\w+thy', 'something', 0, 0)";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "something, wealthy, stealthy and wise");

    // Test flags
    sqlQuery = "SELECT regexpReplaceVar('healthy, wealthy, stealthy and wise','\\w+tHy', 'something', 0, 0, 'i')";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "something, wealthy, stealthy and wise");

    // Negative test. Pattern match not found.
    sqlQuery = "SELECT regexpReplaceVar('healthy, wealthy, stealthy and wise','\\w+tHy', 'something')";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "healthy, wealthy, stealthy and wise");

    // Negative test. Pattern match not found.
    sqlQuery = "SELECT regexpReplaceVar('healthy, wealthy, stealthy and wise','\\w+tHy', 'something', 3, 21, 'i')";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "healthy, wealthy, stealthy and wise");

    // Negative test - incorrect flag
    sqlQuery = "SELECT regexpReplaceVar('healthy, wealthy, stealthy and wise','\\w+tHy', 'something', 3, 12, 'xyz')";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "healthy, wealthy, stealthy and wise");

    // Test in select clause with column values
    sqlQuery = "SELECT regexpReplaceVar(DestCityName, ' ', '', 0, -1, 'i') from mytable where OriginState = 'CA'";
    response = postQuery(sqlQuery);
    JsonNode rows = response.get("resultTable").get("rows");
    for (int i = 0; i < rows.size(); i++) {
      JsonNode row = rows.get(i);
      assertFalse(row.get(0).asText().contains(" "));
    }

    // Test in where clause
    sqlQuery = "SELECT count(*) from mytable where regexpReplaceVar(OriginState, '[VC]A', 'TEST') = 'TEST'";
    response = postQuery(sqlQuery);
    int count1 = response.get("resultTable").get("rows").get(0).get(0).asInt();
    sqlQuery = "SELECT count(*) from mytable where OriginState='CA' or OriginState='VA'";
    response = postQuery(sqlQuery);
    int count2 = response.get("resultTable").get("rows").get(0).get(0).asInt();
    assertEquals(count1, count2);

    // Test nested transform
    sqlQuery =
        "SELECT count(*) from mytable where contains(regexpReplaceVar(OriginState, '(C)(A)', '$1TEST$2'), 'CTESTA')";
    response = postQuery(sqlQuery);
    count1 = response.get("resultTable").get("rows").get(0).get(0).asInt();
    sqlQuery = "SELECT count(*) from mytable where OriginState='CA'";
    response = postQuery(sqlQuery);
    count2 = response.get("resultTable").get("rows").get(0).get(0).asInt();
    assertEquals(count1, count2);
  }

  @Test
  public void testCastMV()
      throws Exception {
    // simple cast
    String sqlQuery = "SELECT DivLongestGTimes, CAST(DivLongestGTimes as DOUBLE) from mytable LIMIT 100";
    JsonNode response = postQuery(sqlQuery);
    JsonNode resultTable = response.get("resultTable");
    JsonNode dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnDataTypes").toString(), "[\"FLOAT_ARRAY\",\"DOUBLE_ARRAY\"]");
    JsonNode rows = response.get("resultTable").get("rows");
    assertEquals(rows.size(), 100);

    for (int i = 0; i < 100; i++) {
      JsonNode row = rows.get(i).get(0);
      JsonNode rowCast = rows.get(i).get(1);
      assertTrue(rowCast.isArray());
      assertTrue(row.isArray());
      assertEquals(row.size(), rowCast.size());
      for (int j = 0; j < rowCast.size(); j++) {
        float original = row.get(j).floatValue();
        assertTrue(rowCast.get(j).isDouble());
        double resultCast = rowCast.get(j).asDouble();
        assertEquals(resultCast, (double) original);
      }
    }

    // nested cast
    sqlQuery = "SELECT DivAirportIDs, CAST(CAST(CAST(DivAirportIDs AS FLOAT) as INT) as STRING),"
        + " DivTotalGTimes, CAST(CAST(DivTotalGTimes AS STRING) AS LONG) from mytable ORDER BY CARRIER LIMIT 100";
    response = postQuery(sqlQuery);
    resultTable = response.get("resultTable");
    dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnDataTypes").toString(),
        "[\"INT_ARRAY\",\"STRING_ARRAY\",\"LONG_ARRAY\",\"LONG_ARRAY\"]");
    rows = response.get("resultTable").get("rows");
    assertEquals(rows.size(), 100);

    for (int i = 0; i < 100; i++) {
      JsonNode col1 = rows.get(i).get(0);
      JsonNode col1Cast = rows.get(i).get(1);
      assertTrue(col1Cast.isArray());
      assertTrue(col1.isArray());
      assertEquals(col1.size(), col1Cast.size());
      for (int j = 0; j < col1Cast.size(); j++) {
        int original = col1.get(j).asInt();
        assertTrue(col1Cast.get(j).isTextual());
        String resultCast = col1Cast.get(j).asText();
        assertEquals(resultCast, String.valueOf((int) ((float) original)));
      }

      JsonNode col2 = rows.get(i).get(2);
      JsonNode col2Cast = rows.get(i).get(3);
      assertTrue(col2Cast.isArray());
      assertTrue(col2.isArray());
      assertEquals(col2.size(), col2Cast.size());
      for (int j = 0; j < col2Cast.size(); j++) {
        long original = col2.get(j).asLong();
        long resultCast = col2Cast.get(j).asLong();
        assertEquals(resultCast, Long.parseLong(String.valueOf(original)));
      }
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testUrlFunc(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String sqlQuery = "SELECT encodeUrl('key1=value 1&key2=value@!$2&key3=value%3'), "
        + "decodeUrl('key1%3Dvalue+1%26key2%3Dvalue%40%21%242%26key3%3Dvalue%253') FROM mytable";
    JsonNode response = postQuery(sqlQuery);
    String encodedString = response.get("resultTable").get("rows").get(0).get(0).asText();
    String expectedUrlStr = encodeUrl("key1=value 1&key2=value@!$2&key3=value%3");
    assertEquals(encodedString, expectedUrlStr);

    String decodedString = response.get("resultTable").get("rows").get(0).get(1).asText();
    expectedUrlStr = decodeUrl("key1%3Dvalue+1%26key2%3Dvalue%40%21%242%26key3%3Dvalue%253");
    assertEquals(decodedString, expectedUrlStr);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testBase64Func(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    // string literal
    String sqlQuery = "SELECT toBase64(toUtf8('hello!')), fromUtf8(fromBase64('aGVsbG8h')) FROM mytable";
    JsonNode response = postQuery(sqlQuery);
    JsonNode resultTable = response.get("resultTable");
    JsonNode dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnDataTypes").toString(), "[\"STRING\",\"STRING\"]");
    JsonNode rows = response.get("resultTable").get("rows");

    String encodedString = rows.get(0).get(0).asText();
    String expectedEncodedStr = toBase64(toUtf8("hello!"));
    assertEquals(encodedString, expectedEncodedStr);
    String decodedString = rows.get(0).get(1).asText();
    String expectedDecodedStr = fromUtf8(fromBase64("aGVsbG8h"));
    assertEquals(decodedString, expectedDecodedStr);

    // long string literal encode
    sqlQuery =
        "SELECT toBase64(toUtf8('this is a long string that will encode to more than 76 characters using base64')) "
            + "FROM mytable";
    response = postQuery(sqlQuery);
    resultTable = response.get("resultTable");
    rows = resultTable.get("rows");
    encodedString = rows.get(0).get(0).asText();
    assertEquals(encodedString,
        toBase64(toUtf8("this is a long string that will encode to more than 76 characters using base64")));

    // long string literal decode
    sqlQuery = "SELECT fromUtf8(fromBase64"
        + "('dGhpcyBpcyBhIGxvbmcgc3RyaW5nIHRoYXQgd2lsbCBlbmNvZGUgdG8gbW9yZSB0aGFuIDc2IGNoYXJhY3RlcnMgdXNpbmcgYmFzZTY0"
        + "')) FROM mytable";
    response = postQuery(sqlQuery);
    resultTable = response.get("resultTable");
    rows = resultTable.get("rows");
    decodedString = rows.get(0).get(0).asText();
    assertEquals(decodedString, fromUtf8(fromBase64(
        "dGhpcyBpcyBhIGxvbmcgc3RyaW5nIHRoYXQgd2lsbCBlbmNvZGUgdG8gbW9yZSB0aGFuIDc2IGNoYXJhY3RlcnMgdXNpbmcgYmFzZTY0")));

    // non-string literal
    sqlQuery = "SELECT toBase64(toUtf8(123)), fromUtf8(fromBase64(toBase64(toUtf8(123)))), 123 FROM mytable";
    response = postQuery(sqlQuery);
    resultTable = response.get("resultTable");
    rows = resultTable.get("rows");
    encodedString = rows.get(0).get(0).asText();
    decodedString = rows.get(0).get(1).asText();
    String originalCol = rows.get(0).get(2).asText();
    assertEquals(decodedString, originalCol);
    assertEquals(encodedString, toBase64(toUtf8("123")));

    // identifier
    sqlQuery = "SELECT Carrier, toBase64(toUtf8(Carrier)), fromUtf8(fromBase64(toBase64(toUtf8(Carrier)))), "
        + "fromBase64(toBase64(toUtf8(Carrier))) FROM mytable LIMIT 100";
    response = postQuery(sqlQuery);
    resultTable = response.get("resultTable");
    dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnDataTypes").toString(), "[\"STRING\",\"STRING\",\"STRING\",\"BYTES\"]");
    rows = response.get("resultTable").get("rows");
    assertEquals(rows.size(), 100);
    for (int i = 0; i < 100; i++) {
      String original = rows.get(0).asText();
      String encoded = rows.get(1).asText();
      String decoded = rows.get(2).asText();
      assertEquals(original, decoded);
      assertEquals(encoded, toBase64(toUtf8(original)));
      assertEquals(decoded, fromUtf8(fromBase64(toBase64(toUtf8(original)))));
    }

    // invalid argument
    sqlQuery = "SELECT toBase64() FROM mytable";
    if (useMultiStageQueryEngine) {
      testQueryError(sqlQuery, QueryErrorCode.QUERY_VALIDATION);
    } else {
      response = postQuery(sqlQuery);
      JsonNode exceptionNode = response.get("exceptions").get(0);
      assertEquals(exceptionNode.get("errorCode").asInt(), QueryErrorCode.QUERY_VALIDATION.getId());
      String errorMsg = exceptionNode.get("message").toString();
      assertTrue(errorMsg.contains("tobase64 with 0 arguments"), errorMsg);
    }

    // invalid argument
    sqlQuery = "SELECT fromBase64() FROM mytable";
    if (useMultiStageQueryEngine) {
      testQueryError(sqlQuery, QueryErrorCode.QUERY_VALIDATION);
    } else {
      response = postQuery(sqlQuery);
      JsonNode exceptionNode = response.get("exceptions").get(0);
      assertEquals(exceptionNode.get("errorCode").asInt(), QueryErrorCode.QUERY_VALIDATION.getId());
      String errorMsg = exceptionNode.get("message").asText();
      assertEquals(errorMsg, "Unsupported function: frombase64 with 0 arguments");
    }

    // invalid argument
    sqlQuery = "SELECT toBase64('hello!') FROM mytable";
    if (useMultiStageQueryEngine) {
      testQueryError(sqlQuery, QueryErrorCode.QUERY_PLANNING);
    } else {
      response = postQuery(sqlQuery);
      JsonNode exceptionNode = response.get("exceptions").get(0);
      assertEquals(exceptionNode.get("errorCode").asInt(), QueryErrorCode.SQL_PARSING.getId());
    }

    // invalid argument
    sqlQuery = "SELECT fromBase64('hello!') FROM mytable";
    if (useMultiStageQueryEngine) {
      testQueryError(sqlQuery, QueryErrorCode.QUERY_PLANNING);
    } else {
      response = postQuery(sqlQuery);
      JsonNode exceptionNode = response.get("exceptions").get(0);
      assertEquals(exceptionNode.get("errorCode").asInt(), QueryErrorCode.SQL_PARSING.getId());
    }

    // string literal used in a filter
    sqlQuery = "SELECT * FROM mytable WHERE fromUtf8(fromBase64('aGVsbG8h')) != Carrier AND "
        + "toBase64(toUtf8('hello!')) != Carrier LIMIT 10";
    response = postQuery(sqlQuery);
    resultTable = response.get("resultTable");
    rows = resultTable.get("rows");
    assertEquals(rows.size(), 10);

    // non-string literal used in a filter
    sqlQuery = "SELECT * FROM mytable WHERE fromUtf8(fromBase64(toBase64(toUtf8(AirlineID)))) != Carrier LIMIT 10";
    response = postQuery(sqlQuery);
    resultTable = response.get("resultTable");
    rows = resultTable.get("rows");
    assertEquals(rows.size(), 10);

    // string identifier used in a filter
    sqlQuery = "SELECT * FROM mytable WHERE fromUtf8(fromBase64(toBase64(toUtf8(Carrier)))) = Carrier LIMIT 10";
    response = postQuery(sqlQuery);
    resultTable = response.get("resultTable");
    rows = resultTable.get("rows");
    assertEquals(rows.size(), 10);

    // non-string identifier used in a filter
    sqlQuery = "SELECT fromUtf8(fromBase64(toBase64(toUtf8(AirlineID)))), AirlineID FROM mytable WHERE "
        + "fromUtf8(fromBase64(toBase64(toUtf8(AirlineID)))) = AirlineID LIMIT 10";
    response = postQuery(sqlQuery);
    resultTable = response.get("resultTable");
    dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnDataTypes").toString(), "[\"STRING\",\"LONG\"]");
    rows = resultTable.get("rows");
    assertEquals(rows.size(), 10);

    // string identifier used in group by order by
    sqlQuery = "SELECT Carrier as originalCol, toBase64(toUtf8(Carrier)) as encoded, "
        + "fromUtf8(fromBase64(toBase64(toUtf8(Carrier)))) as decoded FROM mytable "
        + "GROUP BY Carrier, toBase64(toUtf8(Carrier)), fromUtf8(fromBase64(toBase64(toUtf8(Carrier)))) "
        + "ORDER BY toBase64(toUtf8(Carrier)) LIMIT 10";
    response = postQuery(sqlQuery);
    resultTable = response.get("resultTable");
    dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnDataTypes").toString(), "[\"STRING\",\"STRING\",\"STRING\"]");
    rows = resultTable.get("rows");
    assertEquals(rows.size(), 10);
    for (int i = 0; i < 10; i++) {
      String original = rows.get(0).asText();
      String encoded = rows.get(1).asText();
      String decoded = rows.get(2).asText();
      assertEquals(original, decoded);
      assertEquals(encoded, toBase64(toUtf8(original)));
      assertEquals(decoded, fromUtf8(fromBase64(toBase64(toUtf8(original)))));
    }

    // non-string identifier used in group by order by
    sqlQuery = "SELECT AirlineID as originalCol, toBase64(toUtf8(AirlineID)) as encoded, "
        + "fromUtf8(fromBase64(toBase64(toUtf8(AirlineID)))) as decoded FROM mytable "
        + "GROUP BY AirlineID, toBase64(toUtf8(AirlineID)), fromUtf8(fromBase64(toBase64(toUtf8(AirlineID)))) "
        + "ORDER BY fromUtf8(fromBase64(toBase64(toUtf8(AirlineID)))) LIMIT 10";
    response = postQuery(sqlQuery);
    resultTable = response.get("resultTable");
    dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnDataTypes").toString(), "[\"LONG\",\"STRING\",\"STRING\"]");
    rows = resultTable.get("rows");
    assertEquals(rows.size(), 10);
    for (int i = 0; i < 10; i++) {
      String original = rows.get(0).asText();
      String encoded = rows.get(1).asText();
      String decoded = rows.get(2).asText();
      assertEquals(original, decoded);
      assertEquals(encoded, toBase64(toUtf8(original)));
      assertEquals(decoded, fromUtf8(fromBase64(toBase64(toUtf8(original)))));
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testFunctionWithLiteral(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    testQuery("SELECT SUM(10) FROM mytable");
    testQuery("SELECT ArrDelay + 10 FROM mytable");
    testQuery("SELECT ArrDelay + '10' FROM mytable");
    testQuery("SELECT SUM(ArrDelay + 10) FROM mytable");
    testQuery("SELECT SUM(ArrDelay + '10') FROM mytable");
  }

  @Test
  public void testLiteralOnlyFuncV1()
      throws Exception {
    long queryStartTimeMs = System.currentTimeMillis();
    String sqlQuery =
        "SELECT 1, now() as currentTs, ago('PT1H') as oneHourAgoTs, 'abc', toDateTime(now(), 'yyyy-MM-dd z') as "
            + "today, now(), ago('PT1H'), encodeUrl('key1=value 1&key2=value@!$2&key3=value%3') as encodedUrl, "
            + "decodeUrl('key1%3Dvalue+1%26key2%3Dvalue%40%21%242%26key3%3Dvalue%253') as decodedUrl, toBase64"
            + "(toUtf8('hello!')) as toBase64, fromUtf8(fromBase64('aGVsbG8h')) as fromBase64";
    JsonNode response = postQuery(sqlQuery);
    long queryEndTimeMs = System.currentTimeMillis();

    JsonNode resultTable = response.get("resultTable");
    JsonNode dataSchema = resultTable.get("dataSchema");
    JsonNode columnNames = dataSchema.get("columnNames");
    assertEquals(columnNames.get(0).asText(), "1");
    assertEquals(columnNames.get(1).asText(), "currentTs");
    assertEquals(columnNames.get(2).asText(), "oneHourAgoTs");
    assertEquals(columnNames.get(3).asText(), "'abc'");
    assertEquals(columnNames.get(4).asText(), "today");
    String nowColumnName = columnNames.get(5).asText();
    String oneHourAgoColumnName = columnNames.get(6).asText();
    assertEquals(columnNames.get(7).asText(), "encodedUrl");
    assertEquals(columnNames.get(8).asText(), "decodedUrl");
    assertEquals(columnNames.get(9).asText(), "toBase64");
    assertEquals(columnNames.get(10).asText(), "fromBase64");

    JsonNode columnDataTypes = dataSchema.get("columnDataTypes");
    assertEquals(columnDataTypes.get(0).asText(), "INT");
    assertEquals(columnDataTypes.get(1).asText(), "LONG");
    assertEquals(columnDataTypes.get(2).asText(), "LONG");
    assertEquals(columnDataTypes.get(3).asText(), "STRING");
    assertEquals(columnDataTypes.get(4).asText(), "STRING");
    assertEquals(columnDataTypes.get(5).asText(), "LONG");
    assertEquals(columnDataTypes.get(6).asText(), "LONG");
    assertEquals(columnDataTypes.get(7).asText(), "STRING");
    assertEquals(columnDataTypes.get(8).asText(), "STRING");
    assertEquals(columnDataTypes.get(9).asText(), "STRING");
    assertEquals(columnDataTypes.get(10).asText(), "STRING");

    JsonNode results = resultTable.get("rows").get(0);
    assertEquals(results.get(0).asInt(), 1);
    long nowResult = results.get(1).asLong();
    assertTrue(nowResult >= queryStartTimeMs);
    assertTrue(nowResult <= queryEndTimeMs);
    long oneHourAgoResult = results.get(2).asLong();
    assertTrue(oneHourAgoResult >= queryStartTimeMs - TimeUnit.HOURS.toMillis(1));
    assertTrue(oneHourAgoResult <= queryEndTimeMs - TimeUnit.HOURS.toMillis(1));
    assertEquals(results.get(3).asText(), "abc");
    String queryStartTimeDay = Instant.ofEpochMilli(queryStartTimeMs).atZone(ZoneId.of("UTC"))
        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd z"));
    String queryEndTimeDay = Instant.ofEpochMilli(queryEndTimeMs).atZone(ZoneId.of("UTC"))
        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd z"));
    String dateTimeResult = results.get(4).asText();
    assertTrue(dateTimeResult.equals(queryStartTimeDay) || dateTimeResult.equals(queryEndTimeDay));
    assertEquals(results.get(5).asText(), nowColumnName);
    assertEquals(results.get(6).asText(), oneHourAgoColumnName);
    assertEquals(results.get(7).asText(), "key1%3Dvalue+1%26key2%3Dvalue%40%21%242%26key3%3Dvalue%253");
    assertEquals(results.get(8).asText(), "key1=value 1&key2=value@!$2&key3=value%3");
    assertEquals(results.get(9).asText(), "aGVsbG8h");
    assertEquals(results.get(10).asText(), "hello!");
  }

  @Test
  public void testLiteralOnlyFuncV2()
      throws Exception {
    setUseMultiStageQueryEngine(true);
    long queryStartTimeMs = System.currentTimeMillis();
    String sqlQuery =
        "SELECT 1, cast(now() as bigint) as currentTs, ago('PT1H') as oneHourAgoTs, 'abc', "
            + "toDateTime(now(), 'yyyy-MM-dd z') as today, cast(now() as bigint), ago('PT1H'), "
            + "encodeUrl('key1=value 1&key2=value@!$2&key3=value%3') as encodedUrl, "
            + "decodeUrl('key1%3Dvalue+1%26key2%3Dvalue%40%21%242%26key3%3Dvalue%253') as decodedUrl, "
            + "toBase64(toUtf8('hello!')) as toBase64, fromUtf8(fromBase64('aGVsbG8h')) as fromBase64";
    JsonNode response = postQuery(sqlQuery);
    long queryEndTimeMs = System.currentTimeMillis();

    JsonNode resultTable = response.get("resultTable");
    JsonNode dataSchema = resultTable.get("dataSchema");
    JsonNode columnNames = dataSchema.get("columnNames");
    assertEquals(columnNames.get(0).asText(), "EXPR$0");
    assertEquals(columnNames.get(1).asText(), "currentTs");
    assertEquals(columnNames.get(2).asText(), "oneHourAgoTs");
    assertEquals(columnNames.get(3).asText(), "EXPR$3");
    assertEquals(columnNames.get(4).asText(), "today");
    String nowColumnName = columnNames.get(5).asText();
    String oneHourAgoColumnName = columnNames.get(6).asText();
    assertEquals(columnNames.get(7).asText(), "encodedUrl");
    assertEquals(columnNames.get(8).asText(), "decodedUrl");
    assertEquals(columnNames.get(9).asText(), "toBase64");
    assertEquals(columnNames.get(10).asText(), "fromBase64");

    JsonNode columnDataTypes = dataSchema.get("columnDataTypes");
    assertEquals(columnDataTypes.get(0).asText(), "INT");
    assertEquals(columnDataTypes.get(1).asText(), "LONG");
    assertEquals(columnDataTypes.get(2).asText(), "LONG");
    assertEquals(columnDataTypes.get(3).asText(), "STRING");
    assertEquals(columnDataTypes.get(4).asText(), "STRING");
    assertEquals(columnDataTypes.get(5).asText(), "LONG");
    assertEquals(columnDataTypes.get(6).asText(), "LONG");
    assertEquals(columnDataTypes.get(7).asText(), "STRING");
    assertEquals(columnDataTypes.get(8).asText(), "STRING");
    assertEquals(columnDataTypes.get(9).asText(), "STRING");
    assertEquals(columnDataTypes.get(10).asText(), "STRING");

    JsonNode results = resultTable.get("rows").get(0);
    assertEquals(results.get(0).asInt(), 1);
    long nowResult = results.get(1).asLong();
    // Timestamp granularity is seconds
    assertTrue(nowResult >= ((queryStartTimeMs / 1000) * 1000));
    assertTrue(nowResult <= ((queryEndTimeMs / 1000) * 1000));
    long oneHourAgoResult = results.get(2).asLong();
    assertTrue(oneHourAgoResult >= queryStartTimeMs - TimeUnit.HOURS.toMillis(1));
    assertTrue(oneHourAgoResult <= queryEndTimeMs - TimeUnit.HOURS.toMillis(1));
    assertEquals(results.get(3).asText(), "abc");
    String queryStartTimeDay = Instant.ofEpochMilli(queryStartTimeMs).atZone(ZoneId.of("UTC"))
        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd z"));
    String queryEndTimeDay = Instant.ofEpochMilli(queryEndTimeMs).atZone(ZoneId.of("UTC"))
        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd z"));
    String dateTimeResult = results.get(4).asText();
    assertTrue(dateTimeResult.equals(queryStartTimeDay) || dateTimeResult.equals(queryEndTimeDay));
    // In V2 column names and values are not related
//    assertEquals(results.get(5).asText(), nowColumnName);
//    assertEquals(results.get(6).asText(), oneHourAgoColumnName);
    assertEquals(results.get(7).asText(), "key1%3Dvalue+1%26key2%3Dvalue%40%21%242%26key3%3Dvalue%253");
    assertEquals(results.get(8).asText(), "key1=value 1&key2=value@!$2&key3=value%3");
    assertEquals(results.get(9).asText(), "aGVsbG8h");
    assertEquals(results.get(10).asText(), "hello!");
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testRangeIndexTriggering(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    long numTotalDocs = getCountStarResult();
    assertEquals(postQuery(TEST_UPDATED_RANGE_INDEX_QUERY).get("numEntriesScannedInFilter").asLong(), numTotalDocs);

    // Update table config and trigger reload
    addRangeIndex();

    // Update table config to remove the new range index, and check if the new range index is removed
    updateTableConfig(_tableConfig);
    reloadAllSegments(TEST_UPDATED_RANGE_INDEX_QUERY, false, numTotalDocs);
    assertEquals(postQuery(TEST_UPDATED_RANGE_INDEX_QUERY).get("numEntriesScannedInFilter").asLong(), numTotalDocs);
  }

  @Test
  public void testBloomFilterTriggering()
      throws Exception {
    long numTotalDocs = getCountStarResult();
    assertEquals(postQuery(TEST_UPDATED_BLOOM_FILTER_QUERY).get("numSegmentsProcessed").asLong(), NUM_SEGMENTS);

    // Update table config and trigger reload
    TableConfig tableConfig = createOfflineTableConfig();
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    indexingConfig.setBloomFilterColumns(UPDATED_BLOOM_FILTER_COLUMNS);
    updateTableConfig(tableConfig);
    reloadAllSegments(TEST_UPDATED_BLOOM_FILTER_QUERY, false, numTotalDocs);
    assertEquals(postQuery(TEST_UPDATED_BLOOM_FILTER_QUERY).get("numSegmentsProcessed").asLong(), 0L);

    // Update table config to remove the new bloom filter, and reload table to clean the new bloom filter physically.
    updateTableConfig(_tableConfig);
    reloadAllSegments(TEST_UPDATED_BLOOM_FILTER_QUERY, false, numTotalDocs);
    assertEquals(postQuery(TEST_UPDATED_BLOOM_FILTER_QUERY).get("numSegmentsProcessed").asLong(), NUM_SEGMENTS);
  }

  @Test
  public void testForwardIndexTriggering()
      throws Exception {
    String column = "DestCityName";
    JsonNode columnIndexSize = getColumnIndexSize(column);
    assertTrue(columnIndexSize.has(StandardIndexes.DICTIONARY_ID));
    assertTrue(columnIndexSize.has(StandardIndexes.FORWARD_ID));
    double dictionarySize = columnIndexSize.get(StandardIndexes.DICTIONARY_ID).asDouble();
    double forwardIndexSize = columnIndexSize.get(StandardIndexes.FORWARD_ID).asDouble();

    // Convert 'DestCityName' to raw index
    TableConfig tableConfig = createOfflineTableConfig();
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    List<String> noDictionaryColumns = indexingConfig.getNoDictionaryColumns();
    assertNotNull(noDictionaryColumns);
    noDictionaryColumns.add(column);
    updateTableConfig(tableConfig);
    long numTotalDocs = getCountStarResult();
    reloadAllSegments(SELECT_STAR_QUERY, false, numTotalDocs);
    columnIndexSize = getColumnIndexSize(column);
    assertFalse(columnIndexSize.has(StandardIndexes.DICTIONARY_ID));
    assertTrue(columnIndexSize.has(StandardIndexes.FORWARD_ID));
    double v2rawIndexSize = columnIndexSize.get(StandardIndexes.FORWARD_ID).asDouble();
    assertTrue(v2rawIndexSize > forwardIndexSize);

    // NOTE: Currently Pinot doesn't support directly changing raw index version, so we need to first reset it back to
    //       dictionary encoding.
    // TODO: Support it
    resetForwardIndex(dictionarySize, forwardIndexSize);

    // Convert 'DestCityName' to v4 raw index
    List<FieldConfig> fieldConfigs = tableConfig.getFieldConfigList();
    assertNotNull(fieldConfigs);
    ForwardIndexConfig forwardIndexConfig = new ForwardIndexConfig.Builder().withRawIndexWriterVersion(4).build();
    ObjectNode indexes = JsonUtils.newObjectNode();
    indexes.set("forward", forwardIndexConfig.toJsonNode());
    FieldConfig fieldConfig =
        new FieldConfig.Builder(column).withEncodingType(FieldConfig.EncodingType.RAW).withIndexes(indexes).build();
    fieldConfigs.add(fieldConfig);
    updateTableConfig(tableConfig);
    reloadAllSegments(SELECT_STAR_QUERY, false, numTotalDocs);
    columnIndexSize = getColumnIndexSize(column);
    assertFalse(columnIndexSize.has(StandardIndexes.DICTIONARY_ID));
    assertTrue(columnIndexSize.has(StandardIndexes.FORWARD_ID));
    double v4RawIndexSize = columnIndexSize.get(StandardIndexes.FORWARD_ID).asDouble();
    assertTrue(v4RawIndexSize < v2rawIndexSize && v4RawIndexSize > forwardIndexSize);

    // Convert 'DestCityName' to SNAPPY compression
    forwardIndexConfig =
        new ForwardIndexConfig.Builder().withCompressionCodec(CompressionCodec.SNAPPY).withRawIndexWriterVersion(4)
            .build();
    indexes.set("forward", forwardIndexConfig.toJsonNode());
    fieldConfig =
        new FieldConfig.Builder(column).withEncodingType(FieldConfig.EncodingType.RAW).withIndexes(indexes).build();
    fieldConfigs.set(fieldConfigs.size() - 1, fieldConfig);
    updateTableConfig(tableConfig);
    reloadAllSegments(SELECT_STAR_QUERY, false, numTotalDocs);
    columnIndexSize = getColumnIndexSize(column);
    assertFalse(columnIndexSize.has(StandardIndexes.DICTIONARY_ID));
    assertTrue(columnIndexSize.has(StandardIndexes.FORWARD_ID));
    double v4SnappyRawIndexSize = columnIndexSize.get(StandardIndexes.FORWARD_ID).asDouble();
    assertTrue(v4SnappyRawIndexSize > v2rawIndexSize);

    // Removing FieldConfig should be no-op because compression is not explicitly set
    fieldConfigs.remove(fieldConfigs.size() - 1);
    updateTableConfig(tableConfig);
    reloadAllSegments(SELECT_STAR_QUERY, false, numTotalDocs);
    columnIndexSize = getColumnIndexSize(column);
    assertFalse(columnIndexSize.has(StandardIndexes.DICTIONARY_ID));
    assertTrue(columnIndexSize.has(StandardIndexes.FORWARD_ID));
    assertEquals(columnIndexSize.get(StandardIndexes.FORWARD_ID).asDouble(), v4SnappyRawIndexSize);

    // Adding 'LZ4' compression explicitly should trigger the conversion
    forwardIndexConfig = new ForwardIndexConfig.Builder().withCompressionCodec(CompressionCodec.LZ4).build();
    indexes.set("forward", forwardIndexConfig.toJsonNode());
    fieldConfig =
        new FieldConfig.Builder(column).withEncodingType(FieldConfig.EncodingType.RAW).withIndexes(indexes).build();
    fieldConfigs.add(fieldConfig);
    updateTableConfig(tableConfig);
    reloadAllSegments(SELECT_STAR_QUERY, false, numTotalDocs);
    columnIndexSize = getColumnIndexSize(column);
    assertFalse(columnIndexSize.has(StandardIndexes.DICTIONARY_ID));
    assertTrue(columnIndexSize.has(StandardIndexes.FORWARD_ID));
    assertEquals(columnIndexSize.get(StandardIndexes.FORWARD_ID).asDouble(), v2rawIndexSize);

    resetForwardIndex(dictionarySize, forwardIndexSize);
  }

  private void resetForwardIndex(double expectedDictionarySize, double expectedForwardIndexSize)
      throws Exception {
    updateTableConfig(_tableConfig);
    reloadAllSegments(SELECT_STAR_QUERY, false, getCountStarResult());
    JsonNode columnIndexSize = getColumnIndexSize("DestCityName");
    assertEquals(columnIndexSize.get(StandardIndexes.DICTIONARY_ID).asDouble(), expectedDictionarySize);
    assertEquals(columnIndexSize.get(StandardIndexes.FORWARD_ID).asDouble(), expectedForwardIndexSize);
  }

  /**
   * Check if server returns error response quickly without timing out Broker.
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testServerErrorWithBrokerTimeout(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    long startTimeMs = System.currentTimeMillis();
    // The query below will fail execution due to JSON_MATCH on column without json index
    JsonNode queryResponse = postQuery("SELECT count(*) FROM mytable WHERE JSON_MATCH(Dest, '$=123')");
    // NOTE: Broker timeout is 60s
    assertTrue(System.currentTimeMillis() - startTimeMs < 60_000L);

    JsonNode exceptionNode = queryResponse.get("exceptions").get(0);
    assertEquals(exceptionNode.get("errorCode").asInt(), QueryErrorCode.QUERY_EXECUTION.getId());
  }

  @Test
  public void testStarTreeTriggering()
      throws Exception {
    int numTotalDocs = (int) getCountStarResult();

    // Test the first query
    JsonNode firstQueryResponse = postQuery(TEST_STAR_TREE_QUERY_1);
    int firstQueryResult = firstQueryResponse.get("resultTable").get("rows").get(0).get(0).asInt();
    // Initially 'numDocsScanned' should be the same as 'COUNT(*)' result
    verifySingleValueResponse(firstQueryResponse, firstQueryResult, numTotalDocs, firstQueryResult);
    // Verify that inverting the filter to be a filtered agg shows the identical results
    verifySingleValueResponse(postQuery(TEST_STAR_TREE_QUERY_1_FILTER_INVERT), firstQueryResult, numTotalDocs,
        firstQueryResult);

    // Update table config without enabling dynamic star-tree creation and trigger reload, should have no effect
    TableConfig tableConfig = createOfflineTableConfig();
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    indexingConfig.setStarTreeIndexConfigs(List.of(STAR_TREE_INDEX_CONFIG_1));
    indexingConfig.setEnableDynamicStarTreeCreation(false);
    updateTableConfig(tableConfig);
    reloadAllSegments(TEST_STAR_TREE_QUERY_1, false, numTotalDocs);
    verifySingleValueResponse(postQuery(TEST_STAR_TREE_QUERY_1), firstQueryResult, numTotalDocs, firstQueryResult);
    verifySingleValueResponse(postQuery(TEST_STAR_TREE_QUERY_1_FILTER_INVERT), firstQueryResult, numTotalDocs,
        firstQueryResult);

    // Set enableDynamicStarTreeCreation to true and trigger reload
    indexingConfig.setEnableDynamicStarTreeCreation(true);
    updateTableConfig(tableConfig);
    reloadAllSegments(TEST_STAR_TREE_QUERY_1, false, numTotalDocs);
    // With star-tree, 'numDocsScanned' should be the same as number of segments (1 per segment)
    verifySingleValueResponse(postQuery(TEST_STAR_TREE_QUERY_1), firstQueryResult, numTotalDocs, NUM_SEGMENTS);
    // Verify that inverting the filter to be a filtered agg shows the identical results
    verifySingleValueResponse(postQuery(TEST_STAR_TREE_QUERY_1_FILTER_INVERT), firstQueryResult, numTotalDocs,
        NUM_SEGMENTS);

    // Reload again should have no effect
    reloadAllSegments(TEST_STAR_TREE_QUERY_1, false, numTotalDocs);
    verifySingleValueResponse(postQuery(TEST_STAR_TREE_QUERY_1), firstQueryResult, numTotalDocs, NUM_SEGMENTS);
    verifySingleValueResponse(postQuery(TEST_STAR_TREE_QUERY_1_FILTER_INVERT), firstQueryResult, numTotalDocs,
        NUM_SEGMENTS);

    // Should be able to use the star-tree with an additional match-all predicate on another dimension
    verifySingleValueResponse(postQuery(TEST_STAR_TREE_QUERY_1 + " AND DaysSinceEpoch > 16070"), firstQueryResult,
        numTotalDocs, NUM_SEGMENTS);

    // Test the second query
    JsonNode secondQueryResponse = postQuery(TEST_STAR_TREE_QUERY_2);
    int secondQueryResult = secondQueryResponse.get("resultTable").get("rows").get(0).get(0).asInt();
    // Initially 'numDocsScanned' should be the same as 'COUNT(*)' result
    verifySingleValueResponse(secondQueryResponse, secondQueryResult, numTotalDocs, secondQueryResult);

    // Update table config without enabling dynamic star-tree creation and trigger reload, should have no effect
    indexingConfig.setStarTreeIndexConfigs(List.of(STAR_TREE_INDEX_CONFIG_2));
    indexingConfig.setEnableDynamicStarTreeCreation(false);
    updateTableConfig(tableConfig);
    reloadAllSegments(TEST_STAR_TREE_QUERY_2, false, numTotalDocs);
    verifySingleValueResponse(postQuery(TEST_STAR_TREE_QUERY_2), secondQueryResult, numTotalDocs, secondQueryResult);

    // Set enableDynamicStarTreeCreation to true and trigger reload
    indexingConfig.setEnableDynamicStarTreeCreation(true);
    updateTableConfig(tableConfig);
    reloadAllSegments(TEST_STAR_TREE_QUERY_2, false, numTotalDocs);
    // With star-tree, 'numDocsScanned' should be the same as number of segments (1 per segment)
    verifySingleValueResponse(postQuery(TEST_STAR_TREE_QUERY_2), secondQueryResult, numTotalDocs, NUM_SEGMENTS);

    // First query should not be able to use the star-tree
    verifySingleValueResponse(postQuery(TEST_STAR_TREE_QUERY_1), firstQueryResult, numTotalDocs, firstQueryResult);

    // Reload again should have no effect
    reloadAllSegments(TEST_STAR_TREE_QUERY_2, false, numTotalDocs);
    verifySingleValueResponse(postQuery(TEST_STAR_TREE_QUERY_1), firstQueryResult, numTotalDocs, firstQueryResult);
    verifySingleValueResponse(postQuery(TEST_STAR_TREE_QUERY_2), secondQueryResult, numTotalDocs, NUM_SEGMENTS);

    // Should be able to use the star-tree with an additional match-all predicate on another dimension
    verifySingleValueResponse(postQuery(TEST_STAR_TREE_QUERY_2 + " AND DaysSinceEpoch > 16070"), secondQueryResult,
        numTotalDocs, NUM_SEGMENTS);

    // Remove the star-tree index config without enabling dynamic star-tree creation and trigger reload, should have no
    // effect
    indexingConfig.setStarTreeIndexConfigs(null);
    indexingConfig.setEnableDynamicStarTreeCreation(false);
    updateTableConfig(tableConfig);
    reloadAllSegments(TEST_STAR_TREE_QUERY_2, false, numTotalDocs);
    verifySingleValueResponse(postQuery(TEST_STAR_TREE_QUERY_1), firstQueryResult, numTotalDocs, firstQueryResult);
    verifySingleValueResponse(postQuery(TEST_STAR_TREE_QUERY_2), secondQueryResult, numTotalDocs, NUM_SEGMENTS);

    // Set enableDynamicStarTreeCreation to true and trigger reload
    indexingConfig.setEnableDynamicStarTreeCreation(true);
    updateTableConfig(tableConfig);
    reloadAllSegments(TEST_STAR_TREE_QUERY_2, false, numTotalDocs);
    // Without star-tree, 'numDocsScanned' should be the same as the 'COUNT(*)' result
    verifySingleValueResponse(postQuery(TEST_STAR_TREE_QUERY_2), secondQueryResult, numTotalDocs, secondQueryResult);

    // First query should not be able to use the star-tree
    verifySingleValueResponse(postQuery(TEST_STAR_TREE_QUERY_1), firstQueryResult, numTotalDocs, firstQueryResult);

    // Reload again should have no effect
    reloadAllSegments(TEST_STAR_TREE_QUERY_2, false, numTotalDocs);
    verifySingleValueResponse(postQuery(TEST_STAR_TREE_QUERY_1), firstQueryResult, numTotalDocs, firstQueryResult);
    verifySingleValueResponse(postQuery(TEST_STAR_TREE_QUERY_2), secondQueryResult, numTotalDocs, secondQueryResult);

    // Add one default column, one derived column and a star-tree on new added columns without enabling dynamic
    // star-tree creation and trigger reload. New columns should be added, but not the star-tree.
    Schema schema = createSchema();
    schema.addField(new DimensionFieldSpec("NewInt", DataType.INT, true, 1));
    schema.addField(new DimensionFieldSpec("DerivedAirTime", DataType.INT, true));
    updateSchema(schema);
    indexingConfig.setStarTreeIndexConfigs(List.of(STAR_TREE_INDEX_CONFIG_3));
    indexingConfig.setEnableDynamicStarTreeCreation(false);
    List<TransformConfig> transformConfigs = List.of(new TransformConfig("DerivedAirTime", "AirTime / 60"));
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setTransformConfigs(transformConfigs);
    tableConfig.setIngestionConfig(ingestionConfig);
    updateTableConfig(tableConfig);

    // Query the new added columns without reload
    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode response = postQuery(TEST_STAR_TREE_QUERY_3);
        return response.get("resultTable").get("rows").get(0).get(0).asInt() == 0;
      } catch (Exception e) {
        return false;
      }
    }, 60_000L, "Failed to query new added columns without reload");
    // Table size shouldn't change without reload
    assertEquals(getTableSize(getTableName()), _tableSize);

    reloadAllSegments(TEST_STAR_TREE_QUERY_3, false, numTotalDocs);
    int thirdQueryResult =
        postQuery(TEST_STAR_TREE_QUERY_3_REFERENCE).get("resultTable").get("rows").get(0).get(0).asInt();
    verifySingleValueResponse(postQuery(TEST_STAR_TREE_QUERY_3), thirdQueryResult, numTotalDocs, thirdQueryResult);

    // Reload again should have no effect
    reloadAllSegments(TEST_STAR_TREE_QUERY_3, false, numTotalDocs);
    verifySingleValueResponse(postQuery(TEST_STAR_TREE_QUERY_3), thirdQueryResult, numTotalDocs, thirdQueryResult);

    // Set enableDynamicStarTreeCreation to true and trigger reload
    indexingConfig.setEnableDynamicStarTreeCreation(true);
    updateTableConfig(tableConfig);
    reloadAllSegments(TEST_STAR_TREE_QUERY_3, false, numTotalDocs);
    verifySingleValueResponse(postQuery(TEST_STAR_TREE_QUERY_3), thirdQueryResult, numTotalDocs,
        EXPECTED_NUM_DOCS_SCANNED_STAR_TREE_QUERY_3);

    // Reload again should have no effect
    reloadAllSegments(TEST_STAR_TREE_QUERY_3, false, numTotalDocs);
    verifySingleValueResponse(postQuery(TEST_STAR_TREE_QUERY_3), thirdQueryResult, numTotalDocs,
        EXPECTED_NUM_DOCS_SCANNED_STAR_TREE_QUERY_3);

    // Remove the added columns and star-tree index. Need to force update the schema to remove the added columns.
    indexingConfig.setStarTreeIndexConfigs(null);
    tableConfig.setIngestionConfig(null);
    updateTableConfig(tableConfig);
    forceUpdateSchema(_schema);
    reloadAllSegments(SELECT_STAR_QUERY, false, numTotalDocs);
    assertEquals(postQuery(SELECT_STAR_QUERY).get("resultTable").get("dataSchema").get("columnNames").size(), 79);

    // Add new columns and star-tree index with dynamic star-tree creation enabled and trigger reload
    updateSchema(schema);
    indexingConfig.setStarTreeIndexConfigs(List.of(STAR_TREE_INDEX_CONFIG_3));
    tableConfig.setIngestionConfig(ingestionConfig);
    updateTableConfig(tableConfig);
    reloadAllSegments(TEST_STAR_TREE_QUERY_3, false, numTotalDocs);
    verifySingleValueResponse(postQuery(TEST_STAR_TREE_QUERY_3), thirdQueryResult, numTotalDocs,
        EXPECTED_NUM_DOCS_SCANNED_STAR_TREE_QUERY_3);

    // Reload again should have no effect
    reloadAllSegments(TEST_STAR_TREE_QUERY_3, false, numTotalDocs);
    verifySingleValueResponse(postQuery(TEST_STAR_TREE_QUERY_3), thirdQueryResult, numTotalDocs,
        EXPECTED_NUM_DOCS_SCANNED_STAR_TREE_QUERY_3);

    // Remove the added columns and star-tree index. Need to force update the schema to remove the added columns.
    indexingConfig.setStarTreeIndexConfigs(null);
    tableConfig.setIngestionConfig(null);
    updateTableConfig(tableConfig);
    forceUpdateSchema(_schema);
    reloadAllSegments(SELECT_STAR_QUERY, false, numTotalDocs);
    assertEquals(postQuery(SELECT_STAR_QUERY).get("resultTable").get("dataSchema").get("columnNames").size(), 79);

    // Reset the table config and reload, should have no effect
    updateTableConfig(_tableConfig);
    reloadAllSegments(SELECT_STAR_QUERY, false, numTotalDocs);
    verifySingleValueResponse(postQuery(TEST_STAR_TREE_QUERY_1), firstQueryResult, numTotalDocs, firstQueryResult);
    verifySingleValueResponse(postQuery(TEST_STAR_TREE_QUERY_2), secondQueryResult, numTotalDocs, secondQueryResult);
  }

  private void verifySingleValueResponse(JsonNode queryResponse, int expectedResult, int expectedTotalDocs,
      int expectedDocsScanned) {
    assertEquals(queryResponse.get("resultTable").get("rows").get(0).get(0).asInt(), expectedResult);
    assertEquals(queryResponse.get("totalDocs").asInt(), expectedTotalDocs);
    assertEquals(queryResponse.get("numDocsScanned").asInt(), expectedDocsScanned);
  }

  /**
   * We will add extra new columns to the schema to test adding new columns with default value/transform function to the
   * offline segments.
   * <p>New columns are: (name, field type, data type, single/multi value, default null value)
   * <ul>
   *   <li>"NewAddedIntMetric", METRIC, INT, single-value, 1</li>
   *   <li>"NewAddedLongMetric", METRIC, LONG, single-value, 1</li>
   *   <li>"NewAddedFloatMetric", METRIC, FLOAT, single-value, default (0.0)</li>
   *   <li>"NewAddedDoubleMetric", METRIC, DOUBLE, single-value, default (0.0)</li>
   *   <li>"NewAddedBigDecimalMetric", METRIC, BIG_DECIMAL, single-value, default (0)</li>
   *   <li>"NewAddedBytesMetric", METRIC, BYTES, single-value, default (byte[0])</li>
   *   <li>"NewAddedMVIntDimension", DIMENSION, INT, multi-value, default (Integer.MIN_VALUE)</li>
   *   <li>"NewAddedMVLongDimension", DIMENSION, LONG, multi-value, default (Long.MIN_VALUE)</li>
   *   <li>"NewAddedMVFloatDimension", DIMENSION, FLOAT, multi-value, default (Float.NEGATIVE_INFINITY)</li>
   *   <li>"NewAddedMVDoubleDimension", DIMENSION, DOUBLE, multi-value, default (Double.NEGATIVE_INFINITY)</li>
   *   <li>"NewAddedMVBooleanDimension", DIMENSION, BOOLEAN, multi-value, default (false)</li>
   *   <li>"NewAddedMVTimestampDimension", DIMENSION, TIMESTAMP, multi-value, default (EPOCH)</li>
   *   <li>"NewAddedMVStringDimension", DIMENSION, STRING, multi-value, default ("null")</li>
   *   <li>"NewAddedSVJSONDimension", DIMENSION, JSON, single-value, default ("null")</li>
   *   <li>"NewAddedSVBytesDimension", DIMENSION, BYTES, single-value, default (byte[0])</li>
   *   <li>"NewAddedDerivedHoursSinceEpoch", DATE_TIME, INT, single-value, DaysSinceEpoch * 24</li>
   *   <li>"NewAddedDerivedTimestamp", DATE_TIME, TIMESTAMP, single-value, DaysSinceEpoch * 24 * 3600 * 1000</li>
   *   <li>"NewAddedDerivedSVBooleanDimension", DIMENSION, BOOLEAN, single-value, ActualElapsedTime > 0</li>
   *   <li>"NewAddedDerivedMVStringDimension", DIMENSION, STRING, multi-value, split(DestCityName, ', ')</li>
   *   <li>"NewAddedDerivedDivAirportSeqIDs", DIMENSION, INT, multi-value, DivAirportSeqIDs</li>
   *   <li>"NewAddedDerivedDivAirportSeqIDsString", DIMENSION, STRING, multi-value, DivAirportSeqIDs</li>
   *   <li>"NewAddedRawDerivedStringDimension", DIMENSION, STRING, single-value, reverse(DestCityName)</li>
   *   <li>"NewAddedRawDerivedMVIntDimension", DIMENSION, INT, multi-value, array(ActualElapsedTime)</li>
   *   <li>"NewAddedDerivedMVDoubleDimension", DIMENSION, DOUBLE, multi-value, array(ArrDelayMinutes)</li>
   *   <li>"NewAddedDerivedNullString", DIMENSION, STRING, single-value, caseWhen(true, null, null)</li>
   * </ul>
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testDefaultColumns(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    long numTotalDocs = getCountStarResult();
    TableConfig tableConfig = createOfflineTableConfig();
    Schema schema = createSchema();

    // TEST EXTRA COLUMNS

    // Add columns to the schema first to pass the validation of the table config
    schema.addField(new MetricFieldSpec("NewAddedIntMetric", DataType.INT, 1));
    schema.addField(new MetricFieldSpec("NewAddedLongMetric", DataType.LONG, 1));
    schema.addField(new MetricFieldSpec("NewAddedFloatMetric", DataType.FLOAT));
    schema.addField(new MetricFieldSpec("NewAddedDoubleMetric", DataType.DOUBLE));
    schema.addField(new MetricFieldSpec("NewAddedBigDecimalMetric", DataType.BIG_DECIMAL));
    schema.addField(new MetricFieldSpec("NewAddedBytesMetric", DataType.BYTES));
    schema.addField(new DimensionFieldSpec("NewAddedMVIntDimension", DataType.INT, false));
    schema.addField(new DimensionFieldSpec("NewAddedMVLongDimension", DataType.LONG, false));
    schema.addField(new DimensionFieldSpec("NewAddedMVFloatDimension", DataType.FLOAT, false));
    schema.addField(new DimensionFieldSpec("NewAddedMVDoubleDimension", DataType.DOUBLE, false));
    schema.addField(new DimensionFieldSpec("NewAddedMVBooleanDimension", DataType.BOOLEAN, false));
    schema.addField(new DimensionFieldSpec("NewAddedMVTimestampDimension", DataType.TIMESTAMP, false));
    schema.addField(new DimensionFieldSpec("NewAddedMVStringDimension", DataType.STRING, false));
    // NOTE: MV JSON and BYTES are not supported
    schema.addField(new DimensionFieldSpec("NewAddedSVJSONDimension", DataType.JSON, true));
    schema.addField(new DimensionFieldSpec("NewAddedSVBytesDimension", DataType.BYTES, true));
    schema.addField(new DateTimeFieldSpec("NewAddedDerivedHoursSinceEpoch", DataType.INT, "EPOCH|HOURS", "1:DAYS"));
    schema.addField(new DateTimeFieldSpec("NewAddedDerivedTimestamp", DataType.TIMESTAMP, "TIMESTAMP", "1:DAYS"));
    schema.addField(new DimensionFieldSpec("NewAddedDerivedSVBooleanDimension", DataType.BOOLEAN, true));
    schema.addField(new DimensionFieldSpec("NewAddedDerivedMVStringDimension", DataType.STRING, false));
    schema.addField(new DimensionFieldSpec("NewAddedDerivedDivAirportSeqIDs", DataType.INT, false));
    schema.addField(new DimensionFieldSpec("NewAddedDerivedDivAirportSeqIDsString", DataType.STRING, false));
    schema.addField(new DimensionFieldSpec("NewAddedRawDerivedStringDimension", DataType.STRING, true));
    schema.addField(new DimensionFieldSpec("NewAddedRawDerivedMVIntDimension", DataType.INT, false));
    schema.addField(new DimensionFieldSpec("NewAddedDerivedMVDoubleDimension", DataType.DOUBLE, false));
    schema.addField(new DimensionFieldSpec("NewAddedDerivedNullString", DataType.STRING, true, "nil"));
    schema.setEnableColumnBasedNullHandling(true);
    updateSchema(schema);

    List<TransformConfig> transformConfigs = List.of(
        new TransformConfig("NewAddedDerivedHoursSinceEpoch", "DaysSinceEpoch * 24"),
        new TransformConfig("NewAddedDerivedTimestamp", "DaysSinceEpoch * 24 * 3600 * 1000"),
        new TransformConfig("NewAddedDerivedSVBooleanDimension", "ActualElapsedTime > 0"),
        new TransformConfig("NewAddedDerivedMVStringDimension", "split(DestCityName, ', ')"),
        new TransformConfig("NewAddedDerivedDivAirportSeqIDs", "DivAirportSeqIDs"),
        new TransformConfig("NewAddedDerivedDivAirportSeqIDsString", "DivAirportSeqIDs"),
        new TransformConfig("NewAddedRawDerivedStringDimension", "reverse(DestCityName)"),
        new TransformConfig("NewAddedRawDerivedMVIntDimension", "array(ActualElapsedTime)"),
        new TransformConfig("NewAddedDerivedMVDoubleDimension", "array(ArrDelayMinutes)"),
        new TransformConfig("NewAddedDerivedNullString", "caseWhen(true, null, null)")
    );
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setTransformConfigs(transformConfigs);
    tableConfig.setIngestionConfig(ingestionConfig);

    // Ensure that we can reload segments with a new raw derived column
    List<String> noDictionaryColumns = tableConfig.getIndexingConfig().getNoDictionaryColumns();
    assertNotNull(noDictionaryColumns);
    noDictionaryColumns.add("NewAddedRawDerivedStringDimension");
    noDictionaryColumns.add("NewAddedRawDerivedMVIntDimension");
    List<FieldConfig> fieldConfigList = tableConfig.getFieldConfigList();
    assertNotNull(fieldConfigList);
    fieldConfigList.add(
        new FieldConfig("NewAddedDerivedDivAirportSeqIDs", FieldConfig.EncodingType.DICTIONARY, List.of(),
            CompressionCodec.MV_ENTRY_DICT, null));
    fieldConfigList.add(
        new FieldConfig("NewAddedDerivedDivAirportSeqIDsString", FieldConfig.EncodingType.DICTIONARY, List.of(),
            CompressionCodec.MV_ENTRY_DICT, null));
    updateTableConfig(tableConfig);

    // Query the new added columns without reload
    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode response = postQuery(SELECT_STAR_QUERY);
        return response.get("resultTable").get("dataSchema").get("columnNames").size() == 104;
      } catch (Exception e) {
        return false;
      }
    }, 60_000L, "Failed to query new added columns without reload");
    // Table size shouldn't change without reload
    assertEquals(getTableSize(getTableName()), _tableSize);
    // Test REGEXP_LIKE on new added columns
    testRegexpLikeOnNewAddedColumns();

    // Trigger reload and verify column count
    reloadAllSegments(TEST_EXTRA_COLUMNS_QUERY, false, numTotalDocs);
    JsonNode segmentsMetadata = JsonUtils.stringToJsonNode(
        sendGetRequest(_controllerRequestURLBuilder.forSegmentsMetadataFromServer(getTableName(), List.of("*"))));
    assertEquals(segmentsMetadata.size(), 12);
    for (JsonNode segmentMetadata : segmentsMetadata) {
      assertEquals(segmentMetadata.get("columns").size(), 104);
    }
    assertEquals(postQuery(SELECT_STAR_QUERY).get("resultTable").get("dataSchema").get("columnNames").size(), 104);

    // Verify the index sizes
    JsonNode columnIndexSizeMap = JsonUtils.stringToJsonNode(sendGetRequest(
            _controllerRequestURLBuilder.forTableAggregateMetadata(getTableName(),
                List.of("DivAirportSeqIDs", "NewAddedDerivedDivAirportSeqIDs", "NewAddedDerivedDivAirportSeqIDsString",
                    "NewAddedRawDerivedStringDimension", "NewAddedRawDerivedMVIntDimension",
                    "NewAddedDerivedNullString"))))
        .get("columnIndexSizeMap");
    assertEquals(columnIndexSizeMap.size(), 6);
    JsonNode originalColumnIndexSizes = columnIndexSizeMap.get("DivAirportSeqIDs");
    JsonNode derivedColumnIndexSizes = columnIndexSizeMap.get("NewAddedDerivedDivAirportSeqIDs");
    JsonNode derivedStringColumnIndexSizes = columnIndexSizeMap.get("NewAddedDerivedDivAirportSeqIDsString");
    JsonNode derivedRawStringColumnIndex = columnIndexSizeMap.get("NewAddedRawDerivedStringDimension");
    JsonNode derivedRawMVIntColumnIndex = columnIndexSizeMap.get("NewAddedRawDerivedMVIntDimension");
    JsonNode derivedNullStringColumnIndex = columnIndexSizeMap.get("NewAddedDerivedNullString");

    // Derived int column should have the same dictionary size as the original column
    double originalColumnDictionarySize = originalColumnIndexSizes.get(StandardIndexes.DICTIONARY_ID).asDouble();
    assertEquals(derivedColumnIndexSizes.get(StandardIndexes.DICTIONARY_ID).asDouble(), originalColumnDictionarySize);

    // Derived string column should have larger dictionary size than the original column
    assertTrue(
        derivedStringColumnIndexSizes.get(StandardIndexes.DICTIONARY_ID).asDouble() > originalColumnDictionarySize);

    // Both derived columns should have smaller forward index size than the original column because of compression
    double derivedColumnForwardIndexSize = derivedColumnIndexSizes.get(StandardIndexes.FORWARD_ID).asDouble();
    assertTrue(derivedColumnForwardIndexSize < originalColumnIndexSizes.get(StandardIndexes.FORWARD_ID).asDouble());
    assertEquals(derivedStringColumnIndexSizes.get(StandardIndexes.FORWARD_ID).asDouble(),
        derivedColumnForwardIndexSize);

    assertTrue(derivedRawStringColumnIndex.has(StandardIndexes.FORWARD_ID));
    assertFalse(derivedRawStringColumnIndex.has(StandardIndexes.DICTIONARY_ID));

    assertTrue(derivedRawMVIntColumnIndex.has(StandardIndexes.FORWARD_ID));
    assertFalse(derivedRawMVIntColumnIndex.has(StandardIndexes.DICTIONARY_ID));

    assertTrue(derivedNullStringColumnIndex.has(StandardIndexes.NULL_VALUE_VECTOR_ID));

    testNewAddedColumns();
    testRegexpLikeOnNewAddedColumns();

    // The multi-stage query engine doesn't support expression overrides currently
    if (!useMultiStageQueryEngine()) {
      testExpressionOverride();
    }

    // TEST MISSING COLUMNS

    // Reset the table config first to pass the validation
    updateTableConfig(_tableConfig);

    // Need to force update the schema because removing columns is backward-incompatible change
    schema = createSchema();
    schema.removeField("AirlineID");
    schema.removeField("ArrTime");
    schema.removeField("AirTime");
    schema.removeField("ArrDel15");
    forceUpdateSchema(schema);

    // Trigger reload and verify column count
    reloadAllSegments(SELECT_STAR_QUERY, false, numTotalDocs);
    segmentsMetadata = JsonUtils.stringToJsonNode(
        sendGetRequest(_controllerRequestURLBuilder.forSegmentsMetadataFromServer(getTableName(), List.of("*"))));
    assertEquals(segmentsMetadata.size(), 12);
    for (JsonNode segmentMetadata : segmentsMetadata) {
      assertEquals(segmentMetadata.get("columns").size(), 75);
    }
    assertEquals(postQuery(SELECT_STAR_QUERY).get("resultTable").get("dataSchema").get("columnNames").size(), 75);

    // TEST REGULAR COLUMNS

    updateSchema(_schema);

    // Trigger reload and verify column count
    reloadAllSegments(TEST_REGULAR_COLUMNS_QUERY, false, numTotalDocs);
    assertEquals(postQuery(SELECT_STAR_QUERY).get("resultTable").get("dataSchema").get("columnNames").size(), 79);
  }

  private void testNewAddedColumns()
      throws Exception {
    long numTotalDocs = getCountStarResult();
    double numTotalDocsInDouble = (double) numTotalDocs;

    // Test queries with each new added columns
    //@formatter:off
    String h2Query = "SELECT COUNT(*) FROM mytable";
    String pinotQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedIntMetric = 1";
    testQuery(pinotQuery, h2Query);

    pinotQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedLongMetric = 1";
    testQuery(pinotQuery, h2Query);

    pinotQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedFloatMetric = 0";
    testQuery(pinotQuery, h2Query);

    pinotQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedDoubleMetric = 0";
    testQuery(pinotQuery, h2Query);

    pinotQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedBigDecimalMetric = 0";
    testQuery(pinotQuery, h2Query);

    pinotQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedBytesMetric = "
        + (useMultiStageQueryEngine() ? "X''" : "''");
    testQuery(pinotQuery, h2Query);

    pinotQuery = "SELECT COUNT(*) FROM mytable WHERE "
        + (useMultiStageQueryEngine() ? "arrayToMV(NewAddedMVIntDimension)" : "NewAddedMVIntDimension") + " < 0";
    testQuery(pinotQuery, h2Query);

    pinotQuery = "SELECT COUNT(*) FROM mytable WHERE "
        + (useMultiStageQueryEngine() ? "arrayToMV(NewAddedMVLongDimension)" : "NewAddedMVLongDimension") + " < 0";
    testQuery(pinotQuery, h2Query);

    pinotQuery = "SELECT COUNT(*) FROM mytable WHERE "
        + (useMultiStageQueryEngine() ? "arrayToMV(NewAddedMVFloatDimension)" : "NewAddedMVFloatDimension") + " < 0.0";
    testQuery(pinotQuery, h2Query);

    pinotQuery = "SELECT COUNT(*) FROM mytable WHERE "
        + (useMultiStageQueryEngine() ? "arrayToMV(NewAddedMVDoubleDimension)" : "NewAddedMVDoubleDimension")
        + " < 0.0";
    testQuery(pinotQuery, h2Query);

    pinotQuery = "SELECT COUNT(*) FROM mytable WHERE "
        + (useMultiStageQueryEngine() ? "arrayToMV(NewAddedMVBooleanDimension)" : "NewAddedMVBooleanDimension")
        + " = false";
    testQuery(pinotQuery, h2Query);

    pinotQuery = "SELECT COUNT(*) FROM mytable WHERE "
        + (useMultiStageQueryEngine()
        ? "arrayToMV(CAST(NewAddedMVTimestampDimension AS BIGINT ARRAY))"
        : "NewAddedMVTimestampDimension")
        + " = 0";
    testQuery(pinotQuery, h2Query);

    pinotQuery = "SELECT COUNT(*) FROM mytable WHERE "
        + (useMultiStageQueryEngine() ? "arrayToMV(NewAddedMVStringDimension)" : "NewAddedMVStringDimension")
        + " = 'null'";
    testQuery(pinotQuery, h2Query);

    pinotQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedSVJSONDimension = 'null'";
    testQuery(pinotQuery, h2Query);

    pinotQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedSVBytesDimension = "
        + (useMultiStageQueryEngine() ? "X''" : "''");
    testQuery(pinotQuery, h2Query);

    pinotQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedDerivedNullString = 'nil'";
    testQuery(pinotQuery, h2Query);

    pinotQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedDerivedHoursSinceEpoch = 392232";
    h2Query = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch = 16343";
    testQuery(pinotQuery, h2Query);

    pinotQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedDerivedTimestamp = 1411862400000";
    h2Query = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch = 16341";
    testQuery(pinotQuery, h2Query);

    pinotQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedDerivedSVBooleanDimension = true";
    h2Query = "SELECT COUNT(*) FROM mytable WHERE ActualElapsedTime > 0";
    testQuery(pinotQuery, h2Query);

    pinotQuery = "SELECT COUNT(*) FROM mytable WHERE "
        + (useMultiStageQueryEngine()
        ? "arrayToMV(NewAddedDerivedMVStringDimension)"
        : "NewAddedDerivedMVStringDimension")
        + " = 'CA'";
    h2Query = "SELECT COUNT(*) FROM mytable WHERE DestState = 'CA'";
    testQuery(pinotQuery, h2Query);

    pinotQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedRawDerivedStringDimension = 'Washington, DC'";
    h2Query = "SELECT COUNT(*) FROM mytable WHERE DestCityName = 'CD ,notgnihsaW'";
    testQuery(pinotQuery, h2Query);

    pinotQuery = "SELECT COUNT(*) FROM mytable WHERE "
        + (useMultiStageQueryEngine()
        ? "arrayToMV(NewAddedRawDerivedMVIntDimension)"
        : "NewAddedRawDerivedMVIntDimension")
        + " = 332";
    h2Query = "SELECT COUNT(*) FROM mytable WHERE ActualElapsedTime = 332";
    testQuery(pinotQuery, h2Query);

    pinotQuery = "SELECT COUNT(*) FROM mytable WHERE "
        + (useMultiStageQueryEngine()
        ? "arrayToMV(NewAddedDerivedMVDoubleDimension)"
        : "NewAddedDerivedMVDoubleDimension")
        + " = 110.0";
    h2Query = "SELECT COUNT(*) FROM mytable WHERE ArrDelayMinutes = 110.0";
    testQuery(pinotQuery, h2Query);

    pinotQuery = "SELECT COUNT(*) FROM mytable WHERE "
        + (useMultiStageQueryEngine() ? "arrayToMV(DivAirportSeqIDs)" : "DivAirportSeqIDs")
        + " > 1100000";
    JsonNode response = postQuery(pinotQuery);
    JsonNode rows = response.get("resultTable").get("rows");
    long count = rows.get(0).get(0).asLong();
    pinotQuery = "SELECT COUNT(*) FROM mytable WHERE "
        + (useMultiStageQueryEngine()
        ? "arrayToMV(NewAddedDerivedDivAirportSeqIDs)"
        : "NewAddedDerivedDivAirportSeqIDs")
        + " > 1100000";
    response = postQuery(pinotQuery);
    rows = response.get("resultTable").get("rows");
    assertEquals(rows.get(0).get(0).asLong(), count);

    pinotQuery = "SELECT COUNT(*) FROM mytable WHERE "
        + (useMultiStageQueryEngine()
        ? "arrayToMV(NewAddedDerivedDivAirportSeqIDsString)"
        : "CAST(NewAddedDerivedDivAirportSeqIDsString AS INT)")
        + " > 1100000";
    response = postQuery(pinotQuery);
    rows = response.get("resultTable").get("rows");
    assertEquals(rows.get(0).get(0).asLong(), count);

    // Test queries with new added metric column in aggregation function
    pinotQuery = "SELECT SUM(NewAddedIntMetric) FROM mytable WHERE DaysSinceEpoch <= 16312";
    h2Query = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch <= 16312";
    testQuery(pinotQuery, h2Query);

    pinotQuery = "SELECT SUM(NewAddedIntMetric) FROM mytable WHERE DaysSinceEpoch > 16312";
    h2Query = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch > 16312";
    testQuery(pinotQuery, h2Query);

    pinotQuery = "SELECT SUM(NewAddedLongMetric) FROM mytable WHERE DaysSinceEpoch <= 16312";
    h2Query = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch <= 16312";
    testQuery(pinotQuery, h2Query);

    pinotQuery = "SELECT SUM(NewAddedLongMetric) FROM mytable WHERE DaysSinceEpoch > 16312";
    h2Query = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch > 16312";
    testQuery(pinotQuery, h2Query);
    //@formatter:on

    // Test other query forms with new added columns
    //@formatter:off
    pinotQuery = "SELECT "
        + (useMultiStageQueryEngine() ? "arrayToMV(NewAddedMVStringDimension)" : "NewAddedMVStringDimension")
        + ", SUM(NewAddedFloatMetric) FROM mytable GROUP BY "
        + (useMultiStageQueryEngine() ? "arrayToMV(NewAddedMVStringDimension)" : "NewAddedMVStringDimension");
    //@formatter:on
    response = postQuery(pinotQuery);
    rows = response.get("resultTable").get("rows");
    assertEquals(rows.size(), 1);
    JsonNode row = rows.get(0);
    assertEquals(row.size(), 2);
    assertEquals(row.get(0).asText(), "null");
    assertEquals(row.get(1).asDouble(), 0.0);

    // The multi-stage query engine doesn't support BIG_DECIMAL data type currently.
    if (!useMultiStageQueryEngine()) {
      pinotQuery = "SELECT NewAddedSVBytesDimension, SUM(NewAddedBigDecimalMetric) FROM mytable "
          + "GROUP BY NewAddedSVBytesDimension";
      response = postQuery(pinotQuery);
      rows = response.get("resultTable").get("rows");
      assertEquals(rows.size(), 1);
      row = rows.get(0);
      assertEquals(row.size(), 2);
      assertEquals(row.get(0).asText(), "");
      assertEquals(row.get(1).asDouble(), 0.0);
    }

    //@formatter:off
    pinotQuery = "SELECT "
        + (useMultiStageQueryEngine() ? "arrayToMV(NewAddedMVLongDimension)" : "NewAddedMVLongDimension")
        + ", SUM(NewAddedIntMetric) FROM mytable GROUP BY "
        + (useMultiStageQueryEngine() ? "arrayToMV(NewAddedMVLongDimension)" : "NewAddedMVLongDimension");
    //@formatter:on
    response = postQuery(pinotQuery);
    rows = response.get("resultTable").get("rows");
    assertEquals(rows.size(), 1);
    row = rows.get(0);
    assertEquals(row.size(), 2);
    assertEquals(row.get(0).asLong(), Long.MIN_VALUE);
    assertEquals(row.get(1).asDouble(), numTotalDocsInDouble);

    //@formatter:off
    String newAddedDimensions = useMultiStageQueryEngine()
        ?
        "arrayToMV(NewAddedMVIntDimension), arrayToMV(NewAddedMVLongDimension), arrayToMV(NewAddedMVFloatDimension), "
            + "arrayToMV(NewAddedMVDoubleDimension), arrayToMV(NewAddedMVBooleanDimension), "
            + "arrayToMV(NewAddedMVTimestampDimension), arrayToMV(NewAddedMVStringDimension), "
            + "NewAddedSVJSONDimension, NewAddedSVBytesDimension"
        :
        "NewAddedMVIntDimension, NewAddedMVLongDimension, NewAddedMVFloatDimension, NewAddedMVDoubleDimension, "
            + "NewAddedMVBooleanDimension, NewAddedMVTimestampDimension, NewAddedMVStringDimension, "
            + "NewAddedSVJSONDimension, NewAddedSVBytesDimension";
    //@formatter:on
    pinotQuery = "SELECT " + newAddedDimensions + ", SUM(NewAddedIntMetric), SUM(NewAddedLongMetric), "
        + "SUM(NewAddedFloatMetric), SUM(NewAddedDoubleMetric) FROM mytable GROUP BY " + newAddedDimensions;
    response = postQuery(pinotQuery);
    rows = response.get("resultTable").get("rows");
    assertEquals(rows.size(), 1);
    row = rows.get(0);
    assertEquals(row.size(), 13);
    assertEquals(row.get(0).asInt(), Integer.MIN_VALUE);
    assertEquals(row.get(1).asLong(), Long.MIN_VALUE);
    assertEquals(row.get(2).asDouble(), (double) Float.NEGATIVE_INFINITY);
    assertEquals(row.get(3).asDouble(), Double.NEGATIVE_INFINITY);
    assertFalse(row.get(4).asBoolean());
    assertEquals(row.get(5).asText(), new Timestamp(0).toString());
    assertEquals(row.get(6).asText(), "null");
    assertEquals(row.get(7).asText(), "null");
    assertEquals(row.get(8).asText(), "");
    assertEquals(row.get(9).asDouble(), numTotalDocsInDouble);
    assertEquals(row.get(10).asDouble(), numTotalDocsInDouble);
    assertEquals(row.get(11).asDouble(), 0.0);
    assertEquals(row.get(12).asDouble(), 0.0);
  }

  private void testRegexpLikeOnNewAddedColumns()
      throws Exception {
    int numTotalDocs = (int) getCountStarResult();

    // REGEXP_LIKE on new added dictionary-encoded columns should not scan the table when it matches all or nothing
    for (String column : List.of("NewAddedSVJSONDimension", "NewAddedDerivedNullString")) {
      JsonNode response = postQuery("SELECT COUNT(*) FROM mytable WHERE REGEXP_LIKE(" + column + ", 'foo')");
      assertEquals(response.get("resultTable").get("rows").get(0).get(0).asInt(), 0);
      assertEquals(response.get("numEntriesScannedInFilter").asInt(), 0);

      response = postQuery("SELECT COUNT(*) FROM mytable WHERE REGEXP_LIKE(" + column + ", '.*')");
      assertEquals(response.get("resultTable").get("rows").get(0).get(0).asInt(), numTotalDocs);
      assertEquals(response.get("numEntriesScannedInFilter").asInt(), 0);
    }
  }

  private void testExpressionOverride()
      throws Exception {
    String query = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch * 24 = 392184";

    // Initially there is no expression override
    {
      JsonNode response = postQuery(query);
      assertEquals(response.get("numSegmentsProcessed").asInt(), 12);
      assertEquals(response.get("numEntriesScannedInFilter").asInt(), getCountStarResult());
    }

    // Add expression override
    TableConfig tableConfig = createOfflineTableConfig();
    tableConfig.setQueryConfig(
        new QueryConfig(null, null, null, Map.of("DaysSinceEpoch * 24", "NewAddedDerivedHoursSinceEpoch"), null, null));
    updateTableConfig(tableConfig);

    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode response = postQuery(query);
        return response.get("numSegmentsProcessed").asInt() == 1
            && response.get("numEntriesScannedInFilter").asInt() == 0;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 60_000L, "Failed to add expression override");

    // Remove expression override
    updateTableConfig(_tableConfig);

    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode response = postQuery(query);
        return response.get("numSegmentsProcessed").asInt() == 12
            && response.get("numEntriesScannedInFilter").asInt() == getCountStarResult();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 60_000L, "Failed to remove expression override");
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testInvisibleColumns(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    long numTotalDocs = getCountStarResult();
    TableConfig tableConfig = createOfflineTableConfig();
    Schema schema = createSchema();

    schema.addField(new MetricFieldSpec("$IntMetric", DataType.INT, 1));
    schema.addField(new DateTimeFieldSpec("$DerivedTimestamp", DataType.TIMESTAMP, "TIMESTAMP", "1:DAYS"));
    updateSchema(schema);

    List<TransformConfig> transformConfigs =
        List.of(new TransformConfig("$DerivedTimestamp", "DaysSinceEpoch * 24 * 3600 * 1000"));
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setTransformConfigs(transformConfigs);
    tableConfig.setIngestionConfig(ingestionConfig);
    updateTableConfig(tableConfig);

    // Trigger reload and verify column count doesn't change in query but changes in segment metadata
    reloadAllSegments(TEST_REGULAR_COLUMNS_QUERY, false, numTotalDocs);
    assertEquals(postQuery(SELECT_STAR_QUERY).get("resultTable").get("dataSchema").get("columnNames").size(), 79);
    JsonNode segmentsMetadata = JsonUtils.stringToJsonNode(
        sendGetRequest(_controllerRequestURLBuilder.forSegmentsMetadataFromServer(getTableName(), List.of("*"))));
    assertEquals(segmentsMetadata.size(), 12);
    for (JsonNode segmentMetadata : segmentsMetadata) {
      assertEquals(segmentMetadata.get("columns").size(), 81);
    }

    String pinotQuery = "SELECT COUNT(*) FROM mytable WHERE $IntMetric = 1 AND $DerivedTimestamp = 1411862400000";
    String h2Query = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch = 16341";
    testQuery(pinotQuery, h2Query);

    // Reset the table config first to pass the validation
    updateTableConfig(_tableConfig);

    // Need to force update the schema because removing columns is backward-incompatible change
    forceUpdateSchema(_schema);

    reloadAllSegments(TEST_REGULAR_COLUMNS_QUERY, false, numTotalDocs);
    assertEquals(postQuery(SELECT_STAR_QUERY).get("resultTable").get("dataSchema").get("columnNames").size(), 79);
    segmentsMetadata = JsonUtils.stringToJsonNode(
        sendGetRequest(_controllerRequestURLBuilder.forSegmentsMetadataFromServer(getTableName(), List.of("*"))));
    assertEquals(segmentsMetadata.size(), 12);
    for (JsonNode segmentMetadata : segmentsMetadata) {
      assertEquals(segmentMetadata.get("columns").size(), 79);
    }
  }

  @Test
  public void testDisableGroovyQueryTableConfigOverride()
      throws Exception {
    String groovyQuery = "SELECT GROOVY('{\"returnType\":\"STRING\",\"isSingleValue\":true}', "
        + "'arg0 + arg1', FlightNum, Origin) FROM mytable";

    // Query should not throw exception
    postQuery(groovyQuery);

    // Remove query config
    TableConfig tableConfig = createOfflineTableConfig();
    tableConfig.setQueryConfig(null);
    updateTableConfig(tableConfig);
    TestUtils.waitForCondition(aVoid -> {
      try {
        postQuery(groovyQuery);
        return false;
      } catch (Exception e) {
        // expected
        return true;
      }
    }, 60_000L, "Failed to reject Groovy query without query table config override");

    // Reset query config
    updateTableConfig(_tableConfig);
    TestUtils.waitForCondition(aVoid -> {
      try {
        // Query should not throw exception
        postQuery(groovyQuery);
        return true;
      } catch (Exception e) {
        return false;
      }
    }, 60_000L, "Failed to accept Groovy query with table override");
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testBrokerResponseMetadata(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    super.testBrokerResponseMetadata();
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testInBuiltVirtualColumns(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    String query = "SELECT $docId, $hostName, $segmentName FROM mytable LIMIT 10";
    JsonNode response = postQuery(query);
    JsonNode resultTable = response.get("resultTable");
    JsonNode dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnNames").toString(), "[\"$docId\",\"$hostName\",\"$segmentName\"]");
    assertEquals(dataSchema.get("columnDataTypes").toString(), "[\"INT\",\"STRING\",\"STRING\"]");
    JsonNode rows = resultTable.get("rows");
    assertEquals(rows.size(), 10);
    String expectedHostName = NetUtils.getHostnameOrAddress();
    String expectedSegmentNamePrefix = "mytable_";
    for (int i = 0; i < 10; i++) {
      JsonNode row = rows.get(i);
      assertEquals(row.get(0).asInt(), i);
      assertEquals(row.get(1).asText(), expectedHostName);
      String segmentName = row.get(2).asText();
      assertTrue(segmentName.startsWith(expectedSegmentNamePrefix));
    }

    // Collect all segment names
    query = "SELECT DISTINCT $segmentName FROM mytable LIMIT 10000";
    response = postQuery(query);
    rows = response.get("resultTable").get("rows");
    int numSegments = rows.size();
    List<String> segmentNames = new ArrayList<>(numSegments);
    for (int i = 0; i < numSegments; i++) {
      segmentNames.add(rows.get(i).get(0).asText());
    }
    // Test IN clause on $segmentName
    Collections.shuffle(segmentNames);
    int numSegmentsToQuery = RANDOM.nextInt(numSegments) + 1;
    query = "SELECT COUNT(*) FROM mytable WHERE $segmentName IN ('" + String.join("','",
        segmentNames.subList(0, numSegmentsToQuery)) + "')";
    response = postQuery(query);
    assertEquals(response.get("numSegmentsMatched").asInt(), numSegmentsToQuery);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testGroupByUDF(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = "SELECT timeConvert(DaysSinceEpoch,'DAYS','SECONDS'), COUNT(*) FROM mytable "
        + "GROUP BY timeConvert(DaysSinceEpoch,'DAYS','SECONDS') ORDER BY COUNT(*) DESC";
    JsonNode response = postQuery(query);
    JsonNode resultTable = response.get("resultTable");
    JsonNode dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnDataTypes").toString(), "[\"LONG\",\"LONG\"]");
    JsonNode rows = resultTable.get("rows");
    assertFalse(rows.isEmpty());
    JsonNode row = rows.get(0);
    assertEquals(row.size(), 2);
    assertEquals(row.get(0).asLong(), 16138 * 24 * 3600);
    assertEquals(row.get(1).asLong(), 605);

    query = "SELECT dateTimeConvert(DaysSinceEpoch,'1:DAYS:EPOCH','1:HOURS:EPOCH','1:HOURS'), COUNT(*) FROM mytable "
        + "GROUP BY dateTimeConvert(DaysSinceEpoch,'1:DAYS:EPOCH','1:HOURS:EPOCH','1:HOURS') ORDER BY COUNT(*) DESC";
    response = postQuery(query);
    resultTable = response.get("resultTable");
    dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnDataTypes").toString(), "[\"LONG\",\"LONG\"]");
    rows = resultTable.get("rows");
    assertFalse(rows.isEmpty());
    row = rows.get(0);
    assertEquals(row.size(), 2);
    assertEquals(row.get(0).asLong(), 16138 * 24);
    assertEquals(row.get(1).asLong(), 605);

    query = "SELECT arrayLength(DivAirports), COUNT(*) FROM mytable "
        + "GROUP BY arrayLength(DivAirports) ORDER BY COUNT(*) DESC";
    response = postQuery(query);
    resultTable = response.get("resultTable");
    dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnDataTypes").toString(), "[\"INT\",\"LONG\"]");
    rows = resultTable.get("rows");
    assertFalse(rows.isEmpty());
    row = rows.get(0);
    assertEquals(row.size(), 2);
    assertEquals(row.get(0).asInt(), 5);
    assertEquals(row.get(1).asLong(), 115545);

    query = "SELECT arrayLength(valueIn(DivAirports,'DFW','ORD')), COUNT(*) FROM mytable GROUP BY "
        + "arrayLength(valueIn(DivAirports,'DFW','ORD')) ORDER BY COUNT(*) DESC";
    response = postQuery(query);
    resultTable = response.get("resultTable");
    dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnDataTypes").toString(), "[\"INT\",\"LONG\"]");
    rows = resultTable.get("rows");
    assertEquals(rows.size(), 3);
    row = rows.get(0);
    assertEquals(row.size(), 2);
    assertEquals(row.get(0).asInt(), 0);
    assertEquals(row.get(1).asLong(), 114895);
    row = rows.get(1);
    assertEquals(row.size(), 2);
    assertEquals(row.get(0).asInt(), 1);
    assertEquals(row.get(1).asLong(), 648);
    row = rows.get(2);
    assertEquals(row.size(), 2);
    assertEquals(row.get(0).asInt(), 2);
    assertEquals(row.get(1).asLong(), 2);

    if (useMultiStageQueryEngine()) {
      query = "SELECT arrayToMV(valueIn(DivAirports,'DFW','ORD')), COUNT(*) FROM mytable "
          + "GROUP BY arrayToMV(valueIn(DivAirports,'DFW','ORD')) ORDER BY COUNT(*) DESC";
    } else {
      query = "SELECT valueIn(DivAirports,'DFW','ORD'), COUNT(*) FROM mytable "
          + "GROUP BY valueIn(DivAirports,'DFW','ORD') ORDER BY COUNT(*) DESC";
    }
    response = postQuery(query);
    resultTable = response.get("resultTable");
    dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnDataTypes").toString(), "[\"STRING\",\"LONG\"]");
    rows = resultTable.get("rows");
    assertEquals(rows.size(), 2);
    row = rows.get(0);
    assertEquals(row.size(), 2);
    assertEquals(row.get(0).asText(), "ORD");
    assertEquals(row.get(1).asLong(), 336);
    row = rows.get(1);
    assertEquals(row.size(), 2);
    assertEquals(row.get(0).asText(), "DFW");
    assertEquals(row.get(1).asLong(), 316);

    query = "SELECT div(DaysSinceEpoch,2), COUNT(*) FROM mytable "
        + "GROUP BY div(DaysSinceEpoch,2) ORDER BY COUNT(*) DESC";
    response = postQuery(query);
    resultTable = response.get("resultTable");
    dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnDataTypes").toString(), "[\"DOUBLE\",\"LONG\"]");
    rows = resultTable.get("rows");
    assertFalse(rows.isEmpty());
    row = rows.get(0);
    assertEquals(row.size(), 2);
    assertEquals(row.get(0).asDouble(), 16138.0 / 2);
    assertEquals(row.get(1).asLong(), 605);
  }

  @Test
  public void testGroupByUDFV1()
      throws Exception {
    setUseMultiStageQueryEngine(false);
    String query = "SELECT add(DaysSinceEpoch,DaysSinceEpoch,15), COUNT(*) FROM mytable "
        + "GROUP BY add(DaysSinceEpoch,DaysSinceEpoch,15) ORDER BY COUNT(*) DESC";
    JsonNode response = postQuery(query);
    JsonNode resultTable = response.get("resultTable");
    JsonNode dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnDataTypes").toString(), "[\"DOUBLE\",\"LONG\"]");
    JsonNode rows = resultTable.get("rows");
    assertFalse(rows.isEmpty());
    JsonNode row = rows.get(0);
    assertEquals(row.size(), 2);
    assertEquals(row.get(0).asDouble(), 16138.0 + 16138 + 15);
    assertEquals(row.get(1).asLong(), 605);

    query = "SELECT sub(DaysSinceEpoch,25), COUNT(*) FROM mytable "
        + "GROUP BY sub(DaysSinceEpoch,25) ORDER BY COUNT(*) DESC";
    response = postQuery(query);
    resultTable = response.get("resultTable");
    dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnDataTypes").toString(), "[\"DOUBLE\",\"LONG\"]");
    rows = resultTable.get("rows");
    assertFalse(rows.isEmpty());
    row = rows.get(0);
    assertEquals(row.size(), 2);
    assertEquals(row.get(0).asDouble(), 16138.0 - 25);
    assertEquals(row.get(1).asLong(), 605);

    query = "SELECT mult(DaysSinceEpoch,24,3600), COUNT(*) FROM mytable "
        + "GROUP BY mult(DaysSinceEpoch,24,3600) ORDER BY COUNT(*) DESC";
    response = postQuery(query);
    resultTable = response.get("resultTable");
    dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnDataTypes").toString(), "[\"DOUBLE\",\"LONG\"]");
    rows = resultTable.get("rows");
    assertFalse(rows.isEmpty());
    row = rows.get(0);
    assertEquals(row.size(), 2);
    assertEquals(row.get(0).asDouble(), 16138.0 * 24 * 3600);
    assertEquals(row.get(1).asLong(), 605);
  }

  @Test
  public void testGroupByUDFV2()
      throws Exception {
    setUseMultiStageQueryEngine(true);
    String query = "SELECT add(DaysSinceEpoch,add(DaysSinceEpoch,15)), COUNT(*) FROM mytable "
        + "GROUP BY add(DaysSinceEpoch,add(DaysSinceEpoch,15)) ORDER BY COUNT(*) DESC";
    JsonNode response = postQuery(query);
    JsonNode resultTable = response.get("resultTable");
    JsonNode dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnDataTypes").toString(), "[\"INT\",\"LONG\"]");
    JsonNode rows = resultTable.get("rows");
    assertFalse(rows.isEmpty());
    JsonNode row = rows.get(0);
    assertEquals(row.size(), 2);
    assertEquals(row.get(0).asInt(), 16138 + 16138 + 15);
    assertEquals(row.get(1).asLong(), 605);

    query = "SELECT sub(DaysSinceEpoch,25), COUNT(*) FROM mytable "
        + "GROUP BY sub(DaysSinceEpoch,25) ORDER BY COUNT(*) DESC";
    response = postQuery(query);
    resultTable = response.get("resultTable");
    dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnDataTypes").toString(), "[\"INT\",\"LONG\"]");
    rows = resultTable.get("rows");
    assertFalse(rows.isEmpty());
    row = rows.get(0);
    assertEquals(row.size(), 2);
    assertEquals(row.get(0).asInt(), 16138 - 25);
    assertEquals(row.get(1).asLong(), 605);

    query = "SELECT mult(DaysSinceEpoch,mult(24,3600)), COUNT(*) FROM mytable "
        + "GROUP BY mult(DaysSinceEpoch,mult(24,3600)) ORDER BY COUNT(*) DESC";
    response = postQuery(query);
    resultTable = response.get("resultTable");
    dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnDataTypes").toString(), "[\"INT\",\"LONG\"]");
    rows = resultTable.get("rows");
    assertFalse(rows.isEmpty());
    row = rows.get(0);
    assertEquals(row.size(), 2);
    assertEquals(row.get(0).asInt(), 16138 * 24 * 3600);
    assertEquals(row.get(1).asLong(), 605);
  }

  @Test
  public void testAggregationUDFV1()
      throws Exception {
    String query = "SELECT MAX(timeConvert(DaysSinceEpoch,'DAYS','SECONDS')) FROM mytable";
    JsonNode response = postQuery(query);
    JsonNode resultTable = response.get("resultTable");
    JsonNode dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnNames").toString(), "[\"max(timeconvert(DaysSinceEpoch,'DAYS','SECONDS'))\"]");
    assertEquals(dataSchema.get("columnDataTypes").toString(), "[\"DOUBLE\"]");
    JsonNode rows = resultTable.get("rows");
    assertEquals(rows.size(), 1);
    JsonNode row = rows.get(0);
    assertEquals(row.size(), 1);
    assertEquals(row.get(0).asDouble(), 16435.0 * 24 * 3600);

    query = "SELECT MIN(div(DaysSinceEpoch,2)) FROM mytable";
    response = postQuery(query);
    resultTable = response.get("resultTable");
    dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnNames").toString(), "[\"min(div(DaysSinceEpoch,'2'))\"]");
    assertEquals(dataSchema.get("columnDataTypes").toString(), "[\"DOUBLE\"]");
    rows = resultTable.get("rows");
    assertEquals(rows.size(), 1);
    row = rows.get(0);
    assertEquals(row.size(), 1);
    assertEquals(row.get(0).asDouble(), 16071.0 / 2);
  }

  @Test
  public void testAggregationUDFV2()
      throws Exception {
    setUseMultiStageQueryEngine(true);
    String query = "SELECT MAX(timeConvert(DaysSinceEpoch,'DAYS','SECONDS')) FROM mytable";
    JsonNode response = postQuery(query);
    JsonNode resultTable = response.get("resultTable");
    JsonNode dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnDataTypes").toString(), "[\"LONG\"]");
    JsonNode rows = resultTable.get("rows");
    assertEquals(rows.size(), 1);
    JsonNode row = rows.get(0);
    assertEquals(row.size(), 1);
    assertEquals(row.get(0).asDouble(), 16435.0 * 24 * 3600);

    query = "SELECT MIN(div(DaysSinceEpoch,2)) FROM mytable";
    response = postQuery(query);
    resultTable = response.get("resultTable");
    dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnDataTypes").toString(), "[\"DOUBLE\"]");
    rows = resultTable.get("rows");
    assertEquals(rows.size(), 1);
    row = rows.get(0);
    assertEquals(row.size(), 1);
    assertEquals(row.get(0).asDouble(), 16071.0 / 2);
  }

  @Test
  public void testWindowAggregationV2()
      throws Exception {
    setUseMultiStageQueryEngine(true);
    String tmpTableQuery =
        "select DaysSinceEpoch, count(*) as num_trips from mytable GROUP BY DaysSinceEpoch order by DaysSinceEpoch";
    JsonNode tmpTableResult = postQuery(tmpTableQuery).get("resultTable").get("rows");

    //@formatter:off
    String query = "WITH tmp AS (\n"
        + "  select count(*) as num_trips, DaysSinceEpoch  from mytable GROUP BY DaysSinceEpoch\n"
        + ")\n"
        + "\n"
        + "SELECT\n"
        + "    DaysSinceEpoch,\n"
        + "    num_trips,\n"
        + "    LAG(num_trips, 2) OVER (ORDER BY DaysSinceEpoch) AS previous_num_trips,\n"
        + "    num_trips - LAG(num_trips, 2) OVER (ORDER BY DaysSinceEpoch) AS difference\n"
        + "FROM\n"
        + "    tmp";
    //@formatter:on
    JsonNode response = postQuery(query);
    JsonNode resultTable = response.get("resultTable");
    assertEquals(resultTable.get("dataSchema").get("columnDataTypes").toString(),
        "[\"INT\",\"LONG\",\"LONG\",\"LONG\"]");
    JsonNode rows = resultTable.get("rows");
    assertEquals(rows.size(), 364);
    for (int i = 0; i < 2; i++) {
      JsonNode row = rows.get(i);
      JsonNode tmpTableRow = tmpTableResult.get(i);
      assertEquals(row.size(), 4);
      assertEquals(row.get(0).asInt(), tmpTableRow.get(0).asInt());
      assertEquals(row.get(1).asLong(), tmpTableRow.get(1).asLong());
      assertTrue(row.get(2).isNull());
      assertTrue(row.get(3).isNull());
    }
    for (int i = 2; i < 363; i++) {
      JsonNode row = rows.get(i);
      assertEquals(row.size(), 4);
      JsonNode tmpTableRow = tmpTableResult.get(i);
      assertEquals(row.get(0).asInt(), tmpTableRow.get(0).asInt());
      assertEquals(row.get(1).asLong(), tmpTableRow.get(1).asLong());
      assertEquals(rows.get(i - 2).get(1).asLong(), row.get(2).asLong());
      assertEquals(row.get(1).asLong() - row.get(2).asLong(), row.get(3).asLong());
    }

    //@formatter:off
    query = "WITH tmp AS (\n"
        + "  select count(*) as num_trips, DaysSinceEpoch  from mytable GROUP BY DaysSinceEpoch\n"
        + ")\n"
        + "\n"
        + "SELECT\n"
        + "    DaysSinceEpoch,\n"
        + "    num_trips,\n"
        + "    LAG(num_trips, '2') OVER (ORDER BY DaysSinceEpoch) AS previous_num_trips,\n"
        + "    num_trips - LAG(num_trips, '2') OVER (ORDER BY DaysSinceEpoch) AS difference\n"
        + "FROM\n"
        + "    tmp";
    //@formatter:on
    response = postQuery(query);
    resultTable = response.get("resultTable");
    assertEquals(resultTable.get("dataSchema").get("columnDataTypes").toString(),
        "[\"INT\",\"LONG\",\"LONG\",\"LONG\"]");
    rows = resultTable.get("rows");
    assertEquals(rows.size(), 364);
    for (int i = 0; i < 2; i++) {
      JsonNode row = rows.get(i);
      JsonNode tmpTableRow = tmpTableResult.get(i);
      assertEquals(row.size(), 4);
      assertEquals(row.get(0).asInt(), tmpTableRow.get(0).asInt());
      assertEquals(row.get(1).asLong(), tmpTableRow.get(1).asLong());
      assertTrue(row.get(2).isNull());
      assertTrue(row.get(2).isNull());
    }
    for (int i = 2; i < 363; i++) {
      JsonNode row = rows.get(i);
      assertEquals(row.size(), 4);
      JsonNode tmpTableRow = tmpTableResult.get(i);
      assertEquals(row.get(0).asInt(), tmpTableRow.get(0).asInt());
      assertEquals(row.get(1).asLong(), tmpTableRow.get(1).asLong());
      assertEquals(rows.get(i - 2).get(1).asLong(), row.get(2).asLong());
      assertEquals(row.get(1).asLong() - row.get(2).asLong(), row.get(3).asLong());
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testSelectionUDF(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = "SELECT DaysSinceEpoch, timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM mytable limit 10";
    JsonNode response = postQuery(query);
    JsonNode resultTable = response.get("resultTable");
    JsonNode dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnDataTypes").toString(), "[\"INT\",\"LONG\"]");
    JsonNode rows = response.get("resultTable").get("rows");
    assertEquals(rows.size(), 10);
    for (int i = 0; i < 10; i++) {
      long daysSinceEpoch = rows.get(i).get(0).asInt();
      long secondsSinceEpoch = rows.get(i).get(1).asLong();
      assertEquals(daysSinceEpoch * 24 * 60 * 60, secondsSinceEpoch);
    }

    query = "SELECT DaysSinceEpoch, timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM mytable "
        + "ORDER BY DaysSinceEpoch LIMIT 10000";
    response = postQuery(query);
    resultTable = response.get("resultTable");
    dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnDataTypes").toString(), "[\"INT\",\"LONG\"]");
    rows = response.get("resultTable").get("rows");
    assertEquals(rows.size(), 10000);
    long prevValue = -1;
    for (int i = 0; i < 10000; i++) {
      long daysSinceEpoch = rows.get(i).get(0).asInt();
      long secondsSinceEpoch = rows.get(i).get(1).asLong();
      assertEquals(daysSinceEpoch * 24 * 60 * 60, secondsSinceEpoch);
      assertTrue(daysSinceEpoch >= prevValue);
      prevValue = daysSinceEpoch;
    }

    query = "SELECT DaysSinceEpoch, timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM mytable "
        + "ORDER BY timeConvert(DaysSinceEpoch,'DAYS','SECONDS') DESC LIMIT 10000";
    response = postQuery(query);
    resultTable = response.get("resultTable");
    dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnDataTypes").toString(), "[\"INT\",\"LONG\"]");
    rows = response.get("resultTable").get("rows");
    assertEquals(rows.size(), 10000);
    prevValue = Long.MAX_VALUE;
    for (int i = 0; i < 10000; i++) {
      long daysSinceEpoch = rows.get(i).get(0).asInt();
      long secondsSinceEpoch = rows.get(i).get(1).asLong();
      assertEquals(daysSinceEpoch * 24 * 60 * 60, secondsSinceEpoch);
      assertTrue(secondsSinceEpoch <= prevValue);
      prevValue = secondsSinceEpoch;
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testFilterUDF(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    int daysSinceEpoch = 16138;
    long secondsSinceEpoch = 16138 * 24 * 60 * 60;

    String query;
    query = "SELECT count(*) FROM mytable WHERE DaysSinceEpoch = " + daysSinceEpoch;
    long expectedResult = postQuery(query).get("resultTable").get("rows").get(0).get(0).asLong();

    query = "SELECT count(*) FROM mytable WHERE timeConvert(DaysSinceEpoch,'DAYS','SECONDS') = " + secondsSinceEpoch;
    assertEquals(postQuery(query).get("resultTable").get("rows").get(0).get(0).asLong(), expectedResult);

    query = "SELECT count(*) FROM mytable WHERE DaysSinceEpoch = " + daysSinceEpoch
        + " OR timeConvert(DaysSinceEpoch,'DAYS','SECONDS') = " + secondsSinceEpoch;
    assertEquals(postQuery(query).get("resultTable").get("rows").get(0).get(0).asLong(), expectedResult);

    query = "SELECT count(*) FROM mytable WHERE DaysSinceEpoch = " + daysSinceEpoch
        + " AND timeConvert(DaysSinceEpoch,'DAYS','SECONDS') = " + secondsSinceEpoch;
    assertEquals(postQuery(query).get("resultTable").get("rows").get(0).get(0).asLong(), expectedResult);

    query =
        "SELECT count(*) FROM mytable WHERE DIV(timeConvert(DaysSinceEpoch,'DAYS','SECONDS'),1) = " + secondsSinceEpoch;
    assertEquals(postQuery(query).get("resultTable").get("rows").get(0).get(0).asLong(), expectedResult);

    query = String.format("SELECT count(*) FROM mytable WHERE timeConvert(DaysSinceEpoch,'DAYS','SECONDS') IN (%d, %d)",
        secondsSinceEpoch - 100, secondsSinceEpoch);
    assertEquals(postQuery(query).get("resultTable").get("rows").get(0).get(0).asLong(), expectedResult);

    query = String.format(
        "SELECT count(*) FROM mytable WHERE timeConvert(DaysSinceEpoch,'DAYS','SECONDS') BETWEEN %d AND %d",
        secondsSinceEpoch - 100, secondsSinceEpoch);
    assertEquals(postQuery(query).get("resultTable").get("rows").get(0).get(0).asLong(), expectedResult);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testCaseStatementInSelection(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    List<String> origins =
        List.of("ATL", "ORD", "DFW", "DEN", "LAX", "IAH", "SFO", "PHX", "LAS", "EWR", "MCO", "BOS", "SLC", "SEA", "MSP",
            "CLT", "LGA", "DTW", "JFK", "BWI");
    StringBuilder caseStatementBuilder = new StringBuilder("CASE ");
    for (int i = 0; i < origins.size(); i++) {
      // WHEN Origin = 'ATL' THEN 1
      // WHEN Origin = 'ORD' THEN 2
      // WHEN Origin = 'DFW' THEN 3
      // ....
      caseStatementBuilder.append(String.format("WHEN Origin = '%s' THEN %d ", origins.get(i), i + 1));
    }
    caseStatementBuilder.append("ELSE 0 END");
    String sqlQuery = "SELECT Origin, " + caseStatementBuilder + " AS origin_code FROM mytable LIMIT 1000";
    JsonNode response = postQuery(sqlQuery);
    JsonNode rows = response.get("resultTable").get("rows");
    assertTrue(response.get("exceptions").isEmpty());
    for (int i = 0; i < rows.size(); i++) {
      String origin = rows.get(i).get(0).asText();
      int originCode = rows.get(i).get(1).asInt();
      if (originCode > 0) {
        assertEquals(origin, origins.get(originCode - 1));
      } else {
        assertFalse(origins.contains(origin));
      }
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testCaseStatementInSelectionWithTransformFunctionInThen(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String sqlQuery = "SELECT ArrDelay, "
        + "CASE WHEN ArrDelay > 0 THEN ArrDelay WHEN ArrDelay < 0 THEN ArrDelay * -1 ELSE 0 END AS ArrTimeDiff "
        + "FROM mytable LIMIT 1000";
    JsonNode response = postQuery(sqlQuery);
    JsonNode rows = response.get("resultTable").get("rows");
    assertTrue(response.get("exceptions").isEmpty());
    for (int i = 0; i < rows.size(); i++) {
      int arrDelay = rows.get(i).get(0).asInt();
      int arrDelayDiff = rows.get(i).get(1).asInt();
      if (arrDelay > 0) {
        assertEquals(arrDelay, arrDelayDiff);
      } else {
        assertEquals(arrDelay, arrDelayDiff * -1);
      }
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testCaseStatementWithLogicalTransformFunction(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String sqlQuery = "SELECT ArrDelay, "
        + "CASE WHEN ArrDelay > 50 OR ArrDelay < 10 THEN 10 ELSE 0 END, "
        + "CASE WHEN ArrDelay < 50 AND ArrDelay >= 10 THEN 10 ELSE 0 END "
        + "FROM mytable LIMIT 1000";
    JsonNode response = postQuery(sqlQuery);
    JsonNode rows = response.get("resultTable").get("rows");
    assertTrue(response.get("exceptions").isEmpty());
    for (int i = 0; i < rows.size(); i++) {
      int row0 = rows.get(i).get(0).asInt();
      int row1 = rows.get(i).get(1).asInt();
      int row2 = rows.get(i).get(2).asInt();
      if (row0 > 50 || row0 < 10) {
        assertEquals(row1, 10);
      } else {
        assertEquals(row1, 0);
      }
      if (row0 < 50 && row0 >= 10) {
        assertEquals(row2, 10);
      } else {
        assertEquals(row2, 0);
      }
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testCaseStatementWithInAggregation(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    testCountVsCaseQuery("Origin = 'ATL'");
    testCountVsCaseQuery("Origin <> 'ATL'");

    testCountVsCaseQuery("DaysSinceEpoch > 16312");
    testCountVsCaseQuery("DaysSinceEpoch >= 16312");
    testCountVsCaseQuery("DaysSinceEpoch < 16312");
    testCountVsCaseQuery("DaysSinceEpoch <= 16312");
    testCountVsCaseQuery("DaysSinceEpoch = 16312");
    testCountVsCaseQuery("DaysSinceEpoch <> 16312");
  }

  private void testCountVsCaseQuery(String predicate)
      throws Exception {
    String sqlQuery = String.format("SELECT COUNT(*) FROM mytable WHERE %s", predicate);
    JsonNode response = postQuery(sqlQuery);
    long countValue = response.get("resultTable").get("rows").get(0).get(0).asLong();
    sqlQuery = String.format("SELECT SUM(CASE WHEN %s THEN 1 ELSE 0 END) as sum1 FROM mytable", predicate);
    response = postQuery(sqlQuery);
    long caseSum = response.get("resultTable").get("rows").get(0).get(0).asLong();
    assertEquals(caseSum, countValue);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testFilterWithInvertedIndexUDF(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    int daysSinceEpoch = 16138;
    long secondsSinceEpoch = 16138 * 24 * 60 * 60;

    //@formatter:off
    String[] origins = new String[]{
        "ATL", "ORD", "DFW", "DEN", "LAX", "IAH", "SFO", "PHX", "LAS", "EWR", "MCO", "BOS", "SLC", "SEA", "MSP", "CLT",
        "LGA", "DTW", "JFK", "BWI"
    };
    //@formatter:on
    for (String origin : origins) {
      String query =
          "SELECT COUNT(*) FROM mytable WHERE Origin = '" + origin + "' AND DaysSinceEpoch = " + daysSinceEpoch;
      JsonNode response1 = postQuery(query);
      query = "SELECT COUNT(*) FROM mytable WHERE Origin = '" + origin
          + "' AND timeConvert(DaysSinceEpoch,'DAYS','SECONDS') = " + secondsSinceEpoch;
      JsonNode response2 = postQuery(query);
      long value1 = response1.get("resultTable").get("rows").get(0).get(0).asLong();
      long value2 = response2.get("resultTable").get("rows").get(0).get(0).asLong();
      assertEquals(value1, value2);
    }
  }

  @Test
  public void testQueryWithRepeatedColumnsV1()
      throws Exception {
    //test repeated columns in selection query
    String query = "SELECT ArrTime, ArrTime FROM mytable WHERE DaysSinceEpoch <= 16312 AND Carrier = 'DL'";
    testQuery(query);

    //test repeated columns in selection query with order by
    query = "SELECT ArrTime, ArrTime FROM mytable WHERE DaysSinceEpoch <= 16312 AND Carrier = 'DL' order by ArrTime";
    testQuery(query);

    //test repeated columns in agg query
    query = "SELECT COUNT(*), COUNT(*) FROM mytable WHERE DaysSinceEpoch <= 16312 AND Carrier = 'DL'";
    testQuery(query);

    //test repeated columns in agg group by query
    query = "SELECT ArrTime, ArrTime, COUNT(*), COUNT(*) FROM mytable WHERE DaysSinceEpoch <= 16312 AND Carrier = 'DL' "
        + "GROUP BY ArrTime, ArrTime";
    testQuery(query);
  }

  // these tests actually checks a calcite limitation.
  // Once it is fixed in calcite, we should merge this tests with testQueryRepetedColumnsV1
  @Test
  public void testQueryWithRepeatedColumnsV2()
      throws Exception {
    setUseMultiStageQueryEngine(true);
    //test repeated columns in selection query
    String query = "SELECT ArrTime, ArrTime FROM mytable WHERE DaysSinceEpoch <= 16312 AND Carrier = 'DL'";
    testQuery(query);

    //test repeated columns in selection query with order by
    query = "SELECT ArrTime, ArrTime FROM mytable WHERE DaysSinceEpoch <= 16312 AND Carrier = 'DL' order by ArrTime";
    testQueryError(query, QueryErrorCode.QUERY_VALIDATION);

    //test repeated columns in agg query
    query = "SELECT COUNT(*), COUNT(*) FROM mytable WHERE DaysSinceEpoch <= 16312 AND Carrier = 'DL'";
    testQuery(query);

    //test repeated columns in agg group by query
    query = "SELECT ArrTime, ArrTime, COUNT(*), COUNT(*) FROM mytable WHERE DaysSinceEpoch <= 16312 AND Carrier = 'DL' "
        + "GROUP BY ArrTime, ArrTime";
    testQueryError(query, QueryErrorCode.QUERY_VALIDATION);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testQueryWithOrderby(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    //test repeated columns in selection query
    String query = "SELECT ArrTime, Carrier, DaysSinceEpoch FROM mytable ORDER BY DaysSinceEpoch DESC";
    testQuery(query);

    //test repeated columns in selection query
    query = "SELECT ArrTime, DaysSinceEpoch, Carrier FROM mytable ORDER BY Carrier DESC";
    testQuery(query);

    //test repeated columns in selection query
    query = "SELECT ArrTime, DaysSinceEpoch, Carrier FROM mytable ORDER BY Carrier DESC, ArrTime DESC";
    testQuery(query);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testQueryWithAlias(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    {
      String pinotQuery = "SELECT count(*), DaysSinceEpoch as d FROM mytable WHERE d = 16138 GROUP BY d";

      assertQuery(pinotQuery)
          .firstException()
          .hasErrorCode(QueryErrorCode.UNKNOWN_COLUMN);
    }
    {
      //test same alias name with column name
      String query = "SELECT ArrTime AS ArrTime, Carrier AS Carrier, DaysSinceEpoch AS DaysSinceEpoch FROM mytable "
          + "ORDER BY DaysSinceEpoch DESC";
      testQuery(query);

      query = "SELECT ArrTime AS ArrTime, DaysSinceEpoch AS DaysSinceEpoch, Carrier AS Carrier FROM mytable "
          + "ORDER BY Carrier DESC";
      testQuery(query);

      query = "SELECT ArrTime AS ArrTime, DaysSinceEpoch AS DaysSinceEpoch, Carrier AS Carrier FROM mytable "
          + "ORDER BY Carrier DESC, ArrTime DESC";
      testQuery(query);
    }
    {
      //test single alias
      String query = "SELECT ArrTime, Carrier AS CarrierName, DaysSinceEpoch FROM mytable ORDER BY DaysSinceEpoch DESC";
      testQuery(query);

      query = "SELECT count(*) AS cnt, max(ArrTime) as maxArrTime FROM mytable";
      testQuery(query);

      query = "SELECT count(*) AS cnt, Carrier AS CarrierName FROM mytable GROUP BY CarrierName ORDER BY cnt";
      testQuery(query);

      // Test: 1. Alias should not be applied to filter; 2. Ordinal can be properly applied
      query =
          "SELECT DaysSinceEpoch + 100 AS DaysSinceEpoch, COUNT(*) AS cnt FROM mytable WHERE DaysSinceEpoch <= 16312 "
              + "GROUP BY 1 ORDER BY 1 DESC";
      // NOTE: H2 does not support ordinal in GROUP BY
      String h2Query =
          "SELECT DaysSinceEpoch + 100 AS DaysSinceEpoch, COUNT(*) AS cnt FROM mytable WHERE DaysSinceEpoch <= 16312 "
              + "GROUP BY DaysSinceEpoch ORDER BY 1 DESC";
      testQuery(query, h2Query);
    }
    {
      //test multiple alias
      String query =
          "SELECT ArrTime, Carrier, Carrier AS CarrierName1, Carrier AS CarrierName2, DaysSinceEpoch FROM mytable "
              + "ORDER BY DaysSinceEpoch DESC";
      testQuery(query);

      query = "SELECT count(*) AS cnt, max(ArrTime) as maxArrTime1, max(ArrTime) as maxArrTime2 FROM mytable";
      testQuery(query);

      query = "SELECT count(*), count(*) AS cnt1, count(*) AS cnt2, Carrier AS CarrierName FROM mytable "
          + "GROUP BY CarrierName ORDER BY cnt2";
      testQuery(query);
    }
    {
      //test alias with distinct
      String query =
          "SELECT DISTINCT ArrTime, Carrier, Carrier AS CarrierName1, Carrier AS CarrierName2, DaysSinceEpoch "
              + "FROM mytable ORDER BY DaysSinceEpoch DESC";
      testQuery(query);

      query = "SELECT ArrTime, Carrier, Carrier AS CarrierName1, Carrier AS CarrierName2, DaysSinceEpoch FROM mytable "
          + "GROUP BY ArrTime, Carrier, DaysSinceEpoch ORDER BY DaysSinceEpoch DESC";
      testQuery(query);
    }
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    // Test instance decommission before tearing down
    testInstanceDecommission();

    // Brokers and servers has been stopped
    stopController();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  private void testInstanceDecommission()
      throws Exception {
    // Fetch all instances
    JsonNode response = JsonUtils.stringToJsonNode(sendGetRequest(_controllerRequestURLBuilder.forInstanceList()));
    JsonNode instanceList = response.get("instances");
    int numInstances = instanceList.size();
    // The total number of instances is equal to the sum of num brokers, num servers and 1 controller.
    assertEquals(numInstances, getNumBrokers() + getNumServers() + 1);

    // Try to delete a server that does not exist
    String deleteInstanceRequest = _controllerRequestURLBuilder.forInstance("potato");
    try {
      sendDeleteRequest(deleteInstanceRequest);
      fail("Delete should have returned a failure status (404)");
    } catch (IOException e) {
      // Expected exception on 404 status code
    }

    // Get the server name
    String serverName = null;
    String brokerName = null;
    for (int i = 0; i < numInstances; i++) {
      String instanceId = instanceList.get(i).asText();
      InstanceType instanceType = InstanceTypeUtils.getInstanceType(instanceId);
      if (instanceType == InstanceType.SERVER) {
        serverName = instanceId;
      } else if (instanceType == InstanceType.BROKER) {
        brokerName = instanceId;
      }
    }

    // Try to delete a live server
    deleteInstanceRequest = _controllerRequestURLBuilder.forInstance(serverName);
    try {
      sendDeleteRequest(deleteInstanceRequest);
      fail("Delete should have returned a failure status (409)");
    } catch (IOException e) {
      // Expected exception on 409 status code
    }

    // Stop servers
    stopServer();

    // Try to delete a server whose information is still on the ideal state
    try {
      sendDeleteRequest(deleteInstanceRequest);
      fail("Delete should have returned a failure status (409)");
    } catch (IOException e) {
      // Expected exception on 409 status code
    }

    // Delete the table
    dropOfflineTable(getTableName());

    // Now, delete server should work
    response = JsonUtils.stringToJsonNode(sendDeleteRequest(deleteInstanceRequest));
    assertTrue(response.has("status"));

    // Try to delete a broker whose information is still live
    try {
      deleteInstanceRequest = _controllerRequestURLBuilder.forInstance(brokerName);
      sendDeleteRequest(deleteInstanceRequest);
      fail("Delete should have returned a failure status (409)");
    } catch (IOException e) {
      // Expected exception on 409 status code
    }

    // Stop brokers
    stopBroker();

    // TODO: Add test to delete broker instance. Currently, stopBroker() does not work correctly.

    // Check if '/INSTANCES/<serverName>' has been erased correctly
    String instancePath = "/" + getHelixClusterName() + "/INSTANCES/" + serverName;
    assertFalse(_propertyStore.exists(instancePath, 0));

    // Check if '/CONFIGS/PARTICIPANT/<serverName>' has been erased correctly
    String configPath = "/" + getHelixClusterName() + "/CONFIGS/PARTICIPANT/" + serverName;
    assertFalse(_propertyStore.exists(configPath, 0));
  }

  @Test
  void testDual()
      throws Exception {
    setUseMultiStageQueryEngine(false);
    JsonNode queryResponse = postQuery("SELECT 1");
    Assert.assertTrue(queryResponse.get("exceptions").isEmpty());
    Assert.assertEquals(queryResponse.get("numRowsResultSet").asInt(), 1);
  }

  @Test
  void testDualWithNotExistsTableSSE()
      throws Exception {
    setUseMultiStageQueryEngine(false);
    JsonNode queryResponse = postQuery("SELECT 1 from notExistsTable");
    Assert.assertTrue(queryResponse.get("exceptions").isEmpty());
    Assert.assertEquals(queryResponse.get("numRowsResultSet").asInt(), 1);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testDistinctQuery(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    // by default 10 rows will be returned, so use high limit
    String pinotQuery = "SELECT DISTINCT Carrier FROM mytable LIMIT 1000000";
    String h2Query = "SELECT DISTINCT Carrier FROM mytable";
    testQuery(pinotQuery, h2Query);

    pinotQuery = "SELECT DISTINCT Carrier, DestAirportID FROM mytable LIMIT 1000000";
    h2Query = "SELECT DISTINCT Carrier, DestAirportID FROM mytable";
    testQuery(pinotQuery, h2Query);

    pinotQuery = "SELECT DISTINCT Carrier, DestAirportID, DestStateName FROM mytable LIMIT 1000000";
    h2Query = "SELECT DISTINCT Carrier, DestAirportID, DestStateName FROM mytable";
    testQuery(pinotQuery, h2Query);

    pinotQuery = "SELECT DISTINCT Carrier, DestAirportID, DestCityName FROM mytable LIMIT 1000000";
    h2Query = "SELECT DISTINCT Carrier, DestAirportID, DestCityName FROM mytable";
    testQuery(pinotQuery, h2Query);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testNonAggregationGroupByQuery(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    // by default 10 rows will be returned, so use high limit
    String pinotQuery = "SELECT Carrier FROM mytable GROUP BY Carrier LIMIT 1000000";
    String h2Query = "SELECT Carrier FROM mytable GROUP BY Carrier";
    testQuery(pinotQuery, h2Query);

    pinotQuery = "SELECT Carrier, DestAirportID FROM mytable GROUP BY Carrier, DestAirportID LIMIT 1000000";
    h2Query = "SELECT Carrier, DestAirportID FROM mytable GROUP BY Carrier, DestAirportID";
    testQuery(pinotQuery, h2Query);

    pinotQuery = "SELECT Carrier, DestAirportID, DestStateName FROM mytable "
        + "GROUP BY Carrier, DestAirportID, DestStateName LIMIT 1000000";
    h2Query =
        "SELECT Carrier, DestAirportID, DestStateName FROM mytable GROUP BY Carrier, DestAirportID, DestStateName";
    testQuery(pinotQuery, h2Query);

    pinotQuery = "SELECT Carrier, DestAirportID, DestCityName FROM mytable "
        + "GROUP BY Carrier, DestAirportID, DestCityName LIMIT 1000000";
    h2Query = "SELECT Carrier, DestAirportID, DestCityName FROM mytable GROUP BY Carrier, DestAirportID, DestCityName";
    testQuery(pinotQuery, h2Query);

    pinotQuery = "SELECT ArrTime-DepTime FROM mytable GROUP BY ArrTime-DepTime LIMIT 1000000";
    h2Query = "SELECT CAST(ArrTime-DepTime AS FLOAT) FROM mytable GROUP BY CAST(ArrTime-DepTime AS FLOAT)";
    testQuery(pinotQuery, h2Query);

    pinotQuery = "SELECT ArrTime-DepTime FROM mytable GROUP BY ArrTime, DepTime LIMIT 1000000";
    h2Query = "SELECT ArrTime-DepTime FROM mytable GROUP BY ArrTime, DepTime";
    testQuery(pinotQuery, h2Query);

    pinotQuery = "SELECT ArrTime-DepTime,ArrTime/3,DepTime*2 FROM mytable GROUP BY ArrTime, DepTime LIMIT 1000000";
    h2Query = "SELECT ArrTime-DepTime,ArrTime/3,DepTime*2 FROM mytable GROUP BY ArrTime, DepTime";
    testQuery(pinotQuery, h2Query);

    pinotQuery = "SELECT ArrTime+DepTime FROM mytable GROUP BY ArrTime + DepTime LIMIT 1000000";
    h2Query = "SELECT ArrTime+DepTime FROM mytable GROUP BY ArrTime + DepTime";
    testQuery(pinotQuery, h2Query);

    pinotQuery = "SELECT ArrTime+DepTime AS A FROM mytable GROUP BY A LIMIT 1000000";
    h2Query = "SELECT CAST(ArrTime+DepTime AS FLOAT) AS A FROM mytable GROUP BY A";
    testQuery(pinotQuery, h2Query);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testCaseSensitivity(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    int daysSinceEpoch = 16138;
    int hoursSinceEpoch = 16138 * 24;
    int secondsSinceEpoch = 16138 * 24 * 60 * 60;
    List<String> baseQueries = List.of(
        "SELECT * FROM mytable limit 10000",
        "SELECT DaysSinceEpoch, timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM mytable limit 10000",
        "SELECT DaysSinceEpoch, timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM mytable order by DaysSinceEpoch "
            + "limit 10000",
        "SELECT DaysSinceEpoch, timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM mytable order by timeConvert"
            + "(DaysSinceEpoch,'DAYS','SECONDS') DESC limit 10000",
        "SELECT count(*) FROM mytable WHERE DaysSinceEpoch = " + daysSinceEpoch + " limit 10000",
        "SELECT count(*) FROM mytable WHERE timeConvert(DaysSinceEpoch,'DAYS','HOURS') = " + hoursSinceEpoch
            + " limit 10000",
        "SELECT count(*) FROM mytable WHERE timeConvert(DaysSinceEpoch,'DAYS','SECONDS') = " + secondsSinceEpoch
            + " limit 10000",
        "SELECT MAX(timeConvert(DaysSinceEpoch,'DAYS','SECONDS')) FROM mytable limit 10000",
        "SELECT COUNT(*) FROM mytable GROUP BY dateTimeConvert(DaysSinceEpoch,'1:DAYS:EPOCH','1:HOURS:EPOCH',"
            + "'1:HOURS') limit 10000"
    );
    List<String> queries = new ArrayList<>();
    baseQueries.forEach(q -> queries.add(q.replace("mytable", "MYTABLE").replace("DaysSinceEpoch", "DAYSSinceEpOch")));
    baseQueries.forEach(
        q -> queries.add(q.replace("mytable", "DEFAULT.MYTABLE").replace("DaysSinceEpoch", "DAYSSinceEpOch")));

    for (String query : queries) {
      JsonNode response = postQuery(query);
      assertNoError(response);
    }
  }

  @Test
  public void testColumnNameContainsTableName()
      throws Exception {
    int daysSinceEpoch = 16138;
    int hoursSinceEpoch = 16138 * 24;
    int secondsSinceEpoch = 16138 * 24 * 60 * 60;
    List<String> baseQueries = List.of(
        "SELECT * FROM mytable",
        "SELECT DaysSinceEpoch, timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM mytable",
        "SELECT DaysSinceEpoch, timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM mytable order by DaysSinceEpoch "
            + "limit 10000",
        "SELECT DaysSinceEpoch, timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM mytable order by timeConvert"
            + "(DaysSinceEpoch,'DAYS','SECONDS') DESC limit 10000",
        "SELECT count(*) FROM mytable WHERE DaysSinceEpoch = " + daysSinceEpoch,
        "SELECT count(*) FROM mytable WHERE timeConvert(DaysSinceEpoch,'DAYS','HOURS') = " + hoursSinceEpoch,
        "SELECT count(*) FROM mytable WHERE timeConvert(DaysSinceEpoch,'DAYS','SECONDS') = " + secondsSinceEpoch,
        "SELECT MAX(timeConvert(DaysSinceEpoch,'DAYS','SECONDS')) FROM mytable",
        "SELECT COUNT(*) FROM mytable GROUP BY dateTimeConvert(DaysSinceEpoch,'1:DAYS:EPOCH','1:HOURS:EPOCH','1:HOURS')"
    );
    List<String> queries = new ArrayList<>();
    baseQueries.forEach(q -> queries.add(q.replace("DaysSinceEpoch", "mytable.DAYSSinceEpOch")));
    baseQueries.forEach(q -> queries.add(q.replace("DaysSinceEpoch", "mytable.DAYSSinceEpOch")));

    for (String query : queries) {
      JsonNode response = postQuery(query);
      assertTrue(response.get("numSegmentsProcessed").asLong() >= 1L, "Query: " + query + " failed");
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testCaseInsensitivityWithColumnNameContainsTableName(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    int daysSinceEpoch = 16138;
    int hoursSinceEpoch = 16138 * 24;
    int secondsSinceEpoch = 16138 * 24 * 60 * 60;
    List<String> baseQueries = List.of(
        "SELECT * FROM mytable",
        "SELECT DaysSinceEpoch, timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM mytable",
        "SELECT DaysSinceEpoch, timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM mytable order by DaysSinceEpoch "
            + "limit 10000",
        "SELECT DaysSinceEpoch, timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM mytable order by timeConvert"
            + "(DaysSinceEpoch,'DAYS','SECONDS') DESC limit 10000",
        "SELECT count(*) FROM mytable WHERE DaysSinceEpoch = " + daysSinceEpoch,
        "SELECT count(*) FROM mytable WHERE timeConvert(DaysSinceEpoch,'DAYS','HOURS') = " + hoursSinceEpoch,
        "SELECT count(*) FROM mytable WHERE timeConvert(DaysSinceEpoch,'DAYS','SECONDS') = " + secondsSinceEpoch,
        "SELECT MAX(timeConvert(DaysSinceEpoch,'DAYS','SECONDS')) FROM mytable",
        "SELECT COUNT(*) FROM mytable GROUP BY dateTimeConvert(DaysSinceEpoch,'1:DAYS:EPOCH','1:HOURS:EPOCH','1:HOURS')"
    );
    List<String> queries = new ArrayList<>();
    baseQueries.forEach(
        q -> queries.add(q.replace("mytable", "MYTABLE").replace("DaysSinceEpoch", "MYTABLE.DAYSSinceEpOch")));
    // something like "SELECT MYDB.MYTABLE.DAYSSinceEpOch from MYDB.MYTABLE where MYDB.MYTABLE.DAYSSinceEpOch = 16138"
    baseQueries.forEach(
        q -> queries.add(q.replace("mytable", "default.MYTABLE").replace("DaysSinceEpoch", "MYTABLE.DAYSSinceEpOch")));

    for (String query : queries) {
      JsonNode response = postQuery(query);
      assertTrue(response.get("numSegmentsProcessed").asLong() >= 1L, "Query: " + query + " failed");
    }
  }

  @Test
  public void testQuerySourceWithDatabaseNameV1()
      throws Exception {
    // by default 10 rows will be returned, so use high limit
    String pinotQuery = "SELECT DISTINCT(Carrier) FROM mytable LIMIT 1000000";
    String h2Query = "SELECT DISTINCT Carrier FROM mytable";
    testQuery(pinotQuery, h2Query);
    pinotQuery = "SELECT DISTINCT Carrier FROM default.mytable LIMIT 1000000";
    testQuery(pinotQuery, h2Query);
  }

  @Test
  public void testQuerySourceWithDatabaseNameV2()
      throws Exception {
    setUseMultiStageQueryEngine(true);
    // by default 10 rows will be returned, so use high limit
    String pinotQuery = "SELECT DISTINCT(Carrier) FROM mytable LIMIT 1000000";
    String h2Query = "SELECT DISTINCT Carrier FROM mytable";
    testQuery(pinotQuery, h2Query);

    pinotQuery = "SELECT DISTINCT Carrier FROM default.mytable LIMIT 1000000";
    testQuery(pinotQuery, h2Query);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testDistinctCountHll(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query;

    // The Accurate value is 6538.
    query = "SELECT distinctCount(FlightNum) FROM mytable ";
    assertEquals(postQuery(query).get("resultTable").get("rows").get(0).get(0).asLong(), 6538);

    // Expected distinctCountHll with different log2m value from 2 to 19. The Accurate value is 6538.
    long[] expectedResults = new long[]{
        3504, 6347, 8877, 9729, 9046, 7672, 7538, 6993, 6649, 6651, 6553, 6525, 6459, 6523, 6532, 6544, 6538, 6539
    };

    for (int i = 2; i < 20; i++) {
      query = String.format("SELECT distinctCountHLL(FlightNum, %d) FROM mytable ", i);
      assertEquals(postQuery(query).get("resultTable").get("rows").get(0).get(0).asLong(), expectedResults[i - 2]);
    }

    // Default log2m for HLL is set to 12 in V1 and 8 in V2
    long expectedDefault;
    query = "SELECT distinctCountHLL(FlightNum) FROM mytable ";
    if (useMultiStageQueryEngine) {
      expectedDefault = expectedResults[6];
    } else {
      expectedDefault = expectedResults[10];
    }
    assertEquals(postQuery(query).get("resultTable").get("rows").get(0).get(0).asLong(), expectedDefault);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testDistinctCountHllPlus(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query;

    // The Accurate value is 6538.
    query = "SELECT distinctCount(FlightNum) FROM mytable ";
    assertEquals(postQuery(query).get("resultTable").get("rows").get(0).get(0).asLong(), 6538);

    // Expected distinctCountHllPlus with different P value from 4 (minimal value) to 19. The Accurate value is 6538.
    long[] expectedResults = new long[]{
        4901, 5755, 6207, 5651, 6318, 6671, 6559, 6425, 6490, 6486, 6489, 6516, 6532, 6526, 6525, 6534
    };

    for (int i = 4; i < 20; i++) {
      query = String.format("SELECT distinctCountHLLPlus(FlightNum, %d) FROM mytable ", i);
      assertEquals(postQuery(query).get("resultTable").get("rows").get(0).get(0).asLong(), expectedResults[i - 4]);
    }

    // Default HLL Plus is set as p=14
    query = "SELECT distinctCountHLLPlus(FlightNum) FROM mytable ";
    assertEquals(postQuery(query).get("resultTable").get("rows").get(0).get(0).asLong(), expectedResults[10]);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testAggregationFunctionsWithUnderscore(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query;

    // The Accurate value is 6538.
    query = "SELECT distinct_count(FlightNum) FROM mytable";
    assertEquals(postQuery(query).get("resultTable").get("rows").get(0).get(0).asInt(), 6538);

    // The Accurate value is 115545.
    query = "SELECT c_o_u_n_t(FlightNum) FROM mytable";
    assertEquals(postQuery(query).get("resultTable").get("rows").get(0).get(0).asInt(), 115545);
  }

  @Test
  public void testExplainPlanQueryV1()
      throws Exception {
    String query1 = "EXPLAIN PLAN FOR SELECT count(*) AS count, Carrier AS name FROM mytable GROUP BY name ORDER BY 1";
    String response1 = postQuery(query1).get("resultTable").toString();

    // Replace string "docs:[0-9]+" with "docs:*" so that test doesn't fail when number of documents change. This is
    // needed because both OfflineClusterIntegrationTest and MultiNodesOfflineClusterIntegrationTest run this test
    // case with different number of documents in the segment.
    response1 = response1.replaceAll("docs:[0-9]+", "docs:*");

    assertEquals(response1, "{\"dataSchema\":{\"columnNames\":[\"Operator\",\"Operator_Id\",\"Parent_Id\"],"
        + "\"columnDataTypes\":[\"STRING\",\"INT\",\"INT\"]},"
        + "\"rows\":[[\"BROKER_REDUCE(sort:[count(*) ASC],limit:10)\",1,0],[\"COMBINE_GROUP_BY\",2,1],"
        + "[\"PLAN_START(numSegmentsForThisPlan:1)\",-1,-1],"
        + "[\"GROUP_BY(groupKeys:Carrier, aggregations:count(*))\",3,2],[\"PROJECT(Carrier)\",4,3],"
        + "[\"DOC_ID_SET\",5,4],[\"FILTER_MATCH_ENTIRE_SEGMENT(docs:*)\",6,5]]}");

    // In the query below, FlightNum column has an inverted index and there is no data satisfying the predicate
    // "FlightNum < 0". Hence, all segments are pruned out before query execution on the server side.
    String query2 = "EXPLAIN PLAN FOR SELECT * FROM mytable WHERE FlightNum < 0";
    String response2 = postQuery(query2).get("resultTable").toString();

    assertEquals(response2, "{\"dataSchema\":{\"columnNames\":[\"Operator\",\"Operator_Id\",\"Parent_Id\"],"
        + "\"columnDataTypes\":[\"STRING\",\"INT\",\"INT\"]},\"rows\":[[\"BROKER_REDUCE(limit:10)\",1,0],"
        + "[\"PLAN_START(numSegmentsForThisPlan:12)\",-1,-1],[\"ALL_SEGMENTS_PRUNED_ON_SERVER\",2,1]]}");
  }

  @Test
  public void testExplainPlanQueryV2()
      throws Exception {
    setUseMultiStageQueryEngine(true);
    // language=sql
    //@formatter:off
    String query1 = "EXPLAIN PLAN WITHOUT IMPLEMENTATION FOR "
        + "SELECT count(*) AS count, Carrier AS name "
        + "FROM mytable "
        + "GROUP BY name "
        + "ORDER BY 1";
    //@formatter:on
    String response1 = postQuery(query1).get("resultTable").toString();

    // Replace string "docs:[0-9]+" with "docs:*" so that test doesn't fail when number of documents change. This is
    // needed because both OfflineClusterIntegrationTest and MultiNodesOfflineClusterIntegrationTest run this test
    // case with different number of documents in the segment.
    response1 = response1.replaceAll("docs:[0-9]+", "docs:*")
        .replaceAll("Time: \\d+\\.\\d+", "Time:*");

    JsonNode response1Json = JsonUtils.stringToJsonNode(response1);
    assertEquals(response1Json.get("dataSchema").get("columnNames").get(0).asText(), "SQL");
    assertEquals(response1Json.get("dataSchema").get("columnNames").get(1).asText(), "PLAN");
    assertEquals(response1Json.get("dataSchema").get("columnNames").get(2).asText(), "RULE_TIMINGS");
    assertEquals(response1Json.get("dataSchema").get("columnDataTypes").get(0).asText(), "STRING");
    assertEquals(response1Json.get("dataSchema").get("columnDataTypes").get(1).asText(), "STRING");
    assertEquals(response1Json.get("dataSchema").get("columnDataTypes").get(2).asText(), "STRING");

    assertEquals(response1Json.get("rows").get(0).get(0).asText(),
        "EXPLAIN PLAN WITHOUT IMPLEMENTATION FOR SELECT count(*) AS count, Carrier AS name FROM mytable GROUP BY name"
            + " ORDER BY 1");
    assertEquals(response1Json.get("rows").get(0).get(1).asText(), "Execution Plan\n"
        + "LogicalSort(sort0=[$0], dir0=[ASC])\n"
        + "  PinotLogicalSortExchange(distribution=[hash], collation=[[0]], isSortOnSender=[false], "
        + "isSortOnReceiver=[true])\n"
        + "    LogicalProject(count=[$1], name=[$0])\n"
        + "      PinotLogicalAggregate(group=[{0}], agg#0=[COUNT($1)], aggType=[FINAL])\n"
        + "        PinotLogicalExchange(distribution=[hash[0]])\n"
        + "          PinotLogicalAggregate(group=[{17}], agg#0=[COUNT()], aggType=[LEAF])\n"
        + "            PinotLogicalTableScan(table=[[default, mytable]])\n");
    assertEquals(response1Json.get("rows").get(0).get(2).asText(), "Rule Execution Times\n"
        + "Rule: SortRemove -> Time:*\n"
        + "Rule: AggregateProjectMerge -> Time:*\n"
        + "Rule: EvaluateProjectLiteral -> Time:*\n"
        + "Rule: AggregateRemove -> Time:*\n");

    // In the query below, FlightNum column has an inverted index and there is no data satisfying the predicate
    // "FlightNum < 0". Hence, all segments are pruned out before query execution on the server side.
    // language=sql
    String query2 = "EXPLAIN PLAN WITHOUT IMPLEMENTATION FOR SELECT * FROM mytable WHERE FlightNum < 0";
    String response2 = postQuery(query2).get("resultTable").toString()
        .replaceAll("Time: \\d+\\.\\d+", "Time: *");

    JsonNode response2Json = JsonUtils.stringToJsonNode(response2);
    assertEquals(response2Json.get("dataSchema").get("columnNames").get(0).asText(), "SQL");
    assertEquals(response2Json.get("dataSchema").get("columnNames").get(1).asText(), "PLAN");
    assertEquals(response2Json.get("dataSchema").get("columnNames").get(2).asText(), "RULE_TIMINGS");
    assertEquals(response2Json.get("dataSchema").get("columnDataTypes").get(0).asText(), "STRING");
    assertEquals(response2Json.get("dataSchema").get("columnDataTypes").get(1).asText(), "STRING");
    assertEquals(response2Json.get("dataSchema").get("columnDataTypes").get(2).asText(), "STRING");
    assertEquals(response2Json.get("rows").get(0).get(0).asText(),
        "EXPLAIN PLAN WITHOUT IMPLEMENTATION FOR SELECT * FROM mytable WHERE FlightNum < 0");
    assertTrue(Pattern.compile(
        "Execution Plan\n"
            + "LogicalProject\\(.*\\)\n"
            + "  LogicalFilter\\(condition=\\[<\\(.*, 0\\)]\\)\n"
            + "    PinotLogicalTableScan\\(table=\\[\\[default, mytable]]\\)\n"
    ).matcher(response2Json.get("rows").get(0).get(1).asText()).find());
    assertEquals(response2Json.get("rows").get(0).get(2).asText(), "Rule Execution Times\n"
            + "Rule: ProjectFilterTranspose -> Time: *\n"
            + "Rule: EvaluateProjectLiteral -> Time: *\n"
            + "Rule: FilterProjectTranspose -> Time: *\n"
            + "Rule: EvaluateFilterLiteral -> Time: *\n");
  }

  /** Test to make sure we are properly handling string comparisons in predicates. */
  @Test(dataProvider = "useBothQueryEngines")
  public void testStringComparisonInFilter(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    // compare two string columns.
    JsonNode jsonNode = postQuery("SELECT count(*) FROM mytable WHERE OriginState = DestState");
    assertEquals(getType(jsonNode, 0), "LONG");
    assertEquals(getLongCellValue(jsonNode, 0, 0), 14011);

    // compare string function with string column.
    jsonNode = postQuery("SELECT count(*) FROM mytable WHERE trim(OriginState) = DestState");
    assertEquals(getType(jsonNode, 0), "LONG");
    assertEquals(getLongCellValue(jsonNode, 0, 0), 14011);

    // compare string function with string function.
    jsonNode = postQuery("SELECT count(*) FROM mytable WHERE substr(OriginState, 0, 1) = substr(DestState, 0, 1)");
    assertEquals(getType(jsonNode, 0), "LONG");
    assertEquals(getLongCellValue(jsonNode, 0, 0), 19755);
  }

  /**
   * Test queries that can be solved with {@link NonScanBasedAggregationOperator}.
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testNonScanAggregationQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String tableName = getTableName();

    // Test queries with COUNT, MIN, MAX, MIN_MAX_RANGE
    // Dictionary columns
    // INT
    String query = "SELECT COUNT(*) FROM " + tableName;
    testNonScanAggregationQuery(query);
    query = "SELECT MIN(ArrTime) FROM " + tableName;
    testNonScanAggregationQuery(query);
    query = "SELECT MAX(ArrTime) FROM " + tableName;
    testNonScanAggregationQuery(query);
    query = "SELECT MIN_MAX_RANGE(ArrTime) FROM " + tableName;
    String h2Query = "SELECT MAX(ArrTime)-MIN(ArrTime) FROM " + tableName;
    testNonScanAggregationQuery(query, h2Query);
    query = "SELECT COUNT(*), MIN(ArrTime), MAX(ArrTime), MIN_MAX_RANGE(ArrTime) FROM " + tableName;
    h2Query = "SELECT COUNT(*), MIN(ArrTime), MAX(ArrTime), MAX(ArrTime)-MIN(ArrTime) FROM " + tableName;
    testNonScanAggregationQuery(query, h2Query);

    // LONG
    query = "SELECT MIN(AirlineID) FROM " + tableName;
    testNonScanAggregationQuery(query);
    query = "SELECT MAX(AirlineID) FROM " + tableName;
    testNonScanAggregationQuery(query);
    query = "SELECT MIN_MAX_RANGE(AirlineID) FROM " + tableName;
    h2Query = "SELECT MAX(AirlineID)-MIN(AirlineID) FROM " + tableName;
    testNonScanAggregationQuery(query, h2Query);
    query = "SELECT COUNT(*), MIN(AirlineID), MAX(AirlineID), MIN_MAX_RANGE(AirlineID) FROM " + tableName;
    h2Query = "SELECT COUNT(*), MIN(AirlineID), MAX(AirlineID), MAX(AirlineID)-MIN(AirlineID) FROM " + tableName;
    testNonScanAggregationQuery(query, h2Query);

    // FLOAT
    query = "SELECT MIN(DepDelayMinutes) FROM " + tableName;
    testNonScanAggregationQuery(query);
    query = "SELECT MAX(DepDelayMinutes) FROM " + tableName;
    testNonScanAggregationQuery(query);
    query = "SELECT MIN_MAX_RANGE(DepDelayMinutes) FROM " + tableName;
    h2Query = "SELECT MAX(DepDelayMinutes)-MIN(DepDelayMinutes) FROM " + tableName;
    testNonScanAggregationQuery(query, h2Query);
    query =
        "SELECT COUNT(*), MIN(DepDelayMinutes), MAX(DepDelayMinutes), MIN_MAX_RANGE(DepDelayMinutes) FROM " + tableName;
    h2Query =
        "SELECT COUNT(*), MIN(DepDelayMinutes), MAX(DepDelayMinutes), MAX(DepDelayMinutes)-MIN(DepDelayMinutes) FROM "
            + tableName;
    testNonScanAggregationQuery(query, h2Query);

    // DOUBLE
    query = "SELECT MIN(ArrDelayMinutes) FROM " + tableName;
    testNonScanAggregationQuery(query);
    query = "SELECT MAX(ArrDelayMinutes) FROM " + tableName;
    testNonScanAggregationQuery(query);
    query = "SELECT MIN_MAX_RANGE(ArrDelayMinutes) FROM " + tableName;
    h2Query = "SELECT MAX(ArrDelayMinutes)-MIN(ArrDelayMinutes) FROM " + tableName;
    testNonScanAggregationQuery(query, h2Query);
    query =
        "SELECT COUNT(*), MIN(ArrDelayMinutes), MAX(ArrDelayMinutes), MIN_MAX_RANGE(ArrDelayMinutes) FROM " + tableName;
    h2Query =
        "SELECT COUNT(*), MIN(ArrDelayMinutes), MAX(ArrDelayMinutes), MAX(ArrDelayMinutes)-MIN(ArrDelayMinutes) FROM "
            + tableName;
    testNonScanAggregationQuery(query, h2Query);

    // STRING
    // TODO: add test cases for string column when we add support for min and max on string datatype columns

    // Non dictionary columns
    // INT
    query = "SELECT MIN(ActualElapsedTime) FROM " + tableName;
    testNonScanAggregationQuery(query);
    query = "SELECT MAX(ActualElapsedTime) FROM " + tableName;
    testNonScanAggregationQuery(query);
    query = "SELECT MIN_MAX_RANGE(ActualElapsedTime) FROM " + tableName;
    h2Query = "SELECT MAX(ActualElapsedTime)-MIN(ActualElapsedTime) FROM " + tableName;
    testNonScanAggregationQuery(query, h2Query);
    query = "SELECT COUNT(*), MIN(ActualElapsedTime), MAX(ActualElapsedTime), MIN_MAX_RANGE(ActualElapsedTime) FROM "
        + tableName;
    h2Query = "SELECT COUNT(*), MIN(ActualElapsedTime), MAX(ActualElapsedTime), "
        + "MAX(ActualElapsedTime)-MIN(ActualElapsedTime) FROM " + tableName;
    testNonScanAggregationQuery(query, h2Query);

    // FLOAT
    query = "SELECT MIN(ArrDelay) FROM " + tableName;
    testNonScanAggregationQuery(query);
    query = "SELECT MAX(ArrDelay) FROM " + tableName;
    testNonScanAggregationQuery(query);
    query = "SELECT MIN_MAX_RANGE(ArrDelay) FROM " + tableName;
    h2Query = "SELECT MAX(ArrDelay)-MIN(ArrDelay) FROM " + tableName;
    testNonScanAggregationQuery(query, h2Query);
    query = "SELECT COUNT(*), MIN(ArrDelay), MAX(ArrDelay), MIN_MAX_RANGE(ArrDelay) FROM " + tableName;
    h2Query = "SELECT COUNT(*), MIN(ArrDelay), MAX(ArrDelay), MAX(ArrDelay)-MIN(ArrDelay) FROM " + tableName;
    testNonScanAggregationQuery(query, h2Query);

    // DOUBLE
    query = "SELECT MIN(DepDelay) FROM " + tableName;
    testNonScanAggregationQuery(query);
    query = "SELECT MAX(DepDelay) FROM " + tableName;
    testNonScanAggregationQuery(query);
    query = "SELECT MIN_MAX_RANGE(DepDelay) FROM " + tableName;
    h2Query = "SELECT MAX(DepDelay)-MIN(DepDelay) FROM " + tableName;
    testNonScanAggregationQuery(query, h2Query);
    query = "SELECT COUNT(*), MIN(DepDelay), MAX(DepDelay), MIN_MAX_RANGE(DepDelay) FROM " + tableName;
    h2Query = "SELECT COUNT(*), MIN(DepDelay), MAX(DepDelay), MAX(DepDelay)-MIN(DepDelay) FROM " + tableName;
    testNonScanAggregationQuery(query, h2Query);

    // STRING
    // TODO: add test cases for string column when we add support for min and max on string datatype columns

    // Multiple aggregation functions, mix of dictionary based and metadata based: answered by
    // NonScanBasedAggregationOperator
    query = "SELECT MIN(ArrTime), MAX(ActualElapsedTime) FROM " + tableName;
    testNonScanAggregationQuery(query);

    // Group-by in query: not answered by NonScanBasedAggregationOperator
    query = "SELECT MAX(ArrTime) FROM " + tableName + " GROUP BY DaysSinceEpoch";
    JsonNode response = postQuery(query);
    assertEquals(response.get("numEntriesScannedInFilter").asLong(), 0);
    assertTrue(response.get("numEntriesScannedPostFilter").asLong() > 0);

    // Filter in query: not answered by NonScanBasedAggregationOperator
    query = "SELECT MAX(ArrTime) FROM " + tableName + " WHERE DepDelay > 100";
    response = postQuery(query);
    assertTrue(response.get("numEntriesScannedInFilter").asLong() > 0);
    assertTrue(response.get("numEntriesScannedPostFilter").asLong() > 0);
  }

  private void testNonScanAggregationQuery(String query)
      throws Exception {
    testNonScanAggregationQuery(query, null);
  }

  private void testNonScanAggregationQuery(String pinotQuery, @Nullable String h2Query)
      throws Exception {
    testQuery(pinotQuery, h2Query != null ? h2Query : pinotQuery);
    JsonNode response = postQuery(pinotQuery);
    assertEquals(response.get("numEntriesScannedInFilter").asLong(), 0);
    assertEquals(response.get("numEntriesScannedPostFilter").asLong(), 0);
    assertEquals(response.get("numDocsScanned").asLong(), response.get("totalDocs").asLong());
  }

  @Test
  @Override
  public void testHardcodedServerPartitionedSqlQueries()
      throws Exception {
    super.testHardcodedServerPartitionedSqlQueries();
  }

  @Test
  public void testIndexMetadataAPI()
      throws Exception {
    TableIndexMetadataResponse tableIndexMetadataResponse =
        JsonUtils.stringToObject(sendGetRequest(getControllerBaseApiUrl() + "/tables/mytable/indexes?type=OFFLINE"),
            TableIndexMetadataResponse.class);

    getInvertedIndexColumns().forEach(column -> {
      Assert.assertEquals(
          (long) tableIndexMetadataResponse.getColumnToIndexesCount().get(column).get(StandardIndexes.INVERTED_ID),
          tableIndexMetadataResponse.getTotalOnlineSegments());
    });

    getNoDictionaryColumns().forEach(column -> {
      Assert.assertEquals(
          (long) tableIndexMetadataResponse.getColumnToIndexesCount().get(column).get(StandardIndexes.DICTIONARY_ID),
          0);
    });

    getRangeIndexColumns().forEach(column -> {
      Assert.assertEquals(
          (long) tableIndexMetadataResponse.getColumnToIndexesCount().get(column).get(StandardIndexes.RANGE_ID),
          tableIndexMetadataResponse.getTotalOnlineSegments());
    });

    getBloomFilterColumns().forEach(column -> {
      Assert.assertEquals(
          (long) tableIndexMetadataResponse.getColumnToIndexesCount().get(column).get(StandardIndexes.BLOOM_FILTER_ID),
          tableIndexMetadataResponse.getTotalOnlineSegments());
    });
  }

  @Test
  public void testAggregateMetadataAPI()
      throws IOException {
    JsonNode oneSVColumnResponse = JsonUtils.stringToJsonNode(
        sendGetRequest(getControllerBaseApiUrl() + "/tables/mytable/metadata?columns=DestCityMarketID"));
    // DestCityMarketID is a SV column
    validateMetadataResponse(oneSVColumnResponse, 1, 0);

    JsonNode oneMVColumnResponse = JsonUtils.stringToJsonNode(
        sendGetRequest(getControllerBaseApiUrl() + "/tables/mytable/metadata?columns=DivLongestGTimes"));
    // DivLongestGTimes is a MV column
    validateMetadataResponse(oneMVColumnResponse, 1, 1);

    JsonNode threeSVColumnsResponse = JsonUtils.stringToJsonNode(sendGetRequest(getControllerBaseApiUrl()
        + "/tables/mytable/metadata?columns=DivActualElapsedTime&columns=CRSElapsedTime&columns=OriginStateName"));
    validateMetadataResponse(threeSVColumnsResponse, 3, 0);

    JsonNode threeSVColumnsWholeEncodedResponse = JsonUtils.stringToJsonNode(sendGetRequest(
        getControllerBaseApiUrl() + "/tables/mytable/metadata?columns="
            + "DivActualElapsedTime%26columns%3DCRSElapsedTime%26columns%3DOriginStateName"));
    validateMetadataResponse(threeSVColumnsWholeEncodedResponse, 3, 0);

    JsonNode threeMVColumnsResponse = JsonUtils.stringToJsonNode(sendGetRequest(getControllerBaseApiUrl()
        + "/tables/mytable/metadata?columns=DivLongestGTimes&columns=DivWheelsOns&columns=DivAirports"));
    validateMetadataResponse(threeMVColumnsResponse, 3, 3);

    JsonNode threeMVColumnsWholeEncodedResponse = JsonUtils.stringToJsonNode(sendGetRequest(
        getControllerBaseApiUrl() + "/tables/mytable/metadata?columns="
            + "DivLongestGTimes%26columns%3DDivWheelsOns%26columns%3DDivAirports"));
    validateMetadataResponse(threeMVColumnsWholeEncodedResponse, 3, 3);

    JsonNode zeroColumnResponse =
        JsonUtils.stringToJsonNode(sendGetRequest(getControllerBaseApiUrl() + "/tables/mytable/metadata"));
    validateMetadataResponse(zeroColumnResponse, 0, 0);

    JsonNode starColumnResponse =
        JsonUtils.stringToJsonNode(sendGetRequest(getControllerBaseApiUrl() + "/tables/mytable/metadata?columns=*"));
    validateMetadataResponse(starColumnResponse, 82, 9);

    JsonNode starEncodedColumnResponse =
        JsonUtils.stringToJsonNode(sendGetRequest(getControllerBaseApiUrl() + "/tables/mytable/metadata?columns=%2A"));
    validateMetadataResponse(starEncodedColumnResponse, 82, 9);

    JsonNode starWithExtraColumnResponse = JsonUtils.stringToJsonNode(sendGetRequest(
        getControllerBaseApiUrl() + "/tables/mytable/metadata?columns="
            + "CRSElapsedTime&columns=*&columns=OriginStateName"));
    validateMetadataResponse(starWithExtraColumnResponse, 82, 9);

    JsonNode starWithExtraEncodedColumnResponse = JsonUtils.stringToJsonNode(sendGetRequest(
        getControllerBaseApiUrl() + "/tables/mytable/metadata?columns="
            + "CRSElapsedTime&columns=%2A&columns=OriginStateName"));
    validateMetadataResponse(starWithExtraEncodedColumnResponse, 82, 9);

    JsonNode starWithExtraColumnWholeEncodedResponse = JsonUtils.stringToJsonNode(sendGetRequest(
        getControllerBaseApiUrl() + "/tables/mytable/metadata?columns="
            + "CRSElapsedTime%26columns%3D%2A%26columns%3DOriginStateName"));
    validateMetadataResponse(starWithExtraColumnWholeEncodedResponse, 82, 9);
  }

  private void validateMetadataResponse(JsonNode response, int numTotalColumn, int numMVColumn) {
    if (getNumServers() == 1) {
      assertEquals(response.get(DISK_SIZE_IN_BYTES_KEY).asInt(), _tableSize);
    }
    assertEquals(response.get(NUM_SEGMENTS_KEY).asInt(), NUM_SEGMENTS);
    assertEquals(response.get(NUM_ROWS_KEY).asInt(), NUM_ROWS);
    assertEquals(response.get(COLUMN_LENGTH_MAP_KEY).size(), numTotalColumn);
    assertEquals(response.get(COLUMN_CARDINALITY_MAP_KEY).size(), numTotalColumn);
    assertEquals(response.get(MAX_NUM_MULTI_VALUES_MAP_KEY).size(), numMVColumn);
  }

  @Test
  public void testReset() {
    testReset(TableType.OFFLINE);
  }

  @Test
  public void testJDBCClient()
      throws Exception {
    String query = "SELECT count(*) FROM " + getTableName();
    java.sql.Connection connection = getJDBCConnectionFromController(getControllerPort());
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(query);
    resultSet.first();
    Assert.assertTrue(resultSet.getLong(1) > 0);
    connection.close();
    Assert.assertTrue(connection.isClosed());

    connection = getJDBCConnectionFromBrokers(RANDOM.nextInt(), getRandomBrokerPort());
    statement = connection.createStatement();
    resultSet = statement.executeQuery(query);
    resultSet.first();
    Assert.assertTrue(resultSet.getLong(1) > 0);
    connection.close();
    Assert.assertTrue(connection.isClosed());
  }

  @Test
  public void testNoTableQueryThroughJDBCClient()
      throws Exception {
    String query = "SELECT 1";
    java.sql.Connection connection = getJDBCConnectionFromController(getControllerPort());
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(query);
    resultSet.first();
    Assert.assertTrue(resultSet.getLong(1) > 0);
    connection.close();
    Assert.assertTrue(connection.isClosed());
  }

  @Test
  public void testNonExistingTableQueryThroughJDBCClient()
      throws Exception {
    String query = "SELECT 1 from nonExistingTable";
    java.sql.Connection connection = getJDBCConnectionFromController(getControllerPort());
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(query);
    resultSet.first();
    Assert.assertTrue(resultSet.getLong(1) > 0);
    connection.close();
    Assert.assertTrue(connection.isClosed());
  }

  private java.sql.Connection getJDBCConnectionFromController(int controllerPort)
      throws Exception {
    PinotDriver pinotDriver = new PinotDriver();
    Properties jdbcProps = new Properties();
    return pinotDriver.connect("jdbc:pinot://localhost:" + controllerPort, jdbcProps);
  }

  private java.sql.Connection getJDBCConnectionFromBrokers(int controllerPort, int brokerPort)
      throws Exception {
    PinotDriver pinotDriver = new PinotDriver();
    Properties jdbcProps = new Properties();
    jdbcProps.put(PinotConnection.BROKER_LIST, "localhost:" + brokerPort);
    return pinotDriver.connect("jdbc:pinot://localhost:" + controllerPort, jdbcProps);
  }

  @Test
  public void testBooleanLiteralsFunc()
      throws Exception {
    // Test boolean equal true case.
    String sqlQuery = "SELECT (true = true) = true FROM mytable where true = true";
    JsonNode response = postQuery(sqlQuery);
    JsonNode rows = response.get("resultTable").get("rows");
    assertTrue(response.get("exceptions").isEmpty());
    assertEquals(rows.size(), 1);
    assertTrue(rows.get(0).get(0).asBoolean());
    // Test boolean equal false case.
    sqlQuery = "SELECT (true = true) = false FROM mytable";
    response = postQuery(sqlQuery);
    rows = response.get("resultTable").get("rows");
    assertTrue(response.get("exceptions").isEmpty());
    assertEquals(rows.size(), 1);
    assertFalse(rows.get(0).get(0).asBoolean());
    // Test boolean not equal true case.
    sqlQuery = "SELECT true != false FROM mytable";
    response = postQuery(sqlQuery);
    rows = response.get("resultTable").get("rows");
    assertTrue(response.get("exceptions").isEmpty());
    assertEquals(rows.size(), 1);
    assertTrue(rows.get(0).get(0).asBoolean());
    // Test boolean not equal false case.
    sqlQuery = "SELECT true != true FROM mytable";
    response = postQuery(sqlQuery);
    rows = response.get("resultTable").get("rows");
    assertTrue(response.get("exceptions").isEmpty());
    assertEquals(rows.size(), 1);
    assertFalse(rows.get(0).get(0).asBoolean());
  }

  @Test
  public void testBooleanAggregation()
      throws Exception {
    testQuery("SELECT BOOL_AND(CAST(Cancelled AS BOOLEAN)) FROM mytable");
    testQuery("SELECT BOOL_OR(CAST(Diverted AS BOOLEAN)) FROM mytable");
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testGroupByAggregationWithLimitZero(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    String sqlQuery = "SELECT Origin, AVG(ArrDelay) FROM mytable GROUP BY Origin LIMIT 0";
    JsonNode response = postQuery(sqlQuery);
    assertTrue(response.get("exceptions").isEmpty());
    JsonNode rows = response.get("resultTable").get("rows");
    assertEquals(rows.size(), 0);

    // Ensure data schema returned is accurate even if there are no rows returned
    JsonNode columnDataTypes = response.get("resultTable").get("dataSchema").get("columnDataTypes");
    assertEquals(columnDataTypes.size(), 2);
    assertEquals(columnDataTypes.get(1).asText(), "DOUBLE");
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testAggregationWithLimitZero(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    String sqlQuery = "SELECT AVG(ArrDelay) FROM mytable LIMIT 0";
    JsonNode response = postQuery(sqlQuery);
    assertTrue(response.get("exceptions").isEmpty());
    JsonNode rows = response.get("resultTable").get("rows");
    assertEquals(rows.size(), 0);

    // Ensure data schema returned is accurate even if there are no rows returned
    JsonNode columnDataTypes = response.get("resultTable").get("dataSchema").get("columnDataTypes");
    assertEquals(columnDataTypes.size(), 1);
    assertEquals(columnDataTypes.get(0).asText(), "DOUBLE");
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testFilteredAggregationWithGroupByOrdering(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    // Test the ordering is correctly applied to the correct aggregation (the one without FILTER clause)
    // See https://github.com/apache/pinot/pull/13784
    testQuery("SELECT DestCityName, COUNT(*) AS c1, COUNT(*) FILTER (WHERE AirTime = 0) AS c2 FROM mytable "
        + "GROUP BY DestCityName ORDER BY c1 DESC LIMIT 10");
  }

  private String buildSkipIndexesOption(String columnsAndIndexes) {
    return "SET " + SKIP_INDEXES + "='" + columnsAndIndexes + "'; ";
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testSkipIndexes(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    long numTotalDocs = getCountStarResult();
    assertEquals(postQuery(TEST_UPDATED_RANGE_INDEX_QUERY).get("numEntriesScannedInFilter").asLong(), numTotalDocs);

    // Update table config to add range and inverted index, and trigger reload
    TableConfig tableConfig = createOfflineTableConfig();
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    indexingConfig.setRangeIndexColumns(UPDATED_RANGE_INDEX_COLUMNS);
    indexingConfig.setInvertedIndexColumns(UPDATED_INVERTED_INDEX_COLUMNS);
    updateTableConfig(tableConfig);
    reloadAllSegments(TEST_UPDATED_RANGE_INDEX_QUERY, false, numTotalDocs);
    assertEquals(postQuery(TEST_UPDATED_RANGE_INDEX_QUERY).get("numEntriesScannedInFilter").asLong(), 0L);

    // Ensure inv index is operational
    assertEquals(postQuery(TEST_UPDATED_INVERTED_INDEX_QUERY).get("numEntriesScannedInFilter").asLong(), 0L);

    // disallow use of range index on DivActualElapsedTime, inverted should be unaffected
    String skipIndexes = buildSkipIndexesOption("DivActualElapsedTime=range");
    assertEquals(postQuery(skipIndexes + TEST_UPDATED_INVERTED_INDEX_QUERY).get("numEntriesScannedInFilter").asLong(),
        0L);
    assertEquals(postQuery(skipIndexes + TEST_UPDATED_RANGE_INDEX_QUERY).get("numEntriesScannedInFilter").asLong(),
        numTotalDocs);

    // disallow use of inverted index on DivActualElapsedTime, range should be unaffected
    skipIndexes = buildSkipIndexesOption("DivActualElapsedTime=inverted");
    // Confirm that inverted index is not used
    assertFalse(postQuery(skipIndexes + " EXPLAIN PLAN FOR " + TEST_UPDATED_INVERTED_INDEX_QUERY).toString()
        .contains("FILTER_INVERTED_INDEX"));

    // EQ predicate type allows for using range index if one exists, even if inverted index is skipped. That is why
    // we still see no docs scanned even though we skip the inverted index. This is a good test to show that using
    // the skipIndexes can allow fine-grained experimentation of index usage at query time.
    assertEquals(postQuery(skipIndexes + TEST_UPDATED_INVERTED_INDEX_QUERY).get("numEntriesScannedInFilter").asLong(),
        0L);
    assertEquals(postQuery(skipIndexes + TEST_UPDATED_RANGE_INDEX_QUERY).get("numEntriesScannedInFilter").asLong(), 0L);

    // disallow use of both range and inverted indexes on DivActualElapsedTime, neither should be used at query time
    skipIndexes = buildSkipIndexesOption("DivActualElapsedTime=inverted,range");
    assertEquals(postQuery(skipIndexes + TEST_UPDATED_INVERTED_INDEX_QUERY).get("numEntriesScannedInFilter").asLong(),
        numTotalDocs);
    assertEquals(postQuery(skipIndexes + TEST_UPDATED_RANGE_INDEX_QUERY).get("numEntriesScannedInFilter").asLong(),
        numTotalDocs);

    // Update table config to remove the new indexes, and check if the new indexes are removed
    updateTableConfig(_tableConfig);
    reloadAllSegments(TEST_UPDATED_RANGE_INDEX_QUERY, false, numTotalDocs);
    assertEquals(postQuery(TEST_UPDATED_RANGE_INDEX_QUERY).get("numEntriesScannedInFilter").asLong(), numTotalDocs);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testFilteredAggregationWithNoValueMatchingAggregationFilterDefault(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    String sqlQuery =
        "SELECT AirlineID, COUNT(*) FILTER (WHERE Origin = 'garbage') FROM mytable WHERE AirlineID > 20000 GROUP BY "
            + "AirlineID";

    JsonNode result = postQuery(sqlQuery);
    assertNoError(result);

    // Ensure that result set is not empty since all groups should be computed by default
    assertTrue(result.get("numRowsResultSet").asInt() > 0);

    // Ensure that the count is 0 for all groups (because the aggregation filter does not match any rows)
    JsonNode rows = result.get("resultTable").get("rows");
    for (int i = 0; i < rows.size(); i++) {
      assertEquals(rows.get(i).get(1).asInt(), 0);
      // Ensure that the main filter was applied
      assertTrue(rows.get(i).get(0).asInt() > 20000);
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testFilteredAggregationWithNoValueMatchingAggregationFilterWithOption(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String sqlQuery =
        "SET " + CommonConstants.Broker.Request.QueryOptionKey.FILTERED_AGGREGATIONS_SKIP_EMPTY_GROUPS + "=true; "
            + "SELECT AirlineID, COUNT(*) FILTER (WHERE Origin = 'garbage') FROM mytable WHERE AirlineID > 20000 "
            + "GROUP BY AirlineID";
    JsonNode result = postQuery(sqlQuery);
    assertNoError(result);

    // Result set will be empty since the aggregation filter does not match any rows, and we've set the option to skip
    // empty groups
    assertEquals(result.get("numRowsResultSet").asInt(), 0);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testFastFilteredCountWithOrFilterOnBitmapWithExclusiveBitmap(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    // Column "Origin" has a range index, and column "DayofMonth" has a sorted index in several segments. This means
    // that the count aggregation will be executed using the fast filtered count operator in these segments. The range
    // index will produce a non-inverted bitmap collection and the sorted index will produce an inverted bitmap
    // collection (since the filter predicate is exclusive).

    // See this issue - https://github.com/apache/pinot/issues/14486
    testQuery(
        "SELECT COUNT(*) FROM mytable WHERE Origin BETWEEN 'ALB' AND 'LMT' OR DayofMonth <> 2"
    );
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testResponseWithClientRequestId(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String clientRequestId = UUID.randomUUID().toString();
    String sqlQuery =
        "SET " + CommonConstants.Broker.Request.QueryOptionKey.CLIENT_QUERY_ID + "='" + clientRequestId + "'; "
            + "SELECT AirlineID FROM mytable LIMIT 1";
    JsonNode result = postQuery(sqlQuery);
    assertNoError(result);

    assertEquals(result.get("clientRequestId").asText(), clientRequestId);
  }

  @Test
  public void testSegmentReload() {
    try {
      // Reload a single segment in the offline table
      String segmentName = listSegments(DEFAULT_TABLE_NAME + "_OFFLINE").get(0);
      String response = reloadOfflineSegment(DEFAULT_TABLE_NAME + "_OFFLINE", segmentName, true);
      JsonNode responseJson = JsonUtils.stringToJsonNode(response);

      // Single segment reload response: status is a string, parse manually
      String statusString = responseJson.get("status").asText();
      assertTrue(statusString.contains("SUCCESS"), "Segment reload failed: " + statusString);
      int startIdx = statusString.indexOf("reload job id:") + "reload job id:".length();
      int endIdx = statusString.indexOf(',', startIdx);
      String jobId = statusString.substring(startIdx, endIdx).trim();
      TestUtils.waitForCondition(aVoid -> {
        try {
          return isReloadJobCompleted(jobId);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }, 600_000L, "Reload job did not complete in 10 minutes");
    } catch (Exception e) {
      fail("Segment reload failed with exception: " + e.getMessage());
    }
  }

  @Test
  public void testVirtualColumnWithPartialReload() throws Exception {
    Schema oldSchema = getSchema(DEFAULT_SCHEMA_NAME);
    // Pick any existing INT column name for the “valid” cases
    String validColumnName = oldSchema.getAllFieldSpecs().stream()
        .filter(fs -> fs.getDataType() == DataType.INT)
        .findFirst()
        .get()
        .getName();
    //  New column name that is not in the schema
    String newColumn = "newColumn";
    DataType newdataType = DataType.INT;
    // Test queries
    List<String> queries = List.of(
        SELECT_STAR_QUERY,
        SELECT_STAR_QUERY + " WHERE " + validColumnName + " > 0 LIMIT 10000",
        SELECT_STAR_QUERY + " ORDER BY " + validColumnName + " LIMIT 10000",
        SELECT_STAR_QUERY + " ORDER BY " + newColumn + " LIMIT 10000",
        "SELECT " + newColumn + " FROM " + DEFAULT_TABLE_NAME
    );
    for (String query: queries) {
      try {
        // Build new schema with the extra column
        Schema newSchema = new Schema();
        newSchema.setSchemaName(oldSchema.getSchemaName());
        for (FieldSpec fs : oldSchema.getAllFieldSpecs()) {
          newSchema.addField(fs);
        }
        FieldSpec newFieldSpec = new DimensionFieldSpec(newColumn, newdataType, true);
        newSchema.addField(newFieldSpec);
        updateSchema(newSchema);

        // Partially reload one segment
        reloadAndWait(DEFAULT_TABLE_NAME + "_OFFLINE",
            listSegments(DEFAULT_TABLE_NAME + "_OFFLINE").get(0));
        // Column should show since it would be added as a virtual column
        runQueryAndAssert(query, newColumn, newFieldSpec);
        // Now do a full reload and the column should still be there, indicating there is no regression
        reloadAndWait(DEFAULT_TABLE_NAME + "_OFFLINE", null);
        runQueryAndAssert(query, newColumn, newFieldSpec);
      } finally {
        // Reset back to the original schema for the next iteration
        forceUpdateSchema(oldSchema);
        reloadAndWait(DEFAULT_TABLE_NAME + "_OFFLINE", null);
      }
    }
  }

  @Test
  public void testVirtualColumnAfterReloadForDifferentDataTypes() throws Exception {
    Schema oldSchema = getSchema(DEFAULT_SCHEMA_NAME);
    try {
      // Build a new schema: copy everything, then add one virtual column per DataType.
      Schema newSchema = new Schema();
      oldSchema.getAllFieldSpecs().forEach(newSchema::addField);
      // Keep insertion order – helps when debugging.
      Map<String, FieldSpec> newCols = new LinkedHashMap<>();
      List<DataType> newDataTypes = List.of(
          DataType.INT, DataType.LONG, DataType.FLOAT, DataType.DOUBLE,
          DataType.STRING, DataType.BOOLEAN, DataType.BYTES);
      for (DataType dt : newDataTypes) {
        String col = "col_" + dt.name().toLowerCase();
        FieldSpec fs = new DimensionFieldSpec(col, dt, true);
        newCols.put(col, fs);
        newSchema.addField(fs);
      }
      newSchema.setSchemaName(oldSchema.getSchemaName());
      updateSchema(newSchema);

      // Reload segment.
      reloadAndWait(DEFAULT_TABLE_NAME + "_OFFLINE", listSegments(DEFAULT_TABLE_NAME + "_OFFLINE").get(0));

      JsonNode res = postQuery("SELECT * FROM " + DEFAULT_TABLE_NAME + " LIMIT 5000");
      assertNoError(res);
      JsonNode rows = res.get("resultTable").get("rows");
      DataSchema resultSchema =
          JsonUtils.jsonNodeToObject(res.get("resultTable").get("dataSchema"), DataSchema.class);

      // Verify each new column.
      for (Map.Entry<String, FieldSpec> e : newCols.entrySet()) {
        String col = e.getKey();
        FieldSpec fs = e.getValue();

        int idx = IntStream.range(0, resultSchema.size())
            .filter(i -> resultSchema.getColumnName(i).equals(col))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Column " + col + " missing"));

        Assert.assertEquals(resultSchema.getColumnDataType(idx).name(), fs.getDataType().name(),
            "Mismatch in reported type for " + col);

        for (JsonNode row : rows) {
          String expectedDefault;
          if (fs.getDataType() == DataType.BOOLEAN) {
            // Pinot surfaces boolean default nulls as literal "false"
            expectedDefault = "false";
          } else {
            expectedDefault = fs.getDefaultNullValueString();
          }
          Assert.assertEquals(row.get(idx).asText(), expectedDefault, "Unexpected default value for " + col);
        }
      }
    } finally {
      // Clean up the schema to the original state
      forceUpdateSchema(oldSchema);
      reloadAndWait(DEFAULT_TABLE_NAME + "_OFFLINE", null);
    }
  }

  private void reloadAndWait(String tableNameWithType, @Nullable String segmentName) throws Exception {
    String response = (segmentName != null)
        ? reloadOfflineSegment(tableNameWithType, segmentName, true)
        : reloadOfflineTable(tableNameWithType, true);
    JsonNode responseJson = JsonUtils.stringToJsonNode(response);
    String jobId;
    if (segmentName != null) {
      // Single segment reload response: status is a string, parse manually
      String statusString = responseJson.get("status").asText();
      assertTrue(statusString.contains("SUCCESS"), "Segment reload failed: " + statusString);
      int startIdx = statusString.indexOf("reload job id:") + "reload job id:".length();
      int endIdx = statusString.indexOf(',', startIdx);
      jobId = statusString.substring(startIdx, endIdx).trim();
    } else {
      // Full table reload response: structured JSON
      JsonNode tableLevelDetails
          = JsonUtils.stringToJsonNode(responseJson.get("status").asText()).get(tableNameWithType);
      Assert.assertEquals(tableLevelDetails.get("reloadJobMetaZKStorageStatus").asText(), "SUCCESS");
      jobId = tableLevelDetails.get("reloadJobId").asText();
    }
    String finalJobId = jobId;
    TestUtils.waitForCondition(aVoid -> {
      try {
        return isReloadJobCompleted(finalJobId);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 600_000L, "Reload job did not complete in 10 minutes");
  }

  private void runQueryAndAssert(String query, String newAddedColumn, FieldSpec fieldSpec) throws Exception {
    JsonNode response = postQuery(query);
    assertNoError(response);
    JsonNode rows = response.get("resultTable").get("rows");
    JsonNode jsonSchema = response.get("resultTable").get("dataSchema");
    DataSchema resultSchema = JsonUtils.jsonNodeToObject(jsonSchema, DataSchema.class);

    assert !rows.isEmpty();
    boolean columnPresent = false;
    String[] columnNames = resultSchema.getColumnNames();
    for (int columnIndex = 0; columnIndex < columnNames.length; columnIndex++) {
      if (columnNames[columnIndex].equals(newAddedColumn)) {
        columnPresent = true;
        // Check the data type of the new column
        Assert.assertEquals(resultSchema.getColumnDataType(columnIndex).name(), fieldSpec.getDataType().name());
        // Check the value of the new column
        for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
          Assert.assertEquals(rows.get(rowIndex).get(columnIndex).asText(),
              String.valueOf(fieldSpec.getDefaultNullValue()));
        }
      }
    }
    Assert.assertTrue(columnPresent, "Column " + newAddedColumn + " not present in result set");
  }
}
