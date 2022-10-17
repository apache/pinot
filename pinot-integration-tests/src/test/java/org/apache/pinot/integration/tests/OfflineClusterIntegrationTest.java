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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.helix.model.IdealState;
import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.pinot.client.PinotConnection;
import org.apache.pinot.client.PinotDriver;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.core.operator.query.NonScanBasedAggregationOperator;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.QueryConfig;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.InstanceTypeUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.common.function.scalar.StringFunctions.*;
import static org.apache.pinot.controller.helix.core.PinotHelixResourceManager.EXTERNAL_VIEW_CHECK_INTERVAL_MS;
import static org.apache.pinot.controller.helix.core.PinotHelixResourceManager.EXTERNAL_VIEW_ONLINE_SEGMENTS_MAX_WAIT_MS;
import static org.testng.Assert.*;


/**
 * Integration test that converts Avro data for 12 segments and runs queries against it.
 */
public class OfflineClusterIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final int NUM_BROKERS = 1;
  private static final int NUM_SERVERS = 1;
  private static final int NUM_SEGMENTS = 12;
  private static final long ONE_HOUR_IN_MS = TimeUnit.HOURS.toMillis(1);
  private static final String SEGMENT_UPLOAD_TEST_TABLE = "segmentUploadTestTable";

  // For table config refresh test, make an expensive query to ensure the query won't finish in 5ms
  private static final String TEST_TIMEOUT_QUERY =
      "SELECT DISTINCTCOUNT(AirlineID) FROM mytable GROUP BY Carrier LIMIT 10000";

  // For inverted index triggering test
  private static final List<String> UPDATED_INVERTED_INDEX_COLUMNS =
      Arrays.asList("FlightNum", "Origin", "Quarter", "DivActualElapsedTime");
  private static final String TEST_UPDATED_INVERTED_INDEX_QUERY =
      "SELECT COUNT(*) FROM mytable WHERE DivActualElapsedTime = 305";

  // For range index triggering test
  private static final List<String> UPDATED_RANGE_INDEX_COLUMNS = Collections.singletonList("DivActualElapsedTime");
  private static final String TEST_UPDATED_RANGE_INDEX_QUERY =
      "SELECT COUNT(*) FROM mytable WHERE DivActualElapsedTime > 305";

  // For bloom filter triggering test
  private static final List<String> UPDATED_BLOOM_FILTER_COLUMNS = Collections.singletonList("Carrier");
  private static final String TEST_UPDATED_BLOOM_FILTER_QUERY = "SELECT COUNT(*) FROM mytable WHERE Carrier = 'CA'";

  // For star-tree triggering test
  private static final StarTreeIndexConfig STAR_TREE_INDEX_CONFIG_1 =
      new StarTreeIndexConfig(Collections.singletonList("Carrier"), null,
          Collections.singletonList(AggregationFunctionColumnPair.COUNT_STAR.toColumnName()), 100);
  private static final String TEST_STAR_TREE_QUERY_1 = "SELECT COUNT(*) FROM mytable WHERE Carrier = 'UA'";
  private static final StarTreeIndexConfig STAR_TREE_INDEX_CONFIG_2 =
      new StarTreeIndexConfig(Collections.singletonList("DestState"), null,
          Collections.singletonList(AggregationFunctionColumnPair.COUNT_STAR.toColumnName()), 100);
  private static final String TEST_STAR_TREE_QUERY_2 = "SELECT COUNT(*) FROM mytable WHERE DestState = 'CA'";

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
  // TODO: This might lead to flaky test, as this disk size is not deterministic
  //       as it depends on the iteration order of a HashSet.
  private static final int DISK_SIZE_IN_BYTES = 20797128;
  private static final int NUM_ROWS = 115545;

  private final List<ServiceStatus.ServiceStatusCallback> _serviceStatusCallbacks =
      new ArrayList<>(getNumBrokers() + getNumServers());
  private String _schemaFileName = DEFAULT_SCHEMA_FILE_NAME;
  // Cache the table size after removing an index via reloading. Once this value
  // is set, assert that table size always gets back to this value after removing
  // any other kind of index.
  private long _tableSizeAfterRemovingIndex;

  protected int getNumBrokers() {
    return NUM_BROKERS;
  }

  protected int getNumServers() {
    return NUM_SERVERS;
  }

  @Override
  protected String getSchemaFileName() {
    return _schemaFileName;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBrokers();
    startServers();

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create and upload segments. For exhaustive testing, concurrently upload multiple segments with the same name
    // and validate correctness with parallel push protection enabled.
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);
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
      // of the two exception - 409 conflict of the second call enters ProcessExistingSegment ; segmentZkMetadata
      // creation failure if both calls entered ProcessNewSegment. In/such cases ensure that we upload all the
      // segments again/to ensure that the data is setup correctly.
      assertTrue(e.getMessage().contains("Another segment upload is in progress for segment") || e.getMessage()
          .contains("Failed to create ZK metadata for segment"), e.getMessage());
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

  @Override
  protected void testQuery(String pinotQuery, String h2Query)
      throws Exception {
    if (getNumServers() == 1) {
      pinotQuery = "SET serverReturnFinalResult = true;" + pinotQuery;
    }
    super.testQuery(pinotQuery, h2Query);
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
    TableConfig tableConfig = getOfflineTableConfig();
    tableConfig.setQueryConfig(new QueryConfig(5L, null, null, null));
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
        if (errorCode == QueryException.BROKER_TIMEOUT_ERROR_CODE) {
          // Timed out on broker side
          return true;
        }
        if (errorCode == QueryException.SERVER_NOT_RESPONDING_ERROR_CODE) {
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
    tableConfig.setQueryConfig(null);
    updateTableConfig(tableConfig);

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
    TableConfig segmentUploadTestTableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(SEGMENT_UPLOAD_TEST_TABLE).setSchemaName(getSchemaName())
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

    URI uploadSegmentHttpURI = FileUploadDownloadClient.getUploadSegmentHttpURI(LOCAL_HOST, _controllerPort);
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
    cleanupTestTableDataManager(offlineTableName);
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
    throw new TimeoutException(
        String.format("Time out while waiting segments become ONLINE. (tableNameWithType = %s)", tableNameWithType));
  }

  @Test(dependsOnMethods = "testRangeIndexTriggering")
  public void testInvertedIndexTriggering()
      throws Exception {
    long numTotalDocs = getCountStarResult();

    // Without index on DivActualElapsedTime, all docs are scanned at filtering stage.
    assertEquals(postQuery(TEST_UPDATED_INVERTED_INDEX_QUERY).get("numEntriesScannedInFilter").asLong(), numTotalDocs);

    addInvertedIndex();
    long tableSizeWithNewIndex = getTableSize(getTableName());

    // Update table config to remove the new inverted index, and
    // reload table to clean the new inverted indices physically.
    TableConfig tableConfig = getOfflineTableConfig();
    tableConfig.getIndexingConfig().setInvertedIndexColumns(getInvertedIndexColumns());
    updateTableConfig(tableConfig);
    String reloadJobId = reloadOfflineTableAndValidateResponse(getTableName(), false);
    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode queryResponse = postQuery(TEST_UPDATED_INVERTED_INDEX_QUERY);
        // Total docs should not change during reload, but num entries scanned
        // gets back to total number of documents as the index is removed.
        assertEquals(queryResponse.get("totalDocs").asLong(), numTotalDocs);
        return queryResponse.get("numEntriesScannedInFilter").asLong() == numTotalDocs;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 600_000L, "Failed to cleanup obsolete index");
    validateReloadJobSuccess(reloadJobId);

    assertEquals(getTableSize(getTableName()), _tableSizeAfterRemovingIndex);

    // Add the inverted index back to test index removal via force download.
    addInvertedIndex();
    assertEquals(getTableSize(getTableName()), tableSizeWithNewIndex);

    // Update table config to remove the new inverted index.
    tableConfig = getOfflineTableConfig();
    tableConfig.getIndexingConfig().setInvertedIndexColumns(getInvertedIndexColumns());
    updateTableConfig(tableConfig);

    // Force to download a single segment, and disk usage should drop a bit.
    SegmentZKMetadata segmentZKMetadata =
        _helixResourceManager.getSegmentsZKMetadata(TableNameBuilder.OFFLINE.tableNameWithType(getTableName())).get(0);
    String segmentName = segmentZKMetadata.getSegmentName();
    reloadOfflineSegment(getTableName(), segmentName, true);
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
    long tableSizeAfterReloadSegment = getTableSize(getTableName());
    assertTrue(tableSizeAfterReloadSegment > DISK_SIZE_IN_BYTES && tableSizeAfterReloadSegment < tableSizeWithNewIndex,
        String.format("Table size: %d should be between %d and %d after dropping inverted index from segment: %s",
            tableSizeAfterReloadSegment, DISK_SIZE_IN_BYTES, tableSizeWithNewIndex, segmentName));

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
    // The table size gets larger for sure, but it does not necessarily equal to tableSizeWithNewIndex, because the
    // order of entries in the index_map file can change when the raw segment adds/deletes indices back and forth.
    long tableSizeAfterAddIndex = getTableSize(getTableName());
    assertTrue(tableSizeAfterAddIndex > tableSizeAfterReloadSegment,
        String.format("Table size: %d should increase after adding inverted index on segment: %s, as compared with %d",
            tableSizeAfterAddIndex, segmentName, tableSizeAfterReloadSegment));

    // Force to download the whole table and use the original table config, so the disk usage should get back to
    // initial value.
    tableConfig = getOfflineTableConfig();
    tableConfig.getIndexingConfig().setInvertedIndexColumns(getInvertedIndexColumns());
    updateTableConfig(tableConfig);
    String reloadId = reloadOfflineTableAndValidateResponse(getTableName(), true);
    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode queryResponse = postQuery(TEST_UPDATED_INVERTED_INDEX_QUERY);
        // Total docs should not change during reload, but num entries scanned
        // gets back to total number of documents as the index is removed.
        assertEquals(queryResponse.get("totalDocs").asLong(), numTotalDocs);

        long actualTotalDocs = queryResponse.get("numEntriesScannedInFilter").asLong();
        return actualTotalDocs == numTotalDocs;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 600_000L, "Failed to cleanup obsolete index in table");
    validateReloadJobSuccess(reloadJobId);
    // With force download, the table size gets back to the initial value.
    assertEquals(getTableSize(getTableName()), DISK_SIZE_IN_BYTES);
  }

  private void addInvertedIndex()
      throws IOException {
    // Update table config to add inverted index on DivActualElapsedTime column, and
    // reload the table to get config change into effect and add the inverted index.
    TableConfig tableConfig = getOfflineTableConfig();
    tableConfig.getIndexingConfig().setInvertedIndexColumns(UPDATED_INVERTED_INDEX_COLUMNS);
    updateTableConfig(tableConfig);
    String reloadJobId =
        reloadOfflineTableAndValidateResponse(TableNameBuilder.OFFLINE.tableNameWithType(getTableName()), false);

    // It takes a while to reload multiple segments, thus we retry the query for some time.
    // After all segments are reloaded, the inverted index is added on DivActualElapsedTime.
    // It's expected to have numEntriesScannedInFilter equal to 0, i.e. no docs is scanned
    // at filtering stage when inverted index can answer the predicate directly.
    long numTotalDocs = getCountStarResult();
    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode queryResponse = postQuery(TEST_UPDATED_INVERTED_INDEX_QUERY);
        // Total docs should not change during reload
        assertEquals(queryResponse.get("totalDocs").asLong(), numTotalDocs);
        return queryResponse.get("numEntriesScannedInFilter").asLong() == 0L;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 600_000L, "Failed to generate inverted index");
    validateReloadJobSuccess(reloadJobId);
  }

  @Test
  public void testTimeFunc()
      throws Exception {
    String sqlQuery = "SELECT toDateTime(now(), 'yyyy-MM-dd z'), toDateTime(ago('PT1H'), 'yyyy-MM-dd z') FROM mytable";
    JsonNode response = postQuery(sqlQuery, _brokerBaseApiUrl);
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
  public void testRegexpReplace()
      throws Exception {
    // Correctness tests of regexpReplace.

    // Test replace all.
    String sqlQuery = "SELECT regexpReplace('CA', 'C', 'TEST')";
    JsonNode response = postQuery(sqlQuery, _brokerBaseApiUrl);
    String result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "TESTA");

    sqlQuery = "SELECT regexpReplace('foobarbaz', 'b', 'X')";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "fooXarXaz");

    sqlQuery = "SELECT regexpReplace('foobarbaz', 'b', 'XY')";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "fooXYarXYaz");

    sqlQuery = "SELECT regexpReplace('Argentina', '(.)', '$1 ')";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "A r g e n t i n a ");

    sqlQuery = "SELECT regexpReplace('Pinot is  blazing  fast', '( ){2,}', ' ')";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "Pinot is blazing fast");

    sqlQuery = "SELECT regexpReplace('healthy, wealthy, and wise','\\w+thy', 'something')";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "something, something, and wise");

    sqlQuery = "SELECT regexpReplace('11234567898','(\\d)(\\d{3})(\\d{3})(\\d{4})', '$1-($2) $3-$4')";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "1-(123) 456-7898");

    // Test replace starting at index.

    sqlQuery = "SELECT regexpReplace('healthy, wealthy, stealthy and wise','\\w+thy', 'something', 4)";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "healthy, something, something and wise");

    sqlQuery = "SELECT regexpReplace('healthy, wealthy, stealthy and wise','\\w+thy', 'something', 1)";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "hsomething, something, something and wise");

    // Test occurence
    sqlQuery = "SELECT regexpReplace('healthy, wealthy, stealthy and wise','\\w+thy', 'something', 0, 2)";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "healthy, wealthy, something and wise");

    sqlQuery = "SELECT regexpReplace('healthy, wealthy, stealthy and wise','\\w+thy', 'something', 0, 0)";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "something, wealthy, stealthy and wise");

    // Test flags
    sqlQuery = "SELECT regexpReplace('healthy, wealthy, stealthy and wise','\\w+tHy', 'something', 0, 0, 'i')";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "something, wealthy, stealthy and wise");

    // Negative test. Pattern match not found.
    sqlQuery = "SELECT regexpReplace('healthy, wealthy, stealthy and wise','\\w+tHy', 'something')";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "healthy, wealthy, stealthy and wise");

    // Negative test. Pattern match not found.
    sqlQuery = "SELECT regexpReplace('healthy, wealthy, stealthy and wise','\\w+tHy', 'something', 3, 21, 'i')";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "healthy, wealthy, stealthy and wise");

    // Negative test - incorrect flag
    sqlQuery = "SELECT regexpReplace('healthy, wealthy, stealthy and wise','\\w+tHy', 'something', 3, 12, 'xyz')";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "healthy, wealthy, stealthy and wise");

    // Test in select clause with column values
    sqlQuery = "SELECT regexpReplace(DestCityName, ' ', '', 0, -1, 'i') from myTable where OriginState = 'CA'";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    JsonNode rows = response.get("resultTable").get("rows");
    for (int i = 0; i < rows.size(); i++) {
      JsonNode row = rows.get(i);
      assertFalse(row.get(0).asText().contains(" "));
    }

    // Test in where clause
    sqlQuery = "SELECT count(*) from myTable where regexpReplace(originState, '[VC]A', 'TEST') = 'TEST'";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    int count1 = response.get("resultTable").get("rows").get(0).get(0).asInt();
    sqlQuery = "SELECT count(*) from myTable where originState='CA' or originState='VA'";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    int count2 = response.get("resultTable").get("rows").get(0).get(0).asInt();
    assertEquals(count1, count2);

    // Test nested transform
    sqlQuery =
        "SELECT count(*) from myTable where contains(regexpReplace(originState, '(C)(A)', '$1TEST$2'), 'CTESTA')";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    count1 = response.get("resultTable").get("rows").get(0).get(0).asInt();
    sqlQuery = "SELECT count(*) from myTable where originState='CA'";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    count2 = response.get("resultTable").get("rows").get(0).get(0).asInt();
    assertEquals(count1, count2);
  }

  @Test
  public void testCastMV()
      throws Exception {

    // simple cast
    String sqlQuery = "SELECT DivLongestGTimes, CAST(DivLongestGTimes as DOUBLE) from myTable LIMIT 100";
    JsonNode response = postQuery(sqlQuery, _brokerBaseApiUrl);
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
        + " DivTotalGTimes, CAST(CAST(DivTotalGTimes AS STRING) AS LONG) from myTable ORDER BY CARRIER LIMIT 100";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
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

  @Test
  public void testUrlFunc()
      throws Exception {
    String sqlQuery = "SELECT encodeUrl('key1=value 1&key2=value@!$2&key3=value%3'), "
        + "decodeUrl('key1%3Dvalue+1%26key2%3Dvalue%40%21%242%26key3%3Dvalue%253') FROM myTable";
    JsonNode response = postQuery(sqlQuery, _brokerBaseApiUrl);
    String encodedString = response.get("resultTable").get("rows").get(0).get(0).asText();
    String expectedUrlStr = encodeUrl("key1=value 1&key2=value@!$2&key3=value%3");
    assertEquals(encodedString, expectedUrlStr);

    String decodedString = response.get("resultTable").get("rows").get(0).get(1).asText();
    expectedUrlStr = decodeUrl("key1%3Dvalue+1%26key2%3Dvalue%40%21%242%26key3%3Dvalue%253");
    assertEquals(decodedString, expectedUrlStr);
  }

  @Test
  public void testBase64Func()
      throws Exception {

    // string literal
    String sqlQuery = "SELECT toBase64(toUtf8('hello!')), " + "fromUtf8(fromBase64('aGVsbG8h')) FROM myTable";
    JsonNode response = postQuery(sqlQuery, _brokerBaseApiUrl);
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
            + "FROM myTable";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    resultTable = response.get("resultTable");
    rows = resultTable.get("rows");
    encodedString = rows.get(0).get(0).asText();
    assertEquals(encodedString,
        toBase64(toUtf8("this is a long string that will encode to more than 76 characters using base64")));

    // long string literal decode
    sqlQuery = "SELECT fromUtf8(fromBase64"
        + "('dGhpcyBpcyBhIGxvbmcgc3RyaW5nIHRoYXQgd2lsbCBlbmNvZGUgdG8gbW9yZSB0aGFuIDc2IGNoYXJhY3RlcnMgdXNpbmcgYmFzZTY0"
        + "')) FROM myTable";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    resultTable = response.get("resultTable");
    rows = resultTable.get("rows");
    decodedString = rows.get(0).get(0).asText();
    assertEquals(decodedString, fromUtf8(fromBase64(
        "dGhpcyBpcyBhIGxvbmcgc3RyaW5nIHRoYXQgd2lsbCBlbmNvZGUgdG8gbW9yZSB0aGFuIDc2IGNoYXJhY3RlcnMgdXNpbmcgYmFzZTY0")));

    // non-string literal
    sqlQuery = "SELECT toBase64(toUtf8(123)), fromUtf8(fromBase64(toBase64(toUtf8(123)))), 123 FROM myTable";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    resultTable = response.get("resultTable");
    rows = resultTable.get("rows");
    encodedString = rows.get(0).get(0).asText();
    decodedString = rows.get(0).get(1).asText();
    String originalCol = rows.get(0).get(2).asText();
    assertEquals(decodedString, originalCol);
    assertEquals(encodedString, toBase64(toUtf8("123")));

    // identifier
    sqlQuery = "SELECT Carrier, toBase64(toUtf8(Carrier)), fromUtf8(fromBase64(toBase64(toUtf8(Carrier)))), "
        + "fromBase64(toBase64(toUtf8(Carrier))) FROM myTable LIMIT 100";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
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
    sqlQuery = "SELECT toBase64() FROM myTable";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    assertTrue(response.get("exceptions").get(0).get("message").toString().startsWith("\"QueryExecutionError"));

    // invalid argument
    sqlQuery = "SELECT fromBase64() FROM myTable";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    assertTrue(response.get("exceptions").get(0).get("message").toString().startsWith("\"QueryExecutionError"));

    // invalid argument
    sqlQuery = "SELECT toBase64('hello!') FROM myTable";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    assertTrue(response.get("exceptions").get(0).get("message").toString().contains("SqlCompilationException"));

    // invalid argument
    sqlQuery = "SELECT fromBase64('hello!') FROM myTable";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    assertTrue(response.get("exceptions").get(0).get("message").toString().contains("IllegalArgumentException"));

    // string literal used in a filter
    sqlQuery = "SELECT * FROM myTable WHERE fromUtf8(fromBase64('aGVsbG8h')) != Carrier AND "
        + "toBase64(toUtf8('hello!')) != Carrier LIMIT 10";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    resultTable = response.get("resultTable");
    rows = resultTable.get("rows");
    assertEquals(rows.size(), 10);

    // non-string literal used in a filter
    sqlQuery = "SELECT * FROM myTable WHERE fromUtf8(fromBase64(toBase64(toUtf8(AirlineID)))) != Carrier LIMIT 10";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    resultTable = response.get("resultTable");
    rows = resultTable.get("rows");
    assertEquals(rows.size(), 10);

    // string identifier used in a filter
    sqlQuery = "SELECT * FROM myTable WHERE fromUtf8(fromBase64(toBase64(toUtf8(Carrier)))) = Carrier LIMIT 10";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    resultTable = response.get("resultTable");
    rows = resultTable.get("rows");
    assertEquals(rows.size(), 10);

    // non-string identifier used in a filter
    sqlQuery = "SELECT fromUtf8(fromBase64(toBase64(toUtf8(AirlineID)))), AirlineID FROM myTable WHERE "
        + "fromUtf8(fromBase64(toBase64(toUtf8(AirlineID)))) = AirlineID LIMIT 10";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    resultTable = response.get("resultTable");
    dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnDataTypes").toString(), "[\"STRING\",\"LONG\"]");
    rows = resultTable.get("rows");
    assertEquals(rows.size(), 10);

    // string identifier used in group by order by
    sqlQuery = "SELECT Carrier as originalCol, toBase64(toUtf8(Carrier)) as encoded, "
        + "fromUtf8(fromBase64(toBase64(toUtf8(Carrier)))) as decoded FROM myTable "
        + "GROUP BY Carrier, toBase64(toUtf8(Carrier)), fromUtf8(fromBase64(toBase64(toUtf8(Carrier)))) "
        + "ORDER BY toBase64(toUtf8(Carrier)) LIMIT 10";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
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
        + "fromUtf8(fromBase64(toBase64(toUtf8(AirlineID)))) as decoded FROM myTable "
        + "GROUP BY AirlineID, toBase64(toUtf8(AirlineID)), fromUtf8(fromBase64(toBase64(toUtf8(AirlineID)))) "
        + "ORDER BY fromUtf8(fromBase64(toBase64(toUtf8(AirlineID)))) LIMIT 10";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
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

  @Test
  public void testLiteralOnlyFunc()
      throws Exception {
    long currentTsMin = System.currentTimeMillis();
    long oneHourAgoTsMin = currentTsMin - ONE_HOUR_IN_MS;
    String sqlQuery =
        "SELECT 1, now() as currentTs, ago('PT1H') as oneHourAgoTs, 'abc', toDateTime(now(), 'yyyy-MM-dd z') as "
            + "today, now(), ago('PT1H'), encodeUrl('key1=value 1&key2=value@!$2&key3=value%3') as encodedUrl, "
            + "decodeUrl('key1%3Dvalue+1%26key2%3Dvalue%40%21%242%26key3%3Dvalue%253') as decodedUrl, toBase64"
            + "(toUtf8('hello!')) as toBase64, fromUtf8(fromBase64('aGVsbG8h')) as fromBase64";
    JsonNode response = postQuery(sqlQuery, _brokerBaseApiUrl);
    long currentTsMax = System.currentTimeMillis();
    long oneHourAgoTsMax = currentTsMax - ONE_HOUR_IN_MS;

    assertEquals(response.get("resultTable").get("dataSchema").get("columnNames").get(0).asText(), "1");
    assertEquals(response.get("resultTable").get("dataSchema").get("columnNames").get(1).asText(), "currentTs");
    assertEquals(response.get("resultTable").get("dataSchema").get("columnNames").get(2).asText(), "oneHourAgoTs");
    assertEquals(response.get("resultTable").get("dataSchema").get("columnNames").get(3).asText(), "abc");
    assertEquals(response.get("resultTable").get("dataSchema").get("columnNames").get(4).asText(), "today");
    String nowColumnName = response.get("resultTable").get("dataSchema").get("columnNames").get(5).asText();
    String oneHourAgoColumnName = response.get("resultTable").get("dataSchema").get("columnNames").get(6).asText();
    assertEquals(response.get("resultTable").get("dataSchema").get("columnNames").get(7).asText(), "encodedUrl");
    assertEquals(response.get("resultTable").get("dataSchema").get("columnNames").get(8).asText(), "decodedUrl");
    assertEquals(response.get("resultTable").get("dataSchema").get("columnNames").get(9).asText(), "toBase64");
    assertEquals(response.get("resultTable").get("dataSchema").get("columnNames").get(10).asText(), "fromBase64");
    assertTrue(Long.parseLong(nowColumnName) > currentTsMin);
    assertTrue(Long.parseLong(nowColumnName) < currentTsMax);
    assertTrue(Long.parseLong(oneHourAgoColumnName) > oneHourAgoTsMin);
    assertTrue(Long.parseLong(oneHourAgoColumnName) < oneHourAgoTsMax);

    assertEquals(response.get("resultTable").get("dataSchema").get("columnDataTypes").get(0).asText(), "LONG");
    assertEquals(response.get("resultTable").get("dataSchema").get("columnDataTypes").get(1).asText(), "LONG");
    assertEquals(response.get("resultTable").get("dataSchema").get("columnDataTypes").get(2).asText(), "LONG");
    assertEquals(response.get("resultTable").get("dataSchema").get("columnDataTypes").get(3).asText(), "STRING");
    assertEquals(response.get("resultTable").get("dataSchema").get("columnDataTypes").get(4).asText(), "STRING");
    assertEquals(response.get("resultTable").get("dataSchema").get("columnDataTypes").get(5).asText(), "LONG");
    assertEquals(response.get("resultTable").get("dataSchema").get("columnDataTypes").get(6).asText(), "LONG");
    assertEquals(response.get("resultTable").get("dataSchema").get("columnDataTypes").get(7).asText(), "STRING");
    assertEquals(response.get("resultTable").get("dataSchema").get("columnDataTypes").get(8).asText(), "STRING");
    assertEquals(response.get("resultTable").get("dataSchema").get("columnDataTypes").get(9).asText(), "STRING");
    assertEquals(response.get("resultTable").get("dataSchema").get("columnDataTypes").get(10).asText(), "STRING");

    int first = response.get("resultTable").get("rows").get(0).get(0).asInt();
    long second = response.get("resultTable").get("rows").get(0).get(1).asLong();
    long third = response.get("resultTable").get("rows").get(0).get(2).asLong();
    String fourth = response.get("resultTable").get("rows").get(0).get(3).asText();
    assertEquals(first, 1);
    assertTrue(second > currentTsMin);
    assertTrue(second < currentTsMax);
    assertTrue(third > oneHourAgoTsMin);
    assertTrue(third < oneHourAgoTsMax);
    assertEquals(fourth, "abc");
    String todayStr = response.get("resultTable").get("rows").get(0).get(4).asText();
    String expectedTodayStr =
        Instant.now().atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd z"));
    assertEquals(todayStr, expectedTodayStr);
    long nowValue = response.get("resultTable").get("rows").get(0).get(5).asLong();
    assertEquals(nowValue, Long.parseLong(nowColumnName));
    long oneHourAgoValue = response.get("resultTable").get("rows").get(0).get(6).asLong();
    assertEquals(oneHourAgoValue, Long.parseLong(oneHourAgoColumnName));
    assertEquals(response.get("resultTable").get("rows").get(0).get(7).asText(),
        "key1%3Dvalue+1%26key2%3Dvalue%40%21%242%26key3%3Dvalue%253");
    assertEquals(response.get("resultTable").get("rows").get(0).get(8).asText(),
        "key1=value 1&key2=value@!$2&key3=value%3");
    assertEquals(response.get("resultTable").get("rows").get(0).get(9).asText(), "aGVsbG8h");
    assertEquals(response.get("resultTable").get("rows").get(0).get(10).asText(), "hello!");
  }

  @Test(dependsOnMethods = "testBloomFilterTriggering")
  public void testRangeIndexTriggering()
      throws Exception {
    long numTotalDocs = getCountStarResult();
    assertEquals(postQuery(TEST_UPDATED_RANGE_INDEX_QUERY).get("numEntriesScannedInFilter").asLong(), numTotalDocs);

    // Update table config and trigger reload
    TableConfig tableConfig = getOfflineTableConfig();
    tableConfig.getIndexingConfig().setRangeIndexColumns(UPDATED_RANGE_INDEX_COLUMNS);
    updateTableConfig(tableConfig);
    String reloadJobId = reloadOfflineTableAndValidateResponse(getTableName(), false);
    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode queryResponse = postQuery(TEST_UPDATED_RANGE_INDEX_QUERY);
        // Total docs should not change during reload
        assertEquals(queryResponse.get("totalDocs").asLong(), numTotalDocs);
        return queryResponse.get("numEntriesScannedInFilter").asLong() < numTotalDocs;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 600_000L, "Failed to generate range index");
    validateReloadJobSuccess(reloadJobId);

    // Update table config to remove the new range index, and
    // reload table to clean the new range index physically.
    tableConfig = getOfflineTableConfig();
    tableConfig.getIndexingConfig().setRangeIndexColumns(getRangeIndexColumns());
    updateTableConfig(tableConfig);
    reloadJobId = reloadOfflineTableAndValidateResponse(getTableName(), false);
    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode queryResponse = postQuery(TEST_UPDATED_RANGE_INDEX_QUERY);
        // Total docs should not change during reload, but num entries scanned
        // gets back to total number of documents as the index is removed.
        assertEquals(queryResponse.get("totalDocs").asLong(), numTotalDocs);
        return queryResponse.get("numEntriesScannedInFilter").asLong() == numTotalDocs;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 600_000L, "Failed to cleanup obsolete index");
    validateReloadJobSuccess(reloadJobId);

    assertEquals(getTableSize(getTableName()), _tableSizeAfterRemovingIndex);
  }

  @Test(dependsOnMethods = "testDefaultColumns")
  public void testBloomFilterTriggering()
      throws Exception {
    long numTotalDocs = getCountStarResult();
    assertEquals(postQuery(TEST_UPDATED_BLOOM_FILTER_QUERY).get("numSegmentsProcessed").asLong(), NUM_SEGMENTS);

    // Update table config and trigger reload
    TableConfig tableConfig = getOfflineTableConfig();
    tableConfig.getIndexingConfig().setBloomFilterColumns(UPDATED_BLOOM_FILTER_COLUMNS);
    updateTableConfig(tableConfig);
    String reloadJobId = reloadOfflineTableAndValidateResponse(getTableName(), false);
    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode queryResponse = postQuery(TEST_UPDATED_BLOOM_FILTER_QUERY);
        // Total docs should not change during reload
        assertEquals(queryResponse.get("totalDocs").asLong(), numTotalDocs);
        return queryResponse.get("numSegmentsProcessed").asLong() == 0L;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 600_000L, "Failed to generate bloom filter");
    validateReloadJobSuccess(reloadJobId);

    // Update table config to remove the new bloom filter, and
    // reload table to clean the new bloom filter physically.
    tableConfig = getOfflineTableConfig();
    tableConfig.getIndexingConfig().setBloomFilterColumns(getBloomFilterColumns());
    updateTableConfig(tableConfig);
    reloadJobId = reloadOfflineTableAndValidateResponse(getTableName(), false);
    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode queryResponse = postQuery(TEST_UPDATED_BLOOM_FILTER_QUERY);
        // Total docs should not change during reload, but num entries scanned
        // gets back to total number of documents as bloom filter is removed.
        assertEquals(queryResponse.get("totalDocs").asLong(), numTotalDocs);
        return queryResponse.get("numSegmentsProcessed").asLong() == NUM_SEGMENTS;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 600_000L, "Failed to cleanup obsolete index");
    validateReloadJobSuccess(reloadJobId);
    assertEquals(getTableSize(getTableName()), _tableSizeAfterRemovingIndex);
  }

  /**
   * Check if server returns error response quickly without timing out Broker.
   */
  @Test
  public void testServerErrorWithBrokerTimeout()
      throws Exception {
    long startTimeMs = System.currentTimeMillis();
    // The query below will fail execution due to JSON_MATCH on column without json index
    JsonNode queryResponse = postQuery("SELECT count(*) FROM mytable WHERE JSON_MATCH(Dest, '$=123')");
    // NOTE: Broker timeout is 60s
    assertTrue(System.currentTimeMillis() - startTimeMs < 60_000L);
    assertTrue(queryResponse.get("exceptions").get(0).get("message").toString().startsWith("\"QueryExecutionError"));
  }

  @Test
  public void testStarTreeTriggering()
      throws Exception {
    long numTotalDocs = getCountStarResult();
    long tableSizeWithDefaultIndex = getTableSize(getTableName());

    // Test the first query
    JsonNode firstQueryResponse = postQuery(TEST_STAR_TREE_QUERY_1);
    int firstQueryResult = firstQueryResponse.get("resultTable").get("rows").get(0).get(0).asInt();
    assertEquals(firstQueryResponse.get("totalDocs").asLong(), numTotalDocs);
    // Initially 'numDocsScanned' should be the same as 'COUNT(*)' result
    assertEquals(firstQueryResponse.get("numDocsScanned").asInt(), firstQueryResult);

    // Update table config and trigger reload
    TableConfig tableConfig = getOfflineTableConfig();
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    indexingConfig.setStarTreeIndexConfigs(Collections.singletonList(STAR_TREE_INDEX_CONFIG_1));
    indexingConfig.setEnableDynamicStarTreeCreation(true);
    updateTableConfig(tableConfig);
    String reloadJobId = reloadOfflineTableAndValidateResponse(getTableName(), false);

    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode queryResponse = postQuery(TEST_STAR_TREE_QUERY_1);
        // Result should not change during reload
        assertEquals(queryResponse.get("resultTable").get("rows").get(0).get(0).asInt(), firstQueryResult);
        // Total docs should not change during reload
        assertEquals(queryResponse.get("totalDocs").asLong(), numTotalDocs);
        // With star-tree, 'numDocsScanned' should be the same as number of segments (1 per segment)
        return queryResponse.get("numDocsScanned").asInt() == NUM_SEGMENTS;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 600_000L, "Failed to add first star-tree index");
    validateReloadJobSuccess(reloadJobId);

    // Reload again should have no effect
    reloadOfflineTableAndValidateResponse(getTableName(), false);
    firstQueryResponse = postQuery(TEST_STAR_TREE_QUERY_1);
    assertEquals(firstQueryResponse.get("resultTable").get("rows").get(0).get(0).asInt(), firstQueryResult);
    assertEquals(firstQueryResponse.get("totalDocs").asLong(), numTotalDocs);
    assertEquals(firstQueryResponse.get("numDocsScanned").asInt(), NUM_SEGMENTS);

    // Enforce a sleep here since segment reload is async and there is another back-to-back reload below.
    // Otherwise, there is no way to tell whether the 1st reload on server side is finished,
    // which may hit the race condition that the 1st reload finishes after the 2nd reload is fully done.
    // 10 seconds are still better than hitting race condition which will time out after 10 minutes.
    Thread.sleep(10_000L);

    // Should be able to use the star-tree with an additional match-all predicate on another dimension
    firstQueryResponse = postQuery(TEST_STAR_TREE_QUERY_1 + " AND DaysSinceEpoch > 16070");
    assertEquals(firstQueryResponse.get("resultTable").get("rows").get(0).get(0).asInt(), firstQueryResult);
    assertEquals(firstQueryResponse.get("totalDocs").asLong(), numTotalDocs);
    assertEquals(firstQueryResponse.get("numDocsScanned").asInt(), NUM_SEGMENTS);

    // Test the second query
    JsonNode secondQueryResponse = postQuery(TEST_STAR_TREE_QUERY_2);
    int secondQueryResult = secondQueryResponse.get("resultTable").get("rows").get(0).get(0).asInt();
    assertEquals(secondQueryResponse.get("totalDocs").asLong(), numTotalDocs);
    // Initially 'numDocsScanned' should be the same as 'COUNT(*)' result
    assertEquals(secondQueryResponse.get("numDocsScanned").asInt(), secondQueryResult);

    // Update table config with a different star-tree index config and trigger reload
    indexingConfig.setStarTreeIndexConfigs(Collections.singletonList(STAR_TREE_INDEX_CONFIG_2));
    updateTableConfig(tableConfig);
    String reloadId = reloadOfflineTableAndValidateResponse(getTableName(), false);

    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode queryResponse = postQuery(TEST_STAR_TREE_QUERY_2);
        // Result should not change during reload
        assertEquals(queryResponse.get("resultTable").get("rows").get(0).get(0).asInt(), secondQueryResult);
        // Total docs should not change during reload
        assertEquals(queryResponse.get("totalDocs").asLong(), numTotalDocs);
        // With star-tree, 'numDocsScanned' should be the same as number of segments (1 per segment)
        return queryResponse.get("numDocsScanned").asInt() == NUM_SEGMENTS;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 600_000L, "Failed to change to second star-tree index");
    validateReloadJobSuccess(reloadJobId);

    // First query should not be able to use the star-tree
    firstQueryResponse = postQuery(TEST_STAR_TREE_QUERY_1);
    assertEquals(firstQueryResponse.get("numDocsScanned").asInt(), firstQueryResult);

    // Reload again should have no effect
    reloadOfflineTableAndValidateResponse(getTableName(), false);
    firstQueryResponse = postQuery(TEST_STAR_TREE_QUERY_1);
    assertEquals(firstQueryResponse.get("resultTable").get("rows").get(0).get(0).asInt(), firstQueryResult);
    assertEquals(firstQueryResponse.get("totalDocs").asLong(), numTotalDocs);
    assertEquals(firstQueryResponse.get("numDocsScanned").asInt(), firstQueryResult);
    secondQueryResponse = postQuery(TEST_STAR_TREE_QUERY_2);
    assertEquals(secondQueryResponse.get("resultTable").get("rows").get(0).get(0).asInt(), secondQueryResult);
    assertEquals(secondQueryResponse.get("totalDocs").asLong(), numTotalDocs);
    assertEquals(secondQueryResponse.get("numDocsScanned").asInt(), NUM_SEGMENTS);

    // Should be able to use the star-tree with an additional match-all predicate on another dimension
    secondQueryResponse = postQuery(TEST_STAR_TREE_QUERY_2 + " AND DaysSinceEpoch > 16070");
    assertEquals(secondQueryResponse.get("resultTable").get("rows").get(0).get(0).asInt(), secondQueryResult);
    assertEquals(secondQueryResponse.get("totalDocs").asLong(), numTotalDocs);
    assertEquals(secondQueryResponse.get("numDocsScanned").asInt(), NUM_SEGMENTS);

    // Enforce a sleep here since segment reload is async and there is another back-to-back reload below.
    // Otherwise, there is no way to tell whether the 1st reload on server side is finished,
    // which may hit the race condition that the 1st reload finishes after the 2nd reload is fully done.
    // 10 seconds are still better than hitting race condition which will time out after 10 minutes.
    Thread.sleep(10_000L);

    // Remove the star-tree index config and trigger reload
    indexingConfig.setStarTreeIndexConfigs(null);
    updateTableConfig(tableConfig);
    reloadJobId = reloadOfflineTableAndValidateResponse(getTableName(), false);

    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode queryResponse = postQuery(TEST_STAR_TREE_QUERY_2);
        // Result should not change during reload
        assertEquals(queryResponse.get("resultTable").get("rows").get(0).get(0).asInt(), secondQueryResult);
        // Total docs should not change during reload
        assertEquals(queryResponse.get("totalDocs").asLong(), numTotalDocs);
        // Without star-tree, 'numDocsScanned' should be the same as the 'COUNT(*)' result
        return queryResponse.get("numDocsScanned").asInt() == secondQueryResult;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 600_000L, "Failed to remove star-tree index");
    validateReloadJobSuccess(reloadJobId);
    assertEquals(getTableSize(getTableName()), tableSizeWithDefaultIndex);

    // First query should not be able to use the star-tree
    firstQueryResponse = postQuery(TEST_STAR_TREE_QUERY_1);
    assertEquals(firstQueryResponse.get("numDocsScanned").asInt(), firstQueryResult);

    // Reload again should have no effect
    reloadOfflineTableAndValidateResponse(getTableName(), false);
    firstQueryResponse = postQuery(TEST_STAR_TREE_QUERY_1);
    assertEquals(firstQueryResponse.get("resultTable").get("rows").get(0).get(0).asInt(), firstQueryResult);
    assertEquals(firstQueryResponse.get("totalDocs").asLong(), numTotalDocs);
    assertEquals(firstQueryResponse.get("numDocsScanned").asInt(), firstQueryResult);
    secondQueryResponse = postQuery(TEST_STAR_TREE_QUERY_2);
    assertEquals(secondQueryResponse.get("resultTable").get("rows").get(0).get(0).asInt(), secondQueryResult);
    assertEquals(secondQueryResponse.get("totalDocs").asLong(), numTotalDocs);
    assertEquals(secondQueryResponse.get("numDocsScanned").asInt(), secondQueryResult);
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
   *   <li>"NewAddedDerivedHoursSinceEpoch", DATE_TIME, INT, single-value, default (Integer.MIN_VALUE)</li>
   *   <li>"NewAddedDerivedTimestamp", DATE_TIME, TIMESTAMP, single-value, default (EPOCH)</li>
   *   <li>"NewAddedDerivedSVBooleanDimension", DIMENSION, BOOLEAN, single-value, default (false)</li>
   *   <li>"NewAddedDerivedMVStringDimension", DATE_TIME, STRING, multi-value</li>
   * </ul>
   */
  @Test(dependsOnMethods = "testAggregateMetadataAPI")
  public void testDefaultColumns()
      throws Exception {
    long numTotalDocs = getCountStarResult();

    reloadWithExtraColumns();
    JsonNode queryResponse = postQuery(SELECT_STAR_QUERY);
    assertEquals(queryResponse.get("totalDocs").asLong(), numTotalDocs);
    assertEquals(queryResponse.get("resultTable").get("dataSchema").get("columnNames").size(), 98);

    testNewAddedColumns();
    testExpressionOverride();

    reloadWithMissingColumns();
    queryResponse = postQuery(SELECT_STAR_QUERY);
    assertEquals(queryResponse.get("totalDocs").asLong(), numTotalDocs);
    assertEquals(queryResponse.get("resultTable").get("dataSchema").get("columnNames").size(), 75);

    reloadWithRegularColumns();
    queryResponse = postQuery(SELECT_STAR_QUERY);
    assertEquals(queryResponse.get("totalDocs").asLong(), numTotalDocs);
    assertEquals(queryResponse.get("resultTable").get("dataSchema").get("columnNames").size(), 79);

    _tableSizeAfterRemovingIndex = getTableSize(getTableName());
  }

  @Test
  public void testDisableGroovyQueryTableConfigOverride()
      throws Exception {
    String groovyQuery = "SELECT GROOVY('{\"returnType\":\"STRING\",\"isSingleValue\":true}', "
        + "'arg0 + arg1', FlightNum, Origin) FROM myTable";
    TableConfig tableConfig = getOfflineTableConfig();
    tableConfig.setQueryConfig(new QueryConfig(null, false, null, null));
    updateTableConfig(tableConfig);

    TestUtils.waitForCondition(aVoid -> {
      try {
        // Query should not throw exception
        postQuery(groovyQuery);
        return true;
      } catch (Exception e) {
        return false;
      }
    }, 60_000L, "Failed to accept Groovy query with table override");

    // Remove query config
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
  }

  private void reloadWithExtraColumns()
      throws Exception {
    long numTotalDocs = getCountStarResult();

    // Add columns to the schema first to pass the validation of the table config
    Schema schema = createSchema();
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
    addSchema(schema);

    TableConfig tableConfig = getOfflineTableConfig();
    List<TransformConfig> transformConfigs =
        Arrays.asList(new TransformConfig("NewAddedDerivedHoursSinceEpoch", "DaysSinceEpoch * 24"),
            new TransformConfig("NewAddedDerivedTimestamp", "DaysSinceEpoch * 24 * 3600 * 1000"),
            new TransformConfig("NewAddedDerivedSVBooleanDimension", "ActualElapsedTime > 0"),
            new TransformConfig("NewAddedDerivedMVStringDimension", "split(DestCityName, ', ')"));
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setTransformConfigs(transformConfigs);
    tableConfig.setIngestionConfig(ingestionConfig);
    updateTableConfig(tableConfig);

    // Trigger reload
    String reloadJobId = reloadOfflineTableAndValidateResponse(getTableName(), false);

    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode queryResponse = postQuery(TEST_EXTRA_COLUMNS_QUERY);
        if (!queryResponse.get("exceptions").isEmpty()) {
          // Schema is not refreshed on the broker side yet
          return false;
        }
        // Total docs should not change during reload
        assertEquals(queryResponse.get("totalDocs").asLong(), numTotalDocs);
        long count = queryResponse.get("resultTable").get("rows").get(0).get(0).asLong();
        return count == numTotalDocs;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 600_000L, "Failed to add default columns");
    validateReloadJobSuccess(reloadJobId);
  }

  private void reloadWithMissingColumns()
      throws Exception {
    long numTotalDocs = getCountStarResult();

    // Remove columns from the table config first to pass the validation of the table config
    TableConfig tableConfig = getOfflineTableConfig();
    tableConfig.setIngestionConfig(null);
    updateTableConfig(tableConfig);

    // Need to first delete then add the schema because removing columns is backward-incompatible change
    deleteSchema(getSchemaName());
    Schema schema = createSchema();
    schema.removeField("AirlineID");
    schema.removeField("ArrTime");
    schema.removeField("AirTime");
    schema.removeField("ArrDel15");
    addSchema(schema);

    // Trigger reload
    String reloadJobId = reloadOfflineTableAndValidateResponse(getTableName(), false);

    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode queryResponse = postQuery(SELECT_STAR_QUERY);
        // Total docs should not change during reload
        assertEquals(queryResponse.get("totalDocs").asLong(), numTotalDocs);
        JsonNode segmentsMetadata = JsonUtils.stringToJsonNode(
            sendGetRequest(_controllerRequestURLBuilder.forSegmentsMetadataFromServer(getTableName(), "*")));
        assertEquals(segmentsMetadata.size(), 12);
        for (JsonNode segmentMetadata : segmentsMetadata) {
          if (segmentMetadata.get("columns").size() != 75) {
            return false;
          }
        }
        return true;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 600_000L, "Failed to skip missing columns");
    validateReloadJobSuccess(reloadJobId);
  }

  private void reloadWithRegularColumns()
      throws Exception {
    long numTotalDocs = getCountStarResult();

    _schemaFileName = DEFAULT_SCHEMA_FILE_NAME;
    addSchema(createSchema());

    // Trigger reload
    String reloadJobId = reloadOfflineTableAndValidateResponse(getTableName(), false);

    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode queryResponse = postQuery(TEST_REGULAR_COLUMNS_QUERY);
        if (!queryResponse.get("exceptions").isEmpty()) {
          // Schema is not refreshed on the broker side yet
          return false;
        }
        // Total docs should not change during reload
        assertEquals(queryResponse.get("totalDocs").asLong(), numTotalDocs);
        long count = queryResponse.get("resultTable").get("rows").get(0).get(0).asLong();
        return count == numTotalDocs;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 600_000L, "Failed to reload regular columns");
    validateReloadJobSuccess(reloadJobId);
  }

  private void testNewAddedColumns()
      throws Exception {
    long numTotalDocs = getCountStarResult();
    double numTotalDocsInDouble = (double) numTotalDocs;

    // Test queries with each new added columns
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
    pinotQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedBytesMetric = ''";
    testQuery(pinotQuery, h2Query);
    pinotQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedMVIntDimension < 0";
    testQuery(pinotQuery, h2Query);
    pinotQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedMVLongDimension < 0";
    testQuery(pinotQuery, h2Query);
    pinotQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedMVFloatDimension < 0.0";
    testQuery(pinotQuery, h2Query);
    pinotQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedMVDoubleDimension < 0.0";
    testQuery(pinotQuery, h2Query);
    pinotQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedMVBooleanDimension = false";
    testQuery(pinotQuery, h2Query);
    pinotQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedMVTimestampDimension = 0";
    testQuery(pinotQuery, h2Query);
    pinotQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedMVStringDimension = 'null'";
    testQuery(pinotQuery, h2Query);
    pinotQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedSVJSONDimension = 'null'";
    testQuery(pinotQuery, h2Query);
    pinotQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedSVBytesDimension = ''";
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
    pinotQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedDerivedMVStringDimension = 'CA'";
    h2Query = "SELECT COUNT(*) FROM mytable WHERE DestState = 'CA'";
    testQuery(pinotQuery, h2Query);

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

    // Test other query forms with new added columns
    pinotQuery =
        "SELECT NewAddedMVStringDimension, SUM(NewAddedFloatMetric) FROM mytable GROUP BY NewAddedMVStringDimension";
    JsonNode response = postQuery(pinotQuery);
    JsonNode rows = response.get("resultTable").get("rows");
    assertEquals(rows.size(), 1);
    JsonNode row = rows.get(0);
    assertEquals(row.size(), 2);
    assertEquals(row.get(0).asText(), "null");
    assertEquals(row.get(1).asDouble(), 0.0);
    pinotQuery =
        "SELECT NewAddedSVBytesDimension, SUM(NewAddedBigDecimalMetric) FROM mytable GROUP BY NewAddedSVBytesDimension";
    response = postQuery(pinotQuery);
    rows = response.get("resultTable").get("rows");
    assertEquals(rows.size(), 1);
    row = rows.get(0);
    assertEquals(row.size(), 2);
    assertEquals(row.get(0).asText(), "");
    assertEquals(row.get(1).asDouble(), 0.0);
    pinotQuery = "SELECT NewAddedMVLongDimension, SUM(NewAddedIntMetric) FROM mytable GROUP BY NewAddedMVLongDimension";
    response = postQuery(pinotQuery);
    rows = response.get("resultTable").get("rows");
    assertEquals(rows.size(), 1);
    row = rows.get(0);
    assertEquals(row.size(), 2);
    assertEquals(row.get(0).asLong(), Long.MIN_VALUE);
    assertEquals(row.get(1).asDouble(), numTotalDocsInDouble);
    String newAddedDimensions =
        "NewAddedMVIntDimension, NewAddedMVLongDimension, NewAddedMVFloatDimension, NewAddedMVDoubleDimension, "
            + "NewAddedMVBooleanDimension, NewAddedMVTimestampDimension, NewAddedMVStringDimension, "
            + "NewAddedSVJSONDimension, NewAddedSVBytesDimension";
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
    TableConfig tableConfig = getOfflineTableConfig();
    tableConfig.setQueryConfig(new QueryConfig(null, null, null,
        Collections.singletonMap("DaysSinceEpoch * 24", "NewAddedDerivedHoursSinceEpoch")));
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
    tableConfig.setQueryConfig(null);
    updateTableConfig(tableConfig);

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

  @Test
  @Override
  public void testBrokerResponseMetadata()
      throws Exception {
    super.testBrokerResponseMetadata();
  }

  @Test
  public void testInBuiltVirtualColumns()
      throws Exception {
    String query = "SELECT $docId, $HOSTNAME, $segmentname FROM mytable";
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
      assertTrue(row.get(2).asText().startsWith(expectedSegmentNamePrefix));
    }
  }

  @Test
  public void testGroupByUDF()
      throws Exception {
    String query = "SELECT timeConvert(DaysSinceEpoch,'DAYS','SECONDS'), COUNT(*) FROM mytable "
        + "GROUP BY timeConvert(DaysSinceEpoch,'DAYS','SECONDS') ORDER BY COUNT(*) DESC";
    JsonNode response = postQuery(query);
    JsonNode resultTable = response.get("resultTable");
    JsonNode dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnNames").toString(),
        "[\"timeconvert(DaysSinceEpoch,'DAYS','SECONDS')\",\"count(*)\"]");
    assertEquals(dataSchema.get("columnDataTypes").toString(), "[\"LONG\",\"LONG\"]");
    JsonNode rows = resultTable.get("rows");
    assertEquals(rows.size(), 10);
    JsonNode row = rows.get(0);
    assertEquals(row.size(), 2);
    assertEquals(row.get(0).asLong(), 16138 * 24 * 3600);
    assertEquals(row.get(1).asLong(), 605);

    query = "SELECT dateTimeConvert(DaysSinceEpoch,'1:DAYS:EPOCH','1:HOURS:EPOCH','1:HOURS'), COUNT(*) FROM mytable "
        + "GROUP BY dateTimeConvert(DaysSinceEpoch,'1:DAYS:EPOCH','1:HOURS:EPOCH','1:HOURS') ORDER BY COUNT(*) DESC";
    response = postQuery(query);
    resultTable = response.get("resultTable");
    dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnNames").toString(),
        "[\"datetimeconvert(DaysSinceEpoch,'1:DAYS:EPOCH','1:HOURS:EPOCH','1:HOURS')\",\"count(*)\"]");
    assertEquals(dataSchema.get("columnDataTypes").toString(), "[\"LONG\",\"LONG\"]");
    rows = resultTable.get("rows");
    assertEquals(rows.size(), 10);
    row = rows.get(0);
    assertEquals(row.size(), 2);
    assertEquals(row.get(0).asLong(), 16138 * 24);
    assertEquals(row.get(1).asLong(), 605);

    query = "SELECT add(DaysSinceEpoch,DaysSinceEpoch,15), COUNT(*) FROM mytable "
        + "GROUP BY add(DaysSinceEpoch,DaysSinceEpoch,15) ORDER BY COUNT(*) DESC";
    response = postQuery(query);
    resultTable = response.get("resultTable");
    dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnNames").toString(),
        "[\"add(DaysSinceEpoch,DaysSinceEpoch,'15')\",\"count(*)\"]");
    assertEquals(dataSchema.get("columnDataTypes").toString(), "[\"DOUBLE\",\"LONG\"]");
    rows = resultTable.get("rows");
    assertEquals(rows.size(), 10);
    row = rows.get(0);
    assertEquals(row.size(), 2);
    assertEquals(row.get(0).asDouble(), 16138.0 + 16138 + 15);
    assertEquals(row.get(1).asLong(), 605);

    query = "SELECT sub(DaysSinceEpoch,25), COUNT(*) FROM mytable "
        + "GROUP BY sub(DaysSinceEpoch,25) ORDER BY COUNT(*) DESC";
    response = postQuery(query);
    resultTable = response.get("resultTable");
    dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnNames").toString(), "[\"sub(DaysSinceEpoch,'25')\",\"count(*)\"]");
    assertEquals(dataSchema.get("columnDataTypes").toString(), "[\"DOUBLE\",\"LONG\"]");
    rows = resultTable.get("rows");
    assertEquals(rows.size(), 10);
    row = rows.get(0);
    assertEquals(row.size(), 2);
    assertEquals(row.get(0).asDouble(), 16138.0 - 25);
    assertEquals(row.get(1).asLong(), 605);

    query = "SELECT mult(DaysSinceEpoch,24,3600), COUNT(*) FROM mytable "
        + "GROUP BY mult(DaysSinceEpoch,24,3600) ORDER BY COUNT(*) DESC";
    response = postQuery(query);
    resultTable = response.get("resultTable");
    dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnNames").toString(), "[\"mult(DaysSinceEpoch,'24','3600')\",\"count(*)\"]");
    assertEquals(dataSchema.get("columnDataTypes").toString(), "[\"DOUBLE\",\"LONG\"]");
    rows = resultTable.get("rows");
    assertEquals(rows.size(), 10);
    row = rows.get(0);
    assertEquals(row.size(), 2);
    assertEquals(row.get(0).asDouble(), 16138.0 * 24 * 3600);
    assertEquals(row.get(1).asLong(), 605);

    query = "SELECT div(DaysSinceEpoch,2), COUNT(*) FROM mytable "
        + "GROUP BY div(DaysSinceEpoch,2) ORDER BY COUNT(*) DESC";
    response = postQuery(query);
    resultTable = response.get("resultTable");
    dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnNames").toString(), "[\"div(DaysSinceEpoch,'2')\",\"count(*)\"]");
    assertEquals(dataSchema.get("columnDataTypes").toString(), "[\"DOUBLE\",\"LONG\"]");
    rows = resultTable.get("rows");
    assertEquals(rows.size(), 10);
    row = rows.get(0);
    assertEquals(row.size(), 2);
    assertEquals(row.get(0).asDouble(), 16138.0 / 2);
    assertEquals(row.get(1).asLong(), 605);

    query = "SELECT arrayLength(DivAirports), COUNT(*) FROM mytable "
        + "GROUP BY arrayLength(DivAirports) ORDER BY COUNT(*) DESC";
    response = postQuery(query);
    resultTable = response.get("resultTable");
    dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnNames").toString(), "[\"arraylength(DivAirports)\",\"count(*)\"]");
    assertEquals(dataSchema.get("columnDataTypes").toString(), "[\"INT\",\"LONG\"]");
    rows = resultTable.get("rows");
    assertEquals(rows.size(), 1);
    row = rows.get(0);
    assertEquals(row.size(), 2);
    assertEquals(row.get(0).asInt(), 5);
    assertEquals(row.get(1).asLong(), 115545);

    query = "SELECT arrayLength(valueIn(DivAirports,'DFW','ORD')), COUNT(*) FROM mytable GROUP BY "
        + "arrayLength(valueIn(DivAirports,'DFW','ORD')) ORDER BY COUNT(*) DESC";
    response = postQuery(query);
    resultTable = response.get("resultTable");
    dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnNames").toString(),
        "[\"arraylength(valuein(DivAirports,'DFW','ORD'))\",\"count(*)\"]");
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

    query = "SELECT valueIn(DivAirports,'DFW','ORD'), COUNT(*) FROM mytable "
        + "GROUP BY valueIn(DivAirports,'DFW','ORD') ORDER BY COUNT(*) DESC";
    response = postQuery(query);
    resultTable = response.get("resultTable");
    dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnNames").toString(), "[\"valuein(DivAirports,'DFW','ORD')\",\"count(*)\"]");
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
  }

  @Test
  public void testAggregationUDF()
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
  public void testSelectionUDF()
      throws Exception {
    String query = "SELECT DaysSinceEpoch, timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM mytable";
    JsonNode response = postQuery(query);
    JsonNode resultTable = response.get("resultTable");
    JsonNode dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnNames").toString(),
        "[\"DaysSinceEpoch\",\"timeconvert(DaysSinceEpoch,'DAYS','SECONDS')\"]");
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
    assertEquals(dataSchema.get("columnNames").toString(),
        "[\"DaysSinceEpoch\",\"timeconvert(DaysSinceEpoch,'DAYS','SECONDS')\"]");
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
    assertEquals(dataSchema.get("columnNames").toString(),
        "[\"DaysSinceEpoch\",\"timeconvert(DaysSinceEpoch,'DAYS','SECONDS')\"]");
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

  @Test
  public void testFilterUDF()
      throws Exception {
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

  @Test
  public void testCaseStatementInSelection()
      throws Exception {
    List<String> origins =
        Arrays.asList("ATL", "ORD", "DFW", "DEN", "LAX", "IAH", "SFO", "PHX", "LAS", "EWR", "MCO", "BOS", "SLC", "SEA",
            "MSP", "CLT", "LGA", "DTW", "JFK", "BWI");
    StringBuilder caseStatementBuilder = new StringBuilder("CASE ");
    for (int i = 0; i < origins.size(); i++) {
      // WHEN origin = 'ATL' THEN 1
      // WHEN origin = 'ORD' THEN 2
      // WHEN origin = 'DFW' THEN 3
      // ....
      caseStatementBuilder.append(String.format("WHEN origin = '%s' THEN %d ", origins.get(i), i + 1));
    }
    caseStatementBuilder.append("ELSE 0 END");
    String sqlQuery = "SELECT origin, " + caseStatementBuilder + " AS origin_code FROM mytable LIMIT 1000";
    JsonNode response = postQuery(sqlQuery, _brokerBaseApiUrl);
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

  @Test
  public void testCaseStatementInSelectionWithTransformFunctionInThen()
      throws Exception {
    String sqlQuery =
        "SELECT ArrDelay, CASE WHEN ArrDelay > 0 THEN ArrDelay WHEN ArrDelay < 0 THEN ArrDelay * -1 ELSE 0 END AS "
            + "ArrTimeDiff FROM mytable LIMIT 1000";
    JsonNode response = postQuery(sqlQuery, _brokerBaseApiUrl);
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

  @Test
  public void testCaseStatementWithLogicalTransformFunction()
      throws Exception {
    String sqlQuery = "SELECT ArrDelay" + ", CASE WHEN ArrDelay > 50 OR ArrDelay < 10 THEN 10 ELSE 0 END"
        + ", CASE WHEN ArrDelay < 50 AND ArrDelay >= 10 THEN 10 ELSE 0 END" + " FROM mytable LIMIT 1000";
    JsonNode response = postQuery(sqlQuery, _brokerBaseApiUrl);
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

  @Test
  public void testCaseStatementWithInAggregation()
      throws Exception {
    testCountVsCaseQuery("origin = 'ATL'");
    testCountVsCaseQuery("origin <> 'ATL'");

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
    JsonNode response = postQuery(sqlQuery, _brokerBaseApiUrl);
    long countValue = response.get("resultTable").get("rows").get(0).get(0).asLong();
    sqlQuery = String.format("SELECT SUM(CASE WHEN %s THEN 1 ELSE 0 END) as sum1 FROM mytable", predicate);
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    long caseSum = response.get("resultTable").get("rows").get(0).get(0).asLong();
    assertEquals(caseSum, countValue);
  }

  @Test
  public void testFilterWithInvertedIndexUDF()
      throws Exception {
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
  public void testQueryWithRepeatedColumns()
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

  @Test
  public void testQueryWithOrderby()
      throws Exception {
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

  @Test
  public void testQueryWithAlias()
      throws Exception {
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
  public void testDistinctQuery()
      throws Exception {
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

  @Test
  public void testNonAggregationGroupByQuery()
      throws Exception {
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
    h2Query = "SELECT ArrTime-DepTime FROM mytable GROUP BY ArrTime-DepTime";
    testQuery(pinotQuery, h2Query);

    pinotQuery = "SELECT ArrTime+DepTime AS A FROM mytable GROUP BY A LIMIT 1000000";
    h2Query = "SELECT ArrTime+DepTime AS A FROM mytable GROUP BY A";
    testQuery(pinotQuery, h2Query);
  }

  @Test
  public void testCaseInsensitivity()
      throws Exception {
    int daysSinceEpoch = 16138;
    int hoursSinceEpoch = 16138 * 24;
    int secondsSinceEpoch = 16138 * 24 * 60 * 60;
    List<String> baseQueries = Arrays.asList("SELECT * FROM mytable",
        "SELECT DaysSinceEpoch, timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM mytable",
        "SELECT DaysSinceEpoch, timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM mytable order by DaysSinceEpoch "
            + "limit 10000",
        "SELECT DaysSinceEpoch, timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM mytable order by timeConvert"
            + "(DaysSinceEpoch,'DAYS','SECONDS') DESC limit 10000",
        "SELECT count(*) FROM mytable WHERE DaysSinceEpoch = " + daysSinceEpoch,
        "SELECT count(*) FROM mytable WHERE timeConvert(DaysSinceEpoch,'DAYS','HOURS') = " + hoursSinceEpoch,
        "SELECT count(*) FROM mytable WHERE timeConvert(DaysSinceEpoch,'DAYS','SECONDS') = " + secondsSinceEpoch,
        "SELECT MAX(timeConvert(DaysSinceEpoch,'DAYS','SECONDS')) FROM mytable",
        "SELECT COUNT(*) FROM mytable GROUP BY dateTimeConvert(DaysSinceEpoch,'1:DAYS:EPOCH','1:HOURS:EPOCH',"
            + "'1:HOURS')");
    List<String> queries = new ArrayList<>();
    baseQueries.forEach(q -> queries.add(q.replace("mytable", "MYTABLE").replace("DaysSinceEpoch", "DAYSSinceEpOch")));
    baseQueries.forEach(
        q -> queries.add(q.replace("mytable", "MYDB.MYTABLE").replace("DaysSinceEpoch", "DAYSSinceEpOch")));

    for (String query : queries) {
      JsonNode response = postQuery(query);
      assertTrue(response.get("numSegmentsProcessed").asLong() >= 1L, "Query: " + query + " failed");
    }
  }

  @Test
  public void testColumnNameContainsTableName()
      throws Exception {
    int daysSinceEpoch = 16138;
    int hoursSinceEpoch = 16138 * 24;
    int secondsSinceEpoch = 16138 * 24 * 60 * 60;
    List<String> baseQueries = Arrays.asList("SELECT * FROM mytable",
        "SELECT DaysSinceEpoch, timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM mytable",
        "SELECT DaysSinceEpoch, timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM mytable order by DaysSinceEpoch "
            + "limit 10000",
        "SELECT DaysSinceEpoch, timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM mytable order by timeConvert"
            + "(DaysSinceEpoch,'DAYS','SECONDS') DESC limit 10000",
        "SELECT count(*) FROM mytable WHERE DaysSinceEpoch = " + daysSinceEpoch,
        "SELECT count(*) FROM mytable WHERE timeConvert(DaysSinceEpoch,'DAYS','HOURS') = " + hoursSinceEpoch,
        "SELECT count(*) FROM mytable WHERE timeConvert(DaysSinceEpoch,'DAYS','SECONDS') = " + secondsSinceEpoch,
        "SELECT MAX(timeConvert(DaysSinceEpoch,'DAYS','SECONDS')) FROM mytable",
        "SELECT COUNT(*) FROM mytable GROUP BY dateTimeConvert(DaysSinceEpoch,'1:DAYS:EPOCH','1:HOURS:EPOCH',"
            + "'1:HOURS')");
    List<String> queries = new ArrayList<>();
    baseQueries.forEach(q -> queries.add(q.replace("DaysSinceEpoch", "mytable.DAYSSinceEpOch")));
    baseQueries.forEach(q -> queries.add(q.replace("DaysSinceEpoch", "mytable.DAYSSinceEpOch")));

    for (String query : queries) {
      JsonNode response = postQuery(query);
      assertTrue(response.get("numSegmentsProcessed").asLong() >= 1L, "Query: " + query + " failed");
    }
  }

  @Test
  public void testCaseInsensitivityWithColumnNameContainsTableName()
      throws Exception {
    int daysSinceEpoch = 16138;
    int hoursSinceEpoch = 16138 * 24;
    int secondsSinceEpoch = 16138 * 24 * 60 * 60;
    List<String> baseQueries = Arrays.asList("SELECT * FROM mytable",
        "SELECT DaysSinceEpoch, timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM mytable",
        "SELECT DaysSinceEpoch, timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM mytable order by DaysSinceEpoch "
            + "limit 10000",
        "SELECT DaysSinceEpoch, timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM mytable order by timeConvert"
            + "(DaysSinceEpoch,'DAYS','SECONDS') DESC limit 10000",
        "SELECT count(*) FROM mytable WHERE DaysSinceEpoch = " + daysSinceEpoch,
        "SELECT count(*) FROM mytable WHERE timeConvert(DaysSinceEpoch,'DAYS','HOURS') = " + hoursSinceEpoch,
        "SELECT count(*) FROM mytable WHERE timeConvert(DaysSinceEpoch,'DAYS','SECONDS') = " + secondsSinceEpoch,
        "SELECT MAX(timeConvert(DaysSinceEpoch,'DAYS','SECONDS')) FROM mytable",
        "SELECT COUNT(*) FROM mytable GROUP BY dateTimeConvert(DaysSinceEpoch,'1:DAYS:EPOCH','1:HOURS:EPOCH',"
            + "'1:HOURS')");
    List<String> queries = new ArrayList<>();
    baseQueries.forEach(
        q -> queries.add(q.replace("mytable", "MYTABLE").replace("DaysSinceEpoch", "MYTABLE.DAYSSinceEpOch")));
    // something like "SELECT MYDB.MYTABLE.DAYSSinceEpOch from MYDB.MYTABLE where MYDB.MYTABLE.DAYSSinceEpOch = 16138"
    baseQueries.forEach(
        q -> queries.add(q.replace("mytable", "MYDB.MYTABLE").replace("DaysSinceEpoch", "MYTABLE.DAYSSinceEpOch")));

    for (String query : queries) {
      JsonNode response = postQuery(query);
      assertTrue(response.get("numSegmentsProcessed").asLong() >= 1L, "Query: " + query + " failed");
    }
  }

  @Test
  public void testQuerySourceWithDatabaseName()
      throws Exception {
    // by default 10 rows will be returned, so use high limit
    String pinotQuery = "SELECT DISTINCT(Carrier) FROM mytable LIMIT 1000000";
    String h2Query = "SELECT DISTINCT Carrier FROM mytable";
    testQuery(pinotQuery, h2Query);
    pinotQuery = "SELECT DISTINCT Carrier FROM db.mytable LIMIT 1000000";
    testQuery(pinotQuery, h2Query);
  }

  @Test
  public void testDistinctCountHll()
      throws Exception {
    String query;

    // The Accurate value is 6538.
    query = "SELECT distinctCount(FlightNum) FROM mytable ";
    assertEquals(postQuery(query).get("resultTable").get("rows").get(0).get(0).asLong(), 6538);
    assertEquals(postQuery(query, _brokerBaseApiUrl).get("resultTable").get("rows").get(0).get(0).asLong(), 6538);

    // Expected distinctCountHll with different log2m value from 2 to 19. The Accurate value is 6538.
    long[] expectedResults = new long[]{
        3504, 6347, 8877, 9729, 9046, 7672, 7538, 6993, 6649, 6651, 6553, 6525, 6459, 6523, 6532, 6544, 6538, 6539
    };

    for (int i = 2; i < 20; i++) {
      query = String.format("SELECT distinctCountHLL(FlightNum, %d) FROM mytable ", i);
      assertEquals(postQuery(query).get("resultTable").get("rows").get(0).get(0).asLong(), expectedResults[i - 2]);
      assertEquals(postQuery(query, _brokerBaseApiUrl).get("resultTable").get("rows").get(0).get(0).asLong(),
          expectedResults[i - 2]);
    }

    // Default HLL is set as log2m=12
    query = "SELECT distinctCountHLL(FlightNum) FROM mytable ";
    assertEquals(postQuery(query).get("resultTable").get("rows").get(0).get(0).asLong(), expectedResults[10]);
    assertEquals(postQuery(query, _brokerBaseApiUrl).get("resultTable").get("rows").get(0).get(0).asLong(),
        expectedResults[10]);
  }

  @Test
  public void testAggregationFunctionsWithUnderscore()
      throws Exception {
    String query;

    // The Accurate value is 6538.
    query = "SELECT distinct_count(FlightNum) FROM mytable";
    assertEquals(postQuery(query).get("resultTable").get("rows").get(0).get(0).asInt(), 6538);

    // The Accurate value is 115545.
    query = "SELECT c_o_u_n_t(FlightNum) FROM mytable";
    assertEquals(postQuery(query).get("resultTable").get("rows").get(0).get(0).asInt(), 115545);
  }

  @Test
  public void testExplainPlanQuery()
      throws Exception {
    String query1 = "EXPLAIN PLAN FOR SELECT count(*) AS count, Carrier AS name FROM mytable GROUP BY name ORDER BY 1";
    String response1 = postQuery(query1, _brokerBaseApiUrl).get("resultTable").toString();

    // Replace string "docs:[0-9]+" with "docs:*" so that test doesn't fail when number of documents change. This is
    // needed because both OfflineClusterIntegrationTest and MultiNodesOfflineClusterIntegrationTest run this test
    // case with different number of documents in the segment.
    response1 = response1.replaceAll("docs:[0-9]+", "docs:*");

    assertEquals(response1, "{\"dataSchema\":{\"columnNames\":[\"Operator\",\"Operator_Id\",\"Parent_Id\"],"
        + "\"columnDataTypes\":[\"STRING\",\"INT\",\"INT\"]},"
        + "\"rows\":[[\"BROKER_REDUCE(sort:[count(*) ASC],limit:10)\",1,0],[\"COMBINE_GROUP_BY\",2,1],"
        + "[\"PLAN_START(numSegmentsForThisPlan:1)\",-1,-1],"
        + "[\"GROUP_BY(groupKeys:Carrier, aggregations:count(*))\",3,2],"
        + "[\"TRANSFORM_PASSTHROUGH(Carrier)\",4,3],[\"PROJECT(Carrier)\",5,4],[\"DOC_ID_SET\",6,5],"
        + "[\"FILTER_MATCH_ENTIRE_SEGMENT(docs:*)\",7,6]]}");

    // In the query below, FlightNum column has an inverted index and there is no data satisfying the predicate
    // "FlightNum < 0". Hence, all segments are pruned out before query execution on the server side.
    String query2 = "EXPLAIN PLAN FOR SELECT * FROM mytable WHERE FlightNum < 0";
    String response2 = postQuery(query2, _brokerBaseApiUrl).get("resultTable").toString();

    assertEquals(response2, "{\"dataSchema\":{\"columnNames\":[\"Operator\",\"Operator_Id\",\"Parent_Id\"],"
        + "\"columnDataTypes\":[\"STRING\",\"INT\",\"INT\"]},\"rows\":[[\"BROKER_REDUCE(limit:10)\",1,0],"
        + "[\"PLAN_START(numSegmentsForThisPlan:12)\",-1,-1],[\"ALL_SEGMENTS_PRUNED_ON_SERVER\",2,1]]}");
  }

  /** Test to make sure we are properly handling string comparisons in predicates. */
  @Test
  public void testStringComparisonInFilter()
      throws Exception {
    // compare two string columns.
    String query1 = "SELECT count(*) FROM mytable WHERE OriginState = DestState";
    String response1 = postQuery(query1, _brokerBaseApiUrl).get("resultTable").toString();
    assertEquals(response1,
        "{\"dataSchema\":{\"columnNames\":[\"count(*)\"],\"columnDataTypes\":[\"LONG\"]}," + "\"rows\":[[14011]]}");

    // compare string function with string column.
    String query2 = "SELECT count(*) FROM mytable WHERE trim(OriginState) = DestState";
    String response2 = postQuery(query2, _brokerBaseApiUrl).get("resultTable").toString();
    assertEquals(response2,
        "{\"dataSchema\":{\"columnNames\":[\"count(*)\"],\"columnDataTypes\":[\"LONG\"]}," + "\"rows\":[[14011]]}");

    // compare string function with string function.
    String query3 = "SELECT count(*) FROM mytable WHERE substr(OriginState, 0, 1) = substr(DestState, 0, 1)";
    String response3 = postQuery(query3, _brokerBaseApiUrl).get("resultTable").toString();
    assertEquals(response3,
        "{\"dataSchema\":{\"columnNames\":[\"count(*)\"],\"columnDataTypes\":[\"LONG\"]}," + "\"rows\":[[19755]]}");
  }

  /**
   * Test queries that can be solved with {@link NonScanBasedAggregationOperator}.
   */
  @Test
  public void testNonScanAggregationQueries()
      throws Exception {
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
  public void testAggregateMetadataAPI()
      throws IOException {
    JsonNode oneSVColumnResponse = JsonUtils.stringToJsonNode(
        sendGetRequest(_controllerBaseApiUrl + "/tables/mytable/metadata?columns=DestCityMarketID"));
    // DestCityMarketID is a SV column
    validateMetadataResponse(oneSVColumnResponse, 1, 0);

    JsonNode oneMVColumnResponse = JsonUtils.stringToJsonNode(
        sendGetRequest(_controllerBaseApiUrl + "/tables/mytable/metadata?columns=DivLongestGTimes"));
    // DivLongestGTimes is a MV column
    validateMetadataResponse(oneMVColumnResponse, 1, 1);

    JsonNode threeSVColumnsResponse = JsonUtils.stringToJsonNode(sendGetRequest(_controllerBaseApiUrl
        + "/tables/mytable/metadata?columns=DivActualElapsedTime&columns=CRSElapsedTime&columns=OriginStateName"));
    validateMetadataResponse(threeSVColumnsResponse, 3, 0);

    JsonNode threeSVColumnsWholeEncodedResponse = JsonUtils.stringToJsonNode(sendGetRequest(
        _controllerBaseApiUrl + "/tables/mytable/metadata?columns="
            + "DivActualElapsedTime%26columns%3DCRSElapsedTime%26columns%3DOriginStateName"));
    validateMetadataResponse(threeSVColumnsWholeEncodedResponse, 3, 0);

    JsonNode threeMVColumnsResponse = JsonUtils.stringToJsonNode(sendGetRequest(_controllerBaseApiUrl
        + "/tables/mytable/metadata?columns=DivLongestGTimes&columns=DivWheelsOns&columns=DivAirports"));
    validateMetadataResponse(threeMVColumnsResponse, 3, 3);

    JsonNode threeMVColumnsWholeEncodedResponse = JsonUtils.stringToJsonNode(sendGetRequest(
        _controllerBaseApiUrl + "/tables/mytable/metadata?columns="
            + "DivLongestGTimes%26columns%3DDivWheelsOns%26columns%3DDivAirports"));
    validateMetadataResponse(threeMVColumnsWholeEncodedResponse, 3, 3);

    JsonNode zeroColumnResponse =
        JsonUtils.stringToJsonNode(sendGetRequest(_controllerBaseApiUrl + "/tables/mytable/metadata"));
    validateMetadataResponse(zeroColumnResponse, 0, 0);

    JsonNode starColumnResponse =
        JsonUtils.stringToJsonNode(sendGetRequest(_controllerBaseApiUrl + "/tables/mytable/metadata?columns=*"));
    validateMetadataResponse(starColumnResponse, 82, 9);

    JsonNode starEncodedColumnResponse =
        JsonUtils.stringToJsonNode(sendGetRequest(_controllerBaseApiUrl + "/tables/mytable/metadata?columns=%2A"));
    validateMetadataResponse(starEncodedColumnResponse, 82, 9);

    JsonNode starWithExtraColumnResponse = JsonUtils.stringToJsonNode(sendGetRequest(
        _controllerBaseApiUrl + "/tables/mytable/metadata?columns="
            + "CRSElapsedTime&columns=*&columns=OriginStateName"));
    validateMetadataResponse(starWithExtraColumnResponse, 82, 9);

    JsonNode starWithExtraEncodedColumnResponse = JsonUtils.stringToJsonNode(sendGetRequest(
        _controllerBaseApiUrl + "/tables/mytable/metadata?columns="
            + "CRSElapsedTime&columns=%2A&columns=OriginStateName"));
    validateMetadataResponse(starWithExtraEncodedColumnResponse, 82, 9);

    JsonNode starWithExtraColumnWholeEncodedResponse = JsonUtils.stringToJsonNode(sendGetRequest(
        _controllerBaseApiUrl + "/tables/mytable/metadata?columns="
            + "CRSElapsedTime%26columns%3D%2A%26columns%3DOriginStateName"));
    validateMetadataResponse(starWithExtraColumnWholeEncodedResponse, 82, 9);
  }

  private void validateMetadataResponse(JsonNode response, int numTotalColumn, int numMVColumn) {
    assertEquals(response.get(DISK_SIZE_IN_BYTES_KEY).asInt(), DISK_SIZE_IN_BYTES);
    assertEquals(response.get(NUM_SEGMENTS_KEY).asInt(), NUM_SEGMENTS);
    assertEquals(response.get(NUM_ROWS_KEY).asInt(), NUM_ROWS);
    assertEquals(response.get(COLUMN_LENGTH_MAP_KEY).size(), numTotalColumn);
    assertEquals(response.get(COLUMN_CARDINALITY_MAP_KEY).size(), numTotalColumn);
    assertEquals(response.get(MAX_NUM_MULTI_VALUES_MAP_KEY).size(), numMVColumn);
  }

  @Test
  public void testReset()
      throws Exception {
    super.testReset(TableType.OFFLINE);
  }

  @Test
  public void testJDBCClient()
      throws Exception {
    String query = "SELECT count(*) FROM " + getTableName();
    java.sql.Connection connection = getJDBCConnectionFromController(DEFAULT_CONTROLLER_PORT);
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(query);
    resultSet.first();
    Assert.assertTrue(resultSet.getLong(1) > 0);

    connection = getJDBCConnectionFromBrokers(RANDOM.nextInt(), DEFAULT_BROKER_PORT);
    statement = connection.createStatement();
    resultSet = statement.executeQuery(query);
    resultSet.first();
    Assert.assertTrue(resultSet.getLong(1) > 0);
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

  private String reloadOfflineTableAndValidateResponse(String tableName, boolean forceDownload)
      throws IOException {
    String jobId = null;
    String response =
        sendPostRequest(_controllerRequestURLBuilder.forTableReload(tableName, TableType.OFFLINE, forceDownload), null);
    String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    JsonNode tableLevelDetails =
        JsonUtils.stringToJsonNode(StringEscapeUtils.unescapeJava(response.split(": ")[1])).get(tableNameWithType);
    String isZKWriteSuccess = tableLevelDetails.get("reloadJobMetaZKStorageStatus").asText();
    assertEquals("SUCCESS", isZKWriteSuccess);
    jobId = tableLevelDetails.get("reloadJobId").asText();
    String jobStatusResponse = sendGetRequest(_controllerRequestURLBuilder.forControllerJobStatus(jobId));
    JsonNode jobStatus = JsonUtils.stringToJsonNode(jobStatusResponse);

    // Validate all fields are present
    assertEquals(jobStatus.get("metadata").get("jobId").asText(), jobId);
    assertEquals(jobStatus.get("metadata").get("jobType").asText(), "RELOAD_ALL_SEGMENTS");
    assertEquals(jobStatus.get("metadata").get("tableName").asText(), tableNameWithType);
    return jobId;
  }

  private boolean validateReloadJobSuccess(String reloadJobId)
      throws IOException {
    String jobStatusResponse = sendGetRequest(_controllerRequestURLBuilder.forControllerJobStatus(reloadJobId));
    JsonNode jobStatus = JsonUtils.stringToJsonNode(jobStatusResponse);

    assertEquals(jobStatus.get("metadata").get("jobId").asText(), reloadJobId);
    assertEquals(jobStatus.get("metadata").get("jobType").asText(), "RELOAD_ALL_SEGMENTS");
    return jobStatus.get("totalSegmentCount").asInt() == jobStatus.get("successCount").asInt();
  }

  @Test
  public void testBooleanLiteralsFunc()
      throws Exception {
    // Test boolean equal true case.
    String sqlQuery = "SELECT (true = true) = true FROM mytable where true = true";
    JsonNode response = postQuery(sqlQuery, _brokerBaseApiUrl);
    JsonNode rows = response.get("resultTable").get("rows");
    assertTrue(response.get("exceptions").isEmpty());
    assertEquals(rows.size(), 1);
    assertTrue(rows.get(0).get(0).asBoolean());
    // Test boolean equal false case.
    sqlQuery = "SELECT (true = true) = false FROM mytable";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    rows = response.get("resultTable").get("rows");
    assertTrue(response.get("exceptions").isEmpty());
    assertEquals(rows.size(), 1);
    assertFalse(rows.get(0).get(0).asBoolean());
    // Test boolean not equal true case.
    sqlQuery = "SELECT true != false FROM mytable";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    rows = response.get("resultTable").get("rows");
    assertTrue(response.get("exceptions").isEmpty());
    assertEquals(rows.size(), 1);
    assertTrue(rows.get(0).get(0).asBoolean());
    // Test boolean not equal false case.
    sqlQuery = "SELECT true != true FROM mytable";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    rows = response.get("resultTable").get("rows");
    assertTrue(response.get("exceptions").isEmpty());
    assertEquals(rows.size(), 1);
    assertFalse(rows.get(0).get(0).asBoolean());
  }
}
