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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.common.utils.DataTable.MetadataKey;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.common.utils.grpc.GrpcQueryClient;
import org.apache.pinot.common.utils.grpc.GrpcRequestBuilder;
import org.apache.pinot.core.common.datatable.DataTableFactory;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.QueryConfig;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Integration test that converts Avro data for 12 segments and runs queries against it.
 */
public class OfflineClusterIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final int NUM_BROKERS = 1;
  private static final int NUM_SERVERS = 1;
  private static final int NUM_SEGMENTS = 12;

  // For table config refresh test, make an expensive query to ensure the query won't finish in 5ms
  private static final String TEST_TIMEOUT_QUERY =
      "SELECT DISTINCTCOUNT(AirlineID) FROM mytable GROUP BY Carrier TOP 10000";

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
  private static final String SCHEMA_FILE_NAME_WITH_EXTRA_COLUMNS =
      "On_Time_On_Time_Performance_2014_100k_subset_nonulls_default_column_test_extra_columns.schema";
  private static final String SCHEMA_FILE_NAME_WITH_MISSING_COLUMNS =
      "On_Time_On_Time_Performance_2014_100k_subset_nonulls_default_column_test_missing_columns.schema";
  private static final String TEST_EXTRA_COLUMNS_QUERY = "SELECT COUNT(*) FROM mytable WHERE NewAddedIntDimension < 0";
  private static final String TEST_MISSING_COLUMNS_QUERY = "SELECT COUNT(*) FROM mytable WHERE AirlineID > 0";
  private static final String SELECT_STAR_QUERY = "SELECT * FROM mytable";

  private static final String DISK_SIZE_IN_BYTES_KEY = "diskSizeInBytes";
  private static final String NUM_SEGMENTS_KEY = "numSegments";
  private static final String NUM_ROWS_KEY = "numRows";
  private static final String COLUMN_LENGTH_MAP_KEY = "columnLengthMap";
  private static final String COLUMN_CARDINALITY_MAP_KEY = "columnCardinalityMap";
  private static final int DISK_SIZE_IN_BYTES = 20270480;
  private static final int NUM_ROWS = 115545;

  private final List<ServiceStatus.ServiceStatusCallback> _serviceStatusCallbacks =
      new ArrayList<>(getNumBrokers() + getNumServers());
  private String _schemaFileName = DEFAULT_SCHEMA_FILE_NAME;

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

  // NOTE: Only allow removing default columns for v1 segment
  @Override
  protected String getSegmentVersion() {
    return SegmentVersion.v1.name();
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBrokers(getNumBrokers());
    startServers();

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create and upload segments
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(getTableName(), _tarDir);

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

  protected void startServers() {
    // Enable gRPC server
    PinotConfiguration serverConfig = getDefaultServerConfiguration();
    serverConfig.setProperty(CommonConstants.Server.CONFIG_OF_ENABLE_GRPC_SERVER, true);
    startServer(serverConfig);
  }

  private void registerCallbackHandlers() {
    List<String> instances = _helixAdmin.getInstancesInCluster(getHelixClusterName());
    instances.removeIf(instance -> (!instance.startsWith(CommonConstants.Helix.PREFIX_OF_BROKER_INSTANCE) && !instance
        .startsWith(CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE)));
    List<String> resourcesInCluster = _helixAdmin.getResourcesInCluster(getHelixClusterName());
    resourcesInCluster.removeIf(
        resource -> (!TableNameBuilder.isTableResource(resource) && !CommonConstants.Helix.BROKER_RESOURCE_INSTANCE
            .equals(resource)));
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
      _serviceStatusCallbacks.add(new ServiceStatus.MultipleCallbackServiceStatusCallback(ImmutableList
          .of(new ServiceStatus.IdealStateAndCurrentStateMatchServiceStatusCallback(_helixManager,
                  getHelixClusterName(), instance, resourcesToMonitor, 100.0),
              new ServiceStatus.IdealStateAndExternalViewMatchServiceStatusCallback(_helixManager,
                  getHelixClusterName(), instance, resourcesToMonitor, 100.0))));
    }
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
      assertTrue(e.getMessage().startsWith("Server returned HTTP response code: 400"));
    }
  }

  @Test
  public void testRefreshTableConfigAndQueryTimeout()
      throws Exception {
    // Set timeout as 5ms so that query will timeout
    TableConfig tableConfig = getOfflineTableConfig();
    tableConfig.setQueryConfig(new QueryConfig(5L));
    updateTableConfig(tableConfig);

    // Wait for at most 1 minute for broker to receive and process the table config refresh message
    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode queryResponse = postQuery(TEST_TIMEOUT_QUERY);
        JsonNode exceptions = queryResponse.get("exceptions");
        if (exceptions.size() != 0) {
          // Timed out on broker side
          return exceptions.get(0).get("errorCode").asInt() == QueryException.BROKER_TIMEOUT_ERROR_CODE;
        } else {
          // Timed out on server side
          int numServersQueried = queryResponse.get("numServersQueried").asInt();
          int numServersResponded = queryResponse.get("numServersResponded").asInt();
          int numDocsScanned = queryResponse.get("numDocsScanned").asInt();
          return numServersQueried == getNumServers() && numServersResponded == 0 && numDocsScanned == 0;
        }
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
        if (exceptions.size() != 0) {
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
    for (SegmentZKMetadata segmentZKMetadataAfterUpload : _helixResourceManager
        .getSegmentsZKMetadata(offlineTableName)) {
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
  public void testInvertedIndexTriggering()
      throws Exception {
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
    long numTotalDocs = getCountStarResult();
    long tableSizeWithDefaultIndex = getTableSize(offlineTableName);

    // Without index on DivActualElapsedTime, all docs are scanned at filtering stage.
    JsonNode queryResponse = postQuery(TEST_UPDATED_INVERTED_INDEX_QUERY);
    assertEquals(queryResponse.get("numEntriesScannedInFilter").asLong(), numTotalDocs);

    // Update table config to add inverted index on DivActualElapsedTime column, and
    // reload the table to get config change into effect and add the inverted index.
    TableConfig tableConfig = getOfflineTableConfig();
    tableConfig.getIndexingConfig().setInvertedIndexColumns(UPDATED_INVERTED_INDEX_COLUMNS);
    updateTableConfig(tableConfig);
    reloadOfflineTable(offlineTableName);

    // It takes a while to reload multiple segments, thus we retry the query for some time.
    // After all segments are reloaded, the inverted index is added on DivActualElapsedTime.
    // It's expected to have numEntriesScannedInFilter equal to 0, i.e. no docs is scanned
    // at filtering stage when inverted index can answer the predicate directly.
    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode queryResponse1 = postQuery(TEST_UPDATED_INVERTED_INDEX_QUERY);
        // Total docs should not change during reload
        assertEquals(queryResponse1.get("totalDocs").asLong(), numTotalDocs);
        return queryResponse1.get("numEntriesScannedInFilter").asLong() == 0L;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 600_000L, "Failed to generate inverted index");

    long tableSizeWithNewIndex = getTableSize(offlineTableName);
    assertTrue(tableSizeWithNewIndex > tableSizeWithDefaultIndex);

    // Update table config to remove all inverted index.
    tableConfig = getOfflineTableConfig();
    tableConfig.getIndexingConfig().setInvertedIndexColumns(Collections.emptyList());
    updateTableConfig(tableConfig);

    // Reload table just disables those indices, not clean them physically.
    reloadOfflineTable(offlineTableName);
    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode queryResponse2 = postQuery(TEST_UPDATED_INVERTED_INDEX_QUERY);
        // Total docs should not change during reload
        assertEquals(queryResponse2.get("totalDocs").asLong(), numTotalDocs);
        return queryResponse2.get("numEntriesScannedInFilter").asLong() == numTotalDocs;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 600_000L, "Failed to disable indices");
    long tableSizeAfterReload = getTableSize(offlineTableName);
    assertEquals(tableSizeAfterReload, tableSizeWithNewIndex);

    // Reload a single segment and force to download the segment,
    // and we can expect disk usage drops a bit.
    SegmentZKMetadata segmentZKMetadata = _helixResourceManager.getSegmentsZKMetadata(offlineTableName).get(0);
    String segmentName = segmentZKMetadata.getSegmentName();
    reloadOfflineSegment(offlineTableName, segmentName, true);
    AtomicLong tableSizeAfterDownloadSegment = new AtomicLong(0);
    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode queryResponse3 = postQuery(TEST_UPDATED_INVERTED_INDEX_QUERY);
        assertEquals(queryResponse3.get("totalDocs").asLong(), numTotalDocs);
        tableSizeAfterDownloadSegment.set(getTableSize(offlineTableName));
        return tableSizeAfterDownloadSegment.longValue() < tableSizeWithNewIndex;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 600_000L, "Failed to clean up obsolete indices");

    // Reload whole table and we can expect disk usage drops further.
    reloadOfflineTable(offlineTableName, true);
    AtomicLong tableSizeAfterDownloadTable = new AtomicLong(0);
    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode queryResponse3 = postQuery(TEST_UPDATED_INVERTED_INDEX_QUERY);
        assertEquals(queryResponse3.get("totalDocs").asLong(), numTotalDocs);
        tableSizeAfterDownloadTable.set(getTableSize(offlineTableName));
        return tableSizeAfterDownloadTable.longValue() < tableSizeAfterDownloadSegment.longValue();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 600_000L, "Failed to clean up obsolete indices");
  }

  @Test
  public void testTimeFunc()
      throws Exception {
    String sqlQuery = "SELECT toDateTime(now(), 'yyyy-MM-dd z'), toDateTime(ago('PT1H'), 'yyyy-MM-dd z') FROM mytable";
    JsonNode response = postSqlQuery(sqlQuery, _brokerBaseApiUrl);
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
  public void testLiteralOnlyFunc()
      throws Exception {
    long ONE_HOUR_IN_MS = TimeUnit.HOURS.toMillis(1);
    long currentTsMin = System.currentTimeMillis();
    long oneHourAgoTsMin = currentTsMin - ONE_HOUR_IN_MS;
    String sqlQuery =
        "SELECT 1, now() as currentTs, ago('PT1H') as oneHourAgoTs, 'abc', toDateTime(now(), 'yyyy-MM-dd z') as today, now(), ago('PT1H')";
    JsonNode response = postSqlQuery(sqlQuery, _brokerBaseApiUrl);
    long currentTsMax = System.currentTimeMillis();
    long oneHourAgoTsMax = currentTsMax - ONE_HOUR_IN_MS;

    assertEquals(response.get("resultTable").get("dataSchema").get("columnNames").get(0).asText(), "1");
    assertEquals(response.get("resultTable").get("dataSchema").get("columnNames").get(1).asText(), "currentTs");
    assertEquals(response.get("resultTable").get("dataSchema").get("columnNames").get(2).asText(), "oneHourAgoTs");
    assertEquals(response.get("resultTable").get("dataSchema").get("columnNames").get(3).asText(), "abc");
    assertEquals(response.get("resultTable").get("dataSchema").get("columnNames").get(4).asText(), "today");
    String nowColumnName = response.get("resultTable").get("dataSchema").get("columnNames").get(5).asText();
    String oneHourAgoColumnName = response.get("resultTable").get("dataSchema").get("columnNames").get(6).asText();
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
  }

  @Test
  public void testRangeIndexTriggering()
      throws Exception {
    long numTotalDocs = getCountStarResult();

    JsonNode queryResponse = postQuery(TEST_UPDATED_RANGE_INDEX_QUERY);
    assertEquals(queryResponse.get("numEntriesScannedInFilter").asLong(), numTotalDocs);

    // Update table config and trigger reload
    TableConfig tableConfig = getOfflineTableConfig();
    tableConfig.getIndexingConfig().setRangeIndexColumns(UPDATED_RANGE_INDEX_COLUMNS);
    updateTableConfig(tableConfig);
    reloadOfflineTable(getTableName());

    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode queryResponse1 = postQuery(TEST_UPDATED_RANGE_INDEX_QUERY);
        // Total docs should not change during reload
        assertEquals(queryResponse1.get("totalDocs").asLong(), numTotalDocs);
        return queryResponse1.get("numEntriesScannedInFilter").asLong() < numTotalDocs;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 600_000L, "Failed to generate range index");
  }

  @Test
  public void testBloomFilterTriggering()
      throws Exception {
    long numTotalDocs = getCountStarResult();

    JsonNode queryResponse = postQuery(TEST_UPDATED_BLOOM_FILTER_QUERY);
    assertEquals(queryResponse.get("numSegmentsProcessed").asLong(), NUM_SEGMENTS);

    // Update table config and trigger reload
    TableConfig tableConfig = getOfflineTableConfig();
    tableConfig.getIndexingConfig().setBloomFilterColumns(UPDATED_BLOOM_FILTER_COLUMNS);
    updateTableConfig(tableConfig);
    reloadOfflineTable(getTableName());

    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode queryResponse1 = postQuery(TEST_UPDATED_BLOOM_FILTER_QUERY);
        // Total docs should not change during reload
        assertEquals(queryResponse1.get("totalDocs").asLong(), numTotalDocs);
        return queryResponse1.get("numSegmentsProcessed").asLong() == 0L;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 600_000L, "Failed to generate bloom filter");
  }

  /** Check if server returns error response quickly without timing out Broker. */
  @Test
  public void testServerErrorWithBrokerTimeout()
      throws Exception {
    // Set query timeout
    long queryTimeout = 5000;
    TableConfig tableConfig = getOfflineTableConfig();
    tableConfig.setQueryConfig(new QueryConfig(queryTimeout));
    updateTableConfig(tableConfig);

    long startTime = System.currentTimeMillis();
    // The query below will fail execution due to JSON_MATCH on column without json index
    JsonNode queryResponse = postSqlQuery("SELECT count(*) FROM mytable WHERE JSON_MATCH(Dest, '$=123')");

    assertTrue(System.currentTimeMillis() - startTime < queryTimeout);
    assertTrue(queryResponse.get("exceptions").get(0).get("message").toString().startsWith("\"QueryExecutionError"));

    // Remove timeout
    tableConfig.setQueryConfig(null);
    updateTableConfig(tableConfig);
  }

  @Test
  public void testStarTreeTriggering()
      throws Exception {
    long numTotalDocs = getCountStarResult();

    // Test the first query
    JsonNode firstQueryResponse = postQuery(TEST_STAR_TREE_QUERY_1);
    int firstQueryResult = firstQueryResponse.get("aggregationResults").get(0).get("value").asInt();
    assertEquals(firstQueryResponse.get("totalDocs").asLong(), numTotalDocs);
    // Initially 'numDocsScanned' should be the same as 'COUNT(*)' result
    assertEquals(firstQueryResponse.get("numDocsScanned").asInt(), firstQueryResult);

    // Update table config and trigger reload
    TableConfig tableConfig = getOfflineTableConfig();
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    indexingConfig.setStarTreeIndexConfigs(Collections.singletonList(STAR_TREE_INDEX_CONFIG_1));
    indexingConfig.setEnableDynamicStarTreeCreation(true);
    updateTableConfig(tableConfig);
    reloadOfflineTable(getTableName());

    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode queryResponse = postQuery(TEST_STAR_TREE_QUERY_1);
        // Result should not change during reload
        assertEquals(queryResponse.get("aggregationResults").get(0).get("value").asInt(), firstQueryResult);
        // Total docs should not change during reload
        assertEquals(queryResponse.get("totalDocs").asLong(), numTotalDocs);
        // With star-tree, 'numDocsScanned' should be the same as number of segments (1 per segment)
        return queryResponse.get("numDocsScanned").asInt() == NUM_SEGMENTS;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 600_000L, "Failed to star-tree index");

    // Reload again should have no effect
    reloadOfflineTable(getTableName());
    firstQueryResponse = postQuery(TEST_STAR_TREE_QUERY_1);
    assertEquals(firstQueryResponse.get("aggregationResults").get(0).get("value").asInt(), firstQueryResult);
    assertEquals(firstQueryResponse.get("totalDocs").asLong(), numTotalDocs);
    assertEquals(firstQueryResponse.get("numDocsScanned").asInt(), NUM_SEGMENTS);

    // Should be able to use the star-tree with an additional match-all predicate on another dimension
    firstQueryResponse = postQuery(TEST_STAR_TREE_QUERY_1 + " AND DaysSinceEpoch > 16070");
    assertEquals(firstQueryResponse.get("aggregationResults").get(0).get("value").asInt(), firstQueryResult);
    assertEquals(firstQueryResponse.get("totalDocs").asLong(), numTotalDocs);
    assertEquals(firstQueryResponse.get("numDocsScanned").asInt(), NUM_SEGMENTS);

    // Test the second query
    JsonNode secondQueryResponse = postQuery(TEST_STAR_TREE_QUERY_2);
    int secondQueryResult = secondQueryResponse.get("aggregationResults").get(0).get("value").asInt();
    assertEquals(secondQueryResponse.get("totalDocs").asLong(), numTotalDocs);
    // Initially 'numDocsScanned' should be the same as 'COUNT(*)' result
    assertEquals(secondQueryResponse.get("numDocsScanned").asInt(), secondQueryResult);

    // Update table config with a different star-tree index config and trigger reload
    indexingConfig.setStarTreeIndexConfigs(Collections.singletonList(STAR_TREE_INDEX_CONFIG_2));
    updateTableConfig(tableConfig);
    reloadOfflineTable(getTableName());

    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode queryResponse = postQuery(TEST_STAR_TREE_QUERY_2);
        // Result should not change during reload
        assertEquals(queryResponse.get("aggregationResults").get(0).get("value").asInt(), secondQueryResult);
        // Total docs should not change during reload
        assertEquals(queryResponse.get("totalDocs").asLong(), numTotalDocs);
        // With star-tree, 'numDocsScanned' should be the same as number of segments (1 per segment)
        return queryResponse.get("numDocsScanned").asInt() == NUM_SEGMENTS;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 600_000L, "Failed to star-tree index");

    // First query should not be able to use the star-tree
    firstQueryResponse = postQuery(TEST_STAR_TREE_QUERY_1);
    assertEquals(firstQueryResponse.get("numDocsScanned").asInt(), firstQueryResult);

    // Reload again should have no effect
    reloadOfflineTable(getTableName());
    firstQueryResponse = postQuery(TEST_STAR_TREE_QUERY_1);
    assertEquals(firstQueryResponse.get("aggregationResults").get(0).get("value").asInt(), firstQueryResult);
    assertEquals(firstQueryResponse.get("totalDocs").asLong(), numTotalDocs);
    assertEquals(firstQueryResponse.get("numDocsScanned").asInt(), firstQueryResult);
    secondQueryResponse = postQuery(TEST_STAR_TREE_QUERY_2);
    assertEquals(secondQueryResponse.get("aggregationResults").get(0).get("value").asInt(), secondQueryResult);
    assertEquals(secondQueryResponse.get("totalDocs").asLong(), numTotalDocs);
    assertEquals(secondQueryResponse.get("numDocsScanned").asInt(), NUM_SEGMENTS);

    // Should be able to use the star-tree with an additional match-all predicate on another dimension
    secondQueryResponse = postQuery(TEST_STAR_TREE_QUERY_2 + " AND DaysSinceEpoch > 16070");
    assertEquals(secondQueryResponse.get("aggregationResults").get(0).get("value").asInt(), secondQueryResult);
    assertEquals(secondQueryResponse.get("totalDocs").asLong(), numTotalDocs);
    assertEquals(secondQueryResponse.get("numDocsScanned").asInt(), NUM_SEGMENTS);

    // Remove the star-tree index config and trigger reload
    indexingConfig.setStarTreeIndexConfigs(null);
    updateTableConfig(tableConfig);
    reloadOfflineTable(getTableName());

    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode queryResponse = postQuery(TEST_STAR_TREE_QUERY_2);
        // Result should not change during reload
        assertEquals(queryResponse.get("aggregationResults").get(0).get("value").asInt(), secondQueryResult);
        // Total docs should not change during reload
        assertEquals(queryResponse.get("totalDocs").asLong(), numTotalDocs);
        // Without star-tree, 'numDocsScanned' should be the same as the 'COUNT(*)' result
        return queryResponse.get("numDocsScanned").asInt() == secondQueryResult;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 600_000L, "Failed to star-tree index");

    // First query should not be able to use the star-tree
    firstQueryResponse = postQuery(TEST_STAR_TREE_QUERY_1);
    assertEquals(firstQueryResponse.get("numDocsScanned").asInt(), firstQueryResult);

    // Reload again should have no effect
    reloadOfflineTable(getTableName());
    firstQueryResponse = postQuery(TEST_STAR_TREE_QUERY_1);
    assertEquals(firstQueryResponse.get("aggregationResults").get(0).get("value").asInt(), firstQueryResult);
    assertEquals(firstQueryResponse.get("totalDocs").asLong(), numTotalDocs);
    assertEquals(firstQueryResponse.get("numDocsScanned").asInt(), firstQueryResult);
    secondQueryResponse = postQuery(TEST_STAR_TREE_QUERY_2);
    assertEquals(secondQueryResponse.get("aggregationResults").get(0).get("value").asInt(), secondQueryResult);
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
   *   <li>"NewAddedIntDimension", DIMENSION, INT, single-value, default (Integer.MIN_VALUE)</li>
   *   <li>"NewAddedLongDimension", DIMENSION, LONG, single-value, default (Long.MIN_VALUE)</li>
   *   <li>"NewAddedFloatDimension", DIMENSION, FLOAT, single-value, default (Float.NEGATIVE_INFINITY)</li>
   *   <li>"NewAddedDoubleDimension", DIMENSION, DOUBLE, single-value, default (Double.NEGATIVE_INFINITY)</li>
   *   <li>"NewAddedSVStringDimension", DIMENSION, STRING, single-value, default ("null")</li>
   *   <li>"NewAddedMVStringDimension", DIMENSION, STRING, multi-value, ""</li>
   *   <li>"NewAddedDerivedHoursSinceEpoch", DIMENSION, INT, single-value, default (Integer.MIN_VALUE)</li>
   *   <li>"NewAddedDerivedSecondsSinceEpoch", DIMENSION, LONG, single-value, default (LONG.MIN_VALUE)</li>
   * </ul>
   */
  @Test
  public void testDefaultColumns()
      throws Exception {
    long numTotalDocs = getCountStarResult();

    reloadWithExtraColumns();
    JsonNode queryResponse = postQuery(SELECT_STAR_QUERY);
    assertEquals(queryResponse.get("totalDocs").asLong(), numTotalDocs);
    assertEquals(queryResponse.get("selectionResults").get("columns").size(), 91);

    testNewAddedColumns();

    reloadWithMissingColumns();
    queryResponse = postQuery(SELECT_STAR_QUERY);
    assertEquals(queryResponse.get("totalDocs").asLong(), numTotalDocs);
    assertEquals(queryResponse.get("selectionResults").get("columns").size(), 75);

    reloadWithRegularColumns();
    queryResponse = postQuery(SELECT_STAR_QUERY);
    assertEquals(queryResponse.get("totalDocs").asLong(), numTotalDocs);
    assertEquals(queryResponse.get("selectionResults").get("columns").size(), 79);
  }

  private void reloadWithExtraColumns()
      throws Exception {
    long numTotalDocs = getCountStarResult();

    // Add columns to the schema first to pass the validation of the table config
    _schemaFileName = SCHEMA_FILE_NAME_WITH_EXTRA_COLUMNS;
    addSchema(createSchema());
    TableConfig tableConfig = getOfflineTableConfig();
    tableConfig.setIngestionConfig(new IngestionConfig(null, null, null, Arrays
        .asList(new TransformConfig("NewAddedDerivedHoursSinceEpoch", "times(DaysSinceEpoch, 24)"),
            new TransformConfig("NewAddedDerivedSecondsSinceEpoch", "times(times(DaysSinceEpoch, 24), 3600)")), null));
    updateTableConfig(tableConfig);

    // Trigger reload
    reloadOfflineTable(getTableName());

    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode queryResponse = postQuery(TEST_EXTRA_COLUMNS_QUERY);
        // Total docs should not change during reload
        assertEquals(queryResponse.get("totalDocs").asLong(), numTotalDocs);
        long count = queryResponse.get("aggregationResults").get(0).get("value").asLong();
        return count == numTotalDocs;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 600_000L, "Failed to add default columns");
  }

  private void reloadWithMissingColumns()
      throws Exception {
    long numTotalDocs = getCountStarResult();

    // Remove columns from the table config first to pass the validation of the table config
    TableConfig tableConfig = getOfflineTableConfig();
    tableConfig.setIngestionConfig(null);
    updateTableConfig(tableConfig);
    _schemaFileName = SCHEMA_FILE_NAME_WITH_MISSING_COLUMNS;
    addSchema(createSchema());

    // Trigger reload
    reloadOfflineTable(getTableName());

    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode queryResponse = postQuery(TEST_MISSING_COLUMNS_QUERY);
        // Total docs should not change during reload
        assertEquals(queryResponse.get("totalDocs").asLong(), numTotalDocs);
        long count = queryResponse.get("aggregationResults").get(0).get("value").asLong();
        return count == 0;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 600_000L, "Failed to skip missing columns");
  }

  private void reloadWithRegularColumns()
      throws Exception {
    long numTotalDocs = getCountStarResult();

    _schemaFileName = DEFAULT_SCHEMA_FILE_NAME;
    addSchema(createSchema());

    // Trigger reload
    reloadOfflineTable(getTableName());

    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode queryResponse = postQuery(TEST_MISSING_COLUMNS_QUERY);
        // Total docs should not change during reload
        assertEquals(queryResponse.get("totalDocs").asLong(), numTotalDocs);
        long count = queryResponse.get("aggregationResults").get(0).get("value").asLong();
        return count == numTotalDocs;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 600_000L, "Failed to reload regular columns");
  }

  private void testNewAddedColumns()
      throws Exception {
    long numTotalDocs = getCountStarResult();
    double numTotalDocsInDouble = (double) numTotalDocs;

    String pqlQuery;
    String sqlQuery;

    // Test queries with each new added columns
    pqlQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedIntMetric = 1";
    sqlQuery = "SELECT COUNT(*) FROM mytable";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedLongMetric = 1";
    sqlQuery = "SELECT COUNT(*) FROM mytable";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedFloatMetric = 0.0";
    sqlQuery = "SELECT COUNT(*) FROM mytable";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedDoubleMetric = 0.0";
    sqlQuery = "SELECT COUNT(*) FROM mytable";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedIntDimension < 0";
    sqlQuery = "SELECT COUNT(*) FROM mytable";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedLongDimension < 0";
    sqlQuery = "SELECT COUNT(*) FROM mytable";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedFloatDimension < 0.0";
    sqlQuery = "SELECT COUNT(*) FROM mytable";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedDoubleDimension < 0.0";
    sqlQuery = "SELECT COUNT(*) FROM mytable";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedSVStringDimension = 'null'";
    sqlQuery = "SELECT COUNT(*) FROM mytable";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedMVStringDimension = ''";
    sqlQuery = "SELECT COUNT(*) FROM mytable";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedDerivedHoursSinceEpoch = 392232";
    sqlQuery = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch = 16343";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedDerivedSecondsSinceEpoch = 1411862400";
    sqlQuery = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch = 16341";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));

    // Test queries with new added metric column in aggregation function
    pqlQuery = "SELECT SUM(NewAddedIntMetric) FROM mytable WHERE DaysSinceEpoch <= 16312";
    sqlQuery = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch <= 16312";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT SUM(NewAddedIntMetric) FROM mytable WHERE DaysSinceEpoch > 16312";
    sqlQuery = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch > 16312";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT SUM(NewAddedLongMetric) FROM mytable WHERE DaysSinceEpoch <= 16312";
    sqlQuery = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch <= 16312";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT SUM(NewAddedLongMetric) FROM mytable WHERE DaysSinceEpoch > 16312";
    sqlQuery = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch > 16312";
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));

    // Test other query forms with new added columns
    JsonNode response;
    JsonNode groupByResult;
    pqlQuery = "SELECT SUM(NewAddedFloatMetric) FROM mytable GROUP BY NewAddedSVStringDimension";
    response = postQuery(pqlQuery);
    groupByResult = response.get("aggregationResults").get(0).get("groupByResult").get(0);
    assertEquals(groupByResult.get("value").asDouble(), 0.0);
    assertEquals(groupByResult.get("group").get(0).asText(), "null");
    pqlQuery = "SELECT SUM(NewAddedDoubleMetric) FROM mytable GROUP BY NewAddedIntDimension";
    response = postQuery(pqlQuery);
    groupByResult = response.get("aggregationResults").get(0).get("groupByResult").get(0);
    assertEquals(groupByResult.get("value").asDouble(), 0.0);
    assertEquals(groupByResult.get("group").get(0).asInt(), Integer.MIN_VALUE);
    pqlQuery = "SELECT SUM(NewAddedIntMetric) FROM mytable GROUP BY NewAddedLongDimension";
    response = postQuery(pqlQuery);
    groupByResult = response.get("aggregationResults").get(0).get("groupByResult").get(0);
    assertEquals(groupByResult.get("value").asDouble(), numTotalDocsInDouble);
    assertEquals(groupByResult.get("group").get(0).asLong(), Long.MIN_VALUE);
    pqlQuery =
        "SELECT SUM(NewAddedIntMetric), SUM(NewAddedLongMetric), SUM(NewAddedFloatMetric), SUM(NewAddedDoubleMetric) "
            + "FROM mytable GROUP BY NewAddedIntDimension, NewAddedLongDimension, NewAddedFloatDimension, "
            + "NewAddedDoubleDimension, NewAddedSVStringDimension, NewAddedMVStringDimension";
    response = postQuery(pqlQuery);
    JsonNode groupByResultArray = response.get("aggregationResults");
    groupByResult = groupByResultArray.get(0).get("groupByResult").get(0);
    assertEquals(groupByResult.get("value").asDouble(), numTotalDocsInDouble);
    assertEquals(groupByResult.get("group").get(0).asInt(), Integer.MIN_VALUE);
    assertEquals(groupByResult.get("group").get(1).asLong(), Long.MIN_VALUE);
    assertEquals((float) groupByResult.get("group").get(2).asDouble(), Float.NEGATIVE_INFINITY);
    assertEquals(groupByResult.get("group").get(3).asDouble(), Double.NEGATIVE_INFINITY);
    groupByResult = groupByResultArray.get(1).get("groupByResult").get(0);
    assertEquals(groupByResult.get("value").asDouble(), numTotalDocsInDouble);
    assertEquals(groupByResult.get("group").get(0).asInt(), Integer.MIN_VALUE);
    assertEquals(groupByResult.get("group").get(1).asLong(), Long.MIN_VALUE);
    assertEquals((float) groupByResult.get("group").get(2).asDouble(), Float.NEGATIVE_INFINITY);
    assertEquals(groupByResult.get("group").get(3).asDouble(), Double.NEGATIVE_INFINITY);
    groupByResult = groupByResultArray.get(2).get("groupByResult").get(0);
    assertEquals(groupByResult.get("value").asDouble(), 0.0);
    assertEquals(groupByResult.get("group").get(0).asInt(), Integer.MIN_VALUE);
    assertEquals(groupByResult.get("group").get(1).asLong(), Long.MIN_VALUE);
    assertEquals((float) groupByResult.get("group").get(2).asDouble(), Float.NEGATIVE_INFINITY);
    assertEquals(groupByResult.get("group").get(3).asDouble(), Double.NEGATIVE_INFINITY);
    groupByResult = groupByResultArray.get(3).get("groupByResult").get(0);
    assertEquals(groupByResult.get("value").asDouble(), 0.0);
    assertEquals(groupByResult.get("group").get(0).asInt(), Integer.MIN_VALUE);
    assertEquals(groupByResult.get("group").get(1).asLong(), Long.MIN_VALUE);
    assertEquals((float) groupByResult.get("group").get(2).asDouble(), Float.NEGATIVE_INFINITY);
    assertEquals(groupByResult.get("group").get(3).asDouble(), Double.NEGATIVE_INFINITY);
  }

  @Test
  @Override
  public void testBrokerResponseMetadata()
      throws Exception {
    super.testBrokerResponseMetadata();
  }

  @Test
  public void testGroupByUDF()
      throws Exception {
    String pqlQuery = "SELECT COUNT(*) FROM mytable GROUP BY timeConvert(DaysSinceEpoch,'DAYS','SECONDS')";
    JsonNode response = postQuery(pqlQuery);
    JsonNode groupByResult = response.get("aggregationResults").get(0);
    JsonNode groupByEntry = groupByResult.get("groupByResult").get(0);
    assertEquals(groupByEntry.get("value").asDouble(), 605.0);
    assertEquals(groupByEntry.get("group").get(0).asInt(), 16138 * 24 * 3600);
    assertEquals(groupByResult.get("groupByColumns").get(0).asText(), "timeconvert(DaysSinceEpoch,'DAYS','SECONDS')");

    pqlQuery =
        "SELECT COUNT(*) FROM mytable GROUP BY dateTimeConvert(DaysSinceEpoch,'1:DAYS:EPOCH','1:HOURS:EPOCH','1:HOURS')";
    response = postQuery(pqlQuery);
    groupByResult = response.get("aggregationResults").get(0);
    groupByEntry = groupByResult.get("groupByResult").get(0);
    assertEquals(groupByEntry.get("value").asDouble(), 605.0);
    assertEquals(groupByEntry.get("group").get(0).asInt(), 16138 * 24);
    assertEquals(groupByResult.get("groupByColumns").get(0).asText(),
        "datetimeconvert(DaysSinceEpoch,'1:DAYS:EPOCH','1:HOURS:EPOCH','1:HOURS')");

    pqlQuery = "SELECT COUNT(*) FROM mytable GROUP BY add(DaysSinceEpoch,DaysSinceEpoch,15)";
    response = postQuery(pqlQuery);
    groupByResult = response.get("aggregationResults").get(0);
    groupByEntry = groupByResult.get("groupByResult").get(0);
    assertEquals(groupByEntry.get("value").asDouble(), 605.0);
    assertEquals(groupByEntry.get("group").get(0).asDouble(), 16138.0 + 16138 + 15);
    assertEquals(groupByResult.get("groupByColumns").get(0).asText(), "add(DaysSinceEpoch,DaysSinceEpoch,'15')");

    pqlQuery = "SELECT COUNT(*) FROM mytable GROUP BY sub(DaysSinceEpoch,25)";
    response = postQuery(pqlQuery);
    groupByResult = response.get("aggregationResults").get(0);
    groupByEntry = groupByResult.get("groupByResult").get(0);
    assertEquals(groupByEntry.get("value").asDouble(), 605.0);
    assertEquals(groupByEntry.get("group").get(0).asDouble(), 16138.0 - 25);
    assertEquals(groupByResult.get("groupByColumns").get(0).asText(), "sub(DaysSinceEpoch,'25')");

    pqlQuery = "SELECT COUNT(*) FROM mytable GROUP BY mult(DaysSinceEpoch,24,3600)";
    response = postQuery(pqlQuery);
    groupByResult = response.get("aggregationResults").get(0);
    groupByEntry = groupByResult.get("groupByResult").get(0);
    assertEquals(groupByEntry.get("value").asDouble(), 605.0);
    assertEquals(groupByEntry.get("group").get(0).asDouble(), 16138.0 * 24 * 3600);
    assertEquals(groupByResult.get("groupByColumns").get(0).asText(), "mult(DaysSinceEpoch,'24','3600')");

    pqlQuery = "SELECT COUNT(*) FROM mytable GROUP BY div(DaysSinceEpoch,2)";
    response = postQuery(pqlQuery);
    groupByResult = response.get("aggregationResults").get(0);
    groupByEntry = groupByResult.get("groupByResult").get(0);
    assertEquals(groupByEntry.get("value").asDouble(), 605.0);
    assertEquals(groupByEntry.get("group").get(0).asDouble(), 16138.0 / 2);
    assertEquals(groupByResult.get("groupByColumns").get(0).asText(), "div(DaysSinceEpoch,'2')");

    pqlQuery = "SELECT COUNT(*) FROM mytable GROUP BY arrayLength(DivAirports)";
    response = postQuery(pqlQuery);
    groupByResult = response.get("aggregationResults").get(0);
    groupByEntry = groupByResult.get("groupByResult").get(0);
    assertEquals(groupByEntry.get("value").asDouble(), 115545.0);
    assertEquals(groupByEntry.get("group").get(0).asText(), "5");
    assertEquals(groupByResult.get("groupByColumns").get(0).asText(), "arraylength(DivAirports)");

    pqlQuery = "SELECT COUNT(*) FROM mytable GROUP BY arrayLength(valueIn(DivAirports,'DFW','ORD'))";
    response = postQuery(pqlQuery);
    groupByResult = response.get("aggregationResults").get(0);
    groupByEntry = groupByResult.get("groupByResult").get(0);
    assertEquals(groupByEntry.get("value").asDouble(), 114895.0);
    assertEquals(groupByEntry.get("group").get(0).asText(), "0");
    groupByEntry = groupByResult.get("groupByResult").get(1);
    assertEquals(groupByEntry.get("value").asDouble(), 648.0);
    assertEquals(groupByEntry.get("group").get(0).asText(), "1");
    groupByEntry = groupByResult.get("groupByResult").get(2);
    assertEquals(groupByEntry.get("value").asDouble(), 2.0);
    assertEquals(groupByEntry.get("group").get(0).asText(), "2");
    assertEquals(groupByResult.get("groupByColumns").get(0).asText(), "arraylength(valuein(DivAirports,'DFW','ORD'))");

    pqlQuery = "SELECT COUNT(*) FROM mytable GROUP BY valueIn(DivAirports,'DFW','ORD')";
    response = postQuery(pqlQuery);
    groupByResult = response.get("aggregationResults").get(0);
    groupByEntry = groupByResult.get("groupByResult").get(0);
    assertEquals(groupByEntry.get("value").asDouble(), 336.0);
    assertEquals(groupByEntry.get("group").get(0).asText(), "ORD");
    assertEquals(groupByResult.get("groupByColumns").get(0).asText(), "valuein(DivAirports,'DFW','ORD')");

    pqlQuery = "SELECT MAX(timeConvert(DaysSinceEpoch,'DAYS','SECONDS')) FROM mytable";
    response = postQuery(pqlQuery);
    JsonNode aggregationResult = response.get("aggregationResults").get(0);
    assertEquals(aggregationResult.get("function").asText(), "max_timeconvert(DaysSinceEpoch,'DAYS','SECONDS')");
    assertEquals(aggregationResult.get("value").asDouble(), 16435.0 * 24 * 3600);

    pqlQuery = "SELECT MIN(div(DaysSinceEpoch,2)) FROM mytable";
    response = postQuery(pqlQuery);
    aggregationResult = response.get("aggregationResults").get(0);
    assertEquals(aggregationResult.get("function").asText(), "min_div(DaysSinceEpoch,'2')");
    assertEquals(aggregationResult.get("value").asDouble(), 16071.0 / 2);
  }

  @Test
  public void testAggregationUDF()
      throws Exception {

    String pqlQuery = "SELECT MAX(timeConvert(DaysSinceEpoch,'DAYS','SECONDS')) FROM mytable";
    JsonNode response = postQuery(pqlQuery);
    JsonNode aggregationResult = response.get("aggregationResults").get(0);
    assertEquals(aggregationResult.get("function").asText(), "max_timeconvert(DaysSinceEpoch,'DAYS','SECONDS')");
    assertEquals(aggregationResult.get("value").asDouble(), 16435.0 * 24 * 3600);

    pqlQuery = "SELECT MIN(div(DaysSinceEpoch,2)) FROM mytable";
    response = postQuery(pqlQuery);
    aggregationResult = response.get("aggregationResults").get(0);
    assertEquals(aggregationResult.get("function").asText(), "min_div(DaysSinceEpoch,'2')");
    assertEquals(aggregationResult.get("value").asDouble(), 16071.0 / 2);
  }

  @Test
  public void testSelectionUDF()
      throws Exception {
    String pqlQuery = "SELECT DaysSinceEpoch, timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM mytable";
    JsonNode response = postQuery(pqlQuery);
    ArrayNode selectionResults = (ArrayNode) response.get("selectionResults").get("results");
    assertNotNull(selectionResults);
    assertTrue(selectionResults.size() > 0);
    for (int i = 0; i < selectionResults.size(); i++) {
      long daysSinceEpoch = selectionResults.get(i).get(0).asLong();
      long secondsSinceEpoch = selectionResults.get(i).get(1).asLong();
      assertEquals(daysSinceEpoch * 24 * 60 * 60, secondsSinceEpoch);
    }

    pqlQuery =
        "SELECT DaysSinceEpoch, timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM mytable order by DaysSinceEpoch limit 10000";
    response = postQuery(pqlQuery);
    selectionResults = (ArrayNode) response.get("selectionResults").get("results");
    assertNotNull(selectionResults);
    assertTrue(selectionResults.size() > 0);
    long prevValue = -1;
    for (int i = 0; i < selectionResults.size(); i++) {
      long daysSinceEpoch = selectionResults.get(i).get(0).asLong();
      long secondsSinceEpoch = selectionResults.get(i).get(1).asLong();
      assertEquals(daysSinceEpoch * 24 * 60 * 60, secondsSinceEpoch);
      assertTrue(daysSinceEpoch >= prevValue);
      prevValue = daysSinceEpoch;
    }

    pqlQuery =
        "SELECT DaysSinceEpoch, timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM mytable order by timeConvert(DaysSinceEpoch,'DAYS','SECONDS') DESC limit 10000";
    response = postQuery(pqlQuery);
    selectionResults = (ArrayNode) response.get("selectionResults").get("results");
    assertNotNull(selectionResults);
    assertTrue(selectionResults.size() > 0);
    prevValue = Long.MAX_VALUE;
    for (int i = 0; i < selectionResults.size(); i++) {
      long daysSinceEpoch = selectionResults.get(i).get(0).asLong();
      long secondsSinceEpoch = selectionResults.get(i).get(1).asLong();
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

    String pqlQuery;
    pqlQuery = "SELECT count(*) FROM mytable WHERE DaysSinceEpoch = " + daysSinceEpoch;
    long expectedResult = postQuery(pqlQuery).get("aggregationResults").get(0).get("value").asLong();

    pqlQuery = "SELECT count(*) FROM mytable WHERE timeConvert(DaysSinceEpoch,'DAYS','SECONDS') = " + secondsSinceEpoch;
    assertEquals(postQuery(pqlQuery).get("aggregationResults").get(0).get("value").asLong(), expectedResult);

    pqlQuery = "SELECT count(*) FROM mytable WHERE DaysSinceEpoch = " + daysSinceEpoch
        + " OR timeConvert(DaysSinceEpoch,'DAYS','SECONDS') = " + secondsSinceEpoch;
    assertEquals(postQuery(pqlQuery).get("aggregationResults").get(0).get("value").asLong(), expectedResult);

    pqlQuery = "SELECT count(*) FROM mytable WHERE DaysSinceEpoch = " + daysSinceEpoch
        + " AND timeConvert(DaysSinceEpoch,'DAYS','SECONDS') = " + secondsSinceEpoch;
    assertEquals(postQuery(pqlQuery).get("aggregationResults").get(0).get("value").asLong(), expectedResult);

    pqlQuery =
        "SELECT count(*) FROM mytable WHERE DIV(timeConvert(DaysSinceEpoch,'DAYS','SECONDS'),1) = " + secondsSinceEpoch;
    assertEquals(postQuery(pqlQuery).get("aggregationResults").get(0).get("value").asLong(), expectedResult);

    pqlQuery = String
        .format("SELECT count(*) FROM mytable WHERE timeConvert(DaysSinceEpoch,'DAYS','SECONDS') IN (%d, %d)",
            secondsSinceEpoch - 100, secondsSinceEpoch);
    assertEquals(postQuery(pqlQuery).get("aggregationResults").get(0).get("value").asLong(), expectedResult);

    pqlQuery = String
        .format("SELECT count(*) FROM mytable WHERE timeConvert(DaysSinceEpoch,'DAYS','SECONDS') BETWEEN %d AND %d",
            secondsSinceEpoch - 100, secondsSinceEpoch);
    assertEquals(postQuery(pqlQuery).get("aggregationResults").get(0).get("value").asLong(), expectedResult);
  }

  @Test
  public void testCaseStatementInSelection()
      throws Exception {
    List<String> origins = Arrays
        .asList("ATL", "ORD", "DFW", "DEN", "LAX", "IAH", "SFO", "PHX", "LAS", "EWR", "MCO", "BOS", "SLC", "SEA", "MSP",
            "CLT", "LGA", "DTW", "JFK", "BWI");
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
    JsonNode response = postSqlQuery(sqlQuery, _brokerBaseApiUrl);
    JsonNode rows = response.get("resultTable").get("rows");
    assertEquals(response.get("exceptions").size(), 0);
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
        "SELECT ArrDelay, CASE WHEN ArrDelay > 0 THEN ArrDelay WHEN ArrDelay < 0 THEN ArrDelay * -1 ELSE 0 END AS ArrTimeDiff FROM mytable LIMIT 1000";
    JsonNode response = postSqlQuery(sqlQuery, _brokerBaseApiUrl);
    JsonNode rows = response.get("resultTable").get("rows");
    assertEquals(response.get("exceptions").size(), 0);
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
    JsonNode response = postSqlQuery(sqlQuery, _brokerBaseApiUrl);
    JsonNode rows = response.get("resultTable").get("rows");
    assertEquals(response.get("exceptions").size(), 0);
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
    JsonNode response = postSqlQuery(sqlQuery, _brokerBaseApiUrl);
    long countValue = response.get("resultTable").get("rows").get(0).get(0).asLong();
    sqlQuery = String.format("SELECT SUM(CASE WHEN %s THEN 1 ELSE 0 END) as sum1 FROM mytable", predicate);
    response = postSqlQuery(sqlQuery, _brokerBaseApiUrl);
    long caseSum = response.get("resultTable").get("rows").get(0).get(0).asLong();
    assertEquals(caseSum, countValue);
  }

  @Test
  public void testFilterWithInvertedIndexUDF()
      throws Exception {
    int daysSinceEpoch = 16138;
    long secondsSinceEpoch = 16138 * 24 * 60 * 60;

    String[] origins =
        new String[]{"ATL", "ORD", "DFW", "DEN", "LAX", "IAH", "SFO", "PHX", "LAS", "EWR", "MCO", "BOS", "SLC", "SEA", "MSP", "CLT", "LGA", "DTW", "JFK", "BWI"};
    String pqlQuery;
    for (String origin : origins) {
      pqlQuery =
          "SELECT count(*) FROM mytable WHERE Origin = \"" + origin + "\" AND DaysSinceEpoch = " + daysSinceEpoch;
      JsonNode response1 = postQuery(pqlQuery);
      pqlQuery = "SELECT count(*) FROM mytable WHERE Origin = \"" + origin
          + "\" AND timeConvert(DaysSinceEpoch,'DAYS','SECONDS') = " + secondsSinceEpoch;
      JsonNode response2 = postQuery(pqlQuery);
      double val1 = response1.get("aggregationResults").get(0).get("value").asDouble();
      double val2 = response2.get("aggregationResults").get(0).get("value").asDouble();
      assertEquals(val1, val2);
    }
  }

  @Test
  public void testQueryWithRepeatedColumns()
      throws Exception {
    //test repeated columns in selection query
    String query = "SELECT ArrTime, ArrTime FROM mytable WHERE DaysSinceEpoch <= 16312 AND Carrier = 'DL'";
    testQuery(query, Collections.singletonList(query));

    //test repeated columns in selection query with order by
    query = "SELECT ArrTime, ArrTime FROM mytable WHERE DaysSinceEpoch <= 16312 AND Carrier = 'DL' order by ArrTime";
    testQuery(query, Collections.singletonList(query));

    //test repeated columns in agg query
    query = "SELECT count(*), count(*) FROM mytable WHERE DaysSinceEpoch <= 16312 AND Carrier = 'DL'";
    testQuery(query, Arrays.asList("SELECT count(*) FROM mytable WHERE DaysSinceEpoch <= 16312 AND Carrier = 'DL'",
        "SELECT count(*) FROM mytable WHERE DaysSinceEpoch <= 16312 AND Carrier = 'DL'"));

    //test repeated columns in agg group by query
    query =
        "SELECT ArrTime, ArrTime, count(*), count(*) FROM mytable WHERE DaysSinceEpoch <= 16312 AND Carrier = 'DL' group by ArrTime, ArrTime";
    testQuery(query, Arrays.asList(
        "SELECT ArrTime, ArrTime, count(*) FROM mytable WHERE DaysSinceEpoch <= 16312 AND Carrier = 'DL' group by ArrTime, ArrTime",
        "SELECT ArrTime, ArrTime, count(*) FROM mytable WHERE DaysSinceEpoch <= 16312 AND Carrier = 'DL' group by ArrTime, ArrTime"));
  }

  @Test
  public void testQueryWithOrderby()
      throws Exception {
    //test repeated columns in selection query
    String query = "SELECT ArrTime, Carrier, DaysSinceEpoch FROM mytable ORDER BY DaysSinceEpoch DESC";
    testQuery(query, Collections.singletonList(query));

    //test repeated columns in selection query
    query = "SELECT ArrTime, DaysSinceEpoch, Carrier FROM mytable ORDER BY Carrier DESC";
    testQuery(query, Collections.singletonList(query));

    //test repeated columns in selection query
    query = "SELECT ArrTime, DaysSinceEpoch, Carrier FROM mytable ORDER BY Carrier DESC, ArrTime DESC";
    testQuery(query, Collections.singletonList(query));
  }

  @Test
  public void testQueryWithAlias()
      throws Exception {
    {
      //test same alias name with column name
      String query =
          "SELECT ArrTime AS ArrTime, Carrier AS Carrier, DaysSinceEpoch AS DaysSinceEpoch FROM mytable ORDER BY DaysSinceEpoch DESC";
      testSqlQuery(query, Collections.singletonList(query));

      query =
          "SELECT ArrTime AS ArrTime, DaysSinceEpoch AS DaysSinceEpoch, Carrier AS Carrier FROM mytable ORDER BY Carrier DESC";
      testSqlQuery(query, Collections.singletonList(query));

      query =
          "SELECT ArrTime AS ArrTime, DaysSinceEpoch AS DaysSinceEpoch, Carrier AS Carrier FROM mytable ORDER BY Carrier DESC, ArrTime DESC";
      testSqlQuery(query, Collections.singletonList(query));
    }
    {
      //test single alias
      String query = "SELECT ArrTime, Carrier AS CarrierName, DaysSinceEpoch FROM mytable ORDER BY DaysSinceEpoch DESC";
      testSqlQuery(query, Collections.singletonList(query));

      query = "SELECT count(*) AS cnt, max(ArrTime) as maxArrTime FROM mytable";
      testSqlQuery(query, Collections.singletonList(query));

      query = "SELECT count(*) AS cnt, Carrier AS CarrierName FROM mytable GROUP BY CarrierName ORDER BY cnt";
      testSqlQuery(query, Collections.singletonList(query));
    }
    {
      //test multiple alias
      String query =
          "SELECT ArrTime, Carrier, Carrier AS CarrierName1, Carrier AS CarrierName2, DaysSinceEpoch FROM mytable ORDER BY DaysSinceEpoch DESC";
      testSqlQuery(query, Collections.singletonList(query));

      query = "SELECT count(*) AS cnt, max(ArrTime) as maxArrTime1, max(ArrTime) as maxArrTime2 FROM mytable";
      testSqlQuery(query, Collections.singletonList(query));

      query =
          "SELECT count(*), count(*) AS cnt1, count(*) AS cnt2, Carrier AS CarrierName FROM mytable GROUP BY CarrierName ORDER BY cnt2";
      testSqlQuery(query, Collections.singletonList(query));
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
      String instanceName = instanceList.get(i).asText();
      if (instanceName.startsWith(CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE)) {
        serverName = instanceName;
      } else if (instanceName.startsWith(CommonConstants.Helix.PREFIX_OF_BROKER_INSTANCE)) {
        brokerName = instanceName;
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

  /**
   * Test for DISTINCT clause. Run the PQL query against Pinot
   * execution engine and compare with the output of corresponding
   * SQL query run against H2
   * @throws Exception
   */
  @Test
  public void testDistinctQuery()
      throws Exception {
    // by default 10 rows will be returned, so use high limit
    String pql = "SELECT DISTINCT(Carrier) FROM mytable LIMIT 1000000";
    String sql = "SELECT DISTINCT Carrier FROM mytable";
    testQuery(pql, Collections.singletonList(sql));
    pql = "SELECT DISTINCT Carrier FROM mytable LIMIT 1000000";
    testSqlQuery(pql, Collections.singletonList(sql));

    pql = "SELECT DISTINCT(Carrier, DestAirportID) FROM mytable LIMIT 1000000";
    sql = "SELECT DISTINCT Carrier, DestAirportID FROM mytable";
    testQuery(pql, Collections.singletonList(sql));
    pql = "SELECT DISTINCT Carrier, DestAirportID FROM mytable LIMIT 1000000";
    testSqlQuery(pql, Collections.singletonList(sql));

    pql = "SELECT DISTINCT(Carrier, DestAirportID, DestStateName) FROM mytable LIMIT 1000000";
    sql = "SELECT DISTINCT Carrier, DestAirportID, DestStateName FROM mytable";
    testQuery(pql, Collections.singletonList(sql));
    pql = "SELECT DISTINCT Carrier, DestAirportID, DestStateName FROM mytable LIMIT 1000000";
    testSqlQuery(pql, Collections.singletonList(sql));

    pql = "SELECT DISTINCT(Carrier, DestAirportID, DestCityName) FROM mytable LIMIT 1000000";
    sql = "SELECT DISTINCT Carrier, DestAirportID, DestCityName FROM mytable";
    testQuery(pql, Collections.singletonList(sql));
    pql = "SELECT DISTINCT Carrier, DestAirportID, DestCityName FROM mytable LIMIT 1000000";
    testSqlQuery(pql, Collections.singletonList(sql));
  }

  @Test
  public void testNonAggregationGroupByQuery()
      throws Exception {
    // by default 10 rows will be returned, so use high limit
    String pql = "SELECT Carrier FROM mytable GROUP BY Carrier LIMIT 1000000";
    String sql = "SELECT Carrier FROM mytable GROUP BY Carrier";
    testSqlQuery(pql, Collections.singletonList(sql));

    pql = "SELECT Carrier, DestAirportID FROM mytable GROUP BY Carrier, DestAirportID LIMIT 1000000";
    sql = "SELECT Carrier, DestAirportID FROM mytable GROUP BY Carrier, DestAirportID";
    testSqlQuery(pql, Collections.singletonList(sql));

    pql =
        "SELECT Carrier, DestAirportID, DestStateName FROM mytable GROUP BY Carrier, DestAirportID, DestStateName LIMIT 1000000";
    sql = "SELECT Carrier, DestAirportID, DestStateName FROM mytable GROUP BY Carrier, DestAirportID, DestStateName";
    testSqlQuery(pql, Collections.singletonList(sql));

    pql =
        "SELECT Carrier, DestAirportID, DestCityName FROM mytable GROUP BY Carrier, DestAirportID, DestCityName LIMIT 1000000";
    sql = "SELECT Carrier, DestAirportID, DestCityName FROM mytable GROUP BY Carrier, DestAirportID, DestCityName";
    testSqlQuery(pql, Collections.singletonList(sql));

    pql = "SELECT ArrTime-DepTime FROM mytable GROUP BY ArrTime, DepTime LIMIT 1000000";
    sql = "SELECT ArrTime-DepTime FROM mytable GROUP BY ArrTime, DepTime";
    testSqlQuery(pql, Collections.singletonList(sql));

    pql = "SELECT ArrTime-DepTime,ArrTime/3,DepTime*2 FROM mytable GROUP BY ArrTime, DepTime LIMIT 1000000";
    sql = "SELECT ArrTime-DepTime,ArrTime/3,DepTime*2 FROM mytable GROUP BY ArrTime, DepTime";
    testSqlQuery(pql, Collections.singletonList(sql));

    pql = "SELECT ArrTime+DepTime FROM mytable GROUP BY ArrTime + DepTime LIMIT 1000000";
    sql = "SELECT ArrTime+DepTime FROM mytable GROUP BY ArrTime + DepTime";
    testSqlQuery(pql, Collections.singletonList(sql));
  }

  @Test
  public void testCaseInsensitivity() {
    int daysSinceEpoch = 16138;
    int hoursSinceEpoch = 16138 * 24;
    int secondsSinceEpoch = 16138 * 24 * 60 * 60;
    List<String> baseQueries = Arrays.asList("SELECT * FROM mytable",
        "SELECT DaysSinceEpoch, timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM mytable",
        "SELECT DaysSinceEpoch, timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM mytable order by DaysSinceEpoch limit 10000",
        "SELECT DaysSinceEpoch, timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM mytable order by timeConvert(DaysSinceEpoch,'DAYS','SECONDS') DESC limit 10000",
        "SELECT count(*) FROM mytable WHERE DaysSinceEpoch = " + daysSinceEpoch,
        "SELECT count(*) FROM mytable WHERE timeConvert(DaysSinceEpoch,'DAYS','HOURS') = " + hoursSinceEpoch,
        "SELECT count(*) FROM mytable WHERE timeConvert(DaysSinceEpoch,'DAYS','SECONDS') = " + secondsSinceEpoch,
        "SELECT MAX(timeConvert(DaysSinceEpoch,'DAYS','SECONDS')) FROM mytable",
        "SELECT COUNT(*) FROM mytable GROUP BY dateTimeConvert(DaysSinceEpoch,'1:DAYS:EPOCH','1:HOURS:EPOCH','1:HOURS')");
    List<String> queries = new ArrayList<>();
    baseQueries.forEach(q -> queries.add(q.replace("mytable", "MYTABLE").replace("DaysSinceEpoch", "DAYSSinceEpOch")));
    baseQueries
        .forEach(q -> queries.add(q.replace("mytable", "MYDB.MYTABLE").replace("DaysSinceEpoch", "DAYSSinceEpOch")));

    for (String query : queries) {
      try {
        JsonNode response = postQuery(query);
        assertTrue(response.get("numSegmentsProcessed").asLong() >= 1L, "PQL: " + query + " failed");

        response = postSqlQuery(query);
        assertTrue(response.get("numSegmentsProcessed").asLong() >= 1L, "SQL: " + query + " failed");
      } catch (Exception e) {
        // Fail the test when exception caught
        throw new RuntimeException("Got Exceptions from query - " + query);
      }
    }
  }

  @Test
  public void testColumnNameContainsTableName() {
    int daysSinceEpoch = 16138;
    int hoursSinceEpoch = 16138 * 24;
    int secondsSinceEpoch = 16138 * 24 * 60 * 60;
    List<String> baseQueries = Arrays.asList("SELECT * FROM mytable",
        "SELECT DaysSinceEpoch, timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM mytable",
        "SELECT DaysSinceEpoch, timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM mytable order by DaysSinceEpoch limit 10000",
        "SELECT DaysSinceEpoch, timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM mytable order by timeConvert(DaysSinceEpoch,'DAYS','SECONDS') DESC limit 10000",
        "SELECT count(*) FROM mytable WHERE DaysSinceEpoch = " + daysSinceEpoch,
        "SELECT count(*) FROM mytable WHERE timeConvert(DaysSinceEpoch,'DAYS','HOURS') = " + hoursSinceEpoch,
        "SELECT count(*) FROM mytable WHERE timeConvert(DaysSinceEpoch,'DAYS','SECONDS') = " + secondsSinceEpoch,
        "SELECT MAX(timeConvert(DaysSinceEpoch,'DAYS','SECONDS')) FROM mytable",
        "SELECT COUNT(*) FROM mytable GROUP BY dateTimeConvert(DaysSinceEpoch,'1:DAYS:EPOCH','1:HOURS:EPOCH','1:HOURS')");
    List<String> queries = new ArrayList<>();
    baseQueries.forEach(q -> queries.add(q.replace("DaysSinceEpoch", "mytable.DAYSSinceEpOch")));
    baseQueries.forEach(q -> queries.add(q.replace("DaysSinceEpoch", "mytable.DAYSSinceEpOch")));

    for (String query : queries) {
      try {
        JsonNode response = postQuery(query);
        assertTrue(response.get("numSegmentsProcessed").asLong() >= 1L, "PQL: " + query + " failed");

        response = postSqlQuery(query);
        assertTrue(response.get("numSegmentsProcessed").asLong() >= 1L, "SQL: " + query + " failed");
      } catch (Exception e) {
        // Fail the test when exception caught
        throw new RuntimeException("Got Exceptions from query - " + query);
      }
    }
  }

  @Test
  public void testCaseInsensitivityWithColumnNameContainsTableName() {
    int daysSinceEpoch = 16138;
    int hoursSinceEpoch = 16138 * 24;
    int secondsSinceEpoch = 16138 * 24 * 60 * 60;
    List<String> baseQueries = Arrays.asList("SELECT * FROM mytable",
        "SELECT DaysSinceEpoch, timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM mytable",
        "SELECT DaysSinceEpoch, timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM mytable order by DaysSinceEpoch limit 10000",
        "SELECT DaysSinceEpoch, timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM mytable order by timeConvert(DaysSinceEpoch,'DAYS','SECONDS') DESC limit 10000",
        "SELECT count(*) FROM mytable WHERE DaysSinceEpoch = " + daysSinceEpoch,
        "SELECT count(*) FROM mytable WHERE timeConvert(DaysSinceEpoch,'DAYS','HOURS') = " + hoursSinceEpoch,
        "SELECT count(*) FROM mytable WHERE timeConvert(DaysSinceEpoch,'DAYS','SECONDS') = " + secondsSinceEpoch,
        "SELECT MAX(timeConvert(DaysSinceEpoch,'DAYS','SECONDS')) FROM mytable",
        "SELECT COUNT(*) FROM mytable GROUP BY dateTimeConvert(DaysSinceEpoch,'1:DAYS:EPOCH','1:HOURS:EPOCH','1:HOURS')");
    List<String> queries = new ArrayList<>();
    baseQueries
        .forEach(q -> queries.add(q.replace("mytable", "MYTABLE").replace("DaysSinceEpoch", "MYTABLE.DAYSSinceEpOch")));
    baseQueries.forEach(
        q -> queries.add(q.replace("mytable", "MYDB.MYTABLE").replace("DaysSinceEpoch", "MYTABLE.DAYSSinceEpOch")));

    for (String query : queries) {
      try {
        JsonNode response = postQuery(query);
        assertTrue(response.get("numSegmentsProcessed").asLong() >= 1L, "PQL: " + query + " failed");

        response = postSqlQuery(query);
        assertTrue(response.get("numSegmentsProcessed").asLong() >= 1L, "SQL: " + query + " failed");
      } catch (Exception e) {
        // Fail the test when exception caught
        throw new RuntimeException("Got Exceptions from query - " + query);
      }
    }
  }

  @Test
  public void testQuerySourceWithDatabaseName()
      throws Exception {
    // by default 10 rows will be returned, so use high limit
    String pql = "SELECT DISTINCT(Carrier) FROM mytable LIMIT 1000000";
    String sql = "SELECT DISTINCT Carrier FROM mytable";
    testQuery(pql, Collections.singletonList(sql));
    pql = "SELECT DISTINCT Carrier FROM db.mytable LIMIT 1000000";
    testSqlQuery(pql, Collections.singletonList(sql));
  }

  @Test
  public void testDistinctCountHll()
      throws Exception {
    String query;

    // The Accurate value is 6538.
    query = "SELECT distinctCount(FlightNum) FROM mytable ";
    assertEquals(postQuery(query).get("aggregationResults").get(0).get("value").asLong(), 6538);
    assertEquals(postSqlQuery(query, _brokerBaseApiUrl).get("resultTable").get("rows").get(0).get(0).asLong(), 6538);

    // Expected distinctCountHll with different log2m value from 2 to 19. The Accurate value is 6538.
    long[] expectedResults =
        new long[]{3504, 6347, 8877, 9729, 9046, 7672, 7538, 6993, 6649, 6651, 6553, 6525, 6459, 6523, 6532, 6544, 6538, 6539};

    for (int i = 2; i < 20; i++) {
      query = String.format("SELECT distinctCountHLL(FlightNum, %d) FROM mytable ", i);
      assertEquals(postQuery(query).get("aggregationResults").get(0).get("value").asLong(), expectedResults[i - 2]);
      assertEquals(postSqlQuery(query, _brokerBaseApiUrl).get("resultTable").get("rows").get(0).get(0).asLong(),
          expectedResults[i - 2]);
    }

    // Default HLL is set as log2m=12
    query = "SELECT distinctCountHLL(FlightNum) FROM mytable ";
    assertEquals(postQuery(query).get("aggregationResults").get(0).get("value").asLong(), expectedResults[10]);
    assertEquals(postSqlQuery(query, _brokerBaseApiUrl).get("resultTable").get("rows").get(0).get(0).asLong(),
        expectedResults[10]);
  }

  @Test
  public void testAggregationFunctionsWithUnderscore()
      throws Exception {
    String query;

    // The Accurate value is 6538.
    query = "SELECT distinct_count(FlightNum) FROM mytable ";
    assertEquals(postQuery(query).get("aggregationResults").get(0).get("value").asLong(), 6538);
    assertEquals(postSqlQuery(query, _brokerBaseApiUrl).get("resultTable").get("rows").get(0).get(0).asLong(), 6538);

    // The Accurate value is 6538.
    query = "SELECT c_o_u_n_t(FlightNum) FROM mytable ";
    assertEquals(postQuery(query).get("aggregationResults").get(0).get("value").asLong(), 115545);
    assertEquals(postSqlQuery(query, _brokerBaseApiUrl).get("resultTable").get("rows").get(0).get(0).asLong(), 115545);
  }

  @Test
  public void testGrpcQueryServer()
      throws Exception {
    GrpcQueryClient queryClient = new GrpcQueryClient("localhost", CommonConstants.Server.DEFAULT_GRPC_PORT);
    String sql = "SELECT * FROM mytable_OFFLINE LIMIT 1000000";
    BrokerRequest brokerRequest = new Pql2Compiler().compileToBrokerRequest(sql);
    List<String> segments = _helixResourceManager.getSegmentsFor("mytable_OFFLINE");

    GrpcRequestBuilder requestBuilder = new GrpcRequestBuilder().setSegments(segments);
    testNonStreamingRequest(queryClient.submit(requestBuilder.setSql(sql).build()));
    testNonStreamingRequest(queryClient.submit(requestBuilder.setBrokerRequest(brokerRequest).build()));

    requestBuilder.setEnableStreaming(true);
    testStreamingRequest(queryClient.submit(requestBuilder.setSql(sql).build()));
    testStreamingRequest(queryClient.submit(requestBuilder.setBrokerRequest(brokerRequest).build()));
  }

  private void testNonStreamingRequest(Iterator<Server.ServerResponse> nonStreamingResponses)
      throws Exception {
    int expectedNumDocs = (int) getCountStarResult();
    assertTrue(nonStreamingResponses.hasNext());
    Server.ServerResponse nonStreamingResponse = nonStreamingResponses.next();
    assertEquals(nonStreamingResponse.getMetadataMap().get(CommonConstants.Query.Response.MetadataKeys.RESPONSE_TYPE),
        CommonConstants.Query.Response.ResponseType.NON_STREAMING);
    DataTable dataTable = DataTableFactory.getDataTable(nonStreamingResponse.getPayload().asReadOnlyByteBuffer());
    assertNotNull(dataTable.getDataSchema());
    assertEquals(dataTable.getNumberOfRows(), expectedNumDocs);
    Map<String, String> metadata = dataTable.getMetadata();
    assertEquals(metadata.get(MetadataKey.NUM_DOCS_SCANNED.getName()), Integer.toString(expectedNumDocs));
  }

  private void testStreamingRequest(Iterator<Server.ServerResponse> streamingResponses)
      throws Exception {
    int expectedNumDocs = (int) getCountStarResult();
    int numTotalDocs = 0;
    while (streamingResponses.hasNext()) {
      Server.ServerResponse streamingResponse = streamingResponses.next();
      DataTable dataTable = DataTableFactory.getDataTable(streamingResponse.getPayload().asReadOnlyByteBuffer());
      String responseType =
          streamingResponse.getMetadataMap().get(CommonConstants.Query.Response.MetadataKeys.RESPONSE_TYPE);
      if (responseType.equals(CommonConstants.Query.Response.ResponseType.DATA)) {
        // verify the returned data table metadata only contains "threadCpuTimeNs".
        Map<String, String> metadata = dataTable.getMetadata();
        assertTrue(metadata.size() == 1 && metadata.containsKey(MetadataKey.THREAD_CPU_TIME_NS.getName()));
        assertNotNull(dataTable.getDataSchema());
        numTotalDocs += dataTable.getNumberOfRows();
      } else {
        assertEquals(responseType, CommonConstants.Query.Response.ResponseType.METADATA);
        assertFalse(streamingResponses.hasNext());
        assertEquals(numTotalDocs, expectedNumDocs);
        assertNull(dataTable.getDataSchema());
        assertEquals(dataTable.getNumberOfRows(), 0);
        Map<String, String> metadata = dataTable.getMetadata();
        assertEquals(metadata.get(MetadataKey.NUM_DOCS_SCANNED.getName()), Integer.toString(expectedNumDocs));
      }
    }
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
    JsonNode oneColumnResponse = JsonUtils
        .stringToJsonNode(sendGetRequest(_controllerBaseApiUrl + "/tables/mytable/metadata?columns=DestCityMarketID"));
    assertEquals(oneColumnResponse.get(DISK_SIZE_IN_BYTES_KEY).asInt(), DISK_SIZE_IN_BYTES);
    assertEquals(oneColumnResponse.get(NUM_SEGMENTS_KEY).asInt(), NUM_SEGMENTS);
    assertEquals(oneColumnResponse.get(NUM_ROWS_KEY).asInt(), NUM_ROWS);
    assertEquals(oneColumnResponse.get(COLUMN_LENGTH_MAP_KEY).size(), 1);
    assertEquals(oneColumnResponse.get(COLUMN_CARDINALITY_MAP_KEY).size(), 1);

    JsonNode threeColumnsResponse = JsonUtils.stringToJsonNode(sendGetRequest(_controllerBaseApiUrl
        + "/tables/mytable/metadata?columns=DivActualElapsedTime&columns=CRSElapsedTime&columns=OriginStateName"));
    assertEquals(threeColumnsResponse.get(DISK_SIZE_IN_BYTES_KEY).asInt(), DISK_SIZE_IN_BYTES);
    assertEquals(threeColumnsResponse.get(NUM_SEGMENTS_KEY).asInt(), NUM_SEGMENTS);
    assertEquals(threeColumnsResponse.get(NUM_ROWS_KEY).asInt(), NUM_ROWS);
    assertEquals(threeColumnsResponse.get(COLUMN_LENGTH_MAP_KEY).size(), 3);
    assertEquals(threeColumnsResponse.get(COLUMN_CARDINALITY_MAP_KEY).size(), 3);

    JsonNode zeroColumnResponse =
        JsonUtils.stringToJsonNode(sendGetRequest(_controllerBaseApiUrl + "/tables/mytable/metadata"));
    assertEquals(zeroColumnResponse.get(DISK_SIZE_IN_BYTES_KEY).asInt(), DISK_SIZE_IN_BYTES);
    assertEquals(zeroColumnResponse.get(NUM_SEGMENTS_KEY).asInt(), NUM_SEGMENTS);
    assertEquals(zeroColumnResponse.get(NUM_ROWS_KEY).asInt(), NUM_ROWS);
    assertEquals(zeroColumnResponse.get(COLUMN_LENGTH_MAP_KEY).size(), 0);
    assertEquals(zeroColumnResponse.get(COLUMN_CARDINALITY_MAP_KEY).size(), 0);

    JsonNode allColumnResponse =
        JsonUtils.stringToJsonNode(sendGetRequest(_controllerBaseApiUrl + "/tables/mytable/metadata?columns=*"));
    assertEquals(allColumnResponse.get(DISK_SIZE_IN_BYTES_KEY).asInt(), DISK_SIZE_IN_BYTES);
    assertEquals(allColumnResponse.get(NUM_SEGMENTS_KEY).asInt(), NUM_SEGMENTS);
    assertEquals(allColumnResponse.get(NUM_ROWS_KEY).asInt(), NUM_ROWS);
    assertEquals(allColumnResponse.get(COLUMN_LENGTH_MAP_KEY).size(), 82);
    assertEquals(allColumnResponse.get(COLUMN_CARDINALITY_MAP_KEY).size(), 82);

    allColumnResponse = JsonUtils.stringToJsonNode(sendGetRequest(
        _controllerBaseApiUrl + "/tables/mytable/metadata?columns=CRSElapsedTime&columns=*&columns=OriginStateName"));
    assertEquals(allColumnResponse.get(DISK_SIZE_IN_BYTES_KEY).asInt(), DISK_SIZE_IN_BYTES);
    assertEquals(allColumnResponse.get(NUM_SEGMENTS_KEY).asInt(), NUM_SEGMENTS);
    assertEquals(allColumnResponse.get(NUM_ROWS_KEY).asInt(), NUM_ROWS);
    assertEquals(allColumnResponse.get(COLUMN_LENGTH_MAP_KEY).size(), 82);
    assertEquals(allColumnResponse.get(COLUMN_CARDINALITY_MAP_KEY).size(), 82);
  }
}
