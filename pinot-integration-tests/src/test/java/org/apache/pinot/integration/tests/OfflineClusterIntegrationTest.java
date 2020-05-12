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
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.core.indexsegment.generator.SegmentVersion;
import org.apache.pinot.spi.config.table.QueryConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
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

  private static final List<String> UPDATED_BLOOM_FILTER_COLUMNS = Collections.singletonList("Carrier");
  private static final String TEST_UPDATED_BLOOM_FILTER_QUERY = "SELECT COUNT(*) FROM mytable WHERE Carrier = 'CA'";

  // For default columns test
  private static final String SCHEMA_WITH_EXTRA_COLUMNS =
      "On_Time_On_Time_Performance_2014_100k_subset_nonulls_default_column_test_extra_columns.schema";
  private static final String SCHEMA_WITH_MISSING_COLUMNS =
      "On_Time_On_Time_Performance_2014_100k_subset_nonulls_default_column_test_missing_columns.schema";
  private static final String TEST_DEFAULT_COLUMNS_QUERY =
      "SELECT COUNT(*) FROM mytable WHERE NewAddedIntDimension < 0";
  private static final String SELECT_STAR_QUERY = "SELECT * FROM mytable";

  private final List<ServiceStatus.ServiceStatusCallback> _serviceStatusCallbacks =
      new ArrayList<>(getNumBrokers() + getNumServers());

  protected int getNumBrokers() {
    return NUM_BROKERS;
  }

  protected int getNumServers() {
    return NUM_SERVERS;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBrokers(getNumBrokers());
    startServers(getNumServers());

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    ExecutorService executor = Executors.newCachedThreadPool();

    // Create segments from Avro data
    ClusterIntegrationTestUtils
        .buildSegmentsFromAvro(avroFiles, 0, _segmentDir, _tarDir, getTableName(), getTimeColumnName(), null,
            getRawIndexColumns(), null, executor);

    // Load data into H2
    setUpH2Connection(avroFiles, executor);

    // Initialize query generator
    setUpQueryGenerator(avroFiles, executor);

    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.MINUTES);

    // Create the table
    addOfflineTable(getTableName(), null, null, null, null, getLoadMode(), SegmentVersion.v1, getInvertedIndexColumns(),
        getBloomFilterIndexColumns(), getTaskConfig(), null, null);

    // Upload all segments
    uploadSegments(getTableName(), _tarDir);

    // Set up service status callbacks
    // NOTE: put this step after creating the table and uploading all segments so that brokers and servers can find the
    // resources to monitor
    registerCallbackHandlers();

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
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
    TableConfig tableConfig = _helixResourceManager.getOfflineTableConfig(getTableName());
    assertNotNull(tableConfig);

    // Set timeout as 5ms so that query will timeout
    tableConfig.setQueryConfig(new QueryConfig(5L));
    _helixResourceManager.updateTableConfig(tableConfig);

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
    _helixResourceManager.updateTableConfig(tableConfig);

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
    OfflineSegmentZKMetadata segmentZKMetadata = _helixResourceManager.getOfflineSegmentMetadata(getTableName()).get(0);
    String segmentName = segmentZKMetadata.getSegmentName();
    long crc = segmentZKMetadata.getCrc();
    // Creation time is when the segment gets created
    long creationTime = segmentZKMetadata.getCreationTime();
    // Push time is when the segment gets first pushed (new segment)
    long pushTime = segmentZKMetadata.getPushTime();
    // Refresh time is when the segment gets refreshed (existing segment)
    long refreshTime = segmentZKMetadata.getRefreshTime();

    uploadSegments(getTableName(), _tarDir);
    for (OfflineSegmentZKMetadata segmentZKMetadataAfterUpload : _helixResourceManager
        .getOfflineSegmentMetadata(getTableName())) {
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
    final long numTotalDocs = getCountStarResult();

    JsonNode queryResponse = postQuery(TEST_UPDATED_INVERTED_INDEX_QUERY);
    assertEquals(queryResponse.get("numEntriesScannedInFilter").asLong(), numTotalDocs);

    // Update table config and trigger reload
    updateOfflineTable(getTableName(), null, null, null, null, getLoadMode(), SegmentVersion.v1,
        UPDATED_INVERTED_INDEX_COLUMNS, null, getTaskConfig(), null, null);

    sendPostRequest(_controllerBaseApiUrl + "/tables/mytable/segments/reload?type=offline", null);

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
  }

  @Test
  public void testBloomFilterTriggering()
      throws Exception {
    final long numTotalDocs = getCountStarResult();
    JsonNode queryResponse = postQuery(TEST_UPDATED_BLOOM_FILTER_QUERY);
    assertEquals(queryResponse.get("numSegmentsProcessed").asLong(), NUM_SEGMENTS);

    // Update table config and trigger reload
    updateOfflineTable(getTableName(), null, null, null, null, getLoadMode(), SegmentVersion.v1, null,
        UPDATED_BLOOM_FILTER_COLUMNS, getTaskConfig(), null, null);

    sendPostRequest(_controllerBaseApiUrl + "/tables/mytable/segments/reload?type=offline", null);

    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode queryResponse1 = postQuery(TEST_UPDATED_BLOOM_FILTER_QUERY);
        // Total docs should not change during reload
        assertEquals(queryResponse1.get("totalDocs").asLong(), numTotalDocs);
        return queryResponse1.get("numSegmentsProcessed").asLong() == 0L;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 600_000L, "Failed to generate inverted index");
  }

  /**
   * We will add extra new columns to the schema to test adding new columns with default value to the offline segments.
   * <p>New columns are: (name, field type, data type, single/multi value, default null value)
   * <ul>
   *   <li>"newAddedIntMetric", METRIC, INT, single-value, 1</li>
   *   <li>"newAddedLongMetric", METRIC, LONG, single-value, 1</li>
   *   <li>"newAddedFloatMetric", METRIC, FLOAT, single-value, default (0.0)</li>
   *   <li>"newAddedDoubleMetric", METRIC, DOUBLE, single-value, default (0.0)</li>
   *   <li>"newAddedIntDimension", DIMENSION, INT, single-value, default (Integer.MIN_VALUE)</li>
   *   <li>"newAddedLongDimension", DIMENSION, LONG, single-value, default (Long.MIN_VALUE)</li>
   *   <li>"newAddedFloatDimension", DIMENSION, FLOAT, single-value, default (Float.NEGATIVE_INFINITY)</li>
   *   <li>"newAddedDoubleDimension", DIMENSION, DOUBLE, single-value, default (Double.NEGATIVE_INFINITY)</li>
   *   <li>"newAddedSVStringDimension", DIMENSION, STRING, single-value, default ("null")</li>
   *   <li>"newAddedMVStringDimension", DIMENSION, STRING, multi-value, ""</li>
   * </ul>
   */
  @Test
  public void testDefaultColumns()
      throws Exception {
    long numTotalDocs = getCountStarResult();

    reloadDefaultColumns(true);
    JsonNode queryResponse = postQuery(SELECT_STAR_QUERY);
    assertEquals(queryResponse.get("totalDocs").asLong(), numTotalDocs);
    assertEquals(queryResponse.get("selectionResults").get("columns").size(), 89);

    testNewAddedColumns();

    reloadDefaultColumns(false);
    queryResponse = postQuery(SELECT_STAR_QUERY);
    assertEquals(queryResponse.get("totalDocs").asLong(), numTotalDocs);
    assertEquals(queryResponse.get("selectionResults").get("columns").size(), 79);
  }

  private void reloadDefaultColumns(final boolean withExtraColumns)
      throws Exception {
    final long numTotalDocs = getCountStarResult();

    if (withExtraColumns) {
      sendSchema(SCHEMA_WITH_EXTRA_COLUMNS);
    } else {
      sendSchema(SCHEMA_WITH_MISSING_COLUMNS);
    }

    // Trigger reload
    sendPostRequest(_controllerBaseApiUrl + "/tables/mytable/segments/reload?type=offline", null);

    String errorMessage;
    if (withExtraColumns) {
      errorMessage = "Failed to add default columns";
    } else {
      errorMessage = "Failed to remove default columns";
    }

    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode queryResponse = postQuery(TEST_DEFAULT_COLUMNS_QUERY);
        // Total docs should not change during reload
        assertEquals(queryResponse.get("totalDocs").asLong(), numTotalDocs);
        long count = queryResponse.get("aggregationResults").get(0).get("value").asLong();
        if (withExtraColumns) {
          return count == numTotalDocs;
        } else {
          return count == 0;
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 600_000L, errorMessage);
  }

  private void sendSchema(String resourceName)
      throws Exception {
    URL resource = OfflineClusterIntegrationTest.class.getClassLoader().getResource(resourceName);
    assertNotNull(resource);
    File schemaFile = new File(resource.getFile());
    addSchema(schemaFile, getTableName());
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
    Assert.assertNotNull(selectionResults);
    Assert.assertTrue(selectionResults.size() > 0);
    for (int i = 0; i < selectionResults.size(); i++) {
      long daysSinceEpoch = selectionResults.get(i).get(0).asLong();
      long secondsSinceEpoch = selectionResults.get(i).get(1).asLong();
      Assert.assertEquals(daysSinceEpoch * 24 * 60 * 60, secondsSinceEpoch);
    }

    pqlQuery =
        "SELECT DaysSinceEpoch, timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM mytable order by DaysSinceEpoch limit 10000";
    response = postQuery(pqlQuery);
    selectionResults = (ArrayNode) response.get("selectionResults").get("results");
    Assert.assertNotNull(selectionResults);
    Assert.assertTrue(selectionResults.size() > 0);
    long prevValue = -1;
    for (int i = 0; i < selectionResults.size(); i++) {
      long daysSinceEpoch = selectionResults.get(i).get(0).asLong();
      long secondsSinceEpoch = selectionResults.get(i).get(1).asLong();
      Assert.assertEquals(daysSinceEpoch * 24 * 60 * 60, secondsSinceEpoch);
      Assert.assertTrue(daysSinceEpoch >= prevValue);
      prevValue = daysSinceEpoch;
    }

    pqlQuery =
        "SELECT DaysSinceEpoch, timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM mytable order by timeConvert(DaysSinceEpoch,'DAYS','SECONDS') DESC limit 10000";
    response = postQuery(pqlQuery);
    selectionResults = (ArrayNode) response.get("selectionResults").get("results");
    Assert.assertNotNull(selectionResults);
    Assert.assertTrue(selectionResults.size() > 0);
    prevValue = Long.MAX_VALUE;
    for (int i = 0; i < selectionResults.size(); i++) {
      long daysSinceEpoch = selectionResults.get(i).get(0).asLong();
      long secondsSinceEpoch = selectionResults.get(i).get(1).asLong();
      Assert.assertEquals(daysSinceEpoch * 24 * 60 * 60, secondsSinceEpoch);
      Assert.assertTrue(secondsSinceEpoch <= prevValue);
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
    JsonNode response1 = postQuery(pqlQuery);

    pqlQuery = "SELECT count(*) FROM mytable WHERE timeConvert(DaysSinceEpoch,'DAYS','SECONDS') = " + secondsSinceEpoch;
    JsonNode response2 = postQuery(pqlQuery);

    pqlQuery = "SELECT count(*) FROM mytable WHERE DaysSinceEpoch = " + daysSinceEpoch
        + " OR timeConvert(DaysSinceEpoch,'DAYS','SECONDS') = " + secondsSinceEpoch;
    JsonNode response3 = postQuery(pqlQuery);

    pqlQuery = "SELECT count(*) FROM mytable WHERE DaysSinceEpoch = " + daysSinceEpoch
        + " AND timeConvert(DaysSinceEpoch,'DAYS','SECONDS') = " + secondsSinceEpoch;
    JsonNode response4 = postQuery(pqlQuery);

    pqlQuery =
        "SELECT count(*) FROM mytable WHERE DIV(timeConvert(DaysSinceEpoch,'DAYS','SECONDS'),1) = " + secondsSinceEpoch;
    JsonNode response5 = postQuery(pqlQuery);

    double val1 = response1.get("aggregationResults").get(0).get("value").asDouble();
    double val2 = response2.get("aggregationResults").get(0).get("value").asDouble();
    double val3 = response3.get("aggregationResults").get(0).get("value").asDouble();
    double val4 = response4.get("aggregationResults").get(0).get("value").asDouble();
    double val5 = response5.get("aggregationResults").get(0).get("value").asDouble();
    Assert.assertEquals(val1, val2);
    Assert.assertEquals(val1, val3);
    Assert.assertEquals(val1, val4);
    Assert.assertEquals(val1, val5);
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
      //System.out.println(response1);
      pqlQuery = "SELECT count(*) FROM mytable WHERE Origin = \"" + origin
          + "\" AND timeConvert(DaysSinceEpoch,'DAYS','SECONDS') = " + secondsSinceEpoch;
      JsonNode response2 = postQuery(pqlQuery);
      //System.out.println(response2);
      double val1 = response1.get("aggregationResults").get(0).get("value").asDouble();
      double val2 = response2.get("aggregationResults").get(0).get("value").asDouble();
      Assert.assertEquals(val1, val2);
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
    String deleteInstanceRequest = _controllerRequestURLBuilder.forInstanceDelete("potato");
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
    deleteInstanceRequest = _controllerRequestURLBuilder.forInstanceDelete(serverName);
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
      deleteInstanceRequest = _controllerRequestURLBuilder.forInstanceDelete(brokerName);
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

    pql = "SELECT DISTINCT(Carrier, DestAirportID) FROM mytable LIMIT 1000000";
    sql = "SELECT DISTINCT Carrier, DestAirportID FROM mytable";
    testQuery(pql, Collections.singletonList(sql));

    pql = "SELECT DISTINCT(Carrier, DestAirportID, DestStateName) FROM mytable LIMIT 1000000";
    sql = "SELECT DISTINCT Carrier, DestAirportID, DestStateName FROM mytable";
    testQuery(pql, Collections.singletonList(sql));

    pql = "SELECT DISTINCT(Carrier, DestAirportID, DestCityName) FROM mytable LIMIT 1000000";
    sql = "SELECT DISTINCT Carrier, DestAirportID, DestCityName FROM mytable";
    testQuery(pql, Collections.singletonList(sql));
  }

  @Test
  public void testCaseInsensitivity()
      throws Exception {
    addSchema(getSchemaFile(), getTableName());
    List<String> queries = new ArrayList<>();
    int daysSinceEpoch = 16138;
    long secondsSinceEpoch = 16138 * 24 * 60 * 60;
    queries.add("SELECT * FROM mytable");
    queries.add("SELECT DaysSinceEpoch, timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM mytable");
    queries.add(
        "SELECT DaysSinceEpoch, timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM mytable order by DaysSinceEpoch limit 10000");
    queries.add(
        "SELECT DaysSinceEpoch, timeConvert(DaysSinceEpoch,'DAYS','SECONDS') FROM mytable order by timeConvert(DaysSinceEpoch,'DAYS','SECONDS') DESC limit 10000");
    queries.add("SELECT count(*) FROM mytable WHERE DaysSinceEpoch = " + daysSinceEpoch);
    queries
        .add("SELECT count(*) FROM mytable WHERE timeConvert(DaysSinceEpoch,'DAYS','SECONDS') = " + secondsSinceEpoch);
    queries.add("SELECT count(*) FROM mytable WHERE timeConvert(DaysSinceEpoch,'DAYS','SECONDS') = " + daysSinceEpoch);
    queries.add("SELECT MAX(timeConvert(DaysSinceEpoch,'DAYS','SECONDS')) FROM mytable");
    queries.add(
        "SELECT COUNT(*) FROM mytable GROUP BY dateTimeConvert(DaysSinceEpoch,'1:DAYS:EPOCH','1:HOURS:EPOCH','1:HOURS')");
    queries.replaceAll(query -> query.replace("mytable", "MYTABLE").replace("DaysSinceEpoch", "DAYSSinceEpOch"));

    // Wait for at most 10 seconds for broker to get the ZK callback of the schema change
    TestUtils.waitForCondition(aVoid -> {
      try {
        for (String query : queries) {
          JsonNode response = postQuery(query);
          // NOTE: When table does not exist, we will get 'BrokerResourceMissingError'.
          //       When column does not exist, all segments will be pruned and 'numSegmentsProcessed' will be 0.
          return response.get("exceptions").size() == 0 && response.get("numSegmentsProcessed").asInt() > 0;
        }
      } catch (Exception e) {
        // Fail the test when exception caught
        throw new RuntimeException(e);
      }
      return true;
    }, 10_000L, "Failed to get results for case-insensitive queries");
  }
}
