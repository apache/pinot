/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.integration.tests;

import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.FileUploadUtils;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import java.io.File;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration test that tests the auto-generated default columns.
 * <p>In the test, we convert Avro data for 12 segments and run queries against it.
 * <p>We will add extra new columns to the schema to test adding new columns with default value to the offline segments.
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
 *   <li>"newAddedStringDimension", DIMENSION, STRING, multi-value, "newAdded"</li>
 * </ul>
 */
public class DefaultColumnsClusterIntegrationTest extends BaseClusterIntegrationTestWithQueryGenerator {
  protected static final String SCHEMA_WITH_EXTRA_COLUMNS =
      "On_Time_On_Time_Performance_2014_100k_subset_nonulls_default_column_test_extra_columns.schema";
  private static final File TMP_DIR = new File("/tmp/DefaultColumnsClusterIntegrationTest");
  private static final File SEGMENT_DIR = new File("/tmp/DefaultColumnsClusterIntegrationTest/segmentDir");
  private static final File TAR_DIR = new File("/tmp/DefaultColumnsClusterIntegrationTest/tarDir");

  private static final int SEGMENT_COUNT = 12;

  @BeforeClass
  public void setUp()
      throws Exception {
    setUp(true);
  }

  protected void setUp(boolean sendSchema)
      throws Exception {
    // Set up directories.
    FileUtils.deleteQuietly(TMP_DIR);
    Assert.assertTrue(TMP_DIR.mkdirs());
    Assert.assertTrue(SEGMENT_DIR.mkdir());
    Assert.assertTrue(TAR_DIR.mkdir());

    // Start the cluster.
    startZk();
    startController();
    startBroker();
    startServer();

    // Create the table.
    addOfflineTable("DaysSinceEpoch", "daysSinceEpoch", -1, "", null, null, "mytable", SegmentVersion.v1);

    // Add the schema.
    if (sendSchema) {
      sendSchema(SCHEMA_WITH_EXTRA_COLUMNS);
    }

    // Unpack the Avro files.
    List<File> avroFiles = unpackAvroData(TMP_DIR, SEGMENT_COUNT);

    // Load data into H2.
    ExecutorService executor = Executors.newCachedThreadPool();
    setupH2AndInsertAvro(avroFiles, executor);

    // Create segments from Avro data.
    buildSegmentsFromAvro(avroFiles, executor, 0, SEGMENT_DIR, TAR_DIR, "mytable", false, null);

    // Initialize query generator.
    setupQueryGenerator(avroFiles, executor);

    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.MINUTES);

    // Set up a Helix spectator to count the number of segments that are uploaded and unlock the latch once 12 segments
    // are online.
    CountDownLatch latch = setupSegmentCountCountDownLatch("mytable", SEGMENT_COUNT);

    // Upload the segments.
    for (String segmentName : TAR_DIR.list()) {
      File file = new File(TAR_DIR, segmentName);
      FileUploadUtils.sendSegmentFile("localhost", "8998", segmentName, file, file.length());
    }

    // Wait for all segments to be ONLINE.
    latch.await();
    waitForSegmentsOnline();
  }

  @Override
  protected void overrideOfflineServerConf(Configuration configuration) {
    configuration.addProperty(CommonConstants.Server.CONFIG_OF_ENABLE_DEFAULT_COLUMNS, true);
  }

  protected void sendSchema(String resourceName)
      throws Exception {
    URL resource = DefaultColumnsClusterIntegrationTest.class.getClassLoader().getResource(resourceName);
    Assert.assertNotNull(resource);
    File schemaFile = new File(resource.getFile());
    addSchema(schemaFile, "mytable_OFFLINE");
  }

  protected void waitForSegmentsOnline()
      throws Exception {
    long timeInTwoMinutes = System.currentTimeMillis() + 2 * 60 * 1000L;
    while (getCurrentServingNumDocs("mytable") < TOTAL_DOCS) {
      if (System.currentTimeMillis() < timeInTwoMinutes) {
        Thread.sleep(1000);
      } else {
        Assert.fail("Segments were not completely loaded within two minutes");
      }
    }
  }

  @Override
  public void testGeneratedQueries(boolean withMultiValues)
      throws Exception {
    _queryGenerator.setSkipMultiValuePredicates(!withMultiValues);
    int generatedQueryCount = getGeneratedQueryCount();
    for (int i = 0; i < generatedQueryCount; i++) {
      QueryGenerator.Query query = _queryGenerator.generateQuery();
      String pqlQuery = query.generatePql();
      // Exclude "SELECT *" queries because the result will not match.
      while (pqlQuery.startsWith("SELECT *")) {
        query = _queryGenerator.generateQuery();
        pqlQuery = query.generatePql();
      }
      runQuery(pqlQuery, query.generateH2Sql());
    }
  }

  @Override
  protected String getTableName() {
    return "mytable";
  }

  @Test
  public void testNewAddedColumns()
      throws Exception {
    String pqlQuery;
    String sqlQuery;

    // Test queries with each new added columns.
    pqlQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedIntMetric = 1";
    sqlQuery = "SELECT COUNT(*) FROM mytable";
    runQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedLongMetric = 1";
    sqlQuery = "SELECT COUNT(*) FROM mytable";
    runQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedFloatMetric = 0.0";
    sqlQuery = "SELECT COUNT(*) FROM mytable";
    runQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedDoubleMetric = 0.0";
    sqlQuery = "SELECT COUNT(*) FROM mytable";
    runQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedIntDimension < 0";
    sqlQuery = "SELECT COUNT(*) FROM mytable";
    runQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedLongDimension < 0";
    sqlQuery = "SELECT COUNT(*) FROM mytable";
    runQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedFloatDimension < 0.0";
    sqlQuery = "SELECT COUNT(*) FROM mytable";
    runQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedDoubleDimension < 0.0";
    sqlQuery = "SELECT COUNT(*) FROM mytable";
    runQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT COUNT(*) FROM mytable WHERE NewAddedStringDimension = 'newAdded'";
    sqlQuery = "SELECT COUNT(*) FROM mytable";
    runQuery(pqlQuery, Collections.singletonList(sqlQuery));

    // Test queries with new added metric column in aggregation function.
    pqlQuery = "SELECT SUM(NewAddedIntMetric) FROM mytable WHERE DaysSinceEpoch <= 16312";
    sqlQuery = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch <= 16312";
    runQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT SUM(NewAddedIntMetric) FROM mytable WHERE DaysSinceEpoch > 16312";
    sqlQuery = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch > 16312";
    runQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT SUM(NewAddedLongMetric) FROM mytable WHERE DaysSinceEpoch <= 16312";
    sqlQuery = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch <= 16312";
    runQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT SUM(NewAddedLongMetric) FROM mytable WHERE DaysSinceEpoch > 16312";
    sqlQuery = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch > 16312";
    runQuery(pqlQuery, Collections.singletonList(sqlQuery));

    // Test other query forms with new added columns.
    JSONObject response;
    JSONObject groupByResult;
    pqlQuery = "SELECT SUM(NewAddedFloatMetric) FROM mytable GROUP BY NewAddedStringDimension";
    response = postQuery(pqlQuery);
    groupByResult =
        response.getJSONArray("aggregationResults").getJSONObject(0).getJSONArray("groupByResult").getJSONObject(0);
    Assert.assertEquals(groupByResult.getInt("value"), 0);
    Assert.assertEquals(groupByResult.getJSONArray("group").getString(0), "newAdded");
    pqlQuery = "SELECT SUM(NewAddedDoubleMetric) FROM mytable GROUP BY NewAddedIntDimension";
    response = postQuery(pqlQuery);
    groupByResult =
        response.getJSONArray("aggregationResults").getJSONObject(0).getJSONArray("groupByResult").getJSONObject(0);
    Assert.assertEquals(groupByResult.getInt("value"), 0);
    Assert.assertEquals(groupByResult.getJSONArray("group").getString(0), String.valueOf(Integer.MIN_VALUE));
    pqlQuery = "SELECT SUM(NewAddedIntMetric) FROM mytable GROUP BY NewAddedLongDimension";
    response = postQuery(pqlQuery);
    groupByResult =
        response.getJSONArray("aggregationResults").getJSONObject(0).getJSONArray("groupByResult").getJSONObject(0);
    Assert.assertEquals(groupByResult.getInt("value"), TOTAL_DOCS);
    Assert.assertEquals(groupByResult.getJSONArray("group").getString(0), String.valueOf(Long.MIN_VALUE));
    pqlQuery =
        "SELECT SUM(NewAddedIntMetric), SUM(NewAddedLongMetric), SUM(NewAddedFloatMetric), SUM(NewAddedDoubleMetric) "
            + "FROM mytable GROUP BY NewAddedIntDimension, NewAddedLongDimension, NewAddedFloatDimension, "
            + "NewAddedDoubleDimension, NewAddedStringDimension";
    response = postQuery(pqlQuery);
    JSONArray groupByResultArray = response.getJSONArray("aggregationResults");
    groupByResult = groupByResultArray.getJSONObject(0).getJSONArray("groupByResult").getJSONObject(0);
    Assert.assertEquals(groupByResult.getInt("value"), TOTAL_DOCS);
    Assert.assertEquals(groupByResult.getJSONArray("group").getString(0), String.valueOf(Integer.MIN_VALUE));
    Assert.assertEquals(groupByResult.getJSONArray("group").getString(1), String.valueOf(Long.MIN_VALUE));
    Assert.assertEquals(groupByResult.getJSONArray("group").getString(2), String.valueOf(Float.NEGATIVE_INFINITY));
    Assert.assertEquals(groupByResult.getJSONArray("group").getString(3), String.valueOf(Double.NEGATIVE_INFINITY));
    groupByResult = groupByResultArray.getJSONObject(1).getJSONArray("groupByResult").getJSONObject(0);
    Assert.assertEquals(groupByResult.getInt("value"), TOTAL_DOCS);
    Assert.assertEquals(groupByResult.getJSONArray("group").getString(0), String.valueOf(Integer.MIN_VALUE));
    Assert.assertEquals(groupByResult.getJSONArray("group").getString(1), String.valueOf(Long.MIN_VALUE));
    Assert.assertEquals(groupByResult.getJSONArray("group").getString(2), String.valueOf(Float.NEGATIVE_INFINITY));
    Assert.assertEquals(groupByResult.getJSONArray("group").getString(3), String.valueOf(Double.NEGATIVE_INFINITY));
    groupByResult = groupByResultArray.getJSONObject(2).getJSONArray("groupByResult").getJSONObject(0);
    Assert.assertEquals(groupByResult.getInt("value"), 0);
    Assert.assertEquals(groupByResult.getJSONArray("group").getString(0), String.valueOf(Integer.MIN_VALUE));
    Assert.assertEquals(groupByResult.getJSONArray("group").getString(1), String.valueOf(Long.MIN_VALUE));
    Assert.assertEquals(groupByResult.getJSONArray("group").getString(2), String.valueOf(Float.NEGATIVE_INFINITY));
    Assert.assertEquals(groupByResult.getJSONArray("group").getString(3), String.valueOf(Double.NEGATIVE_INFINITY));
    groupByResult = groupByResultArray.getJSONObject(3).getJSONArray("groupByResult").getJSONObject(0);
    Assert.assertEquals(groupByResult.getInt("value"), 0);
    Assert.assertEquals(groupByResult.getJSONArray("group").getString(0), String.valueOf(Integer.MIN_VALUE));
    Assert.assertEquals(groupByResult.getJSONArray("group").getString(1), String.valueOf(Long.MIN_VALUE));
    Assert.assertEquals(groupByResult.getJSONArray("group").getString(2), String.valueOf(Float.NEGATIVE_INFINITY));
    Assert.assertEquals(groupByResult.getJSONArray("group").getString(3), String.valueOf(Double.NEGATIVE_INFINITY));
    pqlQuery = "SELECT * FROM mytable";
    runQuery(pqlQuery, null);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    dropOfflineTable("mytable");
    stopServer();
    stopBroker();
    stopController();
    try {
      stopZk();
    } catch (Exception e) {
      // Swallow ZK Exceptions.
    }
    FileUtils.deleteQuietly(TMP_DIR);
  }
}
