/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.queries;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.linkedin.pinot.core.query.aggregation.function.quantile.tdigest.TDigest;
import org.antlr.runtime.RecognitionException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.client.request.RequestConverter;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.query.QueryExecutor;
import com.linkedin.pinot.common.query.ReduceService;
import com.linkedin.pinot.common.query.gen.AvroQueryGenerator;
import com.linkedin.pinot.common.query.gen.AvroQueryGenerator.TestGroupByAggreationQuery;
import com.linkedin.pinot.common.query.gen.AvroQueryGenerator.TestSimpleAggreationQuery;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.InstanceRequest;
import com.linkedin.pinot.common.response.BrokerResponse;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.core.data.manager.config.FileBasedInstanceDataManagerConfig;
import com.linkedin.pinot.core.data.manager.offline.FileBasedInstanceDataManager;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.query.executor.ServerQueryExecutorV1Impl;
import com.linkedin.pinot.core.query.reduce.DefaultReduceService;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.pql.parsers.PQLCompiler;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;
import com.linkedin.pinot.util.TestUtils;
import com.yammer.metrics.core.MetricsRegistry;


/**
 * Oct 14, 2014
 */

public class QueriesSentinelTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueriesSentinelTest.class);
  private static ReduceService REDUCE_SERVICE = new DefaultReduceService();

  private static final PQLCompiler REQUEST_COMPILER = new PQLCompiler(new HashMap<String, String[]>());
  private final String AVRO_DATA = "data/test_data-mv.avro";
  private static File INDEX_DIR = new File(FileUtils.getTempDirectory() + File.separator + "QueriesSentinelTest");
  private static AvroQueryGenerator AVRO_QUERY_GENERATOR;
  private static QueryExecutor QUERY_EXECUTOR;
  private static TestingServerPropertiesBuilder CONFIG_BUILDER;
  private String segmentName;

  @BeforeClass
  public void setup() throws Exception {
    CONFIG_BUILDER = new TestingServerPropertiesBuilder("testTable");

    setupSegmentFor("testTable");
    setUpTestQueries("testTable");

    final PropertiesConfiguration serverConf = CONFIG_BUILDER.build();
    serverConf.setDelimiterParsingDisabled(false);

    final FileBasedInstanceDataManager instanceDataManager = FileBasedInstanceDataManager.getInstanceDataManager();
    instanceDataManager.init(new FileBasedInstanceDataManagerConfig(serverConf.subset("pinot.server.instance")));
    instanceDataManager.start();

    System.out.println("************************** : " + new File(INDEX_DIR, "segment").getAbsolutePath());
    File segmentFile = new File(INDEX_DIR, "segment").listFiles()[0];
    segmentName = segmentFile.getName();
    final IndexSegment indexSegment = ColumnarSegmentLoader.load(segmentFile, ReadMode.heap);
    instanceDataManager.getTableDataManager("testTable");
    instanceDataManager.getTableDataManager("testTable").addSegment(indexSegment);

    QUERY_EXECUTOR = new ServerQueryExecutorV1Impl(false);
    QUERY_EXECUTOR.init(serverConf.subset("pinot.server.query.executor"), instanceDataManager, new ServerMetrics(
        new MetricsRegistry()));

    TDigest.TEST_ENABLED = true;
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(INDEX_DIR);
    TDigest.TEST_ENABLED = false;
  }

  private void runApproximationQueries(List<? extends AvroQueryGenerator.TestAggreationQuery> queries, double precision)
          throws Exception {
    boolean isAccurate = true;
    Object accurateValue = null;

    int counter = 0;
    final Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    for (final AvroQueryGenerator.TestAggreationQuery query : queries) {
      LOGGER.info("**************************");
      LOGGER.info("running " + counter + " : " + query.getPql());
      final BrokerRequest brokerRequest = RequestConverter.fromJSON(REQUEST_COMPILER.compile(query.getPql()));
      InstanceRequest instanceRequest = new InstanceRequest(counter++, brokerRequest);
      instanceRequest.setSearchSegments(new ArrayList<String>());
      instanceRequest.getSearchSegments().add(segmentName);
      final DataTable instanceResponse = QUERY_EXECUTOR.processQuery(instanceRequest);
      instanceResponseMap.clear();
      instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse);
      final BrokerResponse brokerResponse = REDUCE_SERVICE.reduceOnDataTable(brokerRequest, instanceResponseMap);
      LOGGER.info("BrokerResponse is " + brokerResponse.getAggregationResults().get(0));

      // compute value
      Object val;
      if (query instanceof AvroQueryGenerator.TestSimpleAggreationQuery) {
        val = Double.parseDouble(brokerResponse.getAggregationResults().get(0).getString("value"));
      } else {
        val = brokerResponse.getAggregationResults().get(0).getJSONArray("groupByResult");
      }

      if (isAccurate) {
        // store accurate value
        accurateValue = val;
        isAccurate = false;
      } else {
        // compare value with accurate value
        // it's estimation so we need to test its result within error bound
        if (query instanceof AvroQueryGenerator.TestSimpleAggreationQuery) {
          TestUtils.assertApproximation((Double) val, (Double) accurateValue, precision);
        } else {
          TestUtils.assertJSONArrayApproximation((JSONArray) val, (JSONArray) accurateValue, precision);
        }
        isAccurate = true;
      }
    }
  }

  /**
   * Console output of the last statement may not appear, maybe a result of intellij idea test console redirection.
   * To avoid this, always add assert clauses, and do not rely on the console output.
   *
   * @throws Exception
   */
  @Test
  public void testDistinctCountHLLNoGroupBy() throws Exception {
    final List<TestSimpleAggreationQuery> aggCalls = new ArrayList<TestSimpleAggreationQuery>();
    // distinct count(*) not works
    for (int i = 1; i <= 5; i++) {
      aggCalls.add(new TestSimpleAggreationQuery("select distinctcount(column" + i + ") from testTable limit 0", 0.0));
      aggCalls.add(new TestSimpleAggreationQuery("select distinctcounthll(column" + i + ") from testTable limit 0", 0.0));
    }

    runApproximationQueries(aggCalls, 0.1);
  }

  @Test
  public void testDistinctCountHLLGroupBy() throws Exception {
    final List<TestGroupByAggreationQuery> groupByCalls = new ArrayList<TestGroupByAggreationQuery>();
    for (int i = 1; i <= 5; i++) {
      if (i == 2) {
        continue;
      }
      groupByCalls.add(new TestGroupByAggreationQuery("select distinctcount(column2) from testTable group by column" + i + " limit 0", null));
      groupByCalls.add(new TestGroupByAggreationQuery("select distinctcounthll(column2) from testTable group by column" + i + " limit 0", null));
    }

    runApproximationQueries(groupByCalls, 0.1);
  }

  @Test
  public void testQuantileNoGroupBy() throws Exception {
    final List<TestSimpleAggreationQuery> aggCalls = new ArrayList<TestSimpleAggreationQuery>();

    // 5 single-value columns -- column 3 is String type
    for (int i = 1; i <= 2; i++) {
      aggCalls.add(new TestSimpleAggreationQuery("select percentile50(column" + i + ") from testTable limit 0", 0.0));
      aggCalls.add(new TestSimpleAggreationQuery("select percentileest50(column" + i + ") from testTable limit 0", 0.0));
    }

    runApproximationQueries(aggCalls, 0.1);
  }

  @Test
  public void testQuantileGroupBy() throws Exception {
    final List<TestGroupByAggreationQuery> groupByCalls = new ArrayList<TestGroupByAggreationQuery>();
    final int top = 1000;
    for (int i = 2; i <= 2; i++) {
      if (i == 2) {
        //continue;
      }
      groupByCalls.add(new TestGroupByAggreationQuery("select percentile50(column1) from testTable group by column" + i + " top " + top + " limit 0", null));
      groupByCalls.add(new TestGroupByAggreationQuery("select percentileest50(column1) from testTable group by column" + i + " top " + top + " limit 0", null));
    }

    runApproximationQueries(groupByCalls, 0.1);
  }

  @Test
  public void testAggregation() throws Exception {
    int counter = 0;
    final Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    final List<TestSimpleAggreationQuery> aggCalls = AVRO_QUERY_GENERATOR.giveMeNSimpleAggregationQueries(10000);
    for (final TestSimpleAggreationQuery aggCall : aggCalls) {
      LOGGER.info("running " + counter + " : " + aggCall.pql);
      final BrokerRequest brokerRequest = RequestConverter.fromJSON(REQUEST_COMPILER.compile(aggCall.pql));
      InstanceRequest instanceRequest = new InstanceRequest(counter++, brokerRequest);
      instanceRequest.setSearchSegments(new ArrayList<String>());
      instanceRequest.getSearchSegments().add(segmentName);
      final DataTable instanceResponse = QUERY_EXECUTOR.processQuery(instanceRequest);
      instanceResponseMap.clear();
      instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse);
      final BrokerResponse brokerResponse = REDUCE_SERVICE.reduceOnDataTable(brokerRequest, instanceResponseMap);
      LOGGER.info("BrokerResponse is " + brokerResponse.getAggregationResults().get(0));
      LOGGER.info("Result from avro is : " + aggCall.result);
      try {
        Assert.assertEquals(Double.parseDouble(brokerResponse.getAggregationResults().get(0).getString("value")),
            aggCall.result);
      } catch (AssertionError e) {
        System.out.println(aggCall.pql);
        System.out.println("from broker : "
            + Double.parseDouble(brokerResponse.getAggregationResults().get(0).getString("value")));
        System.out.println("from precomp : " + aggCall.result);
        throw new AssertionError(e);
      }
    }
  }

  @Test
  public void testAggregationGroupBy() throws Exception {
    final List<TestGroupByAggreationQuery> groupByCalls = AVRO_QUERY_GENERATOR.giveMeNGroupByAggregationQueries(10000);
    int counter = 0;
    final Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    for (final TestGroupByAggreationQuery groupBy : groupByCalls) {
      LOGGER.info("running " + counter + " : " + groupBy.pql);
      final BrokerRequest brokerRequest = RequestConverter.fromJSON(REQUEST_COMPILER.compile(groupBy.pql));
      InstanceRequest instanceRequest = new InstanceRequest(counter++, brokerRequest);
      instanceRequest.setSearchSegments(new ArrayList<String>());
      instanceRequest.getSearchSegments().add(segmentName);
      final DataTable instanceResponse = QUERY_EXECUTOR.processQuery(instanceRequest);
      instanceResponseMap.clear();
      instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse);
      final BrokerResponse brokerResponse = REDUCE_SERVICE.reduceOnDataTable(brokerRequest, instanceResponseMap);
      LOGGER.info("BrokerResponse is " + brokerResponse.getAggregationResults().get(0));
      LOGGER.info("Result from avro is : " + groupBy.groupResults);

      try {
        assertGroupByResults(brokerResponse.getAggregationResults().get(0).getJSONArray("groupByResult"),
            groupBy.groupResults);
      } catch (AssertionError e) {
        System.out.println(groupBy.pql);
        System.out.println("from broker : "
            + brokerResponse.getAggregationResults().get(0).getJSONArray("groupByResult").toString());
        System.out.println("from precomp : " + groupBy.groupResults);
        throw new AssertionError(e);
      }
    }
  }

  private void assertGroupByResults(JSONArray jsonArray, Map<Object, Double> groupResultsFromAvro) throws JSONException {
    final Map<String, Double> groupResultsFromQuery = new HashMap<String, Double>();
    if (groupResultsFromAvro.size() > 10) {
      Assert.assertEquals(jsonArray.length(), 10);
    } else {
      Assert.assertTrue(jsonArray.length() >= groupResultsFromAvro.size());
    }
    for (int i = 0; i < jsonArray.length(); ++i) {
      groupResultsFromQuery.put(jsonArray.getJSONObject(i).getJSONArray("group").getString(0),
          jsonArray.getJSONObject(i).getDouble("value"));
    }

    for (final Object key : groupResultsFromAvro.keySet()) {
      String keyString;
      if (key == null) {
        keyString = "null";
      } else {
        keyString = key.toString();
      }
      if (!groupResultsFromQuery.containsKey(keyString)) {
        continue;
      }
      final double actual = groupResultsFromQuery.get(keyString);
      // System.out.println("Result from query - group:" + keyString + ", value:" + actual);
      final double expected = groupResultsFromAvro.get(key);
      // System.out.println("Result from avro - group:" + keyString + ", value:" + expected);
      try {
        Assert.assertEquals(actual, expected);
      } catch (AssertionError e) {
        throw new AssertionError(e);
      }

    }
  }

  private void setUpTestQueries(String table) throws FileNotFoundException, IOException {
    final String filePath = TestUtils.getFileFromResourceUrl(getClass().getClassLoader().getResource(AVRO_DATA));
    System.out.println(filePath);
    final List<String> dims = new ArrayList<String>();
    dims.add("column1");
    dims.add("column2");
    dims.add("column3");
    dims.add("column4");
    dims.add("column5");
    dims.add("column6");
    dims.add("column7");
    dims.add("column8");
    dims.add("column9");
    dims.add("column10");
    dims.add("weeksSinceEpochSunday");
    dims.add("daysSinceEpoch");
    dims.add("count");

    final List<String> mets = new ArrayList<String>();
    mets.add("count");

    final String time = "minutesSinceEpoch";
    AVRO_QUERY_GENERATOR = new AvroQueryGenerator(new File(filePath), dims, mets, time, table);
    AVRO_QUERY_GENERATOR.init();
    AVRO_QUERY_GENERATOR.generateSimpleAggregationOnSingleColumnFilters();
  }

  private void setupSegmentFor(String table) throws Exception {
    final String filePath = TestUtils.getFileFromResourceUrl(getClass().getClassLoader().getResource(AVRO_DATA));

    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }
    INDEX_DIR.mkdir();

    final SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), new File(INDEX_DIR,
            "segment"), "daysSinceEpoch", TimeUnit.DAYS, table);

    final SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();

    driver.init(config);
    driver.build();

    System.out.println("built at : " + INDEX_DIR.getAbsolutePath());
  }

  @Test
  public void testSingleQuery() throws RecognitionException, Exception {
    String query = "select count(*) from testTable where column1='186154188'";
    LOGGER.info("running  : " + query);
    final Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    final BrokerRequest brokerRequest = RequestConverter.fromJSON(REQUEST_COMPILER.compile(query));
    InstanceRequest instanceRequest = new InstanceRequest(1, brokerRequest);
    instanceRequest.setSearchSegments(new ArrayList<String>());
    instanceRequest.getSearchSegments().add(segmentName);
    final DataTable instanceResponse = QUERY_EXECUTOR.processQuery(instanceRequest);
    instanceResponseMap.clear();
    instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse);
    final BrokerResponse brokerResponse = REDUCE_SERVICE.reduceOnDataTable(brokerRequest, instanceResponseMap);
    LOGGER.info("BrokerResponse is " + brokerResponse.getAggregationResults().get(0));
  }

  @Test
  public void testMatchAllQuery() throws RecognitionException, Exception {
    String query = "select count(*),sum(count) from testTable  ";
    LOGGER.info("running  : " + query);
    final Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    final BrokerRequest brokerRequest = RequestConverter.fromJSON(REQUEST_COMPILER.compile(query));
    InstanceRequest instanceRequest = new InstanceRequest(1, brokerRequest);
    instanceRequest.setSearchSegments(new ArrayList<String>());
    instanceRequest.getSearchSegments().add(segmentName);
    final DataTable instanceResponse = QUERY_EXECUTOR.processQuery(instanceRequest);
    instanceResponseMap.clear();
    instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse);
    final BrokerResponse brokerResponse = REDUCE_SERVICE.reduceOnDataTable(brokerRequest, instanceResponseMap);
    LOGGER.info("BrokerResponse is " + brokerResponse);
    LOGGER.info("BrokerResponse is " + brokerResponse.getAggregationResults().get(0));
    LOGGER.info("BrokerResponse is " + brokerResponse.getAggregationResults().get(1));

    Assert.assertEquals(brokerResponse.getAggregationResults().get(0).getInt("value"), 100000);
    Assert.assertEquals(brokerResponse.getAggregationResults().get(1).getDouble("value"), 8.90662862E13);
    Assert.assertEquals(brokerResponse.getNumDocsScanned(), 100000);
  }

  @Test
  public void testRangeQuery() throws RecognitionException, Exception {
    String query = "select count(*) from testTable where column1 in ('999983251', '510705831', '1000720716', '1001058817', '1001099410')";
    LOGGER.info("running  : " + query);
    final Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    final BrokerRequest brokerRequest = RequestConverter.fromJSON(REQUEST_COMPILER.compile(query));
    InstanceRequest instanceRequest = new InstanceRequest(1, brokerRequest);
    instanceRequest.setSearchSegments(new ArrayList<String>());
    instanceRequest.getSearchSegments().add(segmentName);
    final DataTable instanceResponse = QUERY_EXECUTOR.processQuery(instanceRequest);
    instanceResponseMap.clear();
    instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse);
    final BrokerResponse brokerResponse = REDUCE_SERVICE.reduceOnDataTable(brokerRequest, instanceResponseMap);
    LOGGER.info("BrokerResponse is " + brokerResponse.getAggregationResults().get(0));
    Assert.assertEquals(brokerResponse.getAggregationResults().get(0).getInt("value"), 14);
    Assert.assertEquals(brokerResponse.getNumDocsScanned(), 14);
  }
}
