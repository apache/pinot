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
package com.linkedin.pinot.queries;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.query.QueryExecutor;
import com.linkedin.pinot.common.query.QueryRequest;
import com.linkedin.pinot.common.query.ReduceService;
import com.linkedin.pinot.common.query.gen.AvroQueryGenerator;
import com.linkedin.pinot.common.query.gen.AvroQueryGenerator.TestGroupByAggreationQuery;
import com.linkedin.pinot.common.query.gen.AvroQueryGenerator.TestSimpleAggreationQuery;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.InstanceRequest;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.response.broker.BrokerResponseNative;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.manager.config.FileBasedInstanceDataManagerConfig;
import com.linkedin.pinot.core.data.manager.offline.FileBasedInstanceDataManager;
import com.linkedin.pinot.core.data.manager.offline.TableDataManagerProvider;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.utils.AvroUtils;
import com.linkedin.pinot.core.query.executor.ServerQueryExecutorV1Impl;
import com.linkedin.pinot.core.query.reduce.BrokerReduceService;
import com.linkedin.pinot.core.realtime.impl.RealtimeSegmentImpl;
import com.linkedin.pinot.core.realtime.impl.kafka.AvroRecordToPinotRowGenerator;
import com.linkedin.pinot.core.realtime.impl.kafka.RealtimeSegmentImplTest;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;
import com.linkedin.pinot.util.TestUtils;
import com.yammer.metrics.core.MetricsRegistry;


public class RealtimeQueriesSentinelTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeQueriesSentinelTest.class);
  private static ReduceService<BrokerResponseNative> REDUCE_SERVICE = new BrokerReduceService();

  private static final Pql2Compiler REQUEST_COMPILER = new Pql2Compiler();

  private final String AVRO_DATA = "data/test_data-mv.avro";
  private static com.linkedin.pinot.common.data.Schema PINOT_SCHEMA;
  private static AvroQueryGenerator AVRO_QUERY_GENERATOR;
  private static QueryExecutor QUERY_EXECUTOR;
  private static TestingServerPropertiesBuilder CONFIG_BUILDER;
  private static AvroRecordToPinotRowGenerator AVRO_RECORD_TRANSFORMER;
  private static ExecutorService queryRunners = Executors.newFixedThreadPool(20);

  @BeforeClass
  public void setup() throws Exception {
    TableDataManagerProvider.setServerMetrics(new ServerMetrics(new MetricsRegistry()));

    PINOT_SCHEMA = getTestSchema();
    PINOT_SCHEMA.setSchemaName("realtimeSchema");
    AVRO_RECORD_TRANSFORMER = new AvroRecordToPinotRowGenerator(PINOT_SCHEMA);
    final IndexSegment indexSegment = getRealtimeSegment();

    setUpTestQueries("testTable");

    CONFIG_BUILDER = new TestingServerPropertiesBuilder("testTable");
    final PropertiesConfiguration serverConf = CONFIG_BUILDER.build();
    serverConf.setDelimiterParsingDisabled(false);
    final FileBasedInstanceDataManager instanceDataManager = FileBasedInstanceDataManager.getInstanceDataManager();
    instanceDataManager.init(new FileBasedInstanceDataManagerConfig(serverConf.subset("pinot.server.instance")));
    instanceDataManager.start();
    instanceDataManager.getTableDataManager("testTable");
    instanceDataManager.getTableDataManager("testTable").addSegment(indexSegment);

    QUERY_EXECUTOR = new ServerQueryExecutorV1Impl(false);
    QUERY_EXECUTOR.init(serverConf.subset("pinot.server.query.executor"), instanceDataManager, new ServerMetrics(
        new MetricsRegistry()));
  }

  @AfterClass
  public void tearDown() {
  }

  @Test
  public void testAggregation() throws Exception {
    int counter = 0;
    final Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    final List<TestSimpleAggreationQuery> aggCalls = AVRO_QUERY_GENERATOR.giveMeNSimpleAggregationQueries(10000);
    for (final TestSimpleAggreationQuery aggCall : aggCalls) {
      LOGGER.info("running " + counter + " : " + aggCall.pql);
      final BrokerRequest brokerRequest = REQUEST_COMPILER.compileToBrokerRequest(aggCall.pql);
      InstanceRequest instanceRequest = new InstanceRequest(counter++, brokerRequest);
      instanceRequest.setSearchSegments(new ArrayList<String>());
      instanceRequest.getSearchSegments().add("testTable_testTable");
      QueryRequest queryRequest = new QueryRequest(instanceRequest, TableDataManagerProvider.getServerMetrics());
      DataTable instanceResponse = QUERY_EXECUTOR.processQuery(queryRequest, queryRunners);
      instanceResponseMap.clear();
      instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse);
      final BrokerResponseNative brokerResponse = REDUCE_SERVICE.reduceOnDataTable(brokerRequest, instanceResponseMap);
      LOGGER.info("BrokerResponse is " + brokerResponse.getAggregationResults().get(0));
      LOGGER.info("Result from avro is : " + aggCall.result);
      Double actual = Double.parseDouble(brokerResponse.getAggregationResults().get(0).getValue().toString());
      Double expected = aggCall.result;
      try {
        Assert.assertEquals(actual, expected);
      } catch (AssertionError e) {
        System.out.println("********************************");
        System.out.println("query : " + aggCall.pql);
        System.out.println("actual : " + actual);
        System.out.println("expected : " + aggCall.result);
        System.out.println("********************************");
        throw e;
      }

    }
  }

  @Test
  public void test1() throws Exception {
    final Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    String query = "select sum('count') from testTable where column13='1540094560' group by column3 top 100 limit 0";
    Map<Object, Double> fromAvro = new HashMap<Object, Double>();
    fromAvro.put(null, 2.0D);
    fromAvro.put("", 1.2469280068E10D);
    fromAvro.put("F", 127.0D);
    fromAvro.put("A", 20.0D);
    fromAvro.put("H", 29.0D);
    final BrokerRequest brokerRequest = REQUEST_COMPILER.compileToBrokerRequest(query);
    InstanceRequest instanceRequest = new InstanceRequest(485, brokerRequest);
    instanceRequest.setSearchSegments(new ArrayList<String>());
    instanceRequest.getSearchSegments().add("testTable_testTable");
    QueryRequest queryRequest = new QueryRequest(instanceRequest, TableDataManagerProvider.getServerMetrics());
    DataTable instanceResponse = QUERY_EXECUTOR.processQuery(queryRequest, queryRunners);
    instanceResponseMap.clear();
    instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse);
    BrokerResponseNative brokerResponse = REDUCE_SERVICE.reduceOnDataTable(brokerRequest, instanceResponseMap);
    JSONArray actual =
        brokerResponse.toJson().getJSONArray("aggregationResults").getJSONObject(0).getJSONArray("groupByResult");
    assertGroupByResults(actual, fromAvro);
  }

  @Test
  public void testAggregationGroupBy() throws Exception {
    final List<TestGroupByAggreationQuery> groupByCalls = AVRO_QUERY_GENERATOR.giveMeNGroupByAggregationQueries(10000);
    int counter = 0;
    final Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    for (final TestGroupByAggreationQuery groupBy : groupByCalls) {
      LOGGER.info("running " + counter + " : " + groupBy.pql);
      final BrokerRequest brokerRequest = REQUEST_COMPILER.compileToBrokerRequest(groupBy.pql);
      InstanceRequest instanceRequest = new InstanceRequest(counter++, brokerRequest);
      instanceRequest.setSearchSegments(new ArrayList<String>());
      instanceRequest.getSearchSegments().add("testTable_testTable");
      QueryRequest queryRequest = new QueryRequest(instanceRequest, TableDataManagerProvider.getServerMetrics());
      DataTable instanceResponse = QUERY_EXECUTOR.processQuery(queryRequest, queryRunners);
      instanceResponseMap.clear();
      instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse);
      Map<Object, Double> expected = groupBy.groupResults;
      LOGGER.info("Result from avro is : " + expected);
      BrokerResponseNative brokerResponse = REDUCE_SERVICE.reduceOnDataTable(brokerRequest, instanceResponseMap);
      JSONArray actual =
          brokerResponse.toJson().getJSONArray("aggregationResults").getJSONObject(0).getJSONArray("groupByResult");
      try {
        assertGroupByResults(actual, expected);
      } catch (AssertionError e) {
        System.out.println("***************************************");
        System.out.println("query : " + groupBy.pql);
        System.out.println("actual : " + actual.toString(1));
        System.out.println("expected : " + groupBy.groupResults);
        System.out.println("***************************************");
        throw e;
      }

    }
  }

  private void assertGroupByResults(JSONArray jsonArray, Map<Object, Double> groupResultsFromAvro) throws Exception {
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
      // System.out.println("Result from query - group:" + keyString +
      // ", value:" + actual);
      final double expected = groupResultsFromAvro.get(key);
      // System.out.println("Result from avro - group:" + keyString +
      // ", value:" + expected);
      try {
        Assert.assertEquals(actual, expected);
      } catch (AssertionError e) {
        System.out.println("*************************");
        System.out.println("key : " + key);
        System.out.println("keyString : " + keyString);
        System.out.println("actual : " + actual);
        System.out.println("expected : " + expected);
        System.out.println("*************************");
        throw e;
      }

    }
  }

  private void setUpTestQueries(String table) throws FileNotFoundException, IOException {
    final String filePath = TestUtils.getFileFromResourceUrl(getClass().getClassLoader().getResource(AVRO_DATA));
//    System.out.println(filePath);
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
    dims.add("column13");
    dims.add("count");

    final List<String> mets = new ArrayList<String>();
    mets.add("count");

    final String time = "column13";
    AVRO_QUERY_GENERATOR = new AvroQueryGenerator(new File(filePath), dims, mets, time, table, true);
    AVRO_QUERY_GENERATOR.init();
    AVRO_QUERY_GENERATOR.generateSimpleAggregationOnSingleColumnFilters();
  }

  private IndexSegment getRealtimeSegment() throws IOException {
    RealtimeSegmentImpl realtimeSegmentImpl = RealtimeSegmentImplTest.createRealtimeSegmentImpl(PINOT_SCHEMA, 100000, "testTable", "testTable_testTable", AVRO_DATA,
        new ServerMetrics(new MetricsRegistry()));
    realtimeSegmentImpl.setSegmentMetadata(getRealtimeSegmentZKMetadata());
    try {
      DataFileStream<GenericRecord> avroReader =
          AvroUtils.getAvroReader(new File(TestUtils.getFileFromResourceUrl(getClass().getClassLoader().getResource(
              AVRO_DATA))));
      GenericRow genericRow = null;
      while (avroReader.hasNext()) {
        GenericRecord avroRecord = avroReader.next();
        genericRow = GenericRow.createOrReuseRow(genericRow);
        genericRow = AVRO_RECORD_TRANSFORMER.transform(avroRecord, genericRow);
        // System.out.println(genericRow);
        realtimeSegmentImpl.index(genericRow);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
//    System.out.println("Current raw events indexed: " + realtimeSegmentImpl.getRawDocumentCount() + ", totalDocs = "
//        + realtimeSegmentImpl.getSegmentMetadata().getTotalDocs());
    realtimeSegmentImpl.setSegmentMetadata(getRealtimeSegmentZKMetadata());
    return realtimeSegmentImpl;

  }

  private RealtimeSegmentZKMetadata getRealtimeSegmentZKMetadata() {
    RealtimeSegmentZKMetadata realtimeSegmentZKMetadata = new RealtimeSegmentZKMetadata();
    realtimeSegmentZKMetadata.setTableName("testTable");
    return realtimeSegmentZKMetadata;
  }

  private com.linkedin.pinot.common.data.Schema getTestSchema() throws Exception {
    HashMap<String, FieldType> fieldTypeMap = new HashMap<String, FieldSpec.FieldType>();
    fieldTypeMap.put("column1", FieldType.DIMENSION);
    fieldTypeMap.put("column2", FieldType.DIMENSION);
    fieldTypeMap.put("column3", FieldType.DIMENSION);
    fieldTypeMap.put("column4", FieldType.DIMENSION);
    fieldTypeMap.put("column5", FieldType.DIMENSION);
    fieldTypeMap.put("column6", FieldType.DIMENSION);
    fieldTypeMap.put("column7", FieldType.DIMENSION);
    fieldTypeMap.put("column8", FieldType.DIMENSION);
    fieldTypeMap.put("column9", FieldType.DIMENSION);
    fieldTypeMap.put("column10", FieldType.DIMENSION);
    fieldTypeMap.put("weeksSinceEpochSunday", FieldType.DIMENSION);
    fieldTypeMap.put("daysSinceEpoch", FieldType.DIMENSION);
    fieldTypeMap.put("column13", FieldType.TIME);
    fieldTypeMap.put("count", FieldType.METRIC);
    return SegmentTestUtils.extractSchemaFromAvro(
        new File(TestUtils.getFileFromResourceUrl(getClass().getClassLoader().getResource(AVRO_DATA))), fieldTypeMap,
        TimeUnit.MINUTES);
  }
}
