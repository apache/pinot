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

import org.antlr.runtime.RecognitionException;
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

import com.linkedin.pinot.common.client.request.RequestConverter;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
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
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.manager.config.FileBasedInstanceDataManagerConfig;
import com.linkedin.pinot.core.data.manager.offline.FileBasedInstanceDataManager;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.utils.AvroUtils;
import com.linkedin.pinot.core.query.executor.ServerQueryExecutorV1Impl;
import com.linkedin.pinot.core.query.reduce.DefaultReduceService;
import com.linkedin.pinot.core.realtime.impl.RealtimeSegmentImpl;
import com.linkedin.pinot.core.realtime.impl.kafka.AvroRecordToPinotRowGenerator;
import com.linkedin.pinot.pql.parsers.PQLCompiler;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;
import com.linkedin.pinot.util.TestUtils;
import com.yammer.metrics.core.MetricsRegistry;


public class RealtimeQueriesSentinelTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeQueriesSentinelTest.class);
  private static ReduceService REDUCE_SERVICE = new DefaultReduceService();

  private static final PQLCompiler REQUEST_COMPILER = new PQLCompiler(new HashMap<String, String[]>());

  private final String AVRO_DATA = "data/test_data-mv.avro";
  private static com.linkedin.pinot.common.data.Schema PINOT_SCHEMA;
  private static AvroQueryGenerator AVRO_QUERY_GENERATOR;
  private static QueryExecutor QUERY_EXECUTOR;
  private static TestingServerPropertiesBuilder CONFIG_BUILDER;
  private static AvroRecordToPinotRowGenerator AVRO_RECORD_TRANSFORMER;

  @BeforeClass
  public void setup() throws Exception {

    PINOT_SCHEMA = getTestSchema(getSchemaMap());
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
      final BrokerRequest brokerRequest = RequestConverter.fromJSON(REQUEST_COMPILER.compile(aggCall.pql));
      InstanceRequest instanceRequest = new InstanceRequest(counter++, brokerRequest);
      instanceRequest.setSearchSegments(new ArrayList<String>());
      instanceRequest.getSearchSegments().add("testTable_testTable");
      DataTable instanceResponse = QUERY_EXECUTOR.processQuery(instanceRequest);
      instanceResponseMap.clear();
      instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse);
      final BrokerResponse brokerResponse = REDUCE_SERVICE.reduceOnDataTable(brokerRequest, instanceResponseMap);
      LOGGER.info("BrokerResponse is " + brokerResponse.getAggregationResults().get(0));
      LOGGER.info("Result from avro is : " + aggCall.result);
      Double actual = Double.parseDouble(brokerResponse.getAggregationResults().get(0).getString("value"));
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
  public void test1() throws RecognitionException, Exception {
    final Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    String query =
        "select sum('count') from testTable where column13='1540094560' group by column3 top 100 limit 0";
    Map<Object, Double> fromAvro = new HashMap<Object, Double>();
    fromAvro.put(null, 2.0D);
    fromAvro.put("", 1.2469280068E10D);
    fromAvro.put("F", 127.0D);
    fromAvro.put("A", 20.0D);
    fromAvro.put("H", 29.0D);
    final BrokerRequest brokerRequest = RequestConverter.fromJSON(REQUEST_COMPILER.compile(query));
    InstanceRequest instanceRequest = new InstanceRequest(485, brokerRequest);
    instanceRequest.setSearchSegments(new ArrayList<String>());
    instanceRequest.getSearchSegments().add("testTable_testTable");
    DataTable instanceResponse = QUERY_EXECUTOR.processQuery(instanceRequest);
    instanceResponseMap.clear();
    instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse);
    BrokerResponse brokerResponse = REDUCE_SERVICE.reduceOnDataTable(brokerRequest, instanceResponseMap);
    JSONArray actual = brokerResponse.getAggregationResults().get(0).getJSONArray("groupByResult");
    LOGGER.info("actual : {}", brokerResponse.getAggregationResults().get(0).toString(1));
    LOGGER.info("expected : {}", fromAvro);
    assertGroupByResults(actual, fromAvro);
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
      instanceRequest.getSearchSegments().add("testTable_testTable");
      DataTable instanceResponse = QUERY_EXECUTOR.processQuery(instanceRequest);
      instanceResponseMap.clear();
      instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse);
      Map<Object, Double> expected = groupBy.groupResults;
      LOGGER.info("Result from avro is : " + expected);
      BrokerResponse brokerResponse = REDUCE_SERVICE.reduceOnDataTable(brokerRequest, instanceResponseMap);
      LOGGER.info("BrokerResponse is " + brokerResponse.getAggregationResults().get(0));
      JSONArray actual = brokerResponse.getAggregationResults().get(0).getJSONArray("groupByResult");
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
    RealtimeSegmentImpl realtimeSegmentImpl = new RealtimeSegmentImpl(PINOT_SCHEMA, 100000);

    try {
      DataFileStream<GenericRecord> avroReader =
          AvroUtils.getAvroReader(new File(TestUtils.getFileFromResourceUrl(getClass().getClassLoader().getResource(
              AVRO_DATA))));
      while (avroReader.hasNext()) {
        GenericRecord avroRecord = avroReader.next();
        GenericRow genericRow = AVRO_RECORD_TRANSFORMER.transform(avroRecord);
        // System.out.println(genericRow);
        realtimeSegmentImpl.index(genericRow);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    System.out.println("Current raw events indexed: " + realtimeSegmentImpl.getRawDocumentCount() + ", totalDocs = "
        + realtimeSegmentImpl.getTotalDocs());
    realtimeSegmentImpl.setSegmentName("testTable_testTable");
    realtimeSegmentImpl.setSegmentMetadata(getRealtimeSegmentZKMetadata());
    return realtimeSegmentImpl;

  }

  private RealtimeSegmentZKMetadata getRealtimeSegmentZKMetadata() {
    RealtimeSegmentZKMetadata realtimeSegmentZKMetadata = new RealtimeSegmentZKMetadata();
    realtimeSegmentZKMetadata.setTableName("testTable");
    return realtimeSegmentZKMetadata;
  }

  private Map<String, String> getSchemaMap() {
    Map<String, String> schemaMap = new HashMap<String, String>();
    schemaMap.put("schema.column13.fieldType", "TIME");
    schemaMap.put("schema.daysSinceEpoch.fieldType", "DIMENSION");
    schemaMap.put("schema.column9.isSingleValue", "true");
    schemaMap.put("schema.column1.dataType", "INT");
    schemaMap.put("schema.column13.dataType", "INT");
    schemaMap.put("schema.column3.fieldType", "DIMENSION");
    schemaMap.put("schema.column8.isSingleValue", "true");
    schemaMap.put("schema.column9.columnName", "column9");
    schemaMap.put("schema.column7.dataType", "INT");
    schemaMap.put("schema.column7.fieldType", "DIMENSION");
    schemaMap.put("schema.column9.dataType", "INT");
    schemaMap.put("schema.column5.isSingleValue", "true");
    schemaMap.put("schema.column4.columnName", "column4");
    schemaMap.put("schema.column10.columnName", "column10");
    schemaMap.put("schema.column10.isSingleValue", "true");
    schemaMap.put("schema.column5.delimeter", ",");
    schemaMap.put("schema.daysSinceEpoch.isSingleValue", "true");
    schemaMap.put("schema.column4.delimeter", ",");
    schemaMap.put("schema.column3.isSingleValue", "true");
    schemaMap.put("schema.column10.delimeter", ",");
    schemaMap.put("schema.column10.dataType", "INT");
    schemaMap.put("schema.column2.dataType", "INT");
    schemaMap.put("schema.column10.fieldType", "DIMENSION");
    schemaMap.put("schema.column3.delimeter", ",");
    schemaMap.put("schema.daysSinceEpoch.columnName", "daysSinceEpoch");
    schemaMap.put("schema.column9.fieldType", "DIMENSION");
    schemaMap.put("schema.count.fieldType", "METRIC");
    schemaMap.put("schema.weeksSinceEpochSunday.isSingleValue", "true");
    schemaMap.put("schema.minutesSinceEpoch.timeUnit", "MINUTES");
    schemaMap.put("schema.count.columnName", "count");
    schemaMap.put("schema.daysSinceEpoch.dataType", "INT");
    schemaMap.put("schema.column2.fieldType", "DIMENSION");
    schemaMap.put("schema.column2.isSingleValue", "true");
    schemaMap.put("schema.column6.isSingleValue", "false");
    schemaMap.put("schema.column7.columnName", "column7");
    schemaMap.put("schema.count.dataType", "INT");
    schemaMap.put("schema.column2.delimeter", ",");
    schemaMap.put("schema.column8.dataType", "INT");
    schemaMap.put("schema.weeksSinceEpochSunday.dataType", "INT");
    schemaMap.put("schema.column3.columnName", "column3");
    schemaMap.put("schema.column6.columnName", "column6");
    schemaMap.put("schema.column5.fieldType", "DIMENSION");
    schemaMap.put("schema.column3.dataType", "STRING");
    schemaMap.put("schema.column4.fieldType", "DIMENSION");
    schemaMap.put("schema.minutesSinceEpoch.columnName", "minutesSinceEpoch");
    schemaMap.put("schema.weeksSinceEpochSunday.fieldType", "DIMENSION");
    schemaMap.put("schema.column4.dataType", "STRING");
    schemaMap.put("schema.column5.columnName", "column5");
    schemaMap.put("schema.column5.dataType", "STRING");
    schemaMap.put("schema.column6.delimeter", ",");
    schemaMap.put("schema.column1.fieldType", "DIMENSION");
    schemaMap.put("schema.column6.dataType", "INT");
    schemaMap.put("schema.column1.delimeter", ",");
    schemaMap.put("schema.weeksSinceEpochSunday.delimeter", ",");
    schemaMap.put("schema.column1.isSingleValue", "true");
    schemaMap.put("schema.column8.delimeter", ",");
    schemaMap.put("schema.column4.isSingleValue", "true");
    schemaMap.put("schema.column8.columnName", "column8");
    schemaMap.put("schema.column6.fieldType", "DIMENSION");
    schemaMap.put("schema.column9.delimeter", ",");
    schemaMap.put("schema.weeksSinceEpochSunday.columnName", "weeksSinceEpochSunday");
    schemaMap.put("schema.column8.fieldType", "DIMENSION");
    schemaMap.put("schema.daysSinceEpoch.delimeter", ",");
    schemaMap.put("schema.column7.delimeter", ",");
    schemaMap.put("schema.column7.isSingleValue", "false");
    schemaMap.put("schema.column1.columnName", "column1");
    schemaMap.put("schema.column2.columnName", "column2");
    return schemaMap;
  }

  private com.linkedin.pinot.common.data.Schema getTestSchema(Map<String, String> schemaMap) throws Exception {
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
