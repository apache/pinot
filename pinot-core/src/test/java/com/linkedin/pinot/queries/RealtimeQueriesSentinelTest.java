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

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
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
  private static final Logger LOGGER = Logger.getLogger(RealtimeQueriesSentinelTest.class);
  private static ReduceService REDUCE_SERVICE = new DefaultReduceService();

  private static final PQLCompiler REQUEST_COMPILER = new PQLCompiler(new HashMap<String, String[]>());

  private final String AVRO_DATA = "data/mirror-mv.avro";
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

    setUpTestQueries("mirror");

    CONFIG_BUILDER = new TestingServerPropertiesBuilder("mirror");
    final PropertiesConfiguration serverConf = CONFIG_BUILDER.build();
    serverConf.setDelimiterParsingDisabled(false);
    final FileBasedInstanceDataManager instanceDataManager = FileBasedInstanceDataManager.getInstanceDataManager();
    instanceDataManager.init(new FileBasedInstanceDataManagerConfig(serverConf.subset("pinot.server.instance")));
    instanceDataManager.start();
    instanceDataManager.getResourceDataManager("mirror");
    instanceDataManager.getResourceDataManager("mirror").addSegment(indexSegment);

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
    final List<TestSimpleAggreationQuery> aggCalls = AVRO_QUERY_GENERATOR.giveMeNSimpleAggregationQueries(100000);
    for (final TestSimpleAggreationQuery aggCall : aggCalls) {
      LOGGER.info("running " + counter + " : " + aggCall.pql);
      final BrokerRequest brokerRequest = RequestConverter.fromJSON(REQUEST_COMPILER.compile(aggCall.pql));
      InstanceRequest instanceRequest = new InstanceRequest(counter++, brokerRequest);
      instanceRequest.setSearchSegments(new ArrayList<String>());
      instanceRequest.getSearchSegments().add("mirror_mirror");
      DataTable instanceResponse = QUERY_EXECUTOR.processQuery(instanceRequest);
      instanceResponseMap.clear();
      instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse);
      final BrokerResponse brokerResponse = REDUCE_SERVICE.reduceOnDataTable(brokerRequest, instanceResponseMap);
      LOGGER.info("BrokerResponse is " + brokerResponse.getAggregationResults().get(0));
      LOGGER.info("Result from avro is : " + aggCall.result);
      Double actual = Double.parseDouble(brokerResponse.getAggregationResults().get(0).getString("value"));
      Double expected = aggCall.result;
      Assert.assertEquals(actual, expected);
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
      instanceRequest.getSearchSegments().add("mirror_mirror");
      DataTable instanceResponse = QUERY_EXECUTOR.processQuery(instanceRequest);
      instanceResponseMap.clear();
      instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse);
      Map<Object, Double> expected = groupBy.groupResults;
      LOGGER.info("Result from avro is : " + expected);
      BrokerResponse brokerResponse = REDUCE_SERVICE.reduceOnDataTable(brokerRequest, instanceResponseMap);
      LOGGER.info("BrokerResponse is " + brokerResponse.getAggregationResults().get(0));
      JSONArray actual = brokerResponse.getAggregationResults().get(0).getJSONArray("groupByResult");
      assertGroupByResults(actual, expected);
    }
  }

  private void assertGroupByResults(JSONArray jsonArray, Map<Object, Double> groupResultsFromAvro) throws JSONException {
    final Map<String, Double> groupResultsFromQuery = new HashMap<String, Double>();
    if (groupResultsFromAvro.size() > 10) {
      Assert.assertEquals(jsonArray.length(), 10);
    } else {
      Assert.assertEquals(jsonArray.length(), groupResultsFromAvro.size());
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
      Assert.assertEquals(actual, expected);
    }
  }

  private void setUpTestQueries(String resource) throws FileNotFoundException, IOException {
    final String filePath = TestUtils.getFileFromResourceUrl(getClass().getClassLoader().getResource(AVRO_DATA));
    System.out.println(filePath);
    final List<String> dims = new ArrayList<String>();
    dims.add("viewerId");
    dims.add("viewerType");
    dims.add("vieweeId");
    dims.add("viewerCompany");
    dims.add("viewerCountry");
    dims.add("viewerRegionCode");
    dims.add("viewerIndustry");
    dims.add("viewerOccupation");
    dims.add("viewerSchool");
    dims.add("viewerSeniority");
    dims.add("viewerPrivacySetting");
    dims.add("viewerObfuscationType");
    dims.add("vieweePrivacySetting");
    dims.add("weeksSinceEpochSunday");
    dims.add("daysSinceEpoch");
    dims.add("hoursSinceEpoch");

    final List<String> mets = new ArrayList<String>();
    mets.add("count");

    final String time = "minutesSinceEpoch";
    AVRO_QUERY_GENERATOR = new AvroQueryGenerator(new File(filePath), dims, mets, time, resource, true);
    AVRO_QUERY_GENERATOR.init();
    AVRO_QUERY_GENERATOR.generateSimpleAggregationOnSingleColumnFilters();
  }

  private IndexSegment getRealtimeSegment() {
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
    realtimeSegmentImpl.setSegmentName("mirror_mirror");
    realtimeSegmentImpl.setSegmentMetadata(getRealtimeSegmentZKMetadata());
    return realtimeSegmentImpl;

  }

  private RealtimeSegmentZKMetadata getRealtimeSegmentZKMetadata() {
    RealtimeSegmentZKMetadata realtimeSegmentZKMetadata = new RealtimeSegmentZKMetadata();
    realtimeSegmentZKMetadata.setResourceName("mirror");
    realtimeSegmentZKMetadata.setTableName("mirror");
    return realtimeSegmentZKMetadata;
  }

  private Map<String, String> getSchemaMap() {
    Map<String, String> schemaMap = new HashMap<String, String>();
    schemaMap.put("schema.minutesSinceEpoch.fieldType", "TIME");
    schemaMap.put("schema.daysSinceEpoch.fieldType", "DIMENSION");
    schemaMap.put("schema.viewerIndustry.isSingleValue", "true");
    schemaMap.put("schema.viewerId.dataType", "INT");
    schemaMap.put("schema.minutesSinceEpoch.dataType", "INT");
    schemaMap.put("schema.viewerPrivacySetting.fieldType", "DIMENSION");
    schemaMap.put("schema.viewerRegionCode.isSingleValue", "true");
    schemaMap.put("schema.viewerIndustry.columnName", "viewerIndustry");
    schemaMap.put("schema.viewerOccupations.dataType", "INT");
    schemaMap.put("schema.viewerOccupations.fieldType", "DIMENSION");
    schemaMap.put("schema.viewerIndustry.dataType", "INT");
    schemaMap.put("schema.viewerObfuscationType.isSingleValue", "true");
    schemaMap.put("schema.vieweePrivacySetting.columnName", "vieweePrivacySetting");
    schemaMap.put("schema.viewerSchool.columnName", "viewerSchool");
    schemaMap.put("schema.viewerSchool.isSingleValue", "true");
    schemaMap.put("schema.viewerObfuscationType.delimeter", ",");
    schemaMap.put("schema.daysSinceEpoch.isSingleValue", "true");
    schemaMap.put("schema.vieweePrivacySetting.delimeter", ",");
    schemaMap.put("schema.viewerPrivacySetting.isSingleValue", "true");
    schemaMap.put("schema.viewerSchool.delimeter", ",");
    schemaMap.put("schema.viewerSchool.dataType", "INT");
    schemaMap.put("schema.vieweeId.dataType", "INT");
    schemaMap.put("schema.viewerSchool.fieldType", "DIMENSION");
    schemaMap.put("schema.viewerPrivacySetting.delimeter", ",");
    schemaMap.put("schema.daysSinceEpoch.columnName", "daysSinceEpoch");
    schemaMap.put("schema.viewerIndustry.fieldType", "DIMENSION");
    schemaMap.put("schema.count.fieldType", "METRIC");
    schemaMap.put("schema.weeksSinceEpochSunday.isSingleValue", "true");
    schemaMap.put("schema.minutesSinceEpoch.timeUnit", "MINUTES");
    schemaMap.put("schema.count.columnName", "count");
    schemaMap.put("schema.daysSinceEpoch.dataType", "INT");
    schemaMap.put("schema.vieweeId.fieldType", "DIMENSION");
    schemaMap.put("schema.vieweeId.isSingleValue", "true");
    schemaMap.put("schema.viewerCompanies.isSingleValue", "false");
    schemaMap.put("schema.viewerOccupations.columnName", "viewerOccupations");
    schemaMap.put("schema.count.dataType", "INT");
    schemaMap.put("schema.vieweeId.delimeter", ",");
    schemaMap.put("schema.viewerRegionCode.dataType", "INT");
    schemaMap.put("schema.weeksSinceEpochSunday.dataType", "INT");
    schemaMap.put("schema.viewerPrivacySetting.columnName", "viewerPrivacySetting");
    schemaMap.put("schema.viewerCompanies.columnName", "viewerCompanies");
    schemaMap.put("schema.viewerObfuscationType.fieldType", "DIMENSION");
    schemaMap.put("schema.viewerPrivacySetting.dataType", "STRING");
    schemaMap.put("schema.vieweePrivacySetting.fieldType", "DIMENSION");
    schemaMap.put("schema.minutesSinceEpoch.columnName", "minutesSinceEpoch");
    schemaMap.put("schema.weeksSinceEpochSunday.fieldType", "DIMENSION");
    schemaMap.put("schema.vieweePrivacySetting.dataType", "STRING");
    schemaMap.put("schema.viewerObfuscationType.columnName", "viewerObfuscationType");
    schemaMap.put("schema.viewerObfuscationType.dataType", "STRING");
    schemaMap.put("schema.viewerCompanies.delimeter", ",");
    schemaMap.put("schema.viewerId.fieldType", "DIMENSION");
    schemaMap.put("schema.viewerCompanies.dataType", "INT");
    schemaMap.put("schema.viewerId.delimeter", ",");
    schemaMap.put("schema.weeksSinceEpochSunday.delimeter", ",");
    schemaMap.put("schema.viewerId.isSingleValue", "true");
    schemaMap.put("schema.viewerRegionCode.delimeter", ",");
    schemaMap.put("schema.vieweePrivacySetting.isSingleValue", "true");
    schemaMap.put("schema.viewerRegionCode.columnName", "viewerRegionCode");
    schemaMap.put("schema.viewerCompanies.fieldType", "DIMENSION");
    schemaMap.put("schema.viewerIndustry.delimeter", ",");
    schemaMap.put("schema.weeksSinceEpochSunday.columnName", "weeksSinceEpochSunday");
    schemaMap.put("schema.viewerRegionCode.fieldType", "DIMENSION");
    schemaMap.put("schema.daysSinceEpoch.delimeter", ",");
    schemaMap.put("schema.viewerOccupations.delimeter", ",");
    schemaMap.put("schema.viewerOccupations.isSingleValue", "false");
    schemaMap.put("schema.viewerId.columnName", "viewerId");
    schemaMap.put("schema.vieweeId.columnName", "vieweeId");
    return schemaMap;
  }

  private com.linkedin.pinot.common.data.Schema getTestSchema(Map<String, String> schemaMap) throws Exception {
    HashMap<String, FieldType> fieldTypeMap = new HashMap<String, FieldSpec.FieldType>();
    fieldTypeMap.put("viewerId", FieldType.DIMENSION);
    fieldTypeMap.put("vieweeId", FieldType.DIMENSION);
    fieldTypeMap.put("viewerPrivacySetting", FieldType.DIMENSION);
    fieldTypeMap.put("vieweePrivacySetting", FieldType.DIMENSION);
    fieldTypeMap.put("viewerObfuscationType", FieldType.DIMENSION);
    fieldTypeMap.put("viewerCompanies", FieldType.DIMENSION);
    fieldTypeMap.put("viewerOccupations", FieldType.DIMENSION);
    fieldTypeMap.put("viewerRegionCode", FieldType.DIMENSION);
    fieldTypeMap.put("viewerIndustry", FieldType.DIMENSION);
    fieldTypeMap.put("viewerSchool", FieldType.DIMENSION);
    fieldTypeMap.put("weeksSinceEpochSunday", FieldType.DIMENSION);
    fieldTypeMap.put("daysSinceEpoch", FieldType.DIMENSION);
    fieldTypeMap.put("minutesSinceEpoch", FieldType.TIME);
    fieldTypeMap.put("count", FieldType.METRIC);
    return SegmentTestUtils.extractSchemaFromAvro(
        new File(TestUtils.getFileFromResourceUrl(getClass().getClassLoader().getResource(AVRO_DATA))), fieldTypeMap,
        TimeUnit.MINUTES);
  }
}
