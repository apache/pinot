package com.linkedin.pinot.queries;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.client.request.RequestConverter;
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
import com.linkedin.pinot.core.data.manager.InstanceDataManager;
import com.linkedin.pinot.core.data.manager.config.InstanceDataManagerConfig;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.query.executor.ServerQueryExecutorV1Impl;
import com.linkedin.pinot.core.query.reduce.DefaultReduceService;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.pql.parsers.PQLCompiler;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Oct 14, 2014
 */

public class QueriesSentinelTest {
  private static final Logger LOGGER = Logger.getLogger(QueriesSentinelTest.class);
  private static ReduceService REDUCE_SERVICE = new DefaultReduceService();

  private static final PQLCompiler REQUEST_COMPILER = new PQLCompiler(new HashMap<String, String[]>());

  private final String AVRO_DATA = "data/mirror-sv.avro";
  private static File INDEX_DIR = new File(QueriesSentinelTest.class.toString());
  private static AvroQueryGenerator AVRO_QUERY_GENERATOR;
  private static InstanceDataManager INSTANCE_DATA_MANAGER;
  private static QueryExecutor QUERY_EXECUTOR;
  private static TestingServerPropertiesBuilder CONFIG_BUILDER;

  @BeforeClass
  public void setup() throws Exception {
    CONFIG_BUILDER = new TestingServerPropertiesBuilder("mirror");

    setupSegmentFor("mirror");
    setUpTestQueries("mirror");

    final PropertiesConfiguration serverConf = CONFIG_BUILDER.build();
    serverConf.setDelimiterParsingDisabled(false);

    final InstanceDataManager instanceDataManager = InstanceDataManager.getInstanceDataManager();
    instanceDataManager.init(new InstanceDataManagerConfig(serverConf.subset("pinot.server.instance")));
    instanceDataManager.start();

    final IndexSegment indexSegment = ColumnarSegmentLoader.load(new File(INDEX_DIR, "segment"), ReadMode.heap);
    instanceDataManager.getResourceDataManager("mirror");
    instanceDataManager.getResourceDataManager("mirror").addSegment(indexSegment);

    QUERY_EXECUTOR = new ServerQueryExecutorV1Impl(false);
    QUERY_EXECUTOR.init(serverConf.subset("pinot.server.query.executor"), instanceDataManager);
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @Test
  public void testAggregation() throws Exception {
    int counter = 0;
    final Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    final List<TestSimpleAggreationQuery> aggCalls = AVRO_QUERY_GENERATOR.giveMeNSimpleAggregationQueries(10000);
    for (final TestSimpleAggreationQuery aggCall : aggCalls) {
      LOGGER.info("running " + counter + " : " + aggCall.pql);
      final BrokerRequest brokerRequest = RequestConverter.fromJSON(REQUEST_COMPILER.compile(aggCall.pql));
      final DataTable instanceResponse = QUERY_EXECUTOR.processQuery(new InstanceRequest(counter++, brokerRequest));
      instanceResponseMap.clear();
      instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse);
      final BrokerResponse brokerResponse = REDUCE_SERVICE.reduceOnDataTable(brokerRequest, instanceResponseMap);
      LOGGER.info("BrokerResponse is " + brokerResponse.getAggregationResults().get(0));
      LOGGER.info("Result from avro is : " + aggCall.result);
      Assert.assertEquals(Double.parseDouble(brokerResponse.getAggregationResults().get(0).getString("value")),
          aggCall.result);
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
      final DataTable instanceResponse = QUERY_EXECUTOR.processQuery(new InstanceRequest(counter++, brokerRequest));
      instanceResponseMap.clear();
      instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse);
      final BrokerResponse brokerResponse = REDUCE_SERVICE.reduceOnDataTable(brokerRequest, instanceResponseMap);
      LOGGER.info("BrokerResponse is " + brokerResponse.getAggregationResults().get(0));
      LOGGER.info("Result from avro is : " + groupBy.groupResults);

      assertGroupByResults(brokerResponse.getAggregationResults().get(0).getJSONArray("groupByResult"),
          groupBy.groupResults);
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
      // System.out.println("Result from query - group:" + keyString + ", value:" + actual);
      final double expected = groupResultsFromAvro.get(key);
      // System.out.println("Result from avro - group:" + keyString + ", value:" + expected);
      Assert.assertEquals(actual, expected);
    }
  }

  private void setUpTestQueries(String resource) throws FileNotFoundException, IOException {
    final String filePath = getClass().getClassLoader().getResource(AVRO_DATA).getFile();
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
    AVRO_QUERY_GENERATOR = new AvroQueryGenerator(new File(filePath), dims, mets, time, resource);
    AVRO_QUERY_GENERATOR.init();
    AVRO_QUERY_GENERATOR.generateSimpleAggregationOnSingleColumnFilters();
  }

  private void setupSegmentFor(String resource) throws Exception {
    final String filePath = getClass().getClassLoader().getResource(AVRO_DATA).getFile();

    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }
    INDEX_DIR.mkdir();

    final SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), new File(INDEX_DIR,
            "segment"), "daysSinceEpoch", TimeUnit.DAYS, resource, resource);

    final SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();

    driver.init(config);
    driver.build();

    System.out.println("built at : " + INDEX_DIR.getAbsolutePath());
  }
}
