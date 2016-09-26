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
package com.linkedin.pinot.query.aggregation;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.response.broker.AggregationResult;
import com.linkedin.pinot.common.response.broker.BrokerResponseNative;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.common.utils.NamedThreadFactory;
import com.linkedin.pinot.core.data.manager.offline.OfflineSegmentDataManager;
import com.linkedin.pinot.core.data.manager.offline.SegmentDataManager;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.plan.Plan;
import com.linkedin.pinot.core.plan.maker.InstancePlanMakerImplV2;
import com.linkedin.pinot.core.plan.maker.PlanMaker;
import com.linkedin.pinot.core.query.config.QueryExecutorConfig;
import com.linkedin.pinot.core.query.reduce.BrokerReduceService;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;
import com.linkedin.pinot.util.TestUtils;


public class AggregationMVQueriesTest {

  private final static Logger LOGGER = LoggerFactory.getLogger(AggregationMVQueriesTest.class);

  private final static String AVRO_DATA = "data/test_data-mv.avro";
  private final static File INDEXES_DIR = new File(FileUtils.getTempDirectory() + File.separator
      + "AggregationMVQueriesTest");
  private final static Pql2Compiler PQL2_COMPILER = new Pql2Compiler();
  private final static List<SegmentDataManager> INDEX_SEGMENT_LIST = new ArrayList<SegmentDataManager>();
  private final int NUM_SEGMENTS = 20;
  private static PlanMaker INSTANCE_PLAN_MAKER;

  @BeforeClass
  public void setup() throws Exception {
    INSTANCE_PLAN_MAKER = new InstancePlanMakerImplV2(new QueryExecutorConfig(new PropertiesConfiguration()));
    setupSegmentList(NUM_SEGMENTS);
  }

  @AfterClass
  public void tearDown() {
    if (INDEXES_DIR.exists()) {
      FileUtils.deleteQuietly(INDEXES_DIR);
    }
    for (SegmentDataManager segmentDataManager : INDEX_SEGMENT_LIST) {
      segmentDataManager.getSegment().destroy();
    }
    INDEX_SEGMENT_LIST.clear();
  }

  private void setupSegmentList(int numberOfSegments) throws Exception {
    final String filePath = TestUtils.getFileFromResourceUrl(getClass().getClassLoader().getResource(AVRO_DATA));

    if (INDEXES_DIR.exists()) {
      FileUtils.deleteQuietly(INDEXES_DIR);
    }
    INDEXES_DIR.mkdir();

    for (int i = 0; i < numberOfSegments; ++i) {
      final File segmentDir = new File(INDEXES_DIR, "segment_" + i);

      final SegmentGeneratorConfig config =
          SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(
              new File(filePath),
              segmentDir,
              "daysSinceEpoch",
              TimeUnit.DAYS,
              "test");

      final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
      driver.init(config);
      driver.build();

      LOGGER.debug("built at : {}", segmentDir.getAbsolutePath());
      INDEX_SEGMENT_LIST.add(
          new OfflineSegmentDataManager(
              ColumnarSegmentLoader.load(
                  new File(segmentDir, driver.getSegmentName()),
                  ReadMode.heap)));
    }
  }

  @Test
  public void testSumAggrMV() throws Exception {
    final BrokerRequest brokerRequest = PQL2_COMPILER.compileToBrokerRequest(
        "select sumMV(column7) from myTable");
    final BrokerResponseNative brokerResponse = queryAndgetBrokerResponse(brokerRequest);
    Assert.assertEquals(100000 * NUM_SEGMENTS, brokerResponse.getNumDocsScanned());
    Assert.assertEquals(brokerRequest.getAggregationsInfo().size(), brokerResponse.getAggregationResults().size());

    // Assertion
    AggregationResult aggregationResult = brokerResponse.getAggregationResults().get(0);
    Assert.assertEquals(aggregationResult.getFunction(), "sum_column7");

    System.out.println("testSumAggrMV");
    System.out.println(aggregationResult.getValue());
    Assert.assertEquals(aggregationResult.getValue(), "2144864368413420.00000");

  }

  @Test
  public void testAvgAggrMV() throws Exception {
    final BrokerRequest brokerRequest = PQL2_COMPILER.compileToBrokerRequest(
        "select avgMV(column7) from myTable");
    final BrokerResponseNative brokerResponse = queryAndgetBrokerResponse(brokerRequest);
    Assert.assertEquals(100000 * NUM_SEGMENTS, brokerResponse.getNumDocsScanned());
    Assert.assertEquals(brokerRequest.getAggregationsInfo().size(), brokerResponse.getAggregationResults().size());

    // Assertion
    AggregationResult aggregationResult = brokerResponse.getAggregationResults().get(0);
    Assert.assertEquals(aggregationResult.getFunction(), "avg_column7");

    System.out.println("testAvgAggrMV");
    System.out.println(aggregationResult.getValue());
    Assert.assertEquals(aggregationResult.getValue(), "799785356.25827");
  }

  @Test
  public void testMinAggrMV() throws Exception {
    final BrokerRequest brokerRequest = PQL2_COMPILER.compileToBrokerRequest(
        "select minMV(column7) from myTable");
    final BrokerResponseNative brokerResponse = queryAndgetBrokerResponse(brokerRequest);
    Assert.assertEquals(100000 * NUM_SEGMENTS, brokerResponse.getNumDocsScanned());
    Assert.assertEquals(brokerRequest.getAggregationsInfo().size(), brokerResponse.getAggregationResults().size());

    // Assertion
    AggregationResult aggregationResult = brokerResponse.getAggregationResults().get(0);
    Assert.assertEquals(aggregationResult.getFunction(), "min_column7");

    System.out.println("testMinAggrMV");
    System.out.println(aggregationResult.getValue());
    Assert.assertEquals(aggregationResult.getValue(), "201.00000");

  }

  @Test
  public void testMaxAggrMV() throws Exception {
    final BrokerRequest brokerRequest = PQL2_COMPILER.compileToBrokerRequest(
        "select maxMV(column7) from myTable");
    final BrokerResponseNative brokerResponse = queryAndgetBrokerResponse(brokerRequest);
    Assert.assertEquals(100000 * NUM_SEGMENTS, brokerResponse.getNumDocsScanned());
    Assert.assertEquals(brokerRequest.getAggregationsInfo().size(), brokerResponse.getAggregationResults().size());

    // Assertion
    AggregationResult aggregationResult = brokerResponse.getAggregationResults().get(0);
    Assert.assertEquals(aggregationResult.getFunction(), "max_column7");

    System.out.println("testMaxAggrMV");
    System.out.println(aggregationResult.getValue());
    Assert.assertEquals(aggregationResult.getValue(), "2147483647.00000");

  }

  @Test
  public void testMinMaxRangeAggrMV() throws Exception {
    final BrokerRequest brokerRequest = PQL2_COMPILER.compileToBrokerRequest(
        "select minmaxrangeMV(column7) from myTable");
    final BrokerResponseNative brokerResponse = queryAndgetBrokerResponse(brokerRequest);
    Assert.assertEquals(100000 * NUM_SEGMENTS, brokerResponse.getNumDocsScanned());
    Assert.assertEquals(brokerRequest.getAggregationsInfo().size(), brokerResponse.getAggregationResults().size());

    // Assertion
    AggregationResult aggregationResult = brokerResponse.getAggregationResults().get(0);
    Assert.assertEquals(aggregationResult.getFunction(), "minMaxRange_column7");

    System.out.println("testMinMaxRangeAggrMV");
    System.out.println(aggregationResult.getValue());
    Assert.assertEquals(aggregationResult.getValue(), "2147483446.00000");

  }

  @Test
  public void testDistinctCountAggrMV() throws Exception {
    final BrokerRequest brokerRequest = PQL2_COMPILER.compileToBrokerRequest(
        "select distinctCountMV(column7) from myTable");
    final BrokerResponseNative brokerResponse = queryAndgetBrokerResponse(brokerRequest);
    Assert.assertEquals(100000 * NUM_SEGMENTS, brokerResponse.getNumDocsScanned());
    Assert.assertEquals(brokerRequest.getAggregationsInfo().size(), brokerResponse.getAggregationResults().size());

    // Assertion
    AggregationResult aggregationResult = brokerResponse.getAggregationResults().get(0);
    Assert.assertEquals(aggregationResult.getFunction(), "distinctCount_column7");

    System.out.println("testDistinctCountAggrMV");
    System.out.println(aggregationResult.getValue());
    Assert.assertEquals(aggregationResult.getValue(), "359");
  }

  @Test
  public void testDistinctCountHllAggrMV() throws Exception {
    final BrokerRequest brokerRequest = PQL2_COMPILER.compileToBrokerRequest(
        "select distinctCountHllMV(column7) from myTable");
    final BrokerResponseNative brokerResponse = queryAndgetBrokerResponse(brokerRequest);
    Assert.assertEquals(100000 * NUM_SEGMENTS, brokerResponse.getNumDocsScanned());
    Assert.assertEquals(brokerRequest.getAggregationsInfo().size(), brokerResponse.getAggregationResults().size());

    // Assertion
    AggregationResult aggregationResult = brokerResponse.getAggregationResults().get(0);
    Assert.assertEquals(aggregationResult.getFunction(), "distinctCountHLL_column7");

    System.out.println("testDistinctCountHllAggrMV");
    System.out.println(aggregationResult.getValue());
    Assert.assertEquals(aggregationResult.getValue(), "376");
  }

  @Test
  public void testPercentile50AggrMV() throws Exception {
    final BrokerRequest brokerRequest = PQL2_COMPILER.compileToBrokerRequest(
        "select percentile50MV(column7) from myTable");
    final BrokerResponseNative brokerResponse = queryAndgetBrokerResponse(brokerRequest);
    Assert.assertEquals(100000 * NUM_SEGMENTS, brokerResponse.getNumDocsScanned());
    Assert.assertEquals(brokerRequest.getAggregationsInfo().size(), brokerResponse.getAggregationResults().size());

    // Assertion
    AggregationResult aggregationResult = brokerResponse.getAggregationResults().get(0);
    Assert.assertEquals(aggregationResult.getFunction(), "percentile50_column7");

    System.out.println("testPercentile50AggrMV");
    System.out.println(aggregationResult.getValue());
    Assert.assertEquals(aggregationResult.getValue(), "492.00000");
  }

  @Test
  public void testPercentile90AggrMV() throws Exception {
    final BrokerRequest brokerRequest = PQL2_COMPILER.compileToBrokerRequest(
        "select percentile90MV(column7) from myTable");
    final BrokerResponseNative brokerResponse = queryAndgetBrokerResponse(brokerRequest);
    Assert.assertEquals(100000 * NUM_SEGMENTS, brokerResponse.getNumDocsScanned());
    Assert.assertEquals(brokerRequest.getAggregationsInfo().size(), brokerResponse.getAggregationResults().size());

    // Assertion
    AggregationResult aggregationResult = brokerResponse.getAggregationResults().get(0);
    Assert.assertEquals(aggregationResult.getFunction(), "percentile90_column7");

    System.out.println("testPercentile90AggrMV");
    System.out.println(aggregationResult.getValue());
    Assert.assertEquals(aggregationResult.getValue(), "2147483647.00000");
  }

  @Test
  public void testPercentile95AggrMV() throws Exception {
    final BrokerRequest brokerRequest = PQL2_COMPILER.compileToBrokerRequest(
        "select percentile95MV(column7) from myTable");
    final BrokerResponseNative brokerResponse = queryAndgetBrokerResponse(brokerRequest);
    Assert.assertEquals(100000 * NUM_SEGMENTS, brokerResponse.getNumDocsScanned());
    Assert.assertEquals(brokerRequest.getAggregationsInfo().size(), brokerResponse.getAggregationResults().size());

    // Assertion
    AggregationResult aggregationResult = brokerResponse.getAggregationResults().get(0);
    Assert.assertEquals(aggregationResult.getFunction(), "percentile95_column7");

    System.out.println("testPercentile95AggrMV");
    System.out.println(aggregationResult.getValue());
    Assert.assertEquals(aggregationResult.getValue(), "2147483647.00000");
  }

  @Test
  public void testPercentile99AggrMV() throws Exception {
    final BrokerRequest brokerRequest = PQL2_COMPILER.compileToBrokerRequest(
        "select percentile99MV(column7) from myTable");
    final BrokerResponseNative brokerResponse = queryAndgetBrokerResponse(brokerRequest);
    Assert.assertEquals(100000 * NUM_SEGMENTS, brokerResponse.getNumDocsScanned());
    Assert.assertEquals(brokerRequest.getAggregationsInfo().size(), brokerResponse.getAggregationResults().size());

    // Assertion
    AggregationResult aggregationResult = brokerResponse.getAggregationResults().get(0);
    Assert.assertEquals(aggregationResult.getFunction(), "percentile99_column7");

    System.out.println("testPercentile99AggrMV");
    System.out.println(aggregationResult.getValue());
    Assert.assertEquals(aggregationResult.getValue(), "2147483647.00000");
  }

  @Test
  public void testPercentileEst50AggrMV() throws Exception {
    final BrokerRequest brokerRequest = PQL2_COMPILER.compileToBrokerRequest(
        "select percentileEst50MV(column7) from myTable");
    final BrokerResponseNative brokerResponse = queryAndgetBrokerResponse(brokerRequest);
    Assert.assertEquals(100000 * NUM_SEGMENTS, brokerResponse.getNumDocsScanned());
    Assert.assertEquals(brokerRequest.getAggregationsInfo().size(), brokerResponse.getAggregationResults().size());

    // Assertion
    AggregationResult aggregationResult = brokerResponse.getAggregationResults().get(0);
    Assert.assertEquals(aggregationResult.getFunction(), "percentileEst50_column7");

    System.out.println("testPercentileEst50AggrMV");
    System.out.println(aggregationResult.getValue());
    Assert.assertEquals(aggregationResult.getValue(), "495");
  }

  @Test
  public void testPercentileEst90AggrMV() throws Exception {
    final BrokerRequest brokerRequest = PQL2_COMPILER.compileToBrokerRequest(
        "select percentileEst90MV(column7) from myTable");
    final BrokerResponseNative brokerResponse = queryAndgetBrokerResponse(brokerRequest);
    Assert.assertEquals(100000 * NUM_SEGMENTS, brokerResponse.getNumDocsScanned());
    Assert.assertEquals(brokerRequest.getAggregationsInfo().size(), brokerResponse.getAggregationResults().size());

    // Assertion
    AggregationResult aggregationResult = brokerResponse.getAggregationResults().get(0);
    Assert.assertEquals(aggregationResult.getFunction(), "percentileEst90_column7");

    System.out.println("testPercentileEst90AggrMV");
    System.out.println(aggregationResult.getValue());
    Assert.assertEquals(aggregationResult.getValue(), "2147483647");
  }

  @Test
  public void testPercentileEst95AggrMV() throws Exception {
    final BrokerRequest brokerRequest = PQL2_COMPILER.compileToBrokerRequest(
        "select percentileEst95MV(column7) from myTable");
    final BrokerResponseNative brokerResponse = queryAndgetBrokerResponse(brokerRequest);
    Assert.assertEquals(100000 * NUM_SEGMENTS, brokerResponse.getNumDocsScanned());
    Assert.assertEquals(brokerRequest.getAggregationsInfo().size(), brokerResponse.getAggregationResults().size());

    // Assertion
    AggregationResult aggregationResult = brokerResponse.getAggregationResults().get(0);
    Assert.assertEquals(aggregationResult.getFunction(), "percentileEst95_column7");

    System.out.println("testPercentileEst95AggrMV");
    System.out.println(aggregationResult.getValue());
    Assert.assertEquals(aggregationResult.getValue(), "2147483647");
  }

  @Test
  public void testPercentileEst99AggrMV() throws Exception {
    final BrokerRequest brokerRequest = PQL2_COMPILER.compileToBrokerRequest(
        "select percentileEst99MV(column7) from myTable");
    final BrokerResponseNative brokerResponse = queryAndgetBrokerResponse(brokerRequest);
    Assert.assertEquals(100000 * NUM_SEGMENTS, brokerResponse.getNumDocsScanned());
    Assert.assertEquals(brokerRequest.getAggregationsInfo().size(), brokerResponse.getAggregationResults().size());

    // Assertion
    AggregationResult aggregationResult = brokerResponse.getAggregationResults().get(0);
    Assert.assertEquals(aggregationResult.getFunction(), "percentileEst99_column7");

    System.out.println("testPercentileEst99AggrMV");
    System.out.println(aggregationResult.getValue());
    Assert.assertEquals(aggregationResult.getValue(), "2147483647");
  }

  private BrokerResponseNative queryAndgetBrokerResponse(final BrokerRequest brokerRequest) {
    final ExecutorService executorService = Executors.newCachedThreadPool(new NamedThreadFactory("test-plan-maker"));
    final Plan globalPlan =
        INSTANCE_PLAN_MAKER.makeInterSegmentPlan(INDEX_SEGMENT_LIST, brokerRequest, executorService, 150000);
    globalPlan.print();
    globalPlan.execute();
    final DataTable instanceResponse = globalPlan.getInstanceResponse();
    LOGGER.debug("Instance Response : {}", instanceResponse);

    final BrokerReduceService reduceService = new BrokerReduceService();
    final Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse);
    final BrokerResponseNative brokerResponse = reduceService.reduceOnDataTable(brokerRequest, instanceResponseMap);
    return brokerResponse;
  }
}
