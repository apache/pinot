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
import java.util.Arrays;
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
import com.linkedin.pinot.common.response.broker.GroupByResult;
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


public class AggregationMVGroupByMVQueriesTest {

  private final static Logger LOGGER = LoggerFactory.getLogger(AggregationMVGroupByMVQueriesTest.class);

  private final static String AVRO_DATA = "data/test_data-mv.avro";
  private final static File INDEXES_DIR = new File(FileUtils.getTempDirectory() + File.separator
      + "AggregationMVGroupByMVQueriesTest");
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
  public void testSumAggrMVGroupBySV() throws Exception {
    final BrokerRequest brokerRequest = PQL2_COMPILER.compileToBrokerRequest(
        "select sumMV(column7) from myTable group by column6");
    final BrokerResponseNative brokerResponse = queryAndgetBrokerResponse(brokerRequest);
    Assert.assertEquals(100000 * NUM_SEGMENTS, brokerResponse.getNumDocsScanned());
    Assert.assertEquals(brokerRequest.getAggregationsInfo().size(), brokerResponse.getAggregationResults().size());

    // Assertion on GroupBy
    AggregationResult aggregationResult = brokerResponse.getAggregationResults().get(0);
    Assert.assertEquals(aggregationResult.getFunction(), "sum_column7");
    Assert.assertEquals(aggregationResult.getGroupByColumns().get(0), "column6");

    System.out.println("testSumAggrMVGroupBySV");
    System.out.println(aggregationResult.getGroupByResult());

    Map<String, String> expectedGroupByResults = new HashMap<>();
    expectedGroupByResults.put("[2147483647]", "1967352841480140.00000");
    expectedGroupByResults.put("[1417167]", "1460288879960.00000");
    expectedGroupByResults.put("[1044]", "1202591795900.00000");
    expectedGroupByResults.put("[1033]", "1159642469380.00000");
    expectedGroupByResults.put("[36614]", "1073742033720.00000");
    expectedGroupByResults.put("[200683]", "1073741823500.00000");
    expectedGroupByResults.put("[62009]", "1030792150560.00000");
    expectedGroupByResults.put("[1038]", "944893521040.00000");
    expectedGroupByResults.put("[113103]", "944892804680.00000");
    expectedGroupByResults.put("[1028]", "773096958740.00000");

    Map<String, String> actualGroupByResults = convertGroupByResultsToMap(aggregationResult.getGroupByResult());
    assertGroupByReulsts(actualGroupByResults, expectedGroupByResults);
  }

  @Test
  public void testSumAggrMVGroupByMV() throws Exception {
    final BrokerRequest brokerRequest = PQL2_COMPILER.compileToBrokerRequest(
        "select sumMV(column7) from myTable group by column7");
    final BrokerResponseNative brokerResponse = queryAndgetBrokerResponse(brokerRequest);
    Assert.assertEquals(2000000, brokerResponse.getNumDocsScanned());
    Assert.assertEquals(brokerRequest.getAggregationsInfo().size(), brokerResponse.getAggregationResults().size());

    // Assertion on GroupBy
    AggregationResult aggregationResult = brokerResponse.getAggregationResults().get(0);
    Assert.assertEquals(aggregationResult.getFunction(), "sum_column7");
    Assert.assertEquals(aggregationResult.getGroupByColumns().get(0), "column7");

    System.out.println("testSumAggrMVGroupByMV");
    System.out.println(aggregationResult.getGroupByResult());

    Map<String, String> expectedGroupByResults = new HashMap<>();
    expectedGroupByResults.put("[2147483647]", "2144863716950660.00000");
    expectedGroupByResults.put("[363]", "115825340.00000");
    expectedGroupByResults.put("[469]", "111044220.00000");
    expectedGroupByResults.put("[564]", "84726960.00000");
    expectedGroupByResults.put("[246]", "84011760.00000");
    expectedGroupByResults.put("[523]", "60105640.00000");
    expectedGroupByResults.put("[211]", "53276920.00000");
    expectedGroupByResults.put("[288]", "52121900.00000");
    expectedGroupByResults.put("[225]", "36344500.00000");
    expectedGroupByResults.put("[478]", "31704560.00000");

    Map<String, String> actualGroupByResults = convertGroupByResultsToMap(aggregationResult.getGroupByResult());
    assertGroupByReulsts(actualGroupByResults, expectedGroupByResults);

  }

  @Test
  public void testAvgAggrMVGroupBySV() throws Exception {
    final BrokerRequest brokerRequest = PQL2_COMPILER.compileToBrokerRequest(
        "select avgMV(column7) from myTable group by column6");
    final BrokerResponseNative brokerResponse = queryAndgetBrokerResponse(brokerRequest);
    Assert.assertEquals(100000 * NUM_SEGMENTS, brokerResponse.getNumDocsScanned());
    Assert.assertEquals(brokerRequest.getAggregationsInfo().size(), brokerResponse.getAggregationResults().size());

    // Assertion on GroupBy
    AggregationResult aggregationResult = brokerResponse.getAggregationResults().get(0);
    Assert.assertEquals(aggregationResult.getFunction(), "avg_column7");
    Assert.assertEquals(aggregationResult.getGroupByColumns().get(0), "column6");

    System.out.println("testAvgAggrMVGroupBySV");
    System.out.println(aggregationResult.getGroupByResult());
  }

  @Test
  public void testAvgAggrMVGroupByMV() throws Exception {
    final BrokerRequest brokerRequest = PQL2_COMPILER.compileToBrokerRequest(
        "select avgMV(column7) from myTable group by column7");
    final BrokerResponseNative brokerResponse = queryAndgetBrokerResponse(brokerRequest);
    Assert.assertEquals(2000000, brokerResponse.getNumDocsScanned());
    Assert.assertEquals(brokerRequest.getAggregationsInfo().size(), brokerResponse.getAggregationResults().size());

    // Assertion on GroupBy
    AggregationResult aggregationResult = brokerResponse.getAggregationResults().get(0);
    Assert.assertEquals(aggregationResult.getFunction(), "avg_column7");
    Assert.assertEquals(aggregationResult.getGroupByColumns().get(0), "column7");

    System.out.println("testAvgAggrMVGroupByMV");
    System.out.println(aggregationResult.getGroupByResult());

    Map<String, String> expectedGroupByResults = new HashMap<>();
    expectedGroupByResults.put("[2147483647]", "2147483647.00000");
    expectedGroupByResults.put("[536]", "536.00000");
    expectedGroupByResults.put("[566]", "522.00000");
    expectedGroupByResults.put("[532]", "519.75000");
    expectedGroupByResults.put("[552]", "504.96429");
    expectedGroupByResults.put("[529]", "499.44444");
    expectedGroupByResults.put("[555]", "495.66667");
    expectedGroupByResults.put("[570]", "495.00000");
    expectedGroupByResults.put("[554]", "493.92857");
    expectedGroupByResults.put("[545]", "493.03659");

    Map<String, String> actualGroupByResults = convertGroupByResultsToMap(aggregationResult.getGroupByResult());
    assertGroupByReulsts(actualGroupByResults, expectedGroupByResults);
  }

  @Test
  public void testMinAggrMVGroupBySV() throws Exception {
    final BrokerRequest brokerRequest = PQL2_COMPILER.compileToBrokerRequest(
        "select minMV(column7) from myTable group by column6");
    final BrokerResponseNative brokerResponse = queryAndgetBrokerResponse(brokerRequest);
    Assert.assertEquals(100000 * NUM_SEGMENTS, brokerResponse.getNumDocsScanned());
    Assert.assertEquals(brokerRequest.getAggregationsInfo().size(), brokerResponse.getAggregationResults().size());

    // Assertion on GroupBy
    AggregationResult aggregationResult = brokerResponse.getAggregationResults().get(0);
    Assert.assertEquals(aggregationResult.getFunction(), "min_column7");
    Assert.assertEquals(aggregationResult.getGroupByColumns().get(0), "column6");

    System.out.println("testMinAggrMVGroupBySV");
    System.out.println(aggregationResult.getGroupByResult());

  }

  @Test
  public void testMinAggrMVGroupByMV() throws Exception {
    final BrokerRequest brokerRequest = PQL2_COMPILER.compileToBrokerRequest(
        "select minMV(column7) from myTable group by column7");
    final BrokerResponseNative brokerResponse = queryAndgetBrokerResponse(brokerRequest);
    Assert.assertEquals(2000000, brokerResponse.getNumDocsScanned());
    Assert.assertEquals(brokerRequest.getAggregationsInfo().size(), brokerResponse.getAggregationResults().size());

    // Assertion on GroupBy
    AggregationResult aggregationResult = brokerResponse.getAggregationResults().get(0);
    Assert.assertEquals(aggregationResult.getFunction(), "min_column7");
    Assert.assertEquals(aggregationResult.getGroupByColumns().get(0), "column7");

    System.out.println("testMinAggrMVGroupByMV");
    System.out.println(aggregationResult.getGroupByResult());

  }

  @Test
  public void testMaxAggrMVGroupBySV() throws Exception {
    final BrokerRequest brokerRequest = PQL2_COMPILER.compileToBrokerRequest(
        "select maxMV(column7) from myTable group by column6");
    final BrokerResponseNative brokerResponse = queryAndgetBrokerResponse(brokerRequest);
    Assert.assertEquals(100000 * NUM_SEGMENTS, brokerResponse.getNumDocsScanned());
    Assert.assertEquals(brokerRequest.getAggregationsInfo().size(), brokerResponse.getAggregationResults().size());

    // Assertion on GroupBy
    AggregationResult aggregationResult = brokerResponse.getAggregationResults().get(0);
    Assert.assertEquals(aggregationResult.getFunction(), "max_column7");
    Assert.assertEquals(aggregationResult.getGroupByColumns().get(0), "column6");

    System.out.println("testMaxAggrMVGroupBySV");
    System.out.println(aggregationResult.getGroupByResult());

  }

  @Test
  public void testMaxAggrMVGroupByMV() throws Exception {
    final BrokerRequest brokerRequest = PQL2_COMPILER.compileToBrokerRequest(
        "select maxMV(column7) from myTable group by column7");
    final BrokerResponseNative brokerResponse = queryAndgetBrokerResponse(brokerRequest);
    Assert.assertEquals(2000000, brokerResponse.getNumDocsScanned());
    Assert.assertEquals(brokerRequest.getAggregationsInfo().size(), brokerResponse.getAggregationResults().size());

    // Assertion on GroupBy
    AggregationResult aggregationResult = brokerResponse.getAggregationResults().get(0);
    Assert.assertEquals(aggregationResult.getFunction(), "max_column7");
    Assert.assertEquals(aggregationResult.getGroupByColumns().get(0), "column7");

    System.out.println("testMaxAggrMVGroupByMV");
    System.out.println(aggregationResult.getGroupByResult());

    Map<String, String> expectedGroupByResults = new HashMap<>();
    expectedGroupByResults.put("[2147483647]", "2147483647.00000");

    Map<String, String> actualGroupByResults = convertGroupByResultsToMap(aggregationResult.getGroupByResult());
    assertGroupByReulsts(actualGroupByResults, expectedGroupByResults);

  }

  @Test
  public void testDistinctCountAggrMVGroupBySV() throws Exception {
    final BrokerRequest brokerRequest = PQL2_COMPILER.compileToBrokerRequest(
        "select distinctCountMV(column7) from myTable group by column6");
    final BrokerResponseNative brokerResponse = queryAndgetBrokerResponse(brokerRequest);
    Assert.assertEquals(100000 * NUM_SEGMENTS, brokerResponse.getNumDocsScanned());
    Assert.assertEquals(brokerRequest.getAggregationsInfo().size(), brokerResponse.getAggregationResults().size());

    // Assertion on GroupBy
    AggregationResult aggregationResult = brokerResponse.getAggregationResults().get(0);
    Assert.assertEquals(aggregationResult.getFunction(), "distinctCount_column7");
    Assert.assertEquals(aggregationResult.getGroupByColumns().get(0), "column6");

    System.out.println("testDistinctCountAggrMVGroupBySV");
    System.out.println(aggregationResult.getGroupByResult());
    Map<String, String> expectedGroupByResults = new HashMap<>();
    expectedGroupByResults.put("[2147483647]", "301");
    expectedGroupByResults.put("[1035]", "85");
    expectedGroupByResults.put("[1441]", "81");
    expectedGroupByResults.put("[1009]", "80");
    expectedGroupByResults.put("[1025]", "71");
    expectedGroupByResults.put("[1063]", "69");
    expectedGroupByResults.put("[1586]", "62");
    expectedGroupByResults.put("[1028]", "62");
    expectedGroupByResults.put("[1033]", "56");
    expectedGroupByResults.put("[162479]", "53");

    Map<String, String> actualGroupByResults = convertGroupByResultsToMap(aggregationResult.getGroupByResult());
    assertGroupByReulsts(actualGroupByResults, expectedGroupByResults);

  }

  @Test
  public void testDistinctCountAggrMVGroupByMV() throws Exception {
    final BrokerRequest brokerRequest = PQL2_COMPILER.compileToBrokerRequest(
        "select distinctCountMV(column7) from myTable group by column7");
    final BrokerResponseNative brokerResponse = queryAndgetBrokerResponse(brokerRequest);
    Assert.assertEquals(2000000, brokerResponse.getNumDocsScanned());
    Assert.assertEquals(brokerRequest.getAggregationsInfo().size(), brokerResponse.getAggregationResults().size());

    // Assertion on GroupBy
    AggregationResult aggregationResult = brokerResponse.getAggregationResults().get(0);
    Assert.assertEquals(aggregationResult.getFunction(), "distinctCount_column7");
    Assert.assertEquals(aggregationResult.getGroupByColumns().get(0), "column7");

    System.out.println("testDistinctCountAggrMVGroupByMV");
    System.out.println(aggregationResult.getGroupByResult());
    Map<String, String> expectedGroupByResults = new HashMap<>();
    expectedGroupByResults.put("[469]", "228");
    expectedGroupByResults.put("[363]", "227");
    expectedGroupByResults.put("[288]", "226");
    expectedGroupByResults.put("[211]", "223");
    expectedGroupByResults.put("[246]", "222");
    expectedGroupByResults.put("[523]", "202");
    expectedGroupByResults.put("[225]", "192");
    expectedGroupByResults.put("[483]", "174");

    Map<String, String> actualGroupByResults = convertGroupByResultsToMap(aggregationResult.getGroupByResult());
    assertGroupByReulsts(actualGroupByResults, expectedGroupByResults);

  }

  private void assertGroupByReulsts(Map<String, String> actualGroupByResults, Map<String, String> expectedGroupByResults) {
    for (String groupKey : expectedGroupByResults.keySet()) {
      Assert.assertTrue(actualGroupByResults.containsKey(groupKey));
      Assert.assertEquals(
          actualGroupByResults.get(groupKey),
          expectedGroupByResults.get(groupKey));
    }
  }

  private Map<String, String> convertGroupByResultsToMap(List<GroupByResult> groupByResultList) {
    Map<String, String> groupByResultsMap = new HashMap<>();
    for (GroupByResult groupByResult : groupByResultList) {
      String key = Arrays.toString(groupByResult.getGroup().toArray());
      String value = groupByResult.getValue().toString();
      groupByResultsMap.put(key, value);
    }
    return groupByResultsMap;
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
