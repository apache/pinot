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

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.response.broker.AggregationResult;
import com.linkedin.pinot.common.response.broker.BrokerResponseNative;
import com.linkedin.pinot.common.response.broker.GroupByResult;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.common.utils.NamedThreadFactory;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.data.manager.offline.OfflineSegmentDataManager;
import com.linkedin.pinot.core.data.manager.offline.SegmentDataManager;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.operator.BReusableFilteredDocIdSetOperator;
import com.linkedin.pinot.core.operator.BaseOperator;
import com.linkedin.pinot.core.operator.MProjectionOperator;
import com.linkedin.pinot.core.operator.UReplicatedProjectionOperator;
import com.linkedin.pinot.core.operator.blocks.IntermediateResultsBlock;
import com.linkedin.pinot.core.operator.filter.MatchEntireSegmentOperator;
import com.linkedin.pinot.core.operator.query.AggregationFunctionGroupByOperator;
import com.linkedin.pinot.core.operator.query.MAggregationFunctionGroupByWithDictionaryAndTrieTreeOperator;
import com.linkedin.pinot.core.operator.query.MAggregationGroupByOperator;
import com.linkedin.pinot.core.plan.Plan;
import com.linkedin.pinot.core.plan.PlanNode;
import com.linkedin.pinot.core.plan.maker.InstancePlanMakerImplV2;
import com.linkedin.pinot.core.plan.maker.PlanMaker;
import com.linkedin.pinot.core.query.aggregation.CombineService;
import com.linkedin.pinot.core.query.aggregation.groupby.AggregationGroupByOperatorService;
import com.linkedin.pinot.core.query.reduce.BrokerReduceService;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.util.DoubleComparisonUtil;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;
import com.linkedin.pinot.util.TestUtils;
import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class AggregationGroupByWithDictionaryAndTrieTreeOperatorMultiValueTest {
  protected static Logger LOGGER = LoggerFactory.getLogger(AggregationGroupByWithDictionaryAndTrieTreeOperatorMultiValueTest.class);
  private final String AVRO_DATA = "data/test_data-mv.avro";
  private static File INDEX_DIR = new File(FileUtils.getTempDirectory() + File.separator
      + "TestAggGroupByWithDictTrieOpMultiVal");
  private static File INDEXES_DIR = new File(FileUtils.getTempDirectory() + File.separator
      + "TestAggGroupByWithDictTrieOpMultiValList");

  public static IndexSegment _indexSegment;
  private static List<IndexSegment> _indexSegmentList;

  public static List<AggregationInfo> _aggregationInfos;
  public static int _numAggregations = 6;

  public Map<String, ColumnMetadata> _medataMap;
  public static GroupBy _groupBy;

  @BeforeClass
  public void setup() throws Exception {
    setupSegment();
    setupQuery();
    _indexSegmentList = new ArrayList<IndexSegment>();
  }

  @AfterClass
  public void tearDown() {
    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }
    if (INDEXES_DIR.exists()) {
      FileUtils.deleteQuietly(INDEXES_DIR);
    }
    if (_indexSegment != null) {
      _indexSegment.destroy();
    }
    for (IndexSegment segment : _indexSegmentList) {
      segment.destroy();
    }
    _indexSegmentList.clear();
  }

  private void setupSegmentList(int numberOfSegments) throws Exception {
    final String filePath = TestUtils.getFileFromResourceUrl(getClass().getClassLoader().getResource(AVRO_DATA));
    _indexSegmentList.clear();
    if (INDEXES_DIR.exists()) {
      FileUtils.deleteQuietly(INDEXES_DIR);
    }
    INDEXES_DIR.mkdir();

    for (int i = 0; i < numberOfSegments; ++i) {
      final File segmentDir = new File(INDEXES_DIR, "segment_" + i);

      final SegmentGeneratorConfig config =
          SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), segmentDir,
              "daysSinceEpoch", TimeUnit.DAYS, "test");

      final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
      driver.init(config);
      driver.build();

      LOGGER.debug("built at : {}", segmentDir.getAbsolutePath());
      _indexSegmentList.add(ColumnarSegmentLoader.load(new File(segmentDir, driver.getSegmentName()), ReadMode.heap));
    }
  }

  private void setupSegment() throws Exception {
    final String filePath = TestUtils.getFileFromResourceUrl(getClass().getClassLoader().getResource(AVRO_DATA));

    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }

    final SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), INDEX_DIR, "daysSinceEpoch",
            TimeUnit.DAYS, "test");

    final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();

    LOGGER.debug("built at : {}", INDEX_DIR.getAbsolutePath());
    final File indexSegmentDir = new File(INDEX_DIR, driver.getSegmentName());
    _indexSegment = ColumnarSegmentLoader.load(indexSegmentDir, ReadMode.heap);
    _medataMap = ((SegmentMetadataImpl) _indexSegment.getSegmentMetadata()).getColumnMetadataMap();
  }

  private void setupQuery() {
    _aggregationInfos = getAggregationsInfo();
    final List<String> groupbyColumns = new ArrayList<String>();
    groupbyColumns.add("column6");
    _groupBy = new GroupBy();
    _groupBy.setColumns(groupbyColumns);
    _groupBy.setTopN(10);
  }

  @Test
  public void testAggregationGroupBys() {
    final IntermediateResultsBlock block = getIntermediateResultsBlock();
    for (int i = 0; i < _numAggregations; ++i) {
      LOGGER.debug("AggregationResult {} : {}", i, block.getAggregationGroupByOperatorResult().get(i));
    }

  }

  private IntermediateResultsBlock getIntermediateResultsBlock() {
    final List<AggregationFunctionGroupByOperator> aggregationFunctionGroupByOperatorList =
        new ArrayList<AggregationFunctionGroupByOperator>();
    Operator filterOperator = new MatchEntireSegmentOperator(_indexSegment.getSegmentMetadata().getTotalDocs());
    final BReusableFilteredDocIdSetOperator docIdSetOperator =
        new BReusableFilteredDocIdSetOperator(filterOperator, _indexSegment.getSegmentMetadata().getTotalDocs(), 5000);
    final Map<String, BaseOperator> dataSourceMap = getDataSourceMap();
    final MProjectionOperator projectionOperator = new MProjectionOperator(dataSourceMap, docIdSetOperator);

    for (int i = 0; i < _numAggregations; ++i) {
      final MAggregationFunctionGroupByWithDictionaryAndTrieTreeOperator aggregationFunctionGroupByOperator =
          new MAggregationFunctionGroupByWithDictionaryAndTrieTreeOperator(_aggregationInfos.get(i), _groupBy,
              new UReplicatedProjectionOperator(projectionOperator), true);
      aggregationFunctionGroupByOperatorList.add(aggregationFunctionGroupByOperator);
    }

    final MAggregationGroupByOperator aggregationGroupByOperator =
        new MAggregationGroupByOperator(_indexSegment, _aggregationInfos, _groupBy, projectionOperator,
            aggregationFunctionGroupByOperatorList);

    return (IntermediateResultsBlock) aggregationGroupByOperator.nextBlock();
  }

  @Test
  public void testAggregationGroupBysWithCombine() {
    final IntermediateResultsBlock block = getIntermediateResultsBlock();

    for (int i = 0; i < _numAggregations; ++i) {
      LOGGER.debug("Result {} : {}", i, block.getAggregationGroupByOperatorResult().get(i));
    }

    ////////////////////////////////////////////////////////////////////////////////
    final IntermediateResultsBlock block1 = getIntermediateResultsBlock();

    for (int i = 0; i < _numAggregations; ++i) {
      LOGGER.debug("Result {} : {}", i, block1.getAggregationGroupByOperatorResult().get(i));
    }
    CombineService.mergeTwoBlocks(getAggregationGroupByNoFilterBrokerRequest(), block, block1);

    for (int i = 0; i < _numAggregations; ++i) {
      LOGGER.debug("Combine Result {} : {}", i, block.getAggregationGroupByOperatorResult().get(i));
    }

  }

  @Test
  public void testAggregationGroupBysWithDataTableEncodeAndDecode() throws Exception {
    final IntermediateResultsBlock block = getIntermediateResultsBlock();

    for (int i = 0; i < _numAggregations; ++i) {
      LOGGER.debug("Result {} : {}", i, block.getAggregationGroupByOperatorResult().get(i));
    }

    ////////////////////////////////////////////////////////////////////////////////
    final IntermediateResultsBlock block1 = getIntermediateResultsBlock();

    for (int i = 0; i < _numAggregations; ++i) {
      LOGGER.debug("Result {} : {}", i, block1.getAggregationGroupByOperatorResult().get(i));
    }
    CombineService.mergeTwoBlocks(getAggregationGroupByNoFilterBrokerRequest(), block, block1);

    for (int i = 0; i < _numAggregations; ++i) {
      LOGGER.debug("Combine Result {} : {}", i, block.getAggregationGroupByOperatorResult().get(i));
    }

    final DataTable dataTable = block.getAggregationGroupByResultDataTable();

    final List<Map<String, Serializable>> results =
        AggregationGroupByOperatorService.transformDataTableToGroupByResult(dataTable);

    for (int i = 0; i < _numAggregations; ++i) {
      LOGGER.debug("Decode AggregationResult from DataTable {} : {}", i, results.get(i));
    }
  }

  @Test
  public void testInnerSegmentPlanMakerForAggregationGroupByOperatorNoFilter() throws Exception {
    final BrokerRequest brokerRequest = getAggregationGroupByNoFilterBrokerRequest();
    final PlanMaker instancePlanMaker = new InstancePlanMakerImplV2();
    final PlanNode rootPlanNode = instancePlanMaker.makeInnerSegmentPlan(_indexSegment, brokerRequest);
    rootPlanNode.showTree("");
    final IntermediateResultsBlock resultBlock = (IntermediateResultsBlock) rootPlanNode.run().nextBlock();
    LOGGER.debug("RunningTime : {}", resultBlock.getTimeUsedMs());
    LOGGER.debug("NumDocsScanned : {}", resultBlock.getNumDocsScanned());
    LOGGER.debug("TotalDocs : {}", resultBlock.getTotalRawDocs());

    logJsonResult(brokerRequest, resultBlock);
  }

  private void logJsonResult(BrokerRequest brokerRequest, IntermediateResultsBlock resultBlock)
      throws Exception {
    final AggregationGroupByOperatorService aggregationGroupByOperatorService =
        new AggregationGroupByOperatorService(_aggregationInfos, brokerRequest.getGroupBy());

    final Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    instanceResponseMap.put(new ServerInstance("localhost:0000"), resultBlock.getAggregationGroupByResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:1111"), resultBlock.getAggregationGroupByResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:2222"), resultBlock.getAggregationGroupByResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:3333"), resultBlock.getAggregationGroupByResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:4444"), resultBlock.getAggregationGroupByResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:5555"), resultBlock.getAggregationGroupByResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:6666"), resultBlock.getAggregationGroupByResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:7777"), resultBlock.getAggregationGroupByResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:8888"), resultBlock.getAggregationGroupByResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:9999"), resultBlock.getAggregationGroupByResultDataTable());
    final List<Map<String, Serializable>> reducedResults =
        aggregationGroupByOperatorService.reduceGroupByOperators(instanceResponseMap);

    final List<JSONObject> jsonResult = aggregationGroupByOperatorService.renderGroupByOperators(reducedResults);
    LOGGER.debug("Result: {}", jsonResult);
  }

  @Test
  public void testInnerSegmentPlanMakerForAggregationGroupByOperatorWithFilter() throws Exception {
    final BrokerRequest brokerRequest = getAggregationGroupByWithFilterBrokerRequest();
    final PlanMaker instancePlanMaker = new InstancePlanMakerImplV2();
    final PlanNode rootPlanNode = instancePlanMaker.makeInnerSegmentPlan(_indexSegment, brokerRequest);
    rootPlanNode.showTree("");
    final IntermediateResultsBlock resultBlock = (IntermediateResultsBlock) rootPlanNode.run().nextBlock();
    LOGGER.debug("RunningTime : {}", resultBlock.getTimeUsedMs());
    LOGGER.debug("NumDocsScanned : {}", resultBlock.getNumDocsScanned());
    LOGGER.debug("TotalDocs : {}", resultBlock.getTotalRawDocs());
    Assert.assertEquals(resultBlock.getNumDocsScanned(), 5721);
    Assert.assertEquals(resultBlock.getTotalRawDocs(), 100000);

    logJsonResult(brokerRequest, resultBlock);
  }

  @Test
  public void testInterSegmentAggregationGroupByPlanMakerAndRun() throws Exception {
    final int numSegments = 5;
    setupSegmentList(numSegments);
    final PlanMaker instancePlanMaker = new InstancePlanMakerImplV2();
    final BrokerRequest brokerRequest = getAggregationGroupByNoFilterBrokerRequest();
    final ExecutorService executorService = Executors.newCachedThreadPool(new NamedThreadFactory("test-plan-maker"));
    final Plan globalPlan =
        instancePlanMaker.makeInterSegmentPlan(makeSegMgrList(_indexSegmentList), brokerRequest, executorService, 150000);
    globalPlan.print();
    globalPlan.execute();
    final DataTable instanceResponse = globalPlan.getInstanceResponse();
    LOGGER.debug("instanceResponse: {}", instanceResponse);

    final BrokerReduceService reduceService = new BrokerReduceService();
    final Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse);
    final BrokerResponseNative brokerResponse = reduceService.reduceOnDataTable(brokerRequest, instanceResponseMap);
    assertBrokerResponse(numSegments, brokerResponse);
  }

  private List<SegmentDataManager> makeSegMgrList(List<IndexSegment> indexSegmentList) {
    List<SegmentDataManager> segMgrList = new ArrayList(indexSegmentList.size());
    for (IndexSegment segment : indexSegmentList) {
      segMgrList.add(new OfflineSegmentDataManager(segment));
    }
    return segMgrList;
  }

  @Test
  public void testEmptyQueryResultsForInterSegmentAggregationGroupBy() throws Exception {
    final int numSegments = 20;
    setupSegmentList(numSegments);
    final PlanMaker instancePlanMaker = new InstancePlanMakerImplV2();
    final BrokerRequest brokerRequest = getAggregationGroupByWithEmptyFilterBrokerRequest();
    final ExecutorService executorService = Executors.newCachedThreadPool(new NamedThreadFactory("test-plan-maker"));
    final Plan globalPlan =
        instancePlanMaker.makeInterSegmentPlan(makeSegMgrList(_indexSegmentList), brokerRequest, executorService,
            150000);
    globalPlan.print();
    globalPlan.execute();
    final DataTable instanceResponse = globalPlan.getInstanceResponse();
    LOGGER.debug("instanceResponse: {}", instanceResponse);

    final BrokerReduceService reduceService = new BrokerReduceService();
    final Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse);
    final BrokerResponseNative brokerResponse = reduceService.reduceOnDataTable(brokerRequest, instanceResponseMap);
    assertEmptyBrokerResponse(brokerResponse);
  }

  private void assertBrokerResponse(int numSegments, BrokerResponseNative brokerResponse) {
    Assert.assertEquals(100000 * numSegments, brokerResponse.getNumDocsScanned());
    final int groupSize = 15;
    verifyResponse(brokerResponse, groupSize);

    // Assertion on Aggregation Results
    final List<double[]> aggregationResults = getAggregationResult(numSegments);
    final List<String[]> groupByResult = getGroupResult();
    for (int j = 0; j < _numAggregations; ++j) {
      final double[] aggResult = aggregationResults.get(j);
      final String[] expectedGroupByResult = groupByResult.get(j);
      for (int i = 0; i < 15; ++i) {
        AggregationResult aggregationResult = brokerResponse.getAggregationResults().get(j);
        GroupByResult actualGroupByResult = aggregationResult.getGroupByResult().get(i);
        Assert.assertEquals(0, DoubleComparisonUtil.defaultDoubleCompare(aggResult[i],
            Double.valueOf(actualGroupByResult.getValue().toString())));
        if ((i < 14 && aggResult[i] == aggResult[i + 1]) || (i > 0 && aggResult[i] == aggResult[i - 1])) {
          //do nothing, as we have multiple groups within same value.
        } else {
          JSONArray actualGroup = new JSONArray(actualGroupByResult.getGroup());
          Assert.assertEquals(actualGroup.toString(), expectedGroupByResult[i]);
        }
      }
    }
  }

  private void verifyResponse(BrokerResponseNative brokerResponse, int groupLength) {
    List<AggregationResult> aggregationResults = brokerResponse.getAggregationResults();
    Assert.assertEquals(aggregationResults.size(), _numAggregations);

    for (int i = 0; i < _numAggregations; ++i) {
      AggregationResult aggregationResult = aggregationResults.get(i);
      List<String> groupByColumns = aggregationResult.getGroupByColumns();
      Assert.assertEquals(groupByColumns.size(), 2);
      Assert.assertTrue(groupByColumns.contains("column6"));
      Assert.assertTrue(groupByColumns.contains("column7"));
      Assert.assertEquals(aggregationResult.getGroupByResult().size(), groupLength);
    }

    // Assertion on function name
    Assert.assertEquals(aggregationResults.get(0).getFunction(), "count_star");
    Assert.assertEquals(aggregationResults.get(1).getFunction(), "sum_count");
    Assert.assertEquals(aggregationResults.get(2).getFunction(), "max_count");
    Assert.assertEquals(aggregationResults.get(3).getFunction(), "min_count");
    Assert.assertEquals(aggregationResults.get(4).getFunction(), "avg_count");
    Assert.assertEquals(aggregationResults.get(5).getFunction(), "distinctCount_column2");
  }

  private void assertEmptyBrokerResponse(BrokerResponseNative brokerResponse) {
    Assert.assertEquals(0, brokerResponse.getNumDocsScanned());
    final int groupLength = 0;
    verifyResponse(brokerResponse, groupLength);
  }

  private static List<double[]> getAggregationResult(int numSegments) {
    final List<double[]> aggregationResultList = new ArrayList<double[]>();
    aggregationResultList.add(getCountResult(numSegments));
    aggregationResultList.add(getSumResult(numSegments));
    aggregationResultList.add(getMaxResult());
    aggregationResultList.add(getMinResult());
    aggregationResultList.add(getAvgResult());
    aggregationResultList.add(getDistinctCountResult());
    return aggregationResultList;
  }

  private static List<String[]> getGroupResult() {
    final List<String[]> groupResults = new ArrayList<String[]>();
    groupResults.add(getCountGroupResult());
    groupResults.add(getSumGroupResult());
    groupResults.add(getMaxGroupResult());
    groupResults.add(getMinGroupResult());
    groupResults.add(getAvgGroupResult());
    groupResults.add(getDistinctCountGroupResult());
    return groupResults;
  }

  private static double[] getCountResult(int numSegments) {
    return new double[] { 45806 * numSegments, 1621 * numSegments, 1182 * numSegments, 1080 * numSegments, 1041 * numSegments, 712 * numSegments, 622 * numSegments, 545 * numSegments, 418 * numSegments, 418 * numSegments, 418 * numSegments, 418 * numSegments, 295 * numSegments, 256 * numSegments, 254 * numSegments };
  }

  private static String[] getCountGroupResult() {
    return new String[] { "[\"2147483647\",\"2147483647\"]", "[\"363\",\"2147483647\"]", "[\"523\",\"2147483647\"]", "[\"469\",\"2147483647\"]", "[\"564\",\"2147483647\"]", "[\"288\",\"2147483647\"]", "[\"246\",\"2147483647\"]", "[\"225\",\"2147483647\"]", "[\"478\",\"3311739\"]", "[\"314\",\"3311739\"]", "[\"246\",\"3311739\"]", "[\"523\",\"3311739\"]", "[\"211\",\"2147483647\"]", "[\"332\",\"2147483647\"]", "[\"496\",\"2147483647\"]" };
  }

  private static double[] getSumResult(int numSegments) {
    return new double[] { 40797703056772.0 * numSegments, 1443764499302.0 * numSegments, 1052763502884.0 * numSegments, 961915890960.0 * numSegments, 927180039342.0 * numSegments, 634151957744.0 * numSegments, 553992300164.0 * numSegments, 485411259790.0 * numSegments, 372297076316.0 * numSegments, 372297076316.0 * numSegments, 372297076316.0 * numSegments, 372297076316.0 * numSegments, 262745544290.0 * numSegments, 228009692672.0 * numSegments, 226228366948.0 * numSegments };
  }

  private static String[] getSumGroupResult() {
    return new String[] { "[\"2147483647\",\"2147483647\"]", "[\"363\",\"2147483647\"]", "[\"523\",\"2147483647\"]", "[\"469\",\"2147483647\"]", "[\"564\",\"2147483647\"]", "[\"288\",\"2147483647\"]", "[\"246\",\"2147483647\"]", "[\"225\",\"2147483647\"]", "[\"478\",\"3311739\"]", "[\"314\",\"3311739\"]", "[\"246\",\"3311739\"]", "[\"523\",\"3311739\"]", "[\"211\",\"2147483647\"]", "[\"332\",\"2147483647\"]", "[\"496\",\"2147483647\"]" };
  }

  private static double[] getMaxResult() {
    return new double[] { 890662862, 890662862, 890662862, 890662862, 890662862, 890662862, 890662862, 890662862, 890662862, 890662862, 890662862, 890662862, 890662862, 890662862, 890662862 };
  }

  private static String[] getMaxGroupResult() {
    return new String[1];
  }

  private static double[] getMinResult() {
    return new double[] { 890662862, 890662862, 890662862, 890662862, 890662862, 890662862, 890662862, 890662862, 890662862, 890662862, 890662862, 890662862, 890662862, 890662862, 890662862 };
  }

  private static String[] getMinGroupResult() {
    return new String[1];
  }

  private static double[] getAvgResult() {
    return new double[] { 890662862, 890662862, 890662862, 890662862, 890662862, 890662862, 890662862, 890662862, 890662862, 890662862, 890662862, 890662862, 890662862, 890662862, 890662862 };
  }

  private static String[] getAvgGroupResult() {
    return new String[1];
  }

  private static double[] getDistinctCountResult() {
    return new double[] { 22056, 1146, 838, 755, 732, 534, 458, 418, 418, 418, 418, 396, 216, 194, 190 };
  }

  private static String[] getDistinctCountGroupResult() {
    return new String[] { "[\"2147483647\",\"2147483647\"]", "[\"363\",\"2147483647\"]", "[\"523\",\"2147483647\"]", "[\"469\",\"2147483647\"]", "[\"564\",\"2147483647\"]", "[\"288\",\"2147483647\"]", "[\"246\",\"2147483647\"]", "[\"478\",\"3311739\"]", "[\"314\",\"3311739\"]", "[\"246\",\"3311739\"]", "[\"523\",\"3311739\"]", "[\"225\",\"2147483647\"]", "[\"211\",\"2147483647\"]", "[\"332\",\"2147483647\"]", "[\"496\",\"2147483647\"]" };
  }

  private static BrokerRequest getAggregationGroupByNoFilterBrokerRequest() {
    final BrokerRequest brokerRequest = new BrokerRequest();
    final List<AggregationInfo> aggregationsInfo = getAggregationsInfo();
    brokerRequest.setAggregationsInfo(aggregationsInfo);
    brokerRequest.setGroupBy(getGroupBy());
    return brokerRequest;
  }

  private static List<AggregationInfo> getAggregationsInfo() {
    final List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(getCountAggregationInfo());
    aggregationsInfo.add(getSumAggregationInfo());
    aggregationsInfo.add(getMaxAggregationInfo());
    aggregationsInfo.add(getMinAggregationInfo());
    aggregationsInfo.add(getAvgAggregationInfo());
    aggregationsInfo.add(getDistinctCountAggregationInfo("column2"));
    return aggregationsInfo;
  }

  private static Map<String, BaseOperator> getDataSourceMap() {
    final Map<String, BaseOperator> dataSourceMap = new HashMap<String, BaseOperator>();
    dataSourceMap.put("column1", _indexSegment.getDataSource("column1"));
    dataSourceMap.put("column2", _indexSegment.getDataSource("column2"));
    dataSourceMap.put("column3", _indexSegment.getDataSource("column3"));
    dataSourceMap.put("column4", _indexSegment.getDataSource("column4"));
    dataSourceMap.put("column5", _indexSegment.getDataSource("column5"));
    dataSourceMap.put("column6", _indexSegment.getDataSource("column6"));
    dataSourceMap.put("column7", _indexSegment.getDataSource("column7"));
    dataSourceMap.put("column8", _indexSegment.getDataSource("column8"));
    dataSourceMap.put("column9", _indexSegment.getDataSource("column9"));
    dataSourceMap.put("column10", _indexSegment.getDataSource("column10"));
    dataSourceMap.put("daysSinceEpoch", _indexSegment.getDataSource("daysSinceEpoch"));
    dataSourceMap.put("weeksSinceEpochSunday", _indexSegment.getDataSource("weeksSinceEpochSunday"));

    dataSourceMap.put("count", _indexSegment.getDataSource("count"));
    return dataSourceMap;
  }

  private static AggregationInfo getCountAggregationInfo() {
    final String type = "count";
    final Map<String, String> params = new HashMap<String, String>();
    params.put("column", "*");
    final AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType(type);
    aggregationInfo.setAggregationParams(params);
    return aggregationInfo;
  }

  private static AggregationInfo getSumAggregationInfo() {
    final String type = "sum";
    return getAggregationInfo(type);
  }

  private static AggregationInfo getAggregationInfo(String type) {
    final Map<String, String> params = new HashMap<String, String>();
    params.put("column", "count");
    final AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType(type);
    aggregationInfo.setAggregationParams(params);
    return aggregationInfo;
  }

  private static AggregationInfo getMaxAggregationInfo() {
    final String type = "max";
    return getAggregationInfo(type);
  }

  private static AggregationInfo getMinAggregationInfo() {
    final String type = "min";
    return getAggregationInfo(type);
  }

  private static AggregationInfo getAvgAggregationInfo() {
    final String type = "avg";
    return getAggregationInfo(type);
  }

  private static AggregationInfo getDistinctCountAggregationInfo(String dim) {
    final String type = "distinctCount";
    final Map<String, String> params = new HashMap<String, String>();
    params.put("column", dim);

    final AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType(type);
    aggregationInfo.setAggregationParams(params);
    return aggregationInfo;
  }

  private static GroupBy getGroupBy() {
    final GroupBy groupBy = new GroupBy();
    final List<String> columns = new ArrayList<String>();
    columns.add("column7");
    columns.add("column6");
    groupBy.setColumns(columns);
    groupBy.setTopN(15);
    return groupBy;
  }

  private static BrokerRequest getAggregationGroupByWithFilterBrokerRequest() {
    final BrokerRequest brokerRequest = new BrokerRequest();
    final List<AggregationInfo> aggregationsInfo = getAggregationsInfo();
    brokerRequest.setAggregationsInfo(aggregationsInfo);
    brokerRequest.setGroupBy(getGroupBy());
    setFilterQuery(brokerRequest);
    return brokerRequest;
  }

  private static BrokerRequest setFilterQuery(BrokerRequest brokerRequest) {
    FilterQueryTree filterQueryTree;
    final String filterColumn = "column2";
    final String filterVal = "2080179800";
    if (filterColumn.contains(",")) {
      final String[] filterColumns = filterColumn.split(",");
      final String[] filterValues = filterVal.split(",");
      final List<FilterQueryTree> nested = new ArrayList<FilterQueryTree>();
      for (int i = 0; i < filterColumns.length; i++) {

        final List<String> vals = new ArrayList<String>();
        vals.add(filterValues[i]);
        final FilterQueryTree d = new FilterQueryTree(i + 1, filterColumns[i], vals, FilterOperator.EQUALITY, null);
        nested.add(d);
      }
      filterQueryTree = new FilterQueryTree(0, null, null, FilterOperator.AND, nested);
    } else {
      final List<String> vals = new ArrayList<String>();
      vals.add(filterVal);
      filterQueryTree = new FilterQueryTree(0, filterColumn, vals, FilterOperator.EQUALITY, null);
    }
    RequestUtils.generateFilterFromTree(filterQueryTree, brokerRequest);
    return brokerRequest;
  }

  private static BrokerRequest getAggregationGroupByWithEmptyFilterBrokerRequest() {
    final BrokerRequest brokerRequest = new BrokerRequest();
    final List<AggregationInfo> aggregationsInfo = getAggregationsInfo();
    brokerRequest.setAggregationsInfo(aggregationsInfo);
    brokerRequest.setGroupBy(getGroupBy());
    setEmptyFilterQuery(brokerRequest);
    return brokerRequest;
  }

  private static BrokerRequest setEmptyFilterQuery(BrokerRequest brokerRequest) {
    FilterQueryTree filterQueryTree;
    final String filterColumn = "column2";
    final String filterVal = "14125399";
    if (filterColumn.contains(",")) {
      final String[] filterColumns = filterColumn.split(",");
      final String[] filterValues = filterVal.split(",");
      final List<FilterQueryTree> nested = new ArrayList<FilterQueryTree>();
      for (int i = 0; i < filterColumns.length; i++) {

        final List<String> vals = new ArrayList<String>();
        vals.add(filterValues[i]);
        final FilterQueryTree d = new FilterQueryTree(i + 1, filterColumns[i], vals, FilterOperator.EQUALITY, null);
        nested.add(d);
      }
      filterQueryTree = new FilterQueryTree(0, null, null, FilterOperator.AND, nested);
    } else {
      final List<String> vals = new ArrayList<String>();
      vals.add(filterVal);
      filterQueryTree = new FilterQueryTree(0, filterColumn, vals, FilterOperator.EQUALITY, null);
    }
    RequestUtils.generateFilterFromTree(filterQueryTree, brokerRequest);
    return brokerRequest;
  }
}
