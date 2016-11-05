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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.linkedin.pinot.common.query.ReduceService;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.response.broker.BrokerResponseNative;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.common.utils.primitive.MutableLongValue;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.operator.BReusableFilteredDocIdSetOperator;
import com.linkedin.pinot.core.operator.BaseOperator;
import com.linkedin.pinot.core.operator.MProjectionOperator;
import com.linkedin.pinot.core.operator.aggregation.AggregationOperator;
import com.linkedin.pinot.core.operator.blocks.IntermediateResultsBlock;
import com.linkedin.pinot.core.operator.filter.MatchEntireSegmentOperator;
import com.linkedin.pinot.core.plan.PlanNode;
import com.linkedin.pinot.core.plan.maker.InstancePlanMakerImplV2;
import com.linkedin.pinot.core.plan.maker.PlanMaker;
import com.linkedin.pinot.core.query.aggregation.CombineService;
import com.linkedin.pinot.core.query.aggregation.function.quantile.digest.QuantileDigest;
import com.linkedin.pinot.core.query.config.QueryExecutorConfig;
import com.linkedin.pinot.core.query.reduce.BrokerReduceService;
import com.linkedin.pinot.core.query.utils.Pair;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;
import com.linkedin.pinot.util.TestUtils;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;


public class AggregationQueriesOnMultiValueColumnTest {

  protected static Logger LOGGER = LoggerFactory.getLogger(AggregationQueriesOnMultiValueColumnTest.class);
  private final String AVRO_DATA = "data/test_data-mv.avro";
  private static File INDEX_DIR = new File(FileUtils.getTempDirectory() + File.separator + "AggregationQueriesOnMultiValueColumnTest");

  private IndexSegment _indexSegment;
  private Map<String, BaseOperator> _dataSourceMap;

  private BrokerRequest _brokerRequestNoFilter;
  private BrokerRequest _brokerRequestWithFilter;

  private PlanMaker _instancePlanMaker;

  @BeforeClass
  public void setup() throws Exception {
    _instancePlanMaker = new InstancePlanMakerImplV2(new QueryExecutorConfig(new PropertiesConfiguration()));
    setupSegment();
    setupQuery();
    setupDataSourceMap();
  }

  @AfterClass
  public void tearDown() {
    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }
    if (_indexSegment != null) {
      _indexSegment.destroy();
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
    Map<String, ColumnMetadata> medataMap = ((SegmentMetadataImpl) _indexSegment.getSegmentMetadata()).getColumnMetadataMap();
    for (ColumnMetadata columnMetadata : medataMap.values()) {
      LOGGER.debug(columnMetadata.getColumnName() + " : " + columnMetadata.isSingleValue());
    }
  }

  private void setupDataSourceMap() {
    _dataSourceMap = new HashMap<String, BaseOperator>();
    _dataSourceMap.put("column1", _indexSegment.getDataSource("column1"));
    _dataSourceMap.put("column2", _indexSegment.getDataSource("column2"));
    _dataSourceMap.put("column3", _indexSegment.getDataSource("column3"));
    _dataSourceMap.put("column4", _indexSegment.getDataSource("column4"));
    _dataSourceMap.put("column5", _indexSegment.getDataSource("column5"));
    _dataSourceMap.put("column6", _indexSegment.getDataSource("column6"));
    _dataSourceMap.put("column7", _indexSegment.getDataSource("column7"));
    _dataSourceMap.put("column8", _indexSegment.getDataSource("column8"));
    _dataSourceMap.put("column9", _indexSegment.getDataSource("column9"));
    _dataSourceMap.put("column10", _indexSegment.getDataSource("column10"));
    _dataSourceMap.put("column13", _indexSegment.getDataSource("column13"));
    _dataSourceMap.put("daysSinceEpoch", _indexSegment.getDataSource("daysSinceEpoch"));
    _dataSourceMap.put("weeksSinceEpochSunday", _indexSegment.getDataSource("weeksSinceEpochSunday"));
    _dataSourceMap.put("count", _indexSegment.getDataSource("count"));
  }

  private void setupQuery() {
    Pql2Compiler compiler = new Pql2Compiler();
    // Two single quotes in a single quoted string
    _brokerRequestNoFilter = compiler.compileToBrokerRequest(
        "select distinctCountMV(column7) from myTable");
    _brokerRequestWithFilter = compiler.compileToBrokerRequest(
        "select distinctCountMV(column7) from myTable where column6 = 2147483647");
  }

  @Test
  public void testCountOnMultiValue() {
    // Aggregation
    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("countmv");
    aggregationInfo.putToAggregationParams("column", "column7");
    BrokerRequest brokerRequest = new BrokerRequest();
    brokerRequest.setAggregationsInfo(Arrays.asList(aggregationInfo));

    final IntermediateResultsBlock block1 = getAggregationResultBlock(brokerRequest);
    Assert.assertEquals(134090, ((MutableLongValue) block1.getAggregationResult().get(0)).longValue());
    // Combine 
    final IntermediateResultsBlock block2 = getAggregationResultBlock(brokerRequest);
    Assert.assertEquals(134090, ((MutableLongValue) block2.getAggregationResult().get(0)).longValue());
    CombineService.mergeTwoBlocks(brokerRequest, block1, block2);
    Assert.assertEquals(268180, ((MutableLongValue) block1.getAggregationResult().get(0)).longValue());
  }

  @Test
  public void testSumOnMultiValue() {
    // Aggregation
    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("summv");
    aggregationInfo.putToAggregationParams("column", "column7");
    BrokerRequest brokerRequest = new BrokerRequest();
    brokerRequest.setAggregationsInfo(Arrays.asList(aggregationInfo));

    final IntermediateResultsBlock block1 = getAggregationResultBlock(brokerRequest);
    Assert.assertEquals(107243218420671.0, block1.getAggregationResult().get(0));
    // Combine 
    final IntermediateResultsBlock block2 = getAggregationResultBlock(brokerRequest);
    Assert.assertEquals(107243218420671.0, block2.getAggregationResult().get(0));
    CombineService.mergeTwoBlocks(brokerRequest, block1, block2);
    Assert.assertEquals(214486436841342.0, block1.getAggregationResult().get(0));
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testAvgOnMultiValue() {
    // Aggregation
    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("avgmv");
    aggregationInfo.putToAggregationParams("column", "column7");
    BrokerRequest brokerRequest = new BrokerRequest();
    brokerRequest.setAggregationsInfo(Arrays.asList(aggregationInfo));

    final IntermediateResultsBlock block1 = getAggregationResultBlock(brokerRequest);
    Assert.assertEquals(107243218420671.0, ((Pair) block1.getAggregationResult().get(0)).getFirst());
    Assert.assertEquals(134090L, ((Pair) block1.getAggregationResult().get(0)).getSecond());
    // Combine 
    final IntermediateResultsBlock block2 = getAggregationResultBlock(brokerRequest);
    Assert.assertEquals(107243218420671.0, ((Pair) block2.getAggregationResult().get(0)).getFirst());
    Assert.assertEquals(134090L, ((Pair) block2.getAggregationResult().get(0)).getSecond());
    CombineService.mergeTwoBlocks(brokerRequest, block1, block2);
    Assert.assertEquals(214486436841342.0, ((Pair) block1.getAggregationResult().get(0)).getFirst());
    Assert.assertEquals(268180L, ((Pair) block1.getAggregationResult().get(0)).getSecond());
  }

  @Test
  public void testMinOnMultiValue() {
    // Aggregation
    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("minmv");
    aggregationInfo.putToAggregationParams("column", "column7");
    BrokerRequest brokerRequest = new BrokerRequest();
    brokerRequest.setAggregationsInfo(Arrays.asList(aggregationInfo));

    final IntermediateResultsBlock block1 = getAggregationResultBlock(brokerRequest);
    Assert.assertEquals(201.0, block1.getAggregationResult().get(0));
    // Combine 
    final IntermediateResultsBlock block2 = getAggregationResultBlock(brokerRequest);
    Assert.assertEquals(201.0, block2.getAggregationResult().get(0));
    CombineService.mergeTwoBlocks(brokerRequest, block1, block2);
    Assert.assertEquals(201.0, block1.getAggregationResult().get(0));
  }

  @Test
  public void testMaxOnMultiValue() {
    // Aggregation
    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("maxmv");
    aggregationInfo.putToAggregationParams("column", "column7");
    BrokerRequest brokerRequest = new BrokerRequest();
    brokerRequest.setAggregationsInfo(Arrays.asList(aggregationInfo));

    final IntermediateResultsBlock block1 = getAggregationResultBlock(brokerRequest);
    Assert.assertEquals(2147483647.0, block1.getAggregationResult().get(0));
    // Combine 
    final IntermediateResultsBlock block2 = getAggregationResultBlock(brokerRequest);
    Assert.assertEquals(2147483647.0, block2.getAggregationResult().get(0));
    CombineService.mergeTwoBlocks(brokerRequest, block1, block2);
    Assert.assertEquals(2147483647.0, block1.getAggregationResult().get(0));
  }

  @Test
  public void testDistinctCountOnMultiValue() {
    // Aggregation
    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("distinctCountmv");
    aggregationInfo.putToAggregationParams("column", "column7");
    BrokerRequest brokerRequest = new BrokerRequest();
    brokerRequest.setAggregationsInfo(Arrays.asList(aggregationInfo));

    final IntermediateResultsBlock block1 = getAggregationResultBlock(brokerRequest);
    Assert.assertEquals(359, ((IntOpenHashSet) block1.getAggregationResult().get(0)).size());
    // Combine 
    final IntermediateResultsBlock block2 = getAggregationResultBlock(brokerRequest);
    Assert.assertEquals(359, ((IntOpenHashSet) block2.getAggregationResult().get(0)).size());
    CombineService.mergeTwoBlocks(brokerRequest, block1, block2);
    Assert.assertEquals(359, ((IntOpenHashSet) block1.getAggregationResult().get(0)).size());
  }

  @Test
  public void testDistinctCountHllOnMultiValue() {
    // Aggregation
    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("distinctCountHllmv");
    aggregationInfo.putToAggregationParams("column", "column7");
    BrokerRequest brokerRequest = new BrokerRequest();
    brokerRequest.setAggregationsInfo(Arrays.asList(aggregationInfo));

    final IntermediateResultsBlock block1 = getAggregationResultBlock(brokerRequest);
    Assert.assertEquals(376, ((HyperLogLog) block1.getAggregationResult().get(0)).cardinality());
    // Combine 
    final IntermediateResultsBlock block2 = getAggregationResultBlock(brokerRequest);
    Assert.assertEquals(376, ((HyperLogLog) block2.getAggregationResult().get(0)).cardinality());
    CombineService.mergeTwoBlocks(brokerRequest, block1, block2);
    Assert.assertEquals(376, ((HyperLogLog) block1.getAggregationResult().get(0)).cardinality());
  }

  @Test
  public void testPercentile50OnMultiValue() {
    // Aggregation
    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("Percentile50mv");
    aggregationInfo.putToAggregationParams("column", "column7");
    BrokerRequest brokerRequest = new BrokerRequest();
    brokerRequest.setAggregationsInfo(Arrays.asList(aggregationInfo));

    final IntermediateResultsBlock block1 = getAggregationResultBlock(brokerRequest);
    DoubleArrayList doubleArrayList1 = (DoubleArrayList) block1.getAggregationResult().get(0);
    Assert.assertEquals(134090, doubleArrayList1.size());
    // Combine 
    final IntermediateResultsBlock block2 = getAggregationResultBlock(brokerRequest);
    DoubleArrayList doubleArrayList2 = (DoubleArrayList) block2.getAggregationResult().get(0);
    Assert.assertEquals(134090, doubleArrayList2.size());

    CombineService.mergeTwoBlocks(brokerRequest, block1, block2);
    doubleArrayList1 = (DoubleArrayList) block1.getAggregationResult().get(0);
    Assert.assertEquals(268180, doubleArrayList1.size());
  }

  @Test
  public void testPercentileEst50OnMultiValue() {
    // Aggregation
    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("PercentileEst50mv");
    aggregationInfo.putToAggregationParams("column", "column7");
    BrokerRequest brokerRequest = new BrokerRequest();
    brokerRequest.setAggregationsInfo(Arrays.asList(aggregationInfo));

    final IntermediateResultsBlock block1 = getAggregationResultBlock(brokerRequest);
    QuantileDigest digest1 = (QuantileDigest) block1.getAggregationResult().get(0);
    Assert.assertEquals(134090.0, digest1.getCount());
    Assert.assertEquals(492, digest1.getQuantile(0.5));
    Assert.assertEquals(201, digest1.getMin());
    Assert.assertEquals(2147483647, digest1.getMax());

    // Combine 
    final IntermediateResultsBlock block2 = getAggregationResultBlock(brokerRequest);
    QuantileDigest digest2 = (QuantileDigest) block2.getAggregationResult().get(0);
    Assert.assertEquals(134090.0, digest2.getCount());
    Assert.assertEquals(492, digest2.getQuantile(0.5));
    Assert.assertEquals(201, digest2.getMin());
    Assert.assertEquals(2147483647, digest2.getMax());

    CombineService.mergeTwoBlocks(brokerRequest, block1, block2);
    digest1 = (QuantileDigest) block1.getAggregationResult().get(0);
    Assert.assertEquals(268180.0, digest1.getCount());
    Assert.assertEquals(495, digest1.getQuantile(0.5));
    Assert.assertEquals(204, digest1.getMin());
    Assert.assertEquals(2147483647, digest1.getMax());
  }

  private IntermediateResultsBlock getAggregationResultBlock(BrokerRequest brokerRequest) {

    int totalRawDocs = _indexSegment.getSegmentMetadata().getTotalRawDocs();
    Operator filterOperator =
        new MatchEntireSegmentOperator(totalRawDocs);
    final BReusableFilteredDocIdSetOperator docIdSetOperator =
        new BReusableFilteredDocIdSetOperator(
            filterOperator,
            totalRawDocs,
            5000);
    final MProjectionOperator projectionOperator =
        new MProjectionOperator(_dataSourceMap, docIdSetOperator);

    final AggregationOperator aggregationOperator =
        new AggregationOperator(brokerRequest.getAggregationsInfo(), projectionOperator, totalRawDocs);

    final IntermediateResultsBlock block =
        (IntermediateResultsBlock) aggregationOperator.nextBlock();

    return block;
  }

  @Test
  public void testInnerSegmentPlanMakerForAggregationFunctionOperatorNoFilter() throws Exception {
    final PlanNode rootPlanNode = _instancePlanMaker.makeInnerSegmentPlan(_indexSegment, _brokerRequestNoFilter);
    rootPlanNode.showTree("");
    final IntermediateResultsBlock resultBlock = (IntermediateResultsBlock) rootPlanNode.run().nextBlock();
    LOGGER.debug("RunningTime : {}", resultBlock.getTimeUsedMs());
    LOGGER.debug("NumDocsScanned : {}", resultBlock.getNumDocsScanned());
    LOGGER.debug("TotalDocs : {}", resultBlock.getTotalRawDocs());
    Assert.assertEquals(resultBlock.getNumDocsScanned(), 100000);
    Assert.assertEquals(resultBlock.getTotalRawDocs(), 100000);
    logReducedResults(resultBlock);
  }

  @Test
  public void testInnerSegmentPlanMakerForAggregationFunctionOperatorWithFilter() throws Exception {
    final PlanNode rootPlanNode = _instancePlanMaker.makeInnerSegmentPlan(_indexSegment, _brokerRequestWithFilter);
    rootPlanNode.showTree("");
    final IntermediateResultsBlock resultBlock = (IntermediateResultsBlock) rootPlanNode.run().nextBlock();
    LOGGER.debug("RunningTime : {}", resultBlock.getTimeUsedMs());
    LOGGER.debug("NumDocsScanned : {}", resultBlock.getNumDocsScanned());
    LOGGER.debug("TotalDocs : {}", resultBlock.getTotalRawDocs());
    Assert.assertEquals(resultBlock.getNumDocsScanned(), 56360);
    Assert.assertEquals(resultBlock.getTotalRawDocs(), 100000);
    logReducedResults(resultBlock);
  }

  @SuppressWarnings({
      "rawtypes", "unchecked"
  })
  private void logReducedResults(IntermediateResultsBlock resultBlock)
      throws Exception {
    final ReduceService reduceService = new BrokerReduceService();

    final Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    instanceResponseMap.put(new ServerInstance("localhost:0000"), resultBlock.getAggregationResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:1111"), resultBlock.getAggregationResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:2222"), resultBlock.getAggregationResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:3333"), resultBlock.getAggregationResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:4444"), resultBlock.getAggregationResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:5555"), resultBlock.getAggregationResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:6666"), resultBlock.getAggregationResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:7777"), resultBlock.getAggregationResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:8888"), resultBlock.getAggregationResultDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:9999"), resultBlock.getAggregationResultDataTable());
    final BrokerResponseNative reducedResults =
        (BrokerResponseNative) reduceService.reduceOnDataTable(_brokerRequestNoFilter, instanceResponseMap);

    LOGGER.debug("Reduced Result : {}", reducedResults);
  }
}
