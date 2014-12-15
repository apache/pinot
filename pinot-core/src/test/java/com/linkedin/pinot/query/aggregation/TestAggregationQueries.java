package com.linkedin.pinot.query.aggregation;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.query.ReduceService;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.response.BrokerResponse;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.common.utils.NamedThreadFactory;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import com.linkedin.pinot.core.block.query.IntermediateResultsBlock;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.operator.BDocIdSetOperator;
import com.linkedin.pinot.core.operator.DataSource;
import com.linkedin.pinot.core.operator.MProjectionOperator;
import com.linkedin.pinot.core.operator.UReplicatedProjectionOperator;
import com.linkedin.pinot.core.operator.query.BAggregationFunctionOperator;
import com.linkedin.pinot.core.operator.query.MAggregationOperator;
import com.linkedin.pinot.core.plan.Plan;
import com.linkedin.pinot.core.plan.PlanNode;
import com.linkedin.pinot.core.plan.maker.InstancePlanMakerImplV0;
import com.linkedin.pinot.core.plan.maker.PlanMaker;
import com.linkedin.pinot.core.query.aggregation.CombineService;
import com.linkedin.pinot.core.query.reduce.DefaultReduceService;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.core.segment.index.IndexSegmentImpl;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.readers.DictionaryReader;
import com.linkedin.pinot.core.time.SegmentTimeUnit;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;


public class TestAggregationQueries {

  private final String AVRO_DATA = "data/sample_data.avro";
  private static File INDEX_DIR = new File("TestAggregationQueries");
  private static File INDEXES_DIR = new File("TestAggregationQueriesList");
  private static String SEGMENT_ID = "test_testTable_15544_15544_";

  public static IndexSegment _indexSegment;
  private static List<IndexSegment> _indexSegmentList;

  public static AggregationInfo _paramsInfo;
  public static List<AggregationInfo> _aggregationInfos;
  public static int _numAggregations = 6;

  public Map<String, DictionaryReader> _dictionaryMap;
  public Map<String, ColumnMetadata> _medataMap;

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
  }

  private void setupSegment() throws Exception {
    final String filePath = getClass().getClassLoader().getResource(AVRO_DATA).getFile();

    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }

    final SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), INDEX_DIR, "time_day",
            SegmentTimeUnit.days, "test", "testTable");

    final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();

    System.out.println("built at : " + INDEX_DIR.getAbsolutePath());
    File indexSegmentDir = new File(INDEX_DIR, SEGMENT_ID);
    _indexSegment = ColumnarSegmentLoader.load(indexSegmentDir, ReadMode.heap);
    _dictionaryMap = ((IndexSegmentImpl) _indexSegment).getDictionaryMap();
    _medataMap =
        ((SegmentMetadataImpl) ((IndexSegmentImpl) _indexSegment).getSegmentMetadata()).getColumnMetadataMap();
  }

  private void setupSegmentList(int numberOfSegments) throws Exception {
    final String filePath = getClass().getClassLoader().getResource(AVRO_DATA).getFile();

    if (INDEXES_DIR.exists()) {
      FileUtils.deleteQuietly(INDEXES_DIR);
    }
    INDEXES_DIR.mkdir();

    for (int i = 0; i < numberOfSegments; ++i) {
      final File segmentDir = new File(INDEXES_DIR, "segment_" + i);

      final SegmentGeneratorConfig config =
          SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), segmentDir, "time_day",
              SegmentTimeUnit.days, "test", "testTable");

      final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
      driver.init(config);
      driver.build();

      System.out.println("built at : " + segmentDir.getAbsolutePath());
      _indexSegmentList.add(ColumnarSegmentLoader.load(new File(segmentDir, SEGMENT_ID), ReadMode.heap));
    }
  }

  public void setupQuery() {
    _aggregationInfos = getAggregationsInfo();
  }

  @Test
  public void testAggregationFunctions() {
    final List<BAggregationFunctionOperator> aggregationFunctionOperatorList =
        new ArrayList<BAggregationFunctionOperator>();
    final BDocIdSetOperator docIdSetOperator = new BDocIdSetOperator(null, _indexSegment, 5000);
    final Map<String, DataSource> dataSourceMap = getDataSourceMap();

    final MProjectionOperator projectionOperator = new MProjectionOperator(dataSourceMap, docIdSetOperator);

    for (int i = 0; i < _numAggregations; ++i) {
      final BAggregationFunctionOperator aggregationFunctionOperator =
          new BAggregationFunctionOperator(_aggregationInfos.get(i), new UReplicatedProjectionOperator(
              projectionOperator));

      aggregationFunctionOperatorList.add(aggregationFunctionOperator);
    }

    final MAggregationOperator aggregationOperator =
        new MAggregationOperator(_indexSegment, _aggregationInfos, projectionOperator, aggregationFunctionOperatorList);

    final IntermediateResultsBlock block = (IntermediateResultsBlock) aggregationOperator.nextBlock();
    for (int i = 0; i < _numAggregations; ++i) {
      System.out.println(block.getAggregationResult().get(i));
    }
  }

  @Test
  public void testAggregationFunctionsWithCombine() {
    final List<BAggregationFunctionOperator> aggregationFunctionOperatorList =
        new ArrayList<BAggregationFunctionOperator>();
    final BDocIdSetOperator docIdSetOperator = new BDocIdSetOperator(null, _indexSegment, 5000);
    final Map<String, DataSource> dataSourceMap = getDataSourceMap();

    final MProjectionOperator projectionOperator = new MProjectionOperator(dataSourceMap, docIdSetOperator);

    for (int i = 0; i < _numAggregations; ++i) {
      final BAggregationFunctionOperator aggregationFunctionOperator =
          new BAggregationFunctionOperator(_aggregationInfos.get(i), new UReplicatedProjectionOperator(
              projectionOperator));

      aggregationFunctionOperatorList.add(aggregationFunctionOperator);
    }

    final MAggregationOperator aggregationOperator =
        new MAggregationOperator(_indexSegment, _aggregationInfos, projectionOperator, aggregationFunctionOperatorList);

    final IntermediateResultsBlock block = (IntermediateResultsBlock) aggregationOperator.nextBlock();

    System.out.println("Result 1: ");
    for (int i = 0; i < _numAggregations; ++i) {
      System.out.println(block.getAggregationResult().get(i));
    }
    /////////////////////////////////////////////////////////////////////////
    final List<BAggregationFunctionOperator> aggregationFunctionOperatorList1 =
        new ArrayList<BAggregationFunctionOperator>();
    final BDocIdSetOperator docIdSetOperator1 = new BDocIdSetOperator(null, _indexSegment, 5000);
    final Map<String, DataSource> dataSourceMap1 = getDataSourceMap();
    final MProjectionOperator projectionOperator1 = new MProjectionOperator(dataSourceMap1, docIdSetOperator1);

    for (int i = 0; i < _numAggregations; ++i) {
      final BAggregationFunctionOperator aggregationFunctionOperator1 =
          new BAggregationFunctionOperator(_aggregationInfos.get(i), new UReplicatedProjectionOperator(
              projectionOperator1));

      aggregationFunctionOperatorList1.add(aggregationFunctionOperator1);
    }

    final MAggregationOperator aggregationOperator1 =
        new MAggregationOperator(_indexSegment, _aggregationInfos, projectionOperator1,
            aggregationFunctionOperatorList1);

    final IntermediateResultsBlock block1 = (IntermediateResultsBlock) aggregationOperator1.nextBlock();

    System.out.println("Result 2: ");
    for (int i = 0; i < _numAggregations; ++i) {
      System.out.println(block1.getAggregationResult().get(i));
    }
    CombineService.mergeTwoBlocks(getAggregationNoFilterBrokerRequest(), block, block1);

    System.out.println("Combined Result : ");
    for (int i = 0; i < _numAggregations; ++i) {
      System.out.println(block.getAggregationResult().get(i));
    }
  }

  @Test
  public void testInnerSegmentPlanMakerForAggregationFunctionOperatorNoFilter() throws Exception {
    final BrokerRequest brokerRequest = getAggregationNoFilterBrokerRequest();
    final PlanMaker instancePlanMaker = new InstancePlanMakerImplV0();
    final PlanNode rootPlanNode = instancePlanMaker.makeInnerSegmentPlan(_indexSegment, brokerRequest);
    rootPlanNode.showTree("");
    // UAggregationGroupByOperator operator = (UAggregationGroupByOperator) rootPlanNode.run();
    final MAggregationOperator operator = (MAggregationOperator) rootPlanNode.run();
    final IntermediateResultsBlock resultBlock = (IntermediateResultsBlock) operator.nextBlock();
    System.out.println("RunningTime : " + resultBlock.getTimeUsedMs());
    System.out.println("NumDocsScanned : " + resultBlock.getNumDocsScanned());
    System.out.println("TotalDocs : " + resultBlock.getTotalDocs());

    ReduceService reduceService = new DefaultReduceService();

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
    final BrokerResponse reducedResults =
        reduceService.reduceOnDataTable(getAggregationNoFilterBrokerRequest(), instanceResponseMap);

    System.out.println(reducedResults);
  }

  @Test
  public void testInnerSegmentPlanMakerForAggregationFunctionOperatorWithFilter() throws Exception {
    final BrokerRequest brokerRequest = getAggregationWithFilterBrokerRequest();
    final PlanMaker instancePlanMaker = new InstancePlanMakerImplV0();
    final PlanNode rootPlanNode = instancePlanMaker.makeInnerSegmentPlan(_indexSegment, brokerRequest);
    rootPlanNode.showTree("");
    // UAggregationGroupByOperator operator = (UAggregationGroupByOperator) rootPlanNode.run();
    final MAggregationOperator operator = (MAggregationOperator) rootPlanNode.run();
    final IntermediateResultsBlock resultBlock = (IntermediateResultsBlock) operator.nextBlock();
    System.out.println("RunningTime : " + resultBlock.getTimeUsedMs());
    System.out.println("NumDocsScanned : " + resultBlock.getNumDocsScanned());
    System.out.println("TotalDocs : " + resultBlock.getTotalDocs());
    Assert.assertEquals(resultBlock.getNumDocsScanned(), 582);
    Assert.assertEquals(resultBlock.getTotalDocs(), 10001);

    ReduceService reduceService = new DefaultReduceService();

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
    final BrokerResponse reducedResults =
        reduceService.reduceOnDataTable(getAggregationNoFilterBrokerRequest(), instanceResponseMap);

    System.out.println(reducedResults);
  }

  @Test
  public void testInterSegmentAggregationFunctionPlanMakerAndRun() throws Exception {
    final int numSegments = 20;
    setupSegmentList(numSegments);
    final PlanMaker instancePlanMaker = new InstancePlanMakerImplV0();
    final BrokerRequest brokerRequest = getAggregationNoFilterBrokerRequest();
    final ExecutorService executorService = Executors.newCachedThreadPool(new NamedThreadFactory("test-plan-maker"));
    final Plan globalPlan = instancePlanMaker.makeInterSegmentPlan(_indexSegmentList, brokerRequest, executorService);
    globalPlan.print();
    globalPlan.execute();
    final DataTable instanceResponse = globalPlan.getInstanceResponse();
    System.out.println(instanceResponse);

    final DefaultReduceService defaultReduceService = new DefaultReduceService();
    final Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse);
    final BrokerResponse brokerResponse = defaultReduceService.reduceOnDataTable(brokerRequest, instanceResponseMap);
    System.out.println(new JSONArray(brokerResponse.getAggregationResults()));
    System.out.println("Time used : " + brokerResponse.getTimeUsedMs());
    assertBrokerResponse(numSegments, brokerResponse);
  }

  private void assertBrokerResponse(int numSegments, BrokerResponse brokerResponse) throws JSONException {
    Assert.assertEquals(10001 * numSegments, brokerResponse.getNumDocsScanned());
    Assert.assertEquals(_numAggregations, brokerResponse.getAggregationResults().size());

    // Assertion on Count
    Assert.assertEquals("count_star", brokerResponse.getAggregationResults().get(0).getString("function").toString());
    Assert.assertEquals(10001 * numSegments,
        Integer.parseInt(brokerResponse.getAggregationResults().get(0).getString("value")));

    Assert.assertEquals("sum_met_impressionCount", brokerResponse.getAggregationResults().get(1).getString("function")
        .toString());
    Assert.assertEquals(24574.0 * numSegments,
        Double.parseDouble(brokerResponse.getAggregationResults().get(1).getString("value")));

    Assert.assertEquals("max_met_impressionCount", brokerResponse.getAggregationResults().get(2).getString("function")
        .toString());
    Assert.assertEquals(53.0, Double.parseDouble(brokerResponse.getAggregationResults().get(2).getString("value")));

    Assert.assertEquals("min_met_impressionCount", brokerResponse.getAggregationResults().get(3).getString("function")
        .toString());
    Assert.assertEquals(1.0, Double.parseDouble(brokerResponse.getAggregationResults().get(3).getString("value")));

    Assert.assertEquals("avg_met_impressionCount", brokerResponse.getAggregationResults().get(4).getString("function")
        .toString());
    Assert.assertEquals(2.45715, Double.parseDouble(brokerResponse.getAggregationResults().get(4).getString("value")));

    Assert.assertEquals("distinctCount_dim_memberIndustry",
        brokerResponse.getAggregationResults().get(5).getString("function").toString());
    Assert.assertEquals(147, Integer.parseInt(brokerResponse.getAggregationResults().get(5).getString("value")));

  }

  private static BrokerRequest getAggregationNoFilterBrokerRequest() {
    final BrokerRequest brokerRequest = new BrokerRequest();
    final List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(getCountAggregationInfo());
    aggregationsInfo.add(getSumAggregationInfo());
    aggregationsInfo.add(getMaxAggregationInfo());
    aggregationsInfo.add(getMinAggregationInfo());
    aggregationsInfo.add(getAvgAggregationInfo());
    aggregationsInfo.add(getDistinctCountAggregationInfo("dim_memberIndustry"));
    brokerRequest.setAggregationsInfo(aggregationsInfo);
    return brokerRequest;
  }

  private static List<AggregationInfo> getAggregationsInfo() {
    final List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(getCountAggregationInfo());
    aggregationsInfo.add(getSumAggregationInfo());
    aggregationsInfo.add(getMaxAggregationInfo());
    aggregationsInfo.add(getMinAggregationInfo());
    aggregationsInfo.add(getAvgAggregationInfo());
    aggregationsInfo.add(getDistinctCountAggregationInfo("dim_memberIndustry"));
    return aggregationsInfo;
  }

  private static Map<String, DataSource> getDataSourceMap() {
    final Map<String, DataSource> dataSourceMap = new HashMap<String, DataSource>();
    dataSourceMap.put("dim_memberGender", _indexSegment.getDataSource("dim_memberGender"));
    dataSourceMap.put("dim_memberIndustry", _indexSegment.getDataSource("dim_memberIndustry"));
    dataSourceMap.put("met_impressionCount", _indexSegment.getDataSource("met_impressionCount"));
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
    final Map<String, String> params = new HashMap<String, String>();
    params.put("column", "met_impressionCount");
    final AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType(type);
    aggregationInfo.setAggregationParams(params);
    return aggregationInfo;
  }

  private static AggregationInfo getMaxAggregationInfo() {
    final String type = "max";
    final Map<String, String> params = new HashMap<String, String>();
    params.put("column", "met_impressionCount");
    final AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType(type);
    aggregationInfo.setAggregationParams(params);
    return aggregationInfo;
  }

  private static AggregationInfo getMinAggregationInfo() {
    final String type = "min";
    final Map<String, String> params = new HashMap<String, String>();
    params.put("column", "met_impressionCount");
    final AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType(type);
    aggregationInfo.setAggregationParams(params);
    return aggregationInfo;
  }

  private static AggregationInfo getAvgAggregationInfo() {
    final String type = "avg";
    final Map<String, String> params = new HashMap<String, String>();
    params.put("column", "met_impressionCount");
    final AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType(type);
    aggregationInfo.setAggregationParams(params);
    return aggregationInfo;
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

  private static BrokerRequest getAggregationWithFilterBrokerRequest() {
    final BrokerRequest brokerRequest = new BrokerRequest();
    final List<AggregationInfo> aggregationsInfo = getAggregationsInfo();
    brokerRequest.setAggregationsInfo(aggregationsInfo);
    setFilterQuery(brokerRequest);
    return brokerRequest;
  }

  private static BrokerRequest setFilterQuery(BrokerRequest brokerRequest) {
    FilterQueryTree filterQueryTree;
    String filterColumn = "dim_memberGender";
    String filterVal = "u";
    if (filterColumn.contains(",")) {
      String[] filterColumns = filterColumn.split(",");
      String[] filterValues = filterVal.split(",");
      List<FilterQueryTree> nested = new ArrayList<FilterQueryTree>();
      for (int i = 0; i < filterColumns.length; i++) {

        List<String> vals = new ArrayList<String>();
        vals.add(filterValues[i]);
        FilterQueryTree d = new FilterQueryTree(i + 1, filterColumns[i], vals, FilterOperator.EQUALITY, null);
        nested.add(d);
      }
      filterQueryTree = new FilterQueryTree(0, null, null, FilterOperator.AND, nested);
    } else {
      List<String> vals = new ArrayList<String>();
      vals.add(filterVal);
      filterQueryTree = new FilterQueryTree(0, filterColumn, vals, FilterOperator.EQUALITY, null);
    }
    RequestUtils.generateFilterFromTree(filterQueryTree, brokerRequest);
    return brokerRequest;
  }
}
