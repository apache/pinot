package com.linkedin.pinot.query.plan;

import static org.testng.AssertJUnit.assertEquals;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.JSONArray;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.common.request.Selection;
import com.linkedin.pinot.common.request.SelectionSort;
import com.linkedin.pinot.common.response.BrokerResponse;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.common.utils.NamedThreadFactory;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import com.linkedin.pinot.core.block.aggregation.IntermediateResultsBlock;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.UAggregationGroupByOperator;
import com.linkedin.pinot.core.operator.UAggregationOperator;
import com.linkedin.pinot.core.operator.USelectionOperator;
import com.linkedin.pinot.core.plan.Plan;
import com.linkedin.pinot.core.plan.PlanNode;
import com.linkedin.pinot.core.plan.maker.InstancePlanMakerImpl;
import com.linkedin.pinot.core.plan.maker.PlanMaker;
import com.linkedin.pinot.core.query.aggregation.groupby.GroupByAggregationService;
import com.linkedin.pinot.core.query.reduce.DefaultReduceService;
import com.linkedin.pinot.core.query.utils.IndexSegmentUtils;


public class TestPlanMaker {

  private static BrokerRequest _brokerRequest;
  private static IndexSegment _indexSegment;
  private static List<IndexSegment> _indexSegmentList;

  @BeforeClass
  public static void setup() {
    _brokerRequest = getAggregationNoFilterBrokerRequest();
    _indexSegment = IndexSegmentUtils.getIndexSegmentWithAscendingOrderValues(20000001);
    _indexSegmentList = new ArrayList<IndexSegment>();
    for (int i = 0; i < 20; ++i) {
      _indexSegmentList.add(IndexSegmentUtils.getIndexSegmentWithAscendingOrderValues(2000001));
    }
  }

  @Test
  public void testInnerSegmentPlanMakerForAggregationNoFilter() {
    BrokerRequest brokerRequest = getAggregationNoFilterBrokerRequest();
    PlanMaker instancePlanMaker = new InstancePlanMakerImpl();
    PlanNode rootPlanNode = instancePlanMaker.makeInnerSegmentPlan(_indexSegment, brokerRequest);
    rootPlanNode.showTree("");
    UAggregationOperator operator = (UAggregationOperator) rootPlanNode.run();
    IntermediateResultsBlock resultBlock = (IntermediateResultsBlock) operator.nextBlock();
    System.out.println(resultBlock.getAggregationResult().get(0));
    System.out.println(resultBlock.getAggregationResult().get(1));
    System.out.println(resultBlock.getAggregationResult().get(2));
    System.out.println(resultBlock.getAggregationResult().get(3));
    Assert.assertEquals(20000001L, resultBlock.getAggregationResult().get(0));
    Assert.assertEquals(200000010000000.0, resultBlock.getAggregationResult().get(1));
    Assert.assertEquals(20000000.0, resultBlock.getAggregationResult().get(2));
    Assert.assertEquals(0.0, resultBlock.getAggregationResult().get(3));
  }

  @Test
  public void testInnerSegmentPlanMakerForAggregationWithFilter() {
    BrokerRequest brokerRequest = getAggregationWithFilterBrokerRequest();
    PlanMaker instancePlanMaker = new InstancePlanMakerImpl();
    PlanNode rootPlanNode = instancePlanMaker.makeInnerSegmentPlan(_indexSegment, brokerRequest);
    rootPlanNode.showTree("");
  }

  @Test
  public void testInnerSegmentPlanMakerForSelectionNoFilter() {
    BrokerRequest brokerRequest = getSelectionNoFilterBrokerRequest();
    PlanMaker instancePlanMaker = new InstancePlanMakerImpl();
    PlanNode rootPlanNode = instancePlanMaker.makeInnerSegmentPlan(_indexSegment, brokerRequest);
    rootPlanNode.showTree("");
    USelectionOperator operator = (USelectionOperator) rootPlanNode.run();
    IntermediateResultsBlock resultBlock = (IntermediateResultsBlock) operator.nextBlock();
    PriorityQueue<Serializable[]> retPriorityQueue = resultBlock.getSelectionResult();
    while (!retPriorityQueue.isEmpty()) {
      System.out.println(Arrays.toString(retPriorityQueue.poll()));
    }
  }

  @Test
  public void testInnerSegmentPlanMakerForSelectionNoFilterNoOrdering() {
    BrokerRequest brokerRequest = getSelectionNoFilterBrokerRequest();
    brokerRequest.getSelections().setSelectionSortSequence(null);
    PlanMaker instancePlanMaker = new InstancePlanMakerImpl();
    PlanNode rootPlanNode = instancePlanMaker.makeInnerSegmentPlan(_indexSegment, brokerRequest);
    rootPlanNode.showTree("");
    USelectionOperator operator = (USelectionOperator) rootPlanNode.run();
    IntermediateResultsBlock resultBlock = (IntermediateResultsBlock) operator.nextBlock();
    PriorityQueue<Serializable[]> retPriorityQueue = resultBlock.getSelectionResult();
    while (!retPriorityQueue.isEmpty()) {
      System.out.println(Arrays.toString(retPriorityQueue.poll()));
    }
  }

  @Test
  public void testInnerSegmentPlanMakerForSelectionWithFilter() {
    BrokerRequest brokerRequest = getSelectionWithFilterBrokerRequest();
    PlanMaker instancePlanMaker = new InstancePlanMakerImpl();
    PlanNode rootPlanNode = instancePlanMaker.makeInnerSegmentPlan(_indexSegment, brokerRequest);
    rootPlanNode.showTree("");
  }

  @Test
  public void testInnerSegmentPlanMakerForAggregationGroupByNoFilter() {
    BrokerRequest brokerRequest = getAggregationGroupByNoFilterBrokerRequest();
    PlanMaker instancePlanMaker = new InstancePlanMakerImpl();
    PlanNode rootPlanNode = instancePlanMaker.makeInnerSegmentPlan(_indexSegment, brokerRequest);
    rootPlanNode.showTree("");
    UAggregationGroupByOperator operator = (UAggregationGroupByOperator) rootPlanNode.run();
    IntermediateResultsBlock resultBlock = (IntermediateResultsBlock) operator.nextBlock();
    System.out.println("RunningTime : " + resultBlock.getTimeUsedMs());
    System.out.println("NumDocsScanned : " + resultBlock.getNumDocsScanned());
    System.out.println("TotalDocs : " + resultBlock.getTotalDocs());
    //    System.out.println(resultBlock.getAggregationGroupByResult().get("0.0"));
    HashMap<String, List<Serializable>> combinedGroupByResult = resultBlock.getAggregationGroupByResult();
    //    Assert.assertEquals(20000001L, resultBlock.getAggregationResult().get(0));
    //    Assert.assertEquals(200000010000000.0, resultBlock.getAggregationResult().get(1));
    //    Assert.assertEquals(20000000.0, resultBlock.getAggregationResult().get(2));
    //    Assert.assertEquals(0.0, resultBlock.getAggregationResult().get(3));
    for (String keyString : combinedGroupByResult.keySet()) {
      if (keyString.equals("0.0")) {
        List<Serializable> resultList = combinedGroupByResult.get(keyString);
        //        System.out.println("grouped key : " + keyString + ", value : " + ((Long) resultList.get(0)).longValue());
        //        System.out.println("grouped key : " + keyString + ", value : " + ((Double) resultList.get(1)).doubleValue());
        //        System.out.println("grouped key : " + keyString + ", value : " + ((Double) resultList.get(2)).doubleValue());
        //        System.out.println("grouped key : " + keyString + ", value : " + ((Double) resultList.get(3)).doubleValue());
        assertEquals(2000001, ((Long) resultList.get(0)).longValue());
        double expectedSumValue =
            (Double.parseDouble(keyString) + 20000000 + Double.parseDouble(keyString)) * 2000001 / 2;
        assertEquals(expectedSumValue, ((Double) resultList.get(1)).doubleValue());
        assertEquals(20000000 + Double.parseDouble(keyString), ((Double) resultList.get(2)).doubleValue());
        assertEquals(Double.parseDouble(keyString), ((Double) resultList.get(3)).doubleValue());
      } else {
        List<Serializable> resultList = combinedGroupByResult.get(keyString);
        //        System.out.println("grouped key : " + keyString + ", value : " + ((Long) resultList.get(0)).longValue());
        //        System.out.println("grouped key : " + keyString + ", value : " + ((Double) resultList.get(1)).doubleValue());
        //        System.out.println("grouped key : " + keyString + ", value : " + ((Double) resultList.get(2)).doubleValue());
        //        System.out.println("grouped key : " + keyString + ", value : " + ((Double) resultList.get(3)).doubleValue());
        assertEquals(2000000, ((Long) resultList.get(0)).longValue());
        double expectedSumValue =
            (Double.parseDouble(keyString) + 20000000 - 10 + Double.parseDouble(keyString)) * 1000000;
        assertEquals(expectedSumValue, ((Double) resultList.get(1)).doubleValue());
        assertEquals(20000000 - 10 + Double.parseDouble(keyString), ((Double) resultList.get(2)).doubleValue());
        assertEquals(Double.parseDouble(keyString), ((Double) resultList.get(3)).doubleValue());
      }
    }
  }

  @Test
  public void testInnerSegmentPlanMakerForAggregationGroupByWithFilter() {
    BrokerRequest brokerRequest = getAggregationGroupByWithFilterBrokerRequest();
    PlanMaker instancePlanMaker = new InstancePlanMakerImpl();
    PlanNode rootPlanNode = instancePlanMaker.makeInnerSegmentPlan(_indexSegment, brokerRequest);
    rootPlanNode.showTree("");
  }

  @Test
  public void testInterSegmentAggregationPlanMaker() {
    PlanMaker instancePlanMaker = new InstancePlanMakerImpl();
    BrokerRequest brokerRequest = _brokerRequest.deepCopy();
    brokerRequest.setSelections(null);
    brokerRequest.setSelectionsIsSet(false);
    ExecutorService executorService = Executors.newCachedThreadPool(new NamedThreadFactory("test-plan-maker"));
    Plan globalPlan = instancePlanMaker.makeInterSegmentPlan(_indexSegmentList, brokerRequest, executorService);
    globalPlan.print();
    System.out.println("/////////////////////////////////////////////////////////////////////////////");
    brokerRequest = setFilterQuery(brokerRequest);
    globalPlan = instancePlanMaker.makeInterSegmentPlan(_indexSegmentList, brokerRequest, executorService);
    globalPlan.print();
  }

  @Test
  public void testInterSegmentAggregationPlanMakerAndRun() {
    PlanMaker instancePlanMaker = new InstancePlanMakerImpl();
    BrokerRequest brokerRequest = _brokerRequest.deepCopy();
    ExecutorService executorService = Executors.newCachedThreadPool(new NamedThreadFactory("test-plan-maker"));
    Plan globalPlan = instancePlanMaker.makeInterSegmentPlan(_indexSegmentList, brokerRequest, executorService);
    globalPlan.print();
    globalPlan.execute();
    DataTable instanceResponse = globalPlan.getInstanceResponse();
    System.out.println(instanceResponse.getLong(0, 0));
    System.out.println(instanceResponse.getDouble(0, 1));
    System.out.println(instanceResponse.getDouble(0, 2));
    System.out.println(instanceResponse.getDouble(0, 3));
    System.out.println(instanceResponse);
    Assert.assertEquals(2000001L * _indexSegmentList.size(), instanceResponse.getLong(0, 0));
    Assert.assertEquals(2000001000000.0 * _indexSegmentList.size(), instanceResponse.getDouble(0, 1));
    Assert.assertEquals(2000000.0, instanceResponse.getDouble(0, 2));
    Assert.assertEquals(0.0, instanceResponse.getDouble(0, 3));
  }

  @Test
  public void testInterSegmentAggregationGroupByPlanMakerAndRun() {
    PlanMaker instancePlanMaker = new InstancePlanMakerImpl();
    BrokerRequest brokerRequest = getAggregationGroupByNoFilterBrokerRequest();
    ExecutorService executorService = Executors.newCachedThreadPool(new NamedThreadFactory("test-plan-maker"));
    Plan globalPlan = instancePlanMaker.makeInterSegmentPlan(_indexSegmentList, brokerRequest, executorService);
    globalPlan.print();
    globalPlan.execute();
    DataTable instanceResponse = globalPlan.getInstanceResponse();

    System.out.println(instanceResponse.getString(0, 0));
    System.out.println(instanceResponse.getLong(0, 1));
    System.out.println(instanceResponse.getDouble(0, 2));
    System.out.println(instanceResponse.getDouble(0, 3));
    System.out.println(instanceResponse.getDouble(0, 4));
    System.out.println(instanceResponse);
    List<DataTable> instanceResponseList = new ArrayList<DataTable>();
    instanceResponseList.add(instanceResponse);
    GroupByAggregationService groupByAggregationService = new GroupByAggregationService();
    groupByAggregationService.init(brokerRequest.getAggregationsInfo(), getGroupBy());
    Map<String, List<Serializable>> combinedGroupByResult = groupByAggregationService.reduce(instanceResponseList);
    for (String keyString : combinedGroupByResult.keySet()) {
      if (keyString.equals("0.0")) {
        List<Serializable> resultList = combinedGroupByResult.get(keyString);
        System.out.println("grouped key : " + keyString + ", value : " + ((Long) resultList.get(0)).longValue());
        System.out.println("grouped key : " + keyString + ", value : " + ((Double) resultList.get(1)).doubleValue());
        System.out.println("grouped key : " + keyString + ", value : " + ((Double) resultList.get(2)).doubleValue());
        System.out.println("grouped key : " + keyString + ", value : " + ((Double) resultList.get(3)).doubleValue());
        assertEquals(200001 * 20, ((Long) resultList.get(0)).longValue());
        double expectedSumValue =
            (Double.parseDouble(keyString) + 2000000 + Double.parseDouble(keyString)) * 200001 / 2 * 20;
        assertEquals(expectedSumValue, ((Double) resultList.get(1)).doubleValue());
        assertEquals(2000000 + Double.parseDouble(keyString), ((Double) resultList.get(2)).doubleValue());
        assertEquals(Double.parseDouble(keyString), ((Double) resultList.get(3)).doubleValue());
      } else {
        List<Serializable> resultList = combinedGroupByResult.get(keyString);
        System.out.println("grouped key : " + keyString + ", value : " + ((Long) resultList.get(0)).longValue());
        System.out.println("grouped key : " + keyString + ", value : " + ((Double) resultList.get(1)).doubleValue());
        System.out.println("grouped key : " + keyString + ", value : " + ((Double) resultList.get(2)).doubleValue());
        System.out.println("grouped key : " + keyString + ", value : " + ((Double) resultList.get(3)).doubleValue());
        assertEquals(4000000, ((Long) resultList.get(0)).longValue());
        double expectedSumValue =
            (Double.parseDouble(keyString) + 2000000 - 10 + Double.parseDouble(keyString)) * 100000 * 20;
        assertEquals(expectedSumValue, ((Double) resultList.get(1)).doubleValue());
        assertEquals(2000000 - 10 + Double.parseDouble(keyString), ((Double) resultList.get(2)).doubleValue());
        assertEquals(Double.parseDouble(keyString), ((Double) resultList.get(3)).doubleValue());
      }
    }

    DefaultReduceService defaultReduceService = new DefaultReduceService();
    Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse);
    BrokerResponse brokerResponse = defaultReduceService.reduceOnDataTable(brokerRequest, instanceResponseMap);
    System.out.println(new JSONArray(brokerResponse.getAggregationResults()));

  }

  @Test
  public void testInterSegmentSelectionPlanMaker() {
    PlanMaker instancePlanMaker = new InstancePlanMakerImpl();
    BrokerRequest brokerRequest = _brokerRequest.deepCopy();
    brokerRequest.setAggregationsInfo(null);
    brokerRequest.setAggregationsInfoIsSet(false);
    brokerRequest.setSelections(getSelectionQuery());
    brokerRequest.getSelections().setOffset(0);
    brokerRequest.getSelections().setSize(20);
    ExecutorService executorService = Executors.newCachedThreadPool(new NamedThreadFactory("test-plan-maker"));
    Plan globalPlan = instancePlanMaker.makeInterSegmentPlan(_indexSegmentList, brokerRequest, executorService);
    globalPlan.print();
    System.out.println("/////////////////////////////////////////////////////////////////////////////");
    brokerRequest = setFilterQuery(brokerRequest);
    globalPlan = instancePlanMaker.makeInterSegmentPlan(_indexSegmentList, brokerRequest, executorService);
    globalPlan.print();
    globalPlan.execute();
    DataTable instanceResponse = globalPlan.getInstanceResponse();

    DefaultReduceService defaultReduceService = new DefaultReduceService();
    Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse);
    instanceResponseMap.put(new ServerInstance("localhost:1111"), instanceResponse);

    BrokerResponse brokerResponse = defaultReduceService.reduceOnDataTable(brokerRequest, instanceResponseMap);
    System.out.println(new JSONArray(brokerResponse.getSelectionResults()));
    System.out.println("TimeUsedMs : " + brokerResponse.getTimeUsedMs());
    System.out.println(brokerResponse);
  }

  @Test
  public void testInterSegmentSelectionNoOrderingPlanMaker() {
    PlanMaker instancePlanMaker = new InstancePlanMakerImpl();
    BrokerRequest brokerRequest = _brokerRequest.deepCopy();
    brokerRequest.setAggregationsInfo(null);
    brokerRequest.setAggregationsInfoIsSet(false);
    brokerRequest.setSelections(getSelectionQuery());
    brokerRequest.getSelections().setSelectionSortSequence(null);
    brokerRequest.getSelections().setOffset(0);
    brokerRequest.getSelections().setSize(20);
    ExecutorService executorService = Executors.newCachedThreadPool(new NamedThreadFactory("test-plan-maker"));
    Plan globalPlan = instancePlanMaker.makeInterSegmentPlan(_indexSegmentList, brokerRequest, executorService);
    globalPlan.print();
    System.out.println("/////////////////////////////////////////////////////////////////////////////");
    brokerRequest = setFilterQuery(brokerRequest);
    globalPlan = instancePlanMaker.makeInterSegmentPlan(_indexSegmentList, brokerRequest, executorService);
    globalPlan.print();
    globalPlan.execute();
    DataTable instanceResponse = globalPlan.getInstanceResponse();

    DefaultReduceService defaultReduceService = new DefaultReduceService();
    Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse);
    instanceResponseMap.put(new ServerInstance("localhost:1111"), instanceResponse);

    BrokerResponse brokerResponse = defaultReduceService.reduceOnDataTable(brokerRequest, instanceResponseMap);
    System.out.println(new JSONArray(brokerResponse.getSelectionResults()));
    System.out.println("TimeUsedMs : " + brokerResponse.getTimeUsedMs());
    System.out.println(brokerResponse);
  }

  private static BrokerRequest getAggregationNoFilterBrokerRequest() {
    BrokerRequest brokerRequest = new BrokerRequest();
    List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(getCountAggregationInfo());
    aggregationsInfo.add(getSumAggregationInfo());
    aggregationsInfo.add(getMaxAggregationInfo());
    aggregationsInfo.add(getMinAggregationInfo());
    brokerRequest.setAggregationsInfo(aggregationsInfo);
    return brokerRequest;
  }

  private static BrokerRequest getAggregationWithFilterBrokerRequest() {
    BrokerRequest brokerRequest = new BrokerRequest();
    List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(getCountAggregationInfo());
    aggregationsInfo.add(getSumAggregationInfo());
    aggregationsInfo.add(getMaxAggregationInfo());
    aggregationsInfo.add(getMinAggregationInfo());
    brokerRequest.setAggregationsInfo(aggregationsInfo);
    brokerRequest = setFilterQuery(brokerRequest);
    return brokerRequest;
  }

  private static BrokerRequest getAggregationGroupByNoFilterBrokerRequest() {
    BrokerRequest brokerRequest = new BrokerRequest();
    List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(getCountAggregationInfo());
    aggregationsInfo.add(getSumAggregationInfo());
    aggregationsInfo.add(getMaxAggregationInfo());
    aggregationsInfo.add(getMinAggregationInfo());
    brokerRequest.setAggregationsInfo(aggregationsInfo);
    brokerRequest.setGroupBy(getGroupBy());
    return brokerRequest;
  }

  private static BrokerRequest getAggregationGroupByWithFilterBrokerRequest() {
    BrokerRequest brokerRequest = new BrokerRequest();
    List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(getCountAggregationInfo());
    aggregationsInfo.add(getSumAggregationInfo());
    aggregationsInfo.add(getMaxAggregationInfo());
    aggregationsInfo.add(getMinAggregationInfo());
    brokerRequest.setAggregationsInfo(aggregationsInfo);
    brokerRequest.setGroupBy(getGroupBy());
    brokerRequest = setFilterQuery(brokerRequest);
    return brokerRequest;
  }

  private static BrokerRequest getSelectionNoFilterBrokerRequest() {
    BrokerRequest brokerRequest = new BrokerRequest();
    brokerRequest.setSelections(getSelectionQuery());
    return brokerRequest;
  }

  private static BrokerRequest getSelectionWithFilterBrokerRequest() {
    BrokerRequest brokerRequest = new BrokerRequest();
    brokerRequest.setSelections(getSelectionQuery());
    brokerRequest = setFilterQuery(brokerRequest);
    return brokerRequest;
  }

  private static BrokerRequest setFilterQuery(BrokerRequest brokerRequest) {
    FilterQueryTree filterQueryTree;
    String filterColumn = "dim0";
    String filterVal = "1.0";
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

  private static Selection getSelectionQuery() {
    Selection selection = new Selection();
    selection.setOffset(10);
    selection.setSize(10);
    List<String> selectionColumns = new ArrayList<String>();
    selectionColumns.add("dim0");
    selectionColumns.add("dim1");
    selectionColumns.add("met");
    selection.setSelectionColumns(selectionColumns);

    List<SelectionSort> selectionSortSequence = new ArrayList<SelectionSort>();
    SelectionSort selectionSort = new SelectionSort();
    selectionSort.setColumn("dim0");
    selectionSort.setIsAsc(false);
    selectionSortSequence.add(selectionSort);
    selectionSort = new SelectionSort();
    selectionSort.setColumn("dim1");
    selectionSort.setIsAsc(false);
    selectionSortSequence.add(selectionSort);

    selection.setSelectionSortSequence(selectionSortSequence);

    return selection;
  }

  private static AggregationInfo getCountAggregationInfo() {
    String type = "count";
    Map<String, String> params = new HashMap<String, String>();
    params.put("column", "met");
    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType(type);
    aggregationInfo.setAggregationParams(params);
    return aggregationInfo;
  }

  private static AggregationInfo getSumAggregationInfo() {
    String type = "sum";
    Map<String, String> params = new HashMap<String, String>();
    params.put("column", "met");
    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType(type);
    aggregationInfo.setAggregationParams(params);
    return aggregationInfo;
  }

  private static AggregationInfo getMaxAggregationInfo() {
    String type = "max";
    Map<String, String> params = new HashMap<String, String>();
    params.put("column", "met");
    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType(type);
    aggregationInfo.setAggregationParams(params);
    return aggregationInfo;
  }

  private static AggregationInfo getMinAggregationInfo() {
    String type = "min";
    Map<String, String> params = new HashMap<String, String>();
    params.put("column", "met");
    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType(type);
    aggregationInfo.setAggregationParams(params);
    return aggregationInfo;
  }

  private static GroupBy getGroupBy() {
    GroupBy groupBy = new GroupBy();
    List<String> columns = new ArrayList<String>();
    columns.add("dim0");
    groupBy.setColumns(columns);
    groupBy.setTopN(15);
    return groupBy;
  }
}
