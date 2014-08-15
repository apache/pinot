package com.linkedin.pinot.query.plan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.request.Selection;
import com.linkedin.pinot.common.request.SelectionSort;
import com.linkedin.pinot.common.response.InstanceResponse;
import com.linkedin.pinot.common.utils.NamedThreadFactory;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import com.linkedin.pinot.core.block.aggregation.AggregationAndSelectionResultBlock;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.UAggregationAndSelectionOperator;
import com.linkedin.pinot.core.plan.Plan;
import com.linkedin.pinot.core.plan.PlanNode;
import com.linkedin.pinot.core.plan.maker.InstancePlanMakerImpl;
import com.linkedin.pinot.core.plan.maker.PlanMaker;
import com.linkedin.pinot.core.query.utils.IndexSegmentUtils;


public class TestPlanMaker {

  private static BrokerRequest _brokerRequest;
  private static IndexSegment _indexSegment;
  private static List<IndexSegment> _indexSegmentList;

  @BeforeClass
  public static void setup() {
    _brokerRequest = getBrokerRequest();
    _indexSegment = IndexSegmentUtils.getIndexSegmentWithAscendingOrderValues(20000001);
    _indexSegmentList = new ArrayList<IndexSegment>();
    for (int i = 0; i < 20; ++i) {
      _indexSegmentList.add(IndexSegmentUtils.getIndexSegmentWithAscendingOrderValues(2000001));
    }
  }

  @Test
  public void testInnerSegmentPlanMaker() {
    PlanMaker instancePlanMaker = new InstancePlanMakerImpl();
    PlanNode rootPlanNode = instancePlanMaker.makeInnerSegmentPlan(_indexSegment, _brokerRequest);
    rootPlanNode.showTree("");
    UAggregationAndSelectionOperator operator = (UAggregationAndSelectionOperator) rootPlanNode.run();
    AggregationAndSelectionResultBlock aggregationAndSelectionResultBlock =
        (AggregationAndSelectionResultBlock) operator.nextBlock();
    System.out.println(aggregationAndSelectionResultBlock.getAggregationResult().get(0));
    System.out.println(aggregationAndSelectionResultBlock.getAggregationResult().get(1));
    System.out.println(aggregationAndSelectionResultBlock.getAggregationResult().get(2));
    System.out.println(aggregationAndSelectionResultBlock.getAggregationResult().get(3));
    Assert.assertEquals(20000001L, aggregationAndSelectionResultBlock.getAggregationResult().get(0).getLongVal());
    Assert
        .assertEquals(200000010000000L, aggregationAndSelectionResultBlock.getAggregationResult().get(1).getLongVal());
    Assert.assertEquals(20000000.0, aggregationAndSelectionResultBlock.getAggregationResult().get(2).getDoubleVal());
    Assert.assertEquals(0.0, aggregationAndSelectionResultBlock.getAggregationResult().get(3).getDoubleVal());

  }

  @Test
  public void testInterSegmentAggregationAndSelectionPlanMaker() {
    PlanMaker instancePlanMaker = new InstancePlanMakerImpl();
    BrokerRequest brokerRequest = _brokerRequest.deepCopy();
    ExecutorService executorService = Executors.newCachedThreadPool(new NamedThreadFactory("test-plan-maker"));
    Plan globalPlan = instancePlanMaker.makeInterSegmentPlan(_indexSegmentList, brokerRequest, executorService);
    globalPlan.print();
    InstanceResponse instanceResponse = globalPlan.execute();
    System.out.println(instanceResponse.getAggregationResults().get(0));
    System.out.println(instanceResponse.getAggregationResults().get(1));
    System.out.println(instanceResponse.getAggregationResults().get(2));
    System.out.println(instanceResponse.getAggregationResults().get(3));
    System.out.println(instanceResponse);
    Assert.assertEquals(2000001L * _indexSegmentList.size(), instanceResponse.getAggregationResults().get(0)
        .getLongVal());
    Assert.assertEquals(2000001000000L * _indexSegmentList.size(), instanceResponse.getAggregationResults().get(1)
        .getLongVal());
    Assert.assertEquals(2000000.0, instanceResponse.getAggregationResults().get(2).getDoubleVal());
    Assert.assertEquals(0.0, instanceResponse.getAggregationResults().get(3).getDoubleVal());

    //    
    //    System.out.println("/////////////////////////////////////////////////////////////////////////////");
    //    brokerRequest = setFilterQuery(brokerRequest);
    //    rootPlanNode = instancePlanMaker.makeInterSegmentPlan(_indexSegmentList, brokerRequest, executorService);
    //    rootPlanNode.showTree("");
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
  public void testInterSegmentSelectionPlanMaker() {
    PlanMaker instancePlanMaker = new InstancePlanMakerImpl();
    BrokerRequest brokerRequest = _brokerRequest.deepCopy();
    brokerRequest.setAggregationsInfo(null);
    brokerRequest.setAggregationsInfoIsSet(false);
    ExecutorService executorService = Executors.newCachedThreadPool(new NamedThreadFactory("test-plan-maker"));
    Plan globalPlan = instancePlanMaker.makeInterSegmentPlan(_indexSegmentList, brokerRequest, executorService);
    globalPlan.print();
    System.out.println("/////////////////////////////////////////////////////////////////////////////");
    brokerRequest = setFilterQuery(brokerRequest);
    globalPlan = instancePlanMaker.makeInterSegmentPlan(_indexSegmentList, brokerRequest, executorService);
    globalPlan.print();
  }

  private static BrokerRequest getBrokerRequest() {
    BrokerRequest brokerRequest = new BrokerRequest();
    List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(getCountAggregationInfo());
    aggregationsInfo.add(getSumAggregationInfo());
    aggregationsInfo.add(getMaxAggregationInfo());
    aggregationsInfo.add(getMinAggregationInfo());
    brokerRequest.setAggregationsInfo(aggregationsInfo);
    brokerRequest.setSelections(getSelectionQuery());
    //    FilterQuery filterQuery = getFilterQuery();
    //    query.setFilterQuery(filterQuery);
    return brokerRequest;
  }

  private static BrokerRequest setFilterQuery(BrokerRequest brokerRequest) {
    FilterQueryTree filterQueryTree;
    String filterColumn = "dim0";
    String filterVal = "1";
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
    selectionColumns.add("met0");
    selectionColumns.add("met1");
    selection.setSelectionColumns(selectionColumns);

    List<SelectionSort> selectionSortSequence = new ArrayList<SelectionSort>();
    SelectionSort selectionSort = new SelectionSort();
    selectionSort.setColumn("met0");
    selectionSort.setIsAsc(true);
    selectionSortSequence.add(selectionSort);
    selectionSort = new SelectionSort();
    selectionSort.setColumn("met1");
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
}
