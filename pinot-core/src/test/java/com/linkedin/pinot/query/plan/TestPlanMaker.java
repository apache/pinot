package com.linkedin.pinot.query.plan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterQuery;
import com.linkedin.pinot.common.request.Selection;
import com.linkedin.pinot.common.request.SelectionSort;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.plan.PlanNode;
import com.linkedin.pinot.core.query.plan.maker.PlanMakerV0Impl;
import com.linkedin.pinot.core.query.plan.maker.PlanMaker;
import com.linkedin.pinot.core.query.utils.IndexSegmentUtils;


public class TestPlanMaker {

  private static BrokerRequest _query;
  private static IndexSegment _indexSegment;

  @BeforeClass
  public static void setup() {
    _query = getQuery();
    _indexSegment = IndexSegmentUtils.getIndexSegmentWithAscendingOrderValues(20000001);
  }

  @Test
  public void testHugePlanMaker() {
    PlanMaker hugePlanMaker = new PlanMakerV0Impl();
    PlanNode rootPlanNode = hugePlanMaker.makePlan(_indexSegment, _query);
    rootPlanNode.showTree("");
  }

  private static BrokerRequest getQuery() {
    BrokerRequest query = new BrokerRequest();
    List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(getCountAggregationInfo());
    aggregationsInfo.add(getSumAggregationInfo());
    aggregationsInfo.add(getMaxAggregationInfo());
    aggregationsInfo.add(getMinAggregationInfo());
    query.setAggregationsInfo(aggregationsInfo);
    query.setSelections(getSelectionQuery());
    FilterQuery filterQuery = getFilterQuery();
    query.setFilterQuery(filterQuery);
    return query;
  }

  private static FilterQuery getFilterQuery() {
    FilterQuery filterQuery = new FilterQuery();
    return filterQuery;
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
