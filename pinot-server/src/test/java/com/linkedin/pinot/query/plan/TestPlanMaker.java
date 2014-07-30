package com.linkedin.pinot.query.plan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.plan.PlanNode;
import com.linkedin.pinot.core.query.FilterQuery;
import com.linkedin.pinot.query.plan.maker.HugePlanMaker;
import com.linkedin.pinot.query.plan.maker.PlanMaker;
import com.linkedin.pinot.query.request.AggregationInfo;
import com.linkedin.pinot.query.request.Query;
import com.linkedin.pinot.query.request.Selection;
import com.linkedin.pinot.query.utils.IndexSegmentUtils;
import com.linkedin.pinot.query.request.SelectionSort;


public class TestPlanMaker {

  private static Query _query;
  private static IndexSegment _indexSegment;

  @BeforeClass
  public static void setup() {
    _query = getQuery();
    _indexSegment = IndexSegmentUtils.getIndexSegmentWithAscendingOrderValues(20000001);
  }

  @Test
  public void testHugePlanMaker() {
    PlanMaker hugePlanMaker = new HugePlanMaker();
    PlanNode rootPlanNode = hugePlanMaker.makePlan(_indexSegment, _query);
    rootPlanNode.showTree("");
  }

  private static Query getQuery() {
    Query query = new Query();
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
    selection.setSelectionColumns(new String[] { "met0,met1" });
    selection.setSelectionSortSequence(new SelectionSort[] { new SelectionSort("met0", true), new SelectionSort("met1",
        false) });

    return selection;
  }

  private static AggregationInfo getCountAggregationInfo() {
    String type = "count";
    Map<String, String> params = new HashMap<String, String>();
    params.put("column", "met");
    return new AggregationInfo(type, params);
  }

  private static AggregationInfo getSumAggregationInfo() {
    String type = "sum";
    Map<String, String> params = new HashMap<String, String>();
    params.put("column", "met");
    return new AggregationInfo(type, params);
  }

  private static AggregationInfo getMaxAggregationInfo() {
    String type = "max";
    Map<String, String> params = new HashMap<String, String>();
    params.put("column", "met");
    return new AggregationInfo(type, params);
  }

  private static AggregationInfo getMinAggregationInfo() {
    String type = "min";
    Map<String, String> params = new HashMap<String, String>();
    params.put("column", "met");
    return new AggregationInfo(type, params);
  }
}
