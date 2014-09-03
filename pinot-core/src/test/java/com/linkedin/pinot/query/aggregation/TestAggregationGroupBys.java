package com.linkedin.pinot.query.aggregation;

import static org.testng.AssertJUnit.assertEquals;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.query.aggregation.CombineLevel;
import com.linkedin.pinot.core.query.aggregation.groupby.GroupByAggregationService;
import com.linkedin.pinot.core.query.utils.DefaultIntArray;
import com.linkedin.pinot.core.query.utils.IndexSegmentUtils;
import com.linkedin.pinot.core.query.utils.IntArray;


public class TestAggregationGroupBys {

  public static int[] _docIdsArray;
  public static IntArray _docIds;
  public static IndexSegment _indexSegment;
  public static int _sizeOfDocIdArray = 5000;
  public static int _sizeOfSegment = 5000;
  public static int _sizeOfCombineList = 5000;
  public static int _sizeOfReduceList = 5000;
  public static String _columnName = "met";
  public static AggregationInfo _paramsInfo;
  public static GroupBy _groupBy;
  public static GroupByAggregationService _groupByAggregationService;

  @BeforeClass
  public static void setup() {
    _docIdsArray = new int[_sizeOfDocIdArray];
    for (int i = 0; i < _sizeOfDocIdArray; ++i) {
      _docIdsArray[i] = i;
    }
    _docIds = new DefaultIntArray(_docIdsArray);
    _indexSegment = IndexSegmentUtils.getIndexSegmentWithAscendingOrderValues(_sizeOfSegment);
    // Setup
    List<AggregationInfo> aggregationInfos = new ArrayList<AggregationInfo>();
    Map<String, String> params = new HashMap<String, String>();
    params.put("column", _columnName);
    AggregationInfo paramsInfo1 = new AggregationInfo();
    paramsInfo1.setAggregationType("count");
    paramsInfo1.setAggregationParams(params);
    aggregationInfos.add(paramsInfo1);
    AggregationInfo paramsInfo2 = new AggregationInfo();
    paramsInfo2.setAggregationType("sum");
    paramsInfo2.setAggregationParams(params);
    aggregationInfos.add(paramsInfo2);
    AggregationInfo paramsInfo3 = new AggregationInfo();
    paramsInfo3.setAggregationType("min");
    paramsInfo3.setAggregationParams(params);
    aggregationInfos.add(paramsInfo3);
    AggregationInfo paramsInfo4 = new AggregationInfo();
    paramsInfo4.setAggregationType("max");
    paramsInfo4.setAggregationParams(params);
    aggregationInfos.add(paramsInfo4);
    List<String> groupbyColumns = new ArrayList<String>();
    groupbyColumns.add("dim0");
    _groupBy = new GroupBy();
    _groupBy.setColumns(groupbyColumns);
    _groupBy.setTopN(10);
    _groupByAggregationService = new GroupByAggregationService();
    _groupByAggregationService.init(aggregationInfos, _groupBy);
  }

  @Test
  public void testAggregation() {

    // Test aggregate
    for (int i = 1; i < _sizeOfDocIdArray; ++i) {
      HashMap<String, List<Serializable>> aggregationGroupByResultMap =
          _groupByAggregationService.aggregate(_docIds, i, _indexSegment);
      // System.out.println("grouped key size : " + aggregationGroupByResultMap.keySet().size());
      for (String keyString : aggregationGroupByResultMap.keySet()) {
        List<Serializable> resultList = aggregationGroupByResultMap.get(keyString);

        // Validate Count GroupBy
        long expectedCountValue = i / 10;
        if (Double.parseDouble(keyString) < i % 10) {
          expectedCountValue++;
        }
        // System.out.println("grouped key : " + keyString + ", value : " + ((Long) resultList.get(0)).longValue());
        assertEquals(expectedCountValue, ((Long) resultList.get(0)).longValue());

        // Validate Sum GroupBy
        double expectedSumValue =
            5 * expectedCountValue * expectedCountValue - 5 * expectedCountValue + expectedCountValue
                * Double.parseDouble(keyString);
        // System.out.println("grouped key : " + keyString + ", value : " + ((Double) resultList.get(1)).doubleValue());
        assertEquals(expectedSumValue, ((Double) resultList.get(1)).doubleValue());

        // Validate Min GroupBy
        double expectedMinValue = Double.parseDouble(keyString);
        // System.out.println("grouped key : " + keyString + ", value : " + ((Double) resultList.get(2)).doubleValue());
        assertEquals(expectedMinValue, ((Double) resultList.get(2)).doubleValue());

        // Validate Max GroupBy
        double expectedMaxValue = ((int) (i / 10) * 10) + Double.parseDouble(keyString);
        if (expectedMaxValue >= i) {
          expectedMaxValue -= 10;
        }

        // System.out.println("grouped key : " + keyString + ", value : " + ((Double) resultList.get(3)).doubleValue());
        assertEquals(expectedMaxValue, ((Double) resultList.get(3)).doubleValue());
      }
    }

  }

  @Test
  public void testCombine() {

    // Test combine
    List<HashMap<String, List<Serializable>>> groupByResultlistToCombine =
        new ArrayList<HashMap<String, List<Serializable>>>();

    groupByResultlistToCombine.add(_groupByAggregationService.aggregate(_docIds, _sizeOfDocIdArray, _indexSegment));
    groupByResultlistToCombine.add(_groupByAggregationService.aggregate(_docIds, _sizeOfDocIdArray, _indexSegment));
    groupByResultlistToCombine.add(_groupByAggregationService.aggregate(_docIds, _sizeOfDocIdArray, _indexSegment));
    groupByResultlistToCombine.add(_groupByAggregationService.aggregate(_docIds, _sizeOfDocIdArray, _indexSegment));
    groupByResultlistToCombine.add(_groupByAggregationService.aggregate(_docIds, _sizeOfDocIdArray, _indexSegment));
    List<HashMap<String, List<Serializable>>> combinedGroupByResultlist =
        _groupByAggregationService.combine(groupByResultlistToCombine, CombineLevel.PARTITION);
    HashMap<String, List<Serializable>> combinedGroupByResult = combinedGroupByResultlist.get(0);

    for (String keyString : combinedGroupByResult.keySet()) {
      List<Serializable> resultList = combinedGroupByResult.get(keyString);
      //System.out.println("grouped key : " + keyString + ", value : " + ((Long) resultList.get(0)).longValue());
      //System.out.println("grouped key : " + keyString + ", value : " + ((Double) resultList.get(1)).doubleValue());
      //System.out.println("grouped key : " + keyString + ", value : " + ((Double) resultList.get(2)).doubleValue());
      //System.out.println("grouped key : " + keyString + ", value : " + ((Double) resultList.get(3)).doubleValue());
      assertEquals(2500, ((Long) resultList.get(0)).longValue());
      double expectedSumValue = 5 * 500 * 499 + 500 * Double.parseDouble(keyString);
      assertEquals(expectedSumValue * 5, ((Double) resultList.get(1)).doubleValue());
      assertEquals(Double.parseDouble(keyString), ((Double) resultList.get(2)).doubleValue());
      assertEquals(4990 + Double.parseDouble(keyString), ((Double) resultList.get(3)).doubleValue());
    }
  }

  @Test
  public void testReduce() throws Exception {
    // Test reduce 
    List<DataTable> combinedResults = new ArrayList<DataTable>();
    combinedResults.add(GroupByAggregationService.transformGroupByResultToDataTable(
        _groupByAggregationService.aggregate(_docIds, _sizeOfDocIdArray, _indexSegment),
        _groupByAggregationService.getAggregationFunctionList()));
    combinedResults.add(GroupByAggregationService.transformGroupByResultToDataTable(
        _groupByAggregationService.aggregate(_docIds, _sizeOfDocIdArray, _indexSegment),
        _groupByAggregationService.getAggregationFunctionList()));
    combinedResults.add(GroupByAggregationService.transformGroupByResultToDataTable(
        _groupByAggregationService.aggregate(_docIds, _sizeOfDocIdArray, _indexSegment),
        _groupByAggregationService.getAggregationFunctionList()));
    combinedResults.add(GroupByAggregationService.transformGroupByResultToDataTable(
        _groupByAggregationService.aggregate(_docIds, _sizeOfDocIdArray, _indexSegment),
        _groupByAggregationService.getAggregationFunctionList()));
    combinedResults.add(GroupByAggregationService.transformGroupByResultToDataTable(
        _groupByAggregationService.aggregate(_docIds, _sizeOfDocIdArray, _indexSegment),
        _groupByAggregationService.getAggregationFunctionList()));

    Map<String, List<Serializable>> reducedGroupByResult = _groupByAggregationService.reduce(combinedResults);

    for (String keyString : reducedGroupByResult.keySet()) {
      List<Serializable> resultList = reducedGroupByResult.get(keyString);
      System.out.println("grouped key : " + keyString + ", value : " + ((Long) resultList.get(0)).longValue());
      System.out.println("grouped key : " + keyString + ", value : " + ((Double) resultList.get(1)).doubleValue());
      System.out.println("grouped key : " + keyString + ", value : " + ((Double) resultList.get(2)).doubleValue());
      System.out.println("grouped key : " + keyString + ", value : " + ((Double) resultList.get(3)).doubleValue());
      assertEquals(2500, ((Long) resultList.get(0)).longValue());
      double expectedSumValue = 5 * 500 * 499 + 500 * Double.parseDouble(keyString);
      assertEquals(expectedSumValue * 5, ((Double) resultList.get(1)).doubleValue());
      assertEquals(Double.parseDouble(keyString), ((Double) resultList.get(2)).doubleValue());
      assertEquals(4990 + Double.parseDouble(keyString), ((Double) resultList.get(3)).doubleValue());
    }
  }
}
