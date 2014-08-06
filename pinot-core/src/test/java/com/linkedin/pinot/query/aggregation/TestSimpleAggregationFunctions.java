package com.linkedin.pinot.query.aggregation;

import static org.testng.AssertJUnit.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.response.AggregationResult;
import com.linkedin.pinot.common.response.AggregationResult._Fields;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.query.aggregation.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.CombineLevel;
import com.linkedin.pinot.core.query.aggregation.function.CountAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.MaxAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.MinAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.SumAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.SumDoubleAggregationFunction;
import com.linkedin.pinot.core.query.utils.DefaultIntArray;
import com.linkedin.pinot.core.query.utils.IndexSegmentUtils;
import com.linkedin.pinot.core.query.utils.IntArray;


public class TestSimpleAggregationFunctions {

  public static int[] _docIdsArray;
  public static IntArray _docIds;
  public static IndexSegment _indexSegment;
  public static int _sizeOfDocIdArray = 5000;
  public static int _sizeOfSegment = 5000;
  public static int _sizeOfCombineList = 5000;
  public static int _sizeOfReduceList = 5000;
  public static String _columnName = "met";
  public static AggregationInfo _paramsInfo;

  @BeforeClass
  public static void setup() {

    _docIdsArray = new int[_sizeOfDocIdArray];
    for (int i = 0; i < _sizeOfDocIdArray; ++i) {
      _docIdsArray[i] = i;
    }
    _docIds = new DefaultIntArray(_docIdsArray);
    _indexSegment = IndexSegmentUtils.getIndexSegmentWithAscendingOrderValues(_sizeOfSegment);
    Map<String, String> params = new HashMap<String, String>();
    params.put("column", _columnName);
    _paramsInfo = new AggregationInfo();
    _paramsInfo.setAggregationType("");
    _paramsInfo.setAggregationParams(params);

  }

  @Test
  public void testCountAggregation() {
    AggregationFunction aggregationFunction = new CountAggregationFunction();
    aggregationFunction.init(_paramsInfo);
    // Test aggregate
    for (int i = 1; i <= _sizeOfDocIdArray; ++i) {
      AggregationResult result = aggregationFunction.aggregate(_docIds, i, _indexSegment);
      assertEquals(i, result.getLongVal());
    }
    // Test combine
    for (int i = 1; i <= _sizeOfCombineList; ++i) {
      List<AggregationResult> aggregationResults = getLongValues(i);
      AggregationResult combinedResults = aggregationFunction.combine(aggregationResults, CombineLevel.SEGMENT);

      assertEquals(((i - 1) * i) / 2, combinedResults.getLongVal());
    }

    // Test reduce
    for (int i = 1; i <= _sizeOfCombineList; ++i) {
      List<AggregationResult> combinedResults = getLongValues(i);
      AggregationResult reduceResults = aggregationFunction.reduce(combinedResults);
      assertEquals(((i - 1) * i) / 2, reduceResults.getLongVal());
    }
  }

  @Test
  public void testSumAggregation() {
    AggregationFunction aggregationFunction = new SumAggregationFunction();
    aggregationFunction.init(_paramsInfo);
    // Test aggregate
    long expectedSum = 0;
    for (int i = 1; i <= _sizeOfDocIdArray; ++i) {
      AggregationResult result = aggregationFunction.aggregate(_docIds, i, _indexSegment);
      assertEquals(expectedSum, result.getLongVal());
      expectedSum += i;
    }
    // Test combine
    for (int i = 1; i <= _sizeOfCombineList; ++i) {
      List<AggregationResult> aggregationResults = getLongValues(i);
      AggregationResult combinedResult = aggregationFunction.combine(aggregationResults, CombineLevel.SEGMENT);
      assertEquals(((i - 1) * i) / 2, combinedResult.getLongVal());
    }

    // Test reduce
    for (int i = 1; i <= _sizeOfCombineList; ++i) {
      List<AggregationResult> combinedResults = getLongValues(i);
      AggregationResult reduceResults = aggregationFunction.reduce(combinedResults);
      assertEquals(((i - 1) * i) / 2, reduceResults.getLongVal());
    }
  }

  @Test
  public void testSumDoubleAggregation() {
    AggregationFunction aggregationFunction = new SumDoubleAggregationFunction();
    aggregationFunction.init(_paramsInfo);
    // Test aggregate
    double expectedSum = 0;
    for (int i = 1; i <= _sizeOfDocIdArray; ++i) {
      AggregationResult result = aggregationFunction.aggregate(_docIds, i, _indexSegment);
      assertEquals(expectedSum, result.getDoubleVal(), 0.1);
      expectedSum += i;
    }
    // Test combine
    for (int i = 1; i <= _sizeOfCombineList; ++i) {
      List<AggregationResult> aggregationResults = getDoubleValues(i);
      AggregationResult combinedResults = aggregationFunction.combine(aggregationResults, CombineLevel.SEGMENT);

      assertEquals(((i - 1) * i) / 2, combinedResults.getDoubleVal(), 0.1);
    }

    // Test reduce
    for (int i = 1; i <= _sizeOfCombineList; ++i) {
      List<AggregationResult> combinedResults = getDoubleValues(i);
      AggregationResult reduceResult = aggregationFunction.reduce(combinedResults);
      assertEquals(((i - 1) * i) / 2, reduceResult.getDoubleVal(), 0.1);
    }
  }

  @Test
  public void testMinAggregation() {
    AggregationFunction aggregationFunction = new MinAggregationFunction();
    aggregationFunction.init(_paramsInfo);
    // Test aggregate
    for (int i = 1; i <= _sizeOfDocIdArray; ++i) {
      AggregationResult result = aggregationFunction.aggregate(_docIds, i, _indexSegment);
      assertEquals(0.0, result.getDoubleVal(), 0.1);
    }
    // Test combine
    for (int i = 1; i <= _sizeOfCombineList; ++i) {
      List<AggregationResult> aggregationResults = getDoubleValues(i);
      AggregationResult combinedResults = aggregationFunction.combine(aggregationResults, CombineLevel.SEGMENT);
      assertEquals(0.0, combinedResults.getDoubleVal(), 0.1);
    }

    // Test reduce
    for (int i = 1; i <= _sizeOfCombineList; ++i) {
      List<AggregationResult> combinedResults = getDoubleValues(i);
      AggregationResult reduceResult = aggregationFunction.reduce(combinedResults);
      assertEquals(0.0, reduceResult.getDoubleVal(), 0.1);
    }
  }

  @Test
  public void testMaxAggregation() {
    AggregationFunction aggregationFunction = new MaxAggregationFunction();
    aggregationFunction.init(_paramsInfo);

    // Test aggregate
    for (int i = 1; i <= _sizeOfDocIdArray; ++i) {
      AggregationResult result = aggregationFunction.aggregate(_docIds, i, _indexSegment);
      assertEquals(i - 1, result.getDoubleVal(), 0.1);
    }
    // Test combine
    for (int i = 1; i <= _sizeOfCombineList; ++i) {
      List<AggregationResult> aggregationResults = getDoubleValues(i);
      AggregationResult combinedResult = aggregationFunction.combine(aggregationResults, CombineLevel.SEGMENT);
      assertEquals(i - 1, combinedResult.getDoubleVal(), 0.1);
    }

    // Test reduce
    for (int i = 1; i <= _sizeOfCombineList; ++i) {
      List<AggregationResult> combinedResults = getDoubleValues(i);
      AggregationResult reduceResult = aggregationFunction.reduce(combinedResults);
      assertEquals(i - 1, reduceResult.getDoubleVal(), 0.1);
    }
  }

  private static List<AggregationResult> getLongValues(int numberOfElements) {
    List<AggregationResult> longContainers = new ArrayList<AggregationResult>();
    for (int i = 0; i < numberOfElements; ++i) {
      longContainers.add(new AggregationResult(_Fields.LONG_VAL, (long) i));
    }
    return longContainers;
  }

  private static List<AggregationResult> getDoubleValues(int numberOfElements) {
    List<AggregationResult> doubleContainers = new ArrayList<AggregationResult>();
    for (int i = 0; i < numberOfElements; ++i) {
      doubleContainers.add(new AggregationResult(_Fields.DOUBLE_VAL, (double) i));
    }
    return doubleContainers;
  }
}
