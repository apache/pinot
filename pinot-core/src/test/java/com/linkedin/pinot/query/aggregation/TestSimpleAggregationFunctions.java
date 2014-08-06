package com.linkedin.pinot.query.aggregation;

import static org.testng.AssertJUnit.assertEquals;

import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.linkedin.pinot.common.query.response.AggregationResult;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.query.aggregation.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.CombineLevel;
import com.linkedin.pinot.core.query.aggregation.data.DoubleContainer;
import com.linkedin.pinot.core.query.aggregation.data.LongContainer;
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
      LongContainer result = (LongContainer) aggregationFunction.aggregate(_docIds, i, _indexSegment);
      assertEquals(i, result.get());
    }
    // Test combine
    for (int i = 1; i <= _sizeOfCombineList; ++i) {
      List<AggregationResult> aggregationResults = getLongContanier(i);
      List<AggregationResult> combinedResults = aggregationFunction.combine(aggregationResults, CombineLevel.SEGMENT);
      assertEquals(1, combinedResults.size());
      assertEquals(((i - 1) * i) / 2, ((LongContainer) combinedResults.get(0)).get());
    }

    // Test reduce
    for (int i = 1; i <= _sizeOfCombineList; ++i) {
      List<AggregationResult> combinedResults = getLongContanier(i);
      AggregationResult reduceResults = aggregationFunction.reduce(combinedResults);
      assertEquals(((i - 1) * i) / 2, ((LongContainer) reduceResults).get());
    }
  }

  @Test
  public void testSumAggregation() {
    AggregationFunction aggregationFunction = new SumAggregationFunction();
    aggregationFunction.init(_paramsInfo);
    // Test aggregate
    long expectedSum = 0;
    for (int i = 1; i <= _sizeOfDocIdArray; ++i) {
      LongContainer result = (LongContainer) aggregationFunction.aggregate(_docIds, i, _indexSegment);
      assertEquals(expectedSum, result.get());
      expectedSum += i;
    }
    // Test combine
    for (int i = 1; i <= _sizeOfCombineList; ++i) {
      List<AggregationResult> aggregationResults = getLongContanier(i);
      List<AggregationResult> combinedResults = aggregationFunction.combine(aggregationResults, CombineLevel.SEGMENT);
      assertEquals(1, combinedResults.size());
      assertEquals(((i - 1) * i) / 2, ((LongContainer) combinedResults.get(0)).get());
    }

    // Test reduce
    for (int i = 1; i <= _sizeOfCombineList; ++i) {
      List<AggregationResult> combinedResults = getLongContanier(i);
      AggregationResult reduceResults = aggregationFunction.reduce(combinedResults);
      assertEquals(((i - 1) * i) / 2, ((LongContainer) reduceResults).get());
    }
  }

  @Test
  public void testSumDoubleAggregation() {
    AggregationFunction aggregationFunction = new SumDoubleAggregationFunction();
    aggregationFunction.init(_paramsInfo);
    // Test aggregate
    double expectedSum = 0;
    for (int i = 1; i <= _sizeOfDocIdArray; ++i) {
      DoubleContainer result = (DoubleContainer) aggregationFunction.aggregate(_docIds, i, _indexSegment);
      assertEquals(expectedSum, result.get(), 0.1);
      expectedSum += i;
    }
    // Test combine
    for (int i = 1; i <= _sizeOfCombineList; ++i) {
      List<AggregationResult> aggregationResults = getDoubleContanier(i);
      List<AggregationResult> combinedResults = aggregationFunction.combine(aggregationResults, CombineLevel.SEGMENT);
      assertEquals(1, combinedResults.size());
      assertEquals(((i - 1) * i) / 2, ((DoubleContainer) combinedResults.get(0)).get(), 0.1);
    }

    // Test reduce
    for (int i = 1; i <= _sizeOfCombineList; ++i) {
      List<AggregationResult> combinedResults = getDoubleContanier(i);
      AggregationResult reduceResults = aggregationFunction.reduce(combinedResults);
      assertEquals(((i - 1) * i) / 2, ((DoubleContainer) reduceResults).get(), 0.1);
    }
  }

  @Test
  public void testMinAggregation() {
    AggregationFunction aggregationFunction = new MinAggregationFunction();
    aggregationFunction.init(_paramsInfo);
    // Test aggregate
    for (int i = 1; i <= _sizeOfDocIdArray; ++i) {
      DoubleContainer result = (DoubleContainer) aggregationFunction.aggregate(_docIds, i, _indexSegment);
      assertEquals(0.0, result.get(), 0.1);
    }
    // Test combine
    for (int i = 1; i <= _sizeOfCombineList; ++i) {
      List<AggregationResult> aggregationResults = getDoubleContanier(i);
      List<AggregationResult> combinedResults = aggregationFunction.combine(aggregationResults, CombineLevel.SEGMENT);
      assertEquals(1, combinedResults.size());
      assertEquals(0.0, ((DoubleContainer) combinedResults.get(0)).get(), 0.1);
    }

    // Test reduce
    for (int i = 1; i <= _sizeOfCombineList; ++i) {
      List<AggregationResult> combinedResults = getDoubleContanier(i);
      AggregationResult reduceResults = aggregationFunction.reduce(combinedResults);
      assertEquals(0.0, ((DoubleContainer) reduceResults).get(), 0.1);
    }
  }

  @Test
  public void testMaxAggregation() {
    AggregationFunction aggregationFunction = new MaxAggregationFunction();
    aggregationFunction.init(_paramsInfo);

    // Test aggregate
    for (int i = 1; i <= _sizeOfDocIdArray; ++i) {
      DoubleContainer result = (DoubleContainer) aggregationFunction.aggregate(_docIds, i, _indexSegment);
      assertEquals(i - 1, result.get(), 0.1);
    }
    // Test combine
    for (int i = 1; i <= _sizeOfCombineList; ++i) {
      List<AggregationResult> aggregationResults = getDoubleContanier(i);
      List<AggregationResult> combinedResults = aggregationFunction.combine(aggregationResults, CombineLevel.SEGMENT);
      assertEquals(1, combinedResults.size());
      assertEquals(i - 1, ((DoubleContainer) combinedResults.get(0)).get(), 0.1);
    }

    // Test reduce
    for (int i = 1; i <= _sizeOfCombineList; ++i) {
      List<AggregationResult> combinedResults = getDoubleContanier(i);
      AggregationResult reduceResults = aggregationFunction.reduce(combinedResults);
      assertEquals(i - 1, ((DoubleContainer) reduceResults).get(), 0.1);
    }
  }

  private static List<AggregationResult> getLongContanier(int numberOfElements) {
    List<AggregationResult> longContainers = new ArrayList<AggregationResult>();
    for (int i = 0; i < numberOfElements; ++i) {
      longContainers.add(new LongContainer(i));
    }
    return longContainers;
  }

  private static List<AggregationResult> getDoubleContanier(int numberOfElements) {
    List<AggregationResult> doubleContainers = new ArrayList<AggregationResult>();
    for (int i = 0; i < numberOfElements; ++i) {
      doubleContainers.add(new DoubleContainer(i));
    }
    return doubleContainers;
  }
}
