package com.linkedin.pinot.query.aggregation;

import static org.testng.AssertJUnit.assertEquals;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.query.aggregation.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.CombineLevel;
import com.linkedin.pinot.core.query.aggregation.function.AvgAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.AvgAggregationFunction.AvgPair;
import com.linkedin.pinot.core.query.aggregation.function.CountAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.DistinctCountAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.MaxAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.MinAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.SumAggregationFunction;
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

    // Test combine
    for (int i = 1; i <= _sizeOfCombineList; ++i) {
      List<Serializable> aggregationResults = getLongValues(i);
      List<Serializable> combinedResults = aggregationFunction.combine(aggregationResults, CombineLevel.SEGMENT);

      assertEquals((long) (((i - 1) * i) / 2), combinedResults.get(0));
    }

    // Test reduce
    for (int i = 1; i <= _sizeOfCombineList; ++i) {
      List<Serializable> combinedResults = getLongValues(i);
      Serializable reduceResults = aggregationFunction.reduce(combinedResults);
      assertEquals((long) ((i - 1) * i) / 2, reduceResults);
    }
  }

  @Test
  public void testSumAggregation() {
    AggregationFunction aggregationFunction = new SumAggregationFunction();
    aggregationFunction.init(_paramsInfo);
    // Test aggregate
    double expectedSum = 0.0;

    // Test combine
    for (int i = 1; i <= _sizeOfCombineList; ++i) {
      List<Serializable> aggregationResults = getDoubleValues(i);
      List<Serializable> combinedResult = aggregationFunction.combine(aggregationResults, CombineLevel.SEGMENT);
      assertEquals((double) ((i - 1) * i) / 2, combinedResult.get(0));
    }

    // Test reduce
    for (int i = 1; i <= _sizeOfCombineList; ++i) {
      List<Serializable> combinedResults = getDoubleValues(i);
      Serializable reduceResults = aggregationFunction.reduce(combinedResults);
      assertEquals((double) ((i - 1) * i) / 2, reduceResults);
    }
  }

  @Test
  public void testMinAggregation() {
    AggregationFunction aggregationFunction = new MinAggregationFunction();
    aggregationFunction.init(_paramsInfo);
    // Test aggregate

    // Test combine
    for (int i = 1; i <= _sizeOfCombineList; ++i) {
      List<Serializable> aggregationResults = getDoubleValues(i);
      List<Serializable> combinedResults = aggregationFunction.combine(aggregationResults, CombineLevel.SEGMENT);
      assertEquals(0.0, combinedResults.get(0));
    }

    // Test reduce
    for (int i = 1; i <= _sizeOfCombineList; ++i) {
      List<Serializable> combinedResults = getDoubleValues(i);
      Serializable reduceResult = aggregationFunction.reduce(combinedResults);
      assertEquals(0.0, reduceResult);
    }
  }

  @Test
  public void testMaxAggregation() {
    AggregationFunction aggregationFunction = new MaxAggregationFunction();
    aggregationFunction.init(_paramsInfo);

    // Test aggregate

    // Test combine
    for (int i = 1; i <= _sizeOfCombineList; ++i) {
      List<Serializable> aggregationResults = getDoubleValues(i);
      List<Serializable> combinedResult = aggregationFunction.combine(aggregationResults, CombineLevel.SEGMENT);
      assertEquals((double) i - 1, combinedResult.get(0));
    }

    // Test reduce
    for (int i = 1; i <= _sizeOfCombineList; ++i) {
      List<Serializable> combinedResults = getDoubleValues(i);
      Serializable reduceResult = aggregationFunction.reduce(combinedResults);
      assertEquals((double) i - 1, reduceResult);
    }
  }

  @Test
  public void testAvgAggregation() {
    AggregationFunction aggregationFunction = new AvgAggregationFunction();
    aggregationFunction.init(_paramsInfo);

    // Test aggregate

    // Test combine
    for (int i = 1; i <= _sizeOfCombineList; ++i) {
      List<Serializable> aggregationResults = getAvgPairValues(i);
      List<Serializable> combinedResult = aggregationFunction.combine(aggregationResults, CombineLevel.SEGMENT);
      AvgPair retAvgPair = (AvgPair) combinedResult.get(0);
      assertEquals(1.0, retAvgPair.getFirst() / retAvgPair.getSecond());
    }

    // Test reduce
    for (int i = 1; i <= _sizeOfCombineList; ++i) {
      List<Serializable> combinedResults = getAvgPairValues(i);
      Serializable reduceResult = aggregationFunction.reduce(combinedResults);
      assertEquals(1.0, reduceResult);
    }
  }

  @Test
  public void testDistinctCountAggregation() {
    AggregationFunction aggregationFunction = new DistinctCountAggregationFunction();
    aggregationFunction.init(_paramsInfo);

    // Test aggregate

    // Test combine
    for (int i = 1; i <= _sizeOfCombineList; ++i) {
      List<Serializable> aggregationResults = getIntOpenHashSets(i);
      List<Serializable> combinedResult = aggregationFunction.combine(aggregationResults, CombineLevel.SEGMENT);
      assertEquals(i, ((IntOpenHashSet) (combinedResult.get(0))).size());
    }

    // Test reduce
    for (int i = 1; i <= _sizeOfCombineList; ++i) {
      List<Serializable> combinedResults = getIntOpenHashSets(i);
      int reduceSize = (Integer) aggregationFunction.reduce(combinedResults);
      assertEquals(i, reduceSize);
    }
  }

  private static List<Serializable> getLongValues(int numberOfElements) {
    List<Serializable> longContainers = new ArrayList<Serializable>();
    for (int i = 0; i < numberOfElements; ++i) {
      longContainers.add((long) i);
    }
    return longContainers;
  }

  private static List<Serializable> getDoubleValues(int numberOfElements) {
    List<Serializable> doubleContainers = new ArrayList<Serializable>();
    for (int i = 0; i < numberOfElements; ++i) {
      doubleContainers.add((double) i);
    }
    return doubleContainers;
  }

  private static List<Serializable> getAvgPairValues(int numberOfElements) {
    List<Serializable> avgPairList = new ArrayList<Serializable>();
    AvgAggregationFunction aggregationFunction = new AvgAggregationFunction();
    for (int i = 1; i <= numberOfElements; ++i) {
      avgPairList.add(aggregationFunction.getAvgPair(i, i));
    }
    return avgPairList;
  }

  private static List<Serializable> getIntOpenHashSets(int numberOfElements) {
    List<Serializable> intOpenHashSets = new ArrayList<Serializable>();
    for (int i = 0; i < numberOfElements; ++i) {
      IntOpenHashSet intOpenHashSet = new IntOpenHashSet();
      intOpenHashSet.add(i);
      intOpenHashSets.add(intOpenHashSet);
    }
    return intOpenHashSets;
  }
}
