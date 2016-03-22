/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import static org.testng.Assert.assertEquals;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.core.query.aggregation.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.CombineLevel;
import com.linkedin.pinot.core.query.aggregation.function.AvgAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.AvgAggregationFunction.AvgPair;
import com.linkedin.pinot.core.query.aggregation.function.CountAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.DistinctCountAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.MaxAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.MinAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.MinMaxRangeAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.MinMaxRangeAggregationFunction.MinMaxRangePair;
import com.linkedin.pinot.core.query.aggregation.function.SumAggregationFunction;

public class SimpleAggregationFunctionsTest {

  public static int[] _docIdsArray;
  public static IntArray _docIds;
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

      assertEquals(((Number) combinedResults.get(0)).longValue(), (long) (((i - 1) * i) / 2));
    }

    // Test reduce
    for (int i = 1; i <= _sizeOfCombineList; ++i) {
      List<Serializable> combinedResults = getLongValues(i);
      Serializable reduceResults = aggregationFunction.reduce(combinedResults);
      assertEquals(((Number) reduceResults).longValue(), (long) ((i - 1) * i) / 2);
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
      assertEquals(combinedResult.get(0), (double) ((i - 1) * i) / 2);
    }

    // Test reduce
    for (int i = 1; i <= _sizeOfCombineList; ++i) {
      List<Serializable> combinedResults = getDoubleValues(i);
      Serializable reduceResults = aggregationFunction.reduce(combinedResults);
      assertEquals(reduceResults, (double) ((i - 1) * i) / 2);
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
      assertEquals(reduceResult, 0.0);
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
      assertEquals(combinedResult.get(0), (double) i - 1);
    }

    // Test reduce
    for (int i = 1; i <= _sizeOfCombineList; ++i) {
      List<Serializable> combinedResults = getDoubleValues(i);
      Serializable reduceResult = aggregationFunction.reduce(combinedResults);
      assertEquals(reduceResult, (double) i - 1);
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
      assertEquals(reduceResult, 1.0);
    }
  }

  @Test
  public void testMinMaxRangeAggregation() {
    AggregationFunction aggregationFunction = new MinMaxRangeAggregationFunction();
    aggregationFunction.init(_paramsInfo);

    // Test aggregate

    // Test combine
    for (int i = 1; i <= _sizeOfCombineList; ++i) {
      List<Serializable> aggregationResults = getMinMaxRangePairValues(i);
      List<Serializable> combinedResult = aggregationFunction.combine(aggregationResults, CombineLevel.SEGMENT);
      MinMaxRangePair retMinMaxRangePair = (MinMaxRangePair) combinedResult.get(0);
      assertEquals(retMinMaxRangePair.getSecond() - retMinMaxRangePair.getFirst(), (double) i);
    }

    // Test reduce
    for (int i = 1; i <= _sizeOfCombineList; ++i) {
      List<Serializable> combinedResults = getMinMaxRangePairValues(i);
      Serializable reduceResult = aggregationFunction.reduce(combinedResults);
      assertEquals(reduceResult, (double) i);
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
      assertEquals(((IntOpenHashSet) (combinedResult.get(0))).size(), i);
    }

    // Test reduce
    for (int i = 1; i <= _sizeOfCombineList; ++i) {
      List<Serializable> combinedResults = getIntOpenHashSets(i);
      int reduceSize = (Integer) aggregationFunction.reduce(combinedResults);
      assertEquals(reduceSize, i);
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

  private static List<Serializable> getMinMaxRangePairValues(int numberOfElements) {
    List<Serializable> minMaxRangePairList = new ArrayList<Serializable>();
    MinMaxRangeAggregationFunction aggregationFunction = new MinMaxRangeAggregationFunction();
    for (int i = 1; i <= numberOfElements; ++i) {
      minMaxRangePairList.add(aggregationFunction.getMinMaxRangePair(0, i));
    }
    return minMaxRangePairList;
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
