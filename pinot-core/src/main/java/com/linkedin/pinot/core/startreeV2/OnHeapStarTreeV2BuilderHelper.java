/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.pinot.core.startreeV2;

import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collections;


public class OnHeapStarTreeV2BuilderHelper {

  /**
   * enumerate dimension set.
   */
  public static List<Integer> enumerateDimensions(List<String>dimensionNames, List<String>dimensionsOrder) {
    List<Integer> enumeratedDimensions = new ArrayList<>();
    if (dimensionsOrder != null) {
      for (String dimensionName : dimensionsOrder) {
        enumeratedDimensions.add(dimensionNames.indexOf(dimensionName));
      }
    }

    return enumeratedDimensions;
  }

  /**
   * compute a defualt split order.
   */
  public static List<Integer> computeDefaultSplitOrder(int dimensionsCount, List<Integer> dimensionCardinality) {
    List<Integer> defaultSplitOrder = new ArrayList<>();
    for (int i = 0; i < dimensionsCount; i++) {
      defaultSplitOrder.add(i);
    }

    Collections.sort(defaultSplitOrder, new Comparator<Integer>() {
      @Override
      public int compare(Integer o1, Integer o2) {
        return dimensionCardinality.get(o2) - dimensionCardinality.get(o1);
      }
    });

    return defaultSplitOrder;
  }

  /**
   * sort the star tree data.
   */
  public static List<Record> sortStarTreeData(int startDocId, int endDocId, List<Integer> sortOrder,
      List<Record> starTreeData) {

    List<Record> newData = new ArrayList<>();
    for ( int i = startDocId; i < endDocId; i++) {
      newData.add(starTreeData.get(i));
    }

    Collections.sort(newData, new Comparator<Record>() {
      @Override
      public int compare(Record o1, Record o2) {
        int compare = 0;
        for (int index : sortOrder) {
          compare = o1.getDimensionValues()[index] - o2.getDimensionValues()[index];
          if (compare != 0) {
            return compare;
          }
        }
        return compare;
      }
    });

    return newData;
  }

  private static List<Object> aggregateMetrics(int start, int end, List<Record> starTreeData, List<Met2AggfuncPair> met2aggfuncPairs) {
    List<Object> aggregatedMetricsValue = new ArrayList<>();

    List<List<Object>> metricValues = new ArrayList<>();
    for (int i = 0; i <  met2aggfuncPairs.size(); i++) {
        List<Object> l = new ArrayList<>();
        metricValues.add(l);
    }
    List<Object> count = new ArrayList<>();
    metricValues.add(count);

    for (int i = start; i < end; i++) {
      Record r = starTreeData.get(i);
      List<Object> metric = r.getMetricValues();
      for (int j = 0; j < met2aggfuncPairs.size() + 1; j++) {
        metricValues.get(j).add(metric.get(j));
      }
    }

    for (int i = 0; i < met2aggfuncPairs.size(); i++) {
      Met2AggfuncPair pair = met2aggfuncPairs.get(i);
      String aggfunc = pair.getAggregatefunction();
      if (aggfunc == StarTreeV2Constant.AggregateFunctions.MAX) {

      } else if (aggfunc == StarTreeV2Constant.AggregateFunctions.MIN) {

      } else if (aggfunc == StarTreeV2Constant.AggregateFunctions.SUM) {

      }
    }

    return aggregatedMetricsValue;
  }

  /**
   * function to condense documents according to sorted order.
   */
  public static List<Record> condenseData( List<Record> starTreeData, List<Met2AggfuncPair> met2aggfuncPairs) {
    int start = 0;
    List<Record> newData = new ArrayList<>();
    Record prevRecord = starTreeData.get(0);

    for ( int i = 1; i < starTreeData.size(); i++) {
      Record nextRecord = starTreeData.get(i);
      int [] prevDimensions = prevRecord.getDimensionValues();
      int [] nextDimensions = nextRecord.getDimensionValues();

      if (!prevDimensions.equals(nextDimensions)) {
        List<Object> aggregatedMetricsValue = aggregateMetrics(start, i, starTreeData, met2aggfuncPairs);
        prevRecord.setMetricValues(aggregatedMetricsValue);
        newData.add(prevRecord);
        newData.add(nextRecord);
        prevRecord = nextRecord;
        start = i;
      }
    }
    return newData;
  }

  /**
   * Filter data by removing the dimension we don't need.
   */
  public static List<Record> filterData(int startDocId, int endDocId, int dimensionIdToRemove, List<Integer>sortOrder, List<Record>starTreeData) {

    List<Record> newData = new ArrayList<>();

    for (int i = startDocId; i < endDocId; i++) {
      Record record = starTreeData.get(i);
      int [] dimension = record.getDimensionValues().clone();
      List<Object> metric = record.getMetricValues();
      dimension[dimensionIdToRemove] = StarTreeV2Constant.STAR_NODE;

      Record newRecord = new Record();
      newRecord.setDimensionValues(dimension);
      newRecord.setMetricValues(metric);

      newData.add(newRecord);
    }
    return sortStarTreeData(0, newData.size(), sortOrder, newData);
  }

  /**
   * Calculate SUM of the range.
   */
  public static Object calculateSum(List<Object>data) {
    int sum = 0;
    for (int i = 0; i < data.size(); i++) {
      Object currentValue = data.get(i);
      sum += (int)currentValue;
    }
    return sum;
  }

  /**
   * Calculate MAX of the range.
   */
  public static Object calculateMax(List<Object>data) {
    int max = Integer.MIN_VALUE;
    for (int i = 0; i < data.size(); i++) {
      Object currentValue = data.get(i);
      if ((int)currentValue > max ) {
        max = (int)currentValue;
      }
    }
    return max;
  }

  /**
   * Calculate MIN of the range.
   */
  public static Object calculateMin(List<Object>data) {
    int min = Integer.MAX_VALUE;
    for (int i = 0; i < data.size(); i++) {
      Object currentValue = data.get(i);
      if ((int)currentValue < min ) {
        min = (int)currentValue;
      }
    }
    return min;
  }
}
