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
import java.util.Queue;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.Collections;
import java.nio.charset.Charset;
import xerial.larray.mmap.MMapBuffer;
import com.linkedin.pinot.core.startree.OffHeapStarTree;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;


public class OnHeapStarTreeV2BuilderHelper {

  private static final Charset UTF_8 = Charset.forName("UTF-8");

  /**
   * enumerate dimension set.
   */
  public static List<Integer> enumerateDimensions(List<String> dimensionNames, List<String> dimensionsOrder) {
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
    for (int i = startDocId; i < endDocId; i++) {
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

  /**
   * aggregate metric values ( raw or aggregated )
   */
  public static List<Object> aggregateMetrics(int start, int end, List<Record> starTreeData,
      List<AggfunColumnPair> aggfunColumnPairs, boolean isRawData) {
    List<Object> aggregatedMetricsValue = new ArrayList<>();

    List<List<Object>> metricValues = new ArrayList<>();
    for (int i = 0; i < aggfunColumnPairs.size(); i++) {
      List<Object> l = new ArrayList<>();
      metricValues.add(l);
    }

    for (int i = start; i < end; i++) {
      Record r = starTreeData.get(i);
      List<Object> metric = r.getMetricValues();
      for (int j = 0; j < aggfunColumnPairs.size(); j++) {
        metricValues.get(j).add(metric.get(j));
      }
    }

    AggregationFunctionFactory functionFactory = new AggregationFunctionFactory();
    for (int i = 0; i < aggfunColumnPairs.size(); i++) {
      AggfunColumnPair pair = aggfunColumnPairs.get(i);
      String aggfunc = pair.getAggregatefunction();
      List<Object> data = metricValues.get(i);
      AggregationFunction function = functionFactory.getAggregationFunction(aggfunc);
      aggregatedMetricsValue.add(aggregate(function, isRawData, data));
    }

    return aggregatedMetricsValue;
  }

  /**
   * aggregate raw or pre aggregated data.
   */
  public static Object aggregate(AggregationFunction function, boolean isRawData, List<Object> data) {
    if (isRawData) {
      return function.aggregateRaw(data);
    } else {
      return function.aggregatePreAggregated(data);
    }
  }

  /**
   * function to condense documents according to sorted order.
   */
  public static List<Record> condenseData(List<Record> starTreeData, List<AggfunColumnPair> aggfunColumnPairs,
      boolean isRawData) {
    int start = 0;
    List<Record> newData = new ArrayList<>();
    Record prevRecord = starTreeData.get(0);

    for (int i = 1; i < starTreeData.size(); i++) {
      Record nextRecord = starTreeData.get(i);
      int[] prevDimensions = prevRecord.getDimensionValues();
      int[] nextDimensions = nextRecord.getDimensionValues();

      if (!RecordUtil.compareDimensions(prevDimensions, nextDimensions)) {
        List<Object> aggregatedMetricsValue = aggregateMetrics(start, i, starTreeData, aggfunColumnPairs, isRawData);
        Record newRecord = new Record();
        newRecord.setMetricValues(aggregatedMetricsValue);
        newRecord.setDimensionValues(prevRecord.getDimensionValues());
        newData.add(newRecord);
        prevRecord = nextRecord;
        start = i;
      }
    }
    Record record = new Record();
    record.setDimensionValues(starTreeData.get(start).getDimensionValues());
    List<Object> aggregatedMetricsValue =
        aggregateMetrics(start, starTreeData.size(), starTreeData, aggfunColumnPairs, isRawData);
    record.setMetricValues(aggregatedMetricsValue);
    newData.add(record);

    return newData;
  }

  /**
   * Filter data by removing the dimension we don't need.
   */
  public static List<Record> filterData(int startDocId, int endDocId, int dimensionIdToRemove, List<Integer> sortOrder,
      List<Record> starTreeData) {

    List<Record> newData = new ArrayList<>();

    for (int i = startDocId; i < endDocId; i++) {
      Record record = starTreeData.get(i);
      int[] dimension = record.getDimensionValues().clone();
      List<Object> metric = record.getMetricValues();
      dimension[dimensionIdToRemove] = StarTreeV2Constant.SKIP_VALUE;

      Record newRecord = new Record();
      newRecord.setDimensionValues(dimension);
      newRecord.setMetricValues(metric);

      newData.add(newRecord);
    }
    return sortStarTreeData(0, newData.size(), sortOrder, newData);
  }

  /**
   * Helper method to compute size of the header of the star tree in bytes.
   */
  public static int computeHeaderSizeInBytes(List<String> dimensionsName) {
    // Magic marker (8), version (4), size of header (4) and number of dimensions (4)
    int headerSizeInBytes = 20;

    for (String dimension : dimensionsName) {
      headerSizeInBytes += V1Constants.Numbers.INTEGER_SIZE; // For dimension index
      headerSizeInBytes += V1Constants.Numbers.INTEGER_SIZE; // For length of dimension name
      headerSizeInBytes += dimension.getBytes(UTF_8).length; // For dimension name
    }

    headerSizeInBytes += V1Constants.Numbers.INTEGER_SIZE; // For number of nodes.
    return headerSizeInBytes;
  }

  /**
   * Helper method to write the header into the data buffer.
   */
  public static long writeHeader(MMapBuffer dataBuffer, int headerSizeInBytes, int dimensionCount,
      List<String> dimensionsName, int nodesCount) {
    long offset = 0L;
    dataBuffer.putLong(offset, OffHeapStarTree.MAGIC_MARKER);
    offset += V1Constants.Numbers.LONG_SIZE;

    dataBuffer.putInt(offset, OffHeapStarTree.VERSION);
    offset += V1Constants.Numbers.INTEGER_SIZE;

    dataBuffer.putInt(offset, headerSizeInBytes);
    offset += V1Constants.Numbers.INTEGER_SIZE;

    dataBuffer.putInt(offset, dimensionCount);
    offset += V1Constants.Numbers.INTEGER_SIZE;

    for (int i = 0; i < dimensionCount; i++) {
      String dimensionName = dimensionsName.get(i);

      dataBuffer.putInt(offset, i);
      offset += V1Constants.Numbers.INTEGER_SIZE;

      byte[] dimensionBytes = dimensionName.getBytes(UTF_8);
      int dimensionLength = dimensionBytes.length;
      dataBuffer.putInt(offset, dimensionLength);
      offset += V1Constants.Numbers.INTEGER_SIZE;

      dataBuffer.readFrom(dimensionBytes, offset);
      offset += dimensionLength;
    }

    dataBuffer.putInt(offset, nodesCount);
    offset += V1Constants.Numbers.INTEGER_SIZE;

    return offset;
  }

  /**
   * Helper method to write the star tree nodes into the data buffer.
   */
  public static void writeNodes(MMapBuffer dataBuffer, long offset, TreeNode rootNode) {
    int index = 0;
    Queue<TreeNode> queue = new LinkedList<>();
    queue.add(rootNode);

    while (!queue.isEmpty()) {
      TreeNode node = queue.remove();

      if (node._children == null) {
        offset =
            writeNode(dataBuffer, node, offset, StarTreeV2Constant.INVALID_INDEX, StarTreeV2Constant.INVALID_INDEX);
      } else {
        int startChildrenIndex = index + queue.size() + 1;
        int endChildrenIndex = startChildrenIndex + node._children.size() - 1;
        offset = writeNode(dataBuffer, node, offset, startChildrenIndex, endChildrenIndex);

        queue.addAll(node._children.values());
      }

      index++;
    }
  }

  /**
   * Helper method to write one node into the data buffer.
   */
  private static long writeNode(MMapBuffer dataBuffer, TreeNode node, long offset, int startChildrenIndex,
      int endChildrenIndex) {
    dataBuffer.putInt(offset, node._dimensionId);
    offset += V1Constants.Numbers.INTEGER_SIZE;

    dataBuffer.putInt(offset, node._dimensionValue);
    offset += V1Constants.Numbers.INTEGER_SIZE;

    dataBuffer.putInt(offset, node._startDocId);
    offset += V1Constants.Numbers.INTEGER_SIZE;

    dataBuffer.putInt(offset, node._endDocId);
    offset += V1Constants.Numbers.INTEGER_SIZE;

    dataBuffer.putInt(offset, node._aggDataDocumentId);
    offset += V1Constants.Numbers.INTEGER_SIZE;

    dataBuffer.putInt(offset, startChildrenIndex);
    offset += V1Constants.Numbers.INTEGER_SIZE;

    dataBuffer.putInt(offset, endChildrenIndex);
    offset += V1Constants.Numbers.INTEGER_SIZE;

    return offset;
  }
}
