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


import java.io.File;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.io.IOException;
import com.linkedin.pinot.common.utils.Pairs;
import com.google.common.collect.ListMultimap;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.data.readers.PinotSegmentColumnReader;
import com.linkedin.pinot.core.segment.creator.ColumnIndexCreationInfo;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegment;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;


public class OnHeapStarTreeV2Builder implements StarTreeV2Builder {

  // Segment
  SegmentMetadata _segmentMetadata;
  ImmutableSegment _immutableSegment;

  // Dimensions
  private int _dimensionsCount;
  private List<String> _dimensionsName;
  private List<String> _dimensionsSplitOrder;
  private List<String> _dimensionsWithoutStarNode;
  private Map<String, DimensionFieldSpec> _dimensionsSpecMap;

  // Metrics
  private int _metricsCount;
  private List<String> _metricsName;
  private int _metricAggfuncPairsCount;
  private ListMultimap<String, String> _met2aggfuncPairs;
  private Map<String, MetricFieldSpec> _metricsSpecMap;
  private List<Met2AggfuncPair> _metricAggfuncPairs = new ArrayList<>();

  // star tree data
  Map<Object, Dictionary> _starTreeData = new HashMap<>();
  Map<Integer, List<Integer>> _aggregatedData = new HashMap<>();

  // General
  private int _nodesCount;
  private int _rawDocsCount;
  private TreeNode _rootNode;
  private int _maxNumLeafRecords;


  @Override
  public void init(File indexDir, StarTreeV2BuilderConfig config) throws Exception {

    // segment
    _immutableSegment = ImmutableSegmentLoader.load(indexDir, ReadMode.mmap);
    _segmentMetadata = _immutableSegment.getSegmentMetadata();
    _rawDocsCount = _segmentMetadata.getTotalRawDocs();

    // dimension
    _dimensionsSpecMap = new HashMap<>();
    _dimensionsName = config.getDimensions();
    _dimensionsCount = _dimensionsName.size();

    List<DimensionFieldSpec> _dimensionsSpecList = _segmentMetadata.getSchema().getDimensionFieldSpecs();
    for ( DimensionFieldSpec dimension : _dimensionsSpecList) {
        if (_dimensionsName.contains(dimension.getName())) {
          _dimensionsSpecMap.put(dimension.getName(), dimension);
      }
    }

    // dimension split order.
    _dimensionsSplitOrder = config.getDimensionsSplitOrder();
    _dimensionsWithoutStarNode = config.getDimensionsWithoutStarNode();

    // metric
    _metricsSpecMap = new HashMap<>();
    _met2aggfuncPairs = config.getMetric2aggFuncPairs();
    for (String metricName: _met2aggfuncPairs.keys()) {
      _metricsCount++;
      _metricsName.add(metricName);
      List<String> aggfuncList = _met2aggfuncPairs.get(metricName);
      for (String agg: aggfuncList) {
        Met2AggfuncPair pair = new Met2AggfuncPair(metricName, agg);
        _metricAggfuncPairs.add(pair);
        _metricAggfuncPairsCount++;
      }
    }
    List<MetricFieldSpec> _metricsSpecList = _segmentMetadata.getSchema().getMetricFieldSpecs();
    for (MetricFieldSpec metric: _metricsSpecList) {
      if (_metricsName.contains(metric.getName())) {
        _metricsSpecMap.put(metric.getName(), metric);
      }
    }

    // other initialisation
    _maxNumLeafRecords = config.getMaxNumLeafRecords();
    _rootNode = new TreeNode();
    _nodesCount++;
  }

  @Override
  public void build() throws IOException {

    /*
     SORTING OF DATA.
    */

    // Recursively construct the star tree
    constructStarTree(_rootNode, 0, _rawDocsCount, 0 );
  }

  @Override
  public void serialize(File starTreeFile, Map<String, ColumnIndexCreationInfo> indexCreationInfoMap)
      throws IOException {

  }

  @Override
  public List<String> getMetaData() {
    return null;
  }

  @Override
  public void close() throws IOException {

  }


  // helper function
  private void constructStarTree(TreeNode node, int startDocId, int endDocId, int level) throws IOException {
    if (level == _dimensionsSplitOrder.size()) {
      return;
    }
    int numDocs = endDocId - startDocId;
    String splitDimensionName = _dimensionsSplitOrder.get(level);
    Map<Object, Pairs.IntPair> dimensionRangeMap = groupOnDimension(startDocId, endDocId, splitDimensionName);


    node._childDimensionName = splitDimensionName;

    // Reserve one space for star node
    Map<Object, TreeNode> children = new HashMap<>(dimensionRangeMap.size() + 1);

    node._children = children;
    for (Object key : dimensionRangeMap.keySet()) {
      Pairs.IntPair value = dimensionRangeMap.get(key);
      Object childDimensionValue = key;
      TreeNode child = new TreeNode();
      children.put(childDimensionValue, child);

      // The range pair value is the relative value to the start document id
      Pairs.IntPair range = dimensionRangeMap.get(childDimensionValue);
      int childStartDocId = range.getLeft();
      child._startDocId = childStartDocId;
      int childEndDocId = range.getRight();
      child._endDocId = childEndDocId;

      if (childEndDocId - childStartDocId > _maxNumLeafRecords) {
        constructStarTree(child, childStartDocId, childEndDocId, level + 1);
      }
      _nodesCount++;
      List<Integer> aggregatedDocument = getAggregatedDocument(childStartDocId, childEndDocId, (String)childDimensionValue);
    }

    // Directly return if we don't need to create star-node
    if (_dimensionsWithoutStarNode != null && _dimensionsWithoutStarNode.contains(splitDimensionName)) {
      return;
    }

    // Create star node
    TreeNode starChild = new TreeNode();
    _nodesCount++;
    children.put(-1, starChild);
    starChild._dimensionName = splitDimensionName;
    starChild._dimensionValue = "ALL";
    starChild._startDocId = startDocId;
    starChild._endDocId = endDocId;

    if (endDocId - startDocId > _maxNumLeafRecords) {
      constructStarTree(starChild, startDocId, startDocId, level + 1);
    }
  }

  /**
   * Group all documents based on a dimension's value.
   *
   * @param startDocId Start document id of the range to be grouped
   * @param endDocId End document id (exclusive) of the range to be grouped
   * @param dimensionName Name of the dimension to group on
   *
   * @return Map from dimension value to a pair of start docId and end docId (exclusive)
   */
  private Map<Object, Pairs.IntPair> groupOnDimension(int startDocId, int endDocId, String dimensionName) {

    Map<Object, Pairs.IntPair> rangeMap = new HashMap<>();
    DimensionFieldSpec dimensionFieldSpec = _dimensionsSpecMap.get(dimensionName);
    switch (dimensionFieldSpec.getDataType()) {
      case INT:
        rangeMap = getRangeMapforInt(startDocId, endDocId, dimensionName);
        break;
      case LONG:
        rangeMap = getRangeMapforInt(startDocId, endDocId, dimensionName);
        break;
      case FLOAT:
        rangeMap = getRangeMapforInt(startDocId, endDocId, dimensionName);
        break;
      case DOUBLE:
        rangeMap = getRangeMapforInt(startDocId, endDocId, dimensionName);
        break;
      case STRING:
        rangeMap = getRangeMapforInt(startDocId, endDocId, dimensionName);
        break;
      case BYTES:
        rangeMap = getRangeMapforInt(startDocId, endDocId, dimensionName);
        break;
    }
    return rangeMap;
  }


  private Map<Object, Pairs.IntPair> getRangeMapforInt(int startDocId, int endDocId, String dimensionName) {
    Map<Object, Pairs.IntPair> rangeMap = new HashMap<>();
    PinotSegmentColumnReader columnReader = new PinotSegmentColumnReader(_immutableSegment, dimensionName);
    Object currentValue = columnReader.readInt(startDocId);
    int groupStartDocId = startDocId;

    for (int i = startDocId + 1; i < endDocId; i++) {
      Object value = columnReader.readInt(i);
      if ((int)value != (int)currentValue) {
        int groupEndDocId = i + startDocId;
        rangeMap.put(currentValue, new Pairs.IntPair(groupStartDocId, groupEndDocId));
        currentValue = value;
        groupStartDocId = groupEndDocId;
      }
    }
    rangeMap.put(currentValue, new Pairs.IntPair(groupStartDocId, endDocId));
    return rangeMap;
   }

  /**
   * create a aggregated document for this range.
   *
   * @param startDocId Start document id of the range to be grouped
   * @param endDocId End document id (exclusive) of the range to be grouped
   * @param dimensionName Name of the dimension to group on
   *
   * @return list of all metric2aggfunc value.
   */
  private List<Integer> getAggregatedDocument(int startDocId, int endDocId, String dimensionName) {

    List<Integer>aggregatedResult = new ArrayList<>();

    for (Met2AggfuncPair pair : _metricAggfuncPairs) {

      String metric = pair.getMetricValue();
      String aggfunc = pair.getAggregatefunction();

      int val = 0;
      if (aggfunc == "SUM") {
        val = calculateSum(metric, startDocId, endDocId);
      } else if (aggfunc == "MAX") {
        val = calculateMax(metric, startDocId, endDocId);
      } else if (aggfunc == "MIN") {
        val = calculateMin(metric, startDocId, endDocId);
      }
      aggregatedResult.add(val);

    }
    return aggregatedResult;
  }

  private Integer calculateSum(String metricName, Integer startDocId, Integer endDocId) {
    int sum = 0;
    PinotSegmentColumnReader columnReader = new PinotSegmentColumnReader(_immutableSegment, metricName);
    for (int i = startDocId; i < endDocId; i++) {
      Object currentValue = columnReader.readInt(startDocId);;
      sum += (int)currentValue;
    }
    return sum;
  }

  private Integer calculateMax(String metricName, Integer startDocId, Integer endDocId) {
    int max = 100;
    PinotSegmentColumnReader columnReader = new PinotSegmentColumnReader(_immutableSegment, metricName);
    for (int i = startDocId; i < endDocId; i++) {
      Object currentValue = columnReader.readInt(startDocId);;
      if ((int)currentValue > max ) {
        max = (int)currentValue;
      }
    }
    return max;
  }

  private Integer calculateMin(String metricName, Integer startDocId, Integer endDocId) {
    int min = -100;
    PinotSegmentColumnReader columnReader = new PinotSegmentColumnReader(_immutableSegment, metricName);
    for (int i = startDocId; i < endDocId; i++) {
      Object currentValue = columnReader.readInt(startDocId);;
      if ((int)currentValue < min ) {
        min = (int)currentValue;
      }
    }
    return min;
  }
}
