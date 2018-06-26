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

import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;
import java.io.File;
import java.util.Set;
import java.util.Map;
import java.util.List;
import java.util.Queue;
import java.util.HashMap;
import java.util.HashSet;
import java.util.ArrayList;
import java.io.IOException;
import java.util.LinkedList;
import com.linkedin.pinot.common.utils.Pairs;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.data.DimensionFieldSpec;
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
  private List<Integer> _dimensionsCardinalty;
  private List<Integer> _dimensionsSplitOrder;
  private List<Integer> _dimensionsWithoutStarNode;
  private Map<String, DimensionFieldSpec> _dimensionsSpecMap;

  // Metrics
  private int _metricsCount;
  private Set<String> _metricsName;
  private int _met2aggfuncPairsCount;
  private List<Met2AggfuncPair> _met2aggfuncPairs;
  private Map<String, MetricFieldSpec> _metricsSpecMap;

  // General
  private int _nodesCount;
  private int _rawDocsCount;
  private TreeNode _rootNode;
  private int _maxNumLeafRecords;

  // Star Tree
  private List<Record> _starTreeData = new ArrayList<>();
  private List<Record> _rawStarTreeData = new ArrayList<>();

  @Override
  public void init(File indexDir, StarTreeV2Config config) throws Exception {

    // segment
    _immutableSegment = ImmutableSegmentLoader.load(indexDir, ReadMode.mmap);
    _segmentMetadata = _immutableSegment.getSegmentMetadata();
    _rawDocsCount = _segmentMetadata.getTotalRawDocs();

    // dimension
    _dimensionsSpecMap = new HashMap<>();
    _dimensionsName = config.getDimensions();
    _dimensionsCount = _dimensionsName.size();

    List<DimensionFieldSpec> _dimensionsSpecList = _segmentMetadata.getSchema().getDimensionFieldSpecs();
    for (DimensionFieldSpec dimension : _dimensionsSpecList) {
      if (_dimensionsName.contains(dimension.getName())) {
        _dimensionsSpecMap.put(dimension.getName(), dimension);
      }
    }

    // dimension split order.
    List<String> dimensionsSplitOrder = config.getDimensionsSplitOrder();
    _dimensionsSplitOrder = OnHeapStarTreeV2BuilderHelper.enumerateDimensions(_dimensionsName, dimensionsSplitOrder);
    List<String> dimensionsWithoutStarNode = config.getDimensionsWithoutStarNode();
    _dimensionsWithoutStarNode =
        OnHeapStarTreeV2BuilderHelper.enumerateDimensions(_dimensionsName, dimensionsWithoutStarNode);

    // metric
    _metricsName = new HashSet<>();
    _metricsSpecMap = new HashMap<>();
    _met2aggfuncPairs = config.getMetric2aggFuncPairs();
    _met2aggfuncPairsCount = _met2aggfuncPairs.size();
    for (Met2AggfuncPair pair : _met2aggfuncPairs) {
      _metricsName.add(pair.getMetricName());
    }
    _metricsCount = _metricsName.size();

    List<MetricFieldSpec> _metricsSpecList = _segmentMetadata.getSchema().getMetricFieldSpecs();
    for (MetricFieldSpec metric : _metricsSpecList) {
      if (_metricsName.contains(metric.getName())) {
        _metricsSpecMap.put(metric.getName(), metric);
      }
    }

    // other initialisation
    _maxNumLeafRecords = config.getMaxNumLeafRecords();
    _rootNode = new TreeNode();
    _rootNode._dimensionId = StarTreeV2Constant.STAR_NODE;
    _nodesCount++;
  }

  @Override
  public void build() throws IOException {

    // storing dimension cardinality for calculating default sorting order.
    _dimensionsCardinalty = new ArrayList<>();
    for (int i = 0; i < _dimensionsCount; i++) {
      String dimensionName = _dimensionsName.get(i);
      ImmutableDictionaryReader dictionary = _immutableSegment.getDictionary(dimensionName);
      _dimensionsCardinalty.add(dictionary.length());
    }

    // generating raw data for star tree.
    List<PinotSegmentColumnReader> dimensionColumnReaders = new ArrayList<>();
    for (String name : _dimensionsName) {
      PinotSegmentColumnReader columnReader = new PinotSegmentColumnReader(_immutableSegment, name);
      dimensionColumnReaders.add(columnReader);
    }

    // gathering dimensions data.
    for (int i = 0; i < _rawDocsCount; i++) {
      Record record = new Record();
      int[] dimensionValues = new int[_dimensionsCount];
      for (int j = 0; j < dimensionColumnReaders.size(); j++) {
        Integer dictId = dimensionColumnReaders.get(j).getDictionaryId(i);
        dimensionValues[j] = dictId;
      }
      record.setDimensionValues(dimensionValues);
      _rawStarTreeData.add(record);
    }

    // gathering metric data.
    List<PinotSegmentColumnReader> metricColumnReaders = new ArrayList<>();
    for (int i = 0; i < _met2aggfuncPairsCount; i++) {
      String metricName = _met2aggfuncPairs.get(i).getMetricName();
      PinotSegmentColumnReader columnReader = new PinotSegmentColumnReader(_immutableSegment, metricName);
      metricColumnReaders.add(columnReader);
    }

    for (int i = 0; i < _rawDocsCount; i++) {
      Record record = _rawStarTreeData.get(i);
      List<Object> metricRawValues = new ArrayList<>();
      for (int j = 0; j < _met2aggfuncPairsCount; j++) {
        String metricName = _met2aggfuncPairs.get(j).getMetricName();
        MetricFieldSpec metricFieldSpec = _metricsSpecMap.get(metricName);
        Object val = readHelper(metricColumnReaders.get(j), metricFieldSpec.getDataType(), i);
        metricRawValues.add(val);
      }
      metricRawValues.add(1);   // for the count(*)
      record.setMetricValues(metricRawValues);
    }

    // calculating default split order in case null provided.
    if (_dimensionsSplitOrder.isEmpty() || _dimensionsSplitOrder == null) {
      _dimensionsSplitOrder =
          OnHeapStarTreeV2BuilderHelper.computeDefaultSplitOrder(_dimensionsCount, _dimensionsCardinalty);
    }

    // sorting the data as per the sort order.
    List<Record> rawSortedStarTreeData =
        OnHeapStarTreeV2BuilderHelper.sortStarTreeData(0, _rawDocsCount, _dimensionsSplitOrder, _rawStarTreeData);
    _starTreeData = OnHeapStarTreeV2BuilderHelper.condenseData(rawSortedStarTreeData, _met2aggfuncPairs);

    // Recursively construct the star tree
    _rootNode._startDocId = 0;
    _rootNode._endDocId = _starTreeData.size();
    constructStarTree(_rootNode, 0, _starTreeData.size(), 0);

    // create aggregated doc for all nodes.
    //createAggregatedDocForAllNodes();

    createAggregatedDocForAllNodesBottom(_rootNode);
    return;
  }

  @Override
  public void serialize(File starTreeFile, Map<String, ColumnIndexCreationInfo> indexCreationInfoMap)
      throws IOException {

  }

  @Override
  public List<String> getMetaData() {
    return null;
  }

  /**
   * Helper function to construct a star tree.
   *
   * @param node TreeNode to start with.
   * @param startDocId Start document id of the range to be grouped
   * @param endDocId End document id (exclusive) of the range to be grouped
   * @param level Name of the dimension to group on
   *
   * @return void.
   */
  private void constructStarTree(TreeNode node, int startDocId, int endDocId, int level) throws IOException {
    if (level == _dimensionsSplitOrder.size()) {
      return;
    }

    int numDocs = endDocId - startDocId;
    int splitDimensionId = _dimensionsSplitOrder.get(level);
    Map<Integer, Pairs.IntPair> dimensionRangeMap = groupOnDimension(startDocId, endDocId, splitDimensionId);

    node._childDimensionId = splitDimensionId;

    // Reserve one space for star node
    Map<Integer, TreeNode> children = new HashMap<>(dimensionRangeMap.size() + 1);

    node._children = children;
    for (Integer key : dimensionRangeMap.keySet()) {
      int childDimensionValue = key;
      Pairs.IntPair range = dimensionRangeMap.get(childDimensionValue);

      TreeNode child = new TreeNode();
      child._dimensionId = splitDimensionId;
      int childStartDocId = range.getLeft();
      child._startDocId = childStartDocId;
      int childEndDocId = range.getRight();
      child._endDocId = childEndDocId;
      children.put(childDimensionValue, child);
      if (childEndDocId - childStartDocId > _maxNumLeafRecords) {
        constructStarTree(child, childStartDocId, childEndDocId, level + 1);
      }
      _nodesCount++;
    }

    // Directly return if we don't need to create star-node
    if (_dimensionsWithoutStarNode != null && _dimensionsWithoutStarNode.contains(splitDimensionId)) {
      return;
    }

    // Create star node
    TreeNode starChild = new TreeNode();
    starChild._dimensionId = splitDimensionId;
    int starChildStartDocId = _starTreeData.size();
    starChild._startDocId = starChildStartDocId;

    children.put(StarTreeV2Constant.STAR_NODE, starChild);
    _nodesCount++;

    List<Record> sortedFilteredData =
        OnHeapStarTreeV2BuilderHelper.filterData(startDocId, endDocId, splitDimensionId, _dimensionsSplitOrder,
            _starTreeData);
    List<Record> condensedData = OnHeapStarTreeV2BuilderHelper.condenseData(sortedFilteredData, _met2aggfuncPairs);
    _starTreeData.addAll(condensedData);

    int starChildEndDocId = _starTreeData.size();
    starChild._endDocId = starChildEndDocId;

    if (starChildEndDocId - starChildStartDocId > _maxNumLeafRecords) {
      constructStarTree(starChild, starChildStartDocId, starChildEndDocId, level + 1);
    }
  }

  /**
   * Group all documents based on a dimension's value.
   *
   * @param startDocId Start document id of the range to be grouped
   * @param endDocId End document id (exclusive) of the range to be grouped
   * @param dimensionId Id of the dimension to group on
   *
   * @return Map from dimension value to a pair of start docId and end docId (exclusive)
   */
  private Map<Integer, Pairs.IntPair> groupOnDimension(int startDocId, int endDocId, Integer dimensionId) {
    Map<Integer, Pairs.IntPair> rangeMap = new HashMap<>();
    int[] rowDimensions = _starTreeData.get(startDocId).getDimensionValues();
    int currentValue = rowDimensions[dimensionId];

    int groupStartDocId = startDocId;

    for (int i = startDocId + 1; i < endDocId; i++) {
      rowDimensions = _starTreeData.get(i).getDimensionValues();
      int value = rowDimensions[dimensionId];
      if (value != currentValue) {
        int groupEndDocId = i;
        rangeMap.put(currentValue, new Pairs.IntPair(groupStartDocId, groupEndDocId));
        currentValue = value;
        groupStartDocId = groupEndDocId;
      }
    }
    rangeMap.put(currentValue, new Pairs.IntPair(groupStartDocId, endDocId));

    return rangeMap;
  }

  /**
   * Helper function to read value of a doc in a column
   *
   * @param reader Pinot segment column reader
   * @param dataType Data type of the column.
   * @param docId document Id for which data has to be read.
   *
   * @return Object
   */
  private Object readHelper(PinotSegmentColumnReader reader, FieldSpec.DataType dataType, int docId) {
    switch (dataType) {
      case INT:
        return reader.readInt(docId);
      case FLOAT:
        return reader.readFloat(docId);
      case LONG:
        return reader.readLong(docId);
      case DOUBLE:
        return reader.readDouble(docId);
      case STRING:
        return reader.readString(docId);
    }

    return null;
  }

//  /**
//   * Helper function to create aggregated document for all nodes
//   *
//   * @return void.
//   */
//  private void createAggregatedDocForAllNodes() {
//    Queue<TreeNode> childNodes = new LinkedList<>();
//
//    Map<Integer, TreeNode> children = _rootNode._children;
//    for (int key : children.keySet()) {
//      TreeNode child = children.get(key);
//      child._value = key;
//      childNodes.add(child);
//    }
//
//    while (!childNodes.isEmpty()) {
//      TreeNode parent = childNodes.remove();
//      if (parent._value != StarTreeV2Constant.STAR_NODE) {
//        List<Object> aggregatedValues = getAggregatedDocument(parent._startDocId, parent._endDocId);
//        int aggDocId = appendAggregatedDocuments(aggregatedValues);
//        parent._aggDataDocumentId = aggDocId;
//      }
//
//      children = parent._children;
//      if (children != null) {
//        for (int key : children.keySet()) {
//          TreeNode child = children.get(key);
//          child._value = key;
//          childNodes.add(child);
//        }
//      }
//    }
//    return;
//  }

  /**
   * Helper function to create aggregated document for all nodes
   *
   * @return void.
   */
  private void createAggregatedDocForAllNodesBottom(TreeNode node) {

    if (node._children == null) {
      if (node._value == StarTreeV2Constant.STAR_NODE) {
        return;
      }

      List<Object> aggregatedValues = getAggregatedDocument(node._startDocId, node._endDocId);
      int aggDocId = appendAggregatedDocuments(aggregatedValues);
      node._aggDataDocumentId = aggDocId;
      return;
    }

    Map<Integer, TreeNode> children = node._children;
    for (int key : children.keySet()) {
      TreeNode child = children.get(key);
      child._value = key;
      createAggregatedDocForAllNodesBottom(child);
    }

    if (node._value != StarTreeV2Constant.STAR_NODE) {
      int aggDocId = calculateAggregatedDocumentFromChildren(node._children);
      node._aggDataDocumentId = aggDocId;
    }

    return;
  }

  /**
   * Helper function to create aggregated document from children for a parent node.
   *
   * @return aggregated document id.
   */
  private Integer calculateAggregatedDocumentFromChildren(Map<Integer, TreeNode> children) {
    List<Record> chilAggRecordsList = new ArrayList<>();
    for (int key : children.keySet()) {
      TreeNode child = children.get(key);
      child._value = key;
      if ( child._value == StarTreeV2Constant.STAR_NODE) {
        return child._startDocId;
      } else {
        chilAggRecordsList.add(_starTreeData.get(child._aggDataDocumentId));
      }
    }
    List<Object> aggregatedValues = OnHeapStarTreeV2BuilderHelper.aggregateMetrics(0, chilAggRecordsList.size(), chilAggRecordsList, _met2aggfuncPairs);
    int aggDocId = appendAggregatedDocuments(aggregatedValues);

    return aggDocId;
  }


  /**
   * Create a aggregated document for this range.
   *
   * @param startDocId Start document id of the range to be grouped
   * @param endDocId End document id (exclusive) of the range to be grouped
   *
   * @return list of all metric2aggfunc value.
   */
  private List<Object> getAggregatedDocument(int startDocId, int endDocId) {

    List<Object> aggregatedValues =
        OnHeapStarTreeV2BuilderHelper.aggregateMetrics(startDocId, endDocId, _starTreeData, _met2aggfuncPairs);

    return aggregatedValues;
  }

  /**
   * Append a aggregated document to star tree data.
   *
   * @param aggregatedValues aggregated values for all met2aggfunc pairs.

   * @return aggregated document id.
   */
  private int appendAggregatedDocuments(List<Object> aggregatedValues) {
    int size = _starTreeData.size();

    Record aggRecord = new Record();
    int[] dimensionsValue = new int[_dimensionsCount];
    for (int i = 0; i < _dimensionsCount; i++) {
      dimensionsValue[i] = StarTreeV2Constant.STAR_NODE;
    }
    aggRecord.setDimensionValues(dimensionsValue);
    aggRecord.setMetricValues(aggregatedValues);
    _starTreeData.add(aggRecord);

    return size;
  }
}
