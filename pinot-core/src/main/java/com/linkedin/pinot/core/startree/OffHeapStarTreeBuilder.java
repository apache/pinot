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
package com.linkedin.pinot.core.startree;

import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.utils.Pairs.IntPair;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.segment.creator.ColumnIndexCreationInfo;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.startree.hll.HllUtil;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xerial.larray.buffer.LBuffer;
import xerial.larray.buffer.LBufferAPI;
import xerial.larray.mmap.MMapBuffer;
import xerial.larray.mmap.MMapMode;


/**
 * Uses file to build the star tree. Each row is divided into dimension and metrics. Time is added
 * to dimension list.
 * We use the split order to build the tree. In most cases, split order will be ranked depending on
 * the cardinality (descending order).
 * Time column will be excluded or last entry in split order irrespective of its cardinality
 * This is a recursive algorithm where we branch on one dimension at every level.
 * <b>Psuedo algo</b>
 * <code>
 *
 * build(){
 *  let table(1,N) consists of N input rows
 *  table.sort(1,N) //sort the table on all dimensions, according to split order
 *  constructTree(table, 0, N, 0);
 * }
 * constructTree(table,start,end, level){
 *    splitDimensionName = dimensionsSplitOrder[level]
 *    groupByResult<dimName, length> = table.groupBy(dimensionsSplitOrder[level]); //returns the number of rows for each value in splitDimension
 *    int rangeStart = 0;
 *    for each ( entry<dimName,length> groupByResult){
 *      if(entry.length > minThreshold){
 *        constructTree(table, rangeStart, rangeStart + entry.length, level +1);
 *      }
 *      rangeStart = rangeStart + entry.length;
 *      updateStarTree() //add new child
 *    }
 *
 *    //create a star tree node
 *
 *    aggregatedRows = table.uniqueAfterRemovingAttributeAndAggregateMetrics(start,end, splitDimensionName);
 *    for(each row in aggregatedRows_
 *    table.add(row);
 *    if(aggregateRows.size > minThreshold) {
 *      table.sort(end, end + aggregatedRows.size);
 *      constructStarTree(table, end, end + aggregatedRows.size, level +1);
 *    }
 * }
 * </code>
 */
public class OffHeapStarTreeBuilder implements StarTreeBuilder {
  private static final Logger LOGGER = LoggerFactory.getLogger(OffHeapStarTreeBuilder.class);
  private static final Charset UTF_8 = Charset.forName("UTF-8");

  // If the temporary buffer needed is larger than 500M, create a file and use MMapBuffer, otherwise use LBuffer
  private static final long MMAP_SIZE_THRESHOLD = 500_000_000;

  private File _tempDir;
  private File _dataFile;
  private DataOutputStream _dataOutputStream;

  private Schema _schema;
  private List<Integer> _dimensionsSplitOrder;
  private Set<Integer> _skipStarNodeCreationDimensions;
  private Set<Integer> _skipMaterializationDimensions;
  private int _skipMaterializationCardinalityThreshold;
  private int _maxNumLeafRecords;
  private boolean _excludeSkipMaterializationDimensionsForStarTreeIndex;

  private int _numRawDocs;
  private int _numAggregatedDocs;
  private TreeNode _rootNode;
  private int _numNodes;

  // Dimensions
  private int _numDimensions;
  private final List<String> _dimensionNames = new ArrayList<>();
  private final List<Object> _dimensionStarValues = new ArrayList<>();
  private final List<BiMap<Object, Integer>> _dimensionDictionaries = new ArrayList<>();
  private int _dimensionSize;
  // Metrics
  private int _numMetrics;
  private final List<String> _metricNames = new ArrayList<>();
  private int _metricSize;

  private long _docSize;
  private int[] _sortOrder;

  // Store data tables that need to be closed in close()
  private final List<StarTreeDataTable> _dataTablesToClose = new ArrayList<>();

  private static final boolean NEED_FLIP_ENDIANNESS = ByteOrder.nativeOrder() != ByteOrder.BIG_ENDIAN;

  /**
   * Flip the endianness of an int if needed.
   * <p>This is required to keep all the int as native order. (FileOutputStream always write int using BIG_ENDIAN)
   */
  private static int flipEndiannessIfNeeded(int value) {
    if (NEED_FLIP_ENDIANNESS) {
      return Integer.reverseBytes(value);
    } else {
      return value;
    }
  }

  /**
   * Helper class to represent a tree node.
   */
  private static class TreeNode {
    int _dimensionId = -1;
    int _dimensionValue = -1;
    int _childDimensionId = -1;
    Map<Integer, TreeNode> _children;
    int _startDocId;
    int _endDocId;
    int _aggregatedDocId;
  }

  @Override
  public void init(StarTreeBuilderConfig builderConfig) throws IOException {
    _tempDir = builderConfig.getOutDir();
    if (_tempDir == null) {
      _tempDir = new File(FileUtils.getTempDirectory(), V1Constants.STAR_TREE_INDEX_DIR + "_" + DateTime.now());
    }
    FileUtils.forceMkdir(_tempDir);
    LOGGER.info("Star tree temporary directory: {}", _tempDir);
    _dataFile = new File(_tempDir, "star-tree.buf");
    LOGGER.info("Star tree data file: {}", _dataFile);
    _dataOutputStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(_dataFile)));

    _schema = builderConfig.getSchema();
    _skipMaterializationCardinalityThreshold = builderConfig.getSkipMaterializationCardinalityThreshold();
    _maxNumLeafRecords = builderConfig.getMaxNumLeafRecords();
    _excludeSkipMaterializationDimensionsForStarTreeIndex =
        builderConfig.isExcludeSkipMaterializationDimensionsForStarTreeIndex();

    // Dimension fields
    for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
      // Count all fields that are not metrics as dimensions
      if (fieldSpec.getFieldType() != FieldSpec.FieldType.METRIC) {
        String dimensionName = fieldSpec.getName();
        _numDimensions++;
        _dimensionNames.add(dimensionName);
        _dimensionStarValues.add(fieldSpec.getDefaultNullValue());
        _dimensionDictionaries.add(HashBiMap.<Object, Integer>create());
      }
    }
    _dimensionSize = _numDimensions * V1Constants.Numbers.INTEGER_SIZE;

    // Convert string based config to index based config
    List<String> dimensionsSplitOrder = builderConfig.getDimensionsSplitOrder();
    if (dimensionsSplitOrder != null) {
      _dimensionsSplitOrder = new ArrayList<>(dimensionsSplitOrder.size());
      for (String dimensionName : dimensionsSplitOrder) {
        _dimensionsSplitOrder.add(_dimensionNames.indexOf(dimensionName));
      }
    }
    Set<String> skipStarNodeCreationDimensions = builderConfig.getSkipStarNodeCreationDimensions();
    if (skipStarNodeCreationDimensions != null) {
      _skipStarNodeCreationDimensions = getDimensionIdSet(skipStarNodeCreationDimensions);
    }
    Set<String> skipMaterializationDimensions = builderConfig.getSkipMaterializationDimensions();
    if (skipMaterializationDimensions != null) {
      _skipMaterializationDimensions = getDimensionIdSet(skipMaterializationDimensions);
    }

    // Metric fields
    // NOTE: the order of _metricNames should be the same as _schema.getMetricFieldSpecs()
    for (MetricFieldSpec metricFieldSpec : _schema.getMetricFieldSpecs()) {
      _numMetrics++;
      _metricNames.add(metricFieldSpec.getName());
      _metricSize += metricFieldSpec.getFieldSize();
    }

    LOGGER.info("Dimension Names: {}", _dimensionNames);
    LOGGER.info("Metric Names: {}", _metricNames);

    _docSize = _dimensionSize + _metricSize;

    // Initialize the root node
    _rootNode = new TreeNode();
    _numNodes++;
  }

  private Set<Integer> getDimensionIdSet(Set<String> dimensionNameSet) {
    Set<Integer> dimensionIdSet = new HashSet<>(dimensionNameSet.size());
    for (int i = 0; i < _numDimensions; i++) {
      if (dimensionNameSet.contains(_dimensionNames.get(i))) {
        dimensionIdSet.add(i);
      }
    }
    return dimensionIdSet;
  }

  @Override
  public void append(GenericRow row) throws IOException {
    // Dimensions
    DimensionBuffer dimensions = new DimensionBuffer(_numDimensions);
    for (int i = 0; i < _numDimensions; i++) {
      String dimensionName = _dimensionNames.get(i);
      Object dimensionValue = row.getValue(dimensionName);
      BiMap<Object, Integer> dimensionDictionary = _dimensionDictionaries.get(i);
      Integer dictId = dimensionDictionary.get(dimensionValue);
      if (dictId == null) {
        dictId = dimensionDictionary.size();
        dimensionDictionary.put(dimensionValue, dictId);
      }
      dimensions.setDimension(i, dictId);
    }

    // Metrics
    Object[] metricValues = new Object[_numMetrics];
    List<MetricFieldSpec> metricFieldSpecs = _schema.getMetricFieldSpecs();
    for (int i = 0; i < _numMetrics; i++) {
      String metricName = _metricNames.get(i);
      Object metricValue = row.getValue(metricName);
      if (metricFieldSpecs.get(i).getDerivedMetricType() == MetricFieldSpec.DerivedMetricType.HLL) {
        // Convert HLL field from string format to HyperLogLog
        metricValues[i] = HllUtil.convertStringToHll((String) metricValue);
      } else {
        // No conversion for standard data types
        metricValues[i] = metricValue;
      }
    }
    MetricBuffer metrics = new MetricBuffer(metricValues, metricFieldSpecs);

    appendToRawBuffer(dimensions, metrics);
  }

  private void appendToRawBuffer(DimensionBuffer dimensions, MetricBuffer metrics) throws IOException {
    appendToBuffer(dimensions, metrics);
    _numRawDocs++;
  }

  private void appendToAggBuffer(DimensionBuffer dimensions, MetricBuffer metrics) throws IOException {
    appendToBuffer(dimensions, metrics);
    _numAggregatedDocs++;
  }

  private void appendToBuffer(DimensionBuffer dimensions, MetricBuffer metricHolder) throws IOException {
    for (int i = 0; i < _numDimensions; i++) {
      _dataOutputStream.writeInt(flipEndiannessIfNeeded(dimensions.getDimension(i)));
    }
    _dataOutputStream.write(metricHolder.toBytes(_metricSize));
  }

  @Override
  public void build() throws IOException {
    // From this point, all raw documents have been appended
    _dataOutputStream.flush();

    if (_skipMaterializationDimensions == null) {
      _skipMaterializationDimensions = computeDefaultDimensionsToSkipMaterialization();
    }

    // For default split order, give preference to skipMaterializationForDimensions.
    // For user-defined split order, give preference to split-order.
    if (_dimensionsSplitOrder == null || _dimensionsSplitOrder.isEmpty()) {
      _dimensionsSplitOrder = computeDefaultSplitOrder();
    } else {
      _skipMaterializationDimensions.removeAll(_dimensionsSplitOrder);
    }

    LOGGER.info("Split Order: {}", _dimensionsSplitOrder);
    LOGGER.info("Skip Materialization Dimensions: {}", _skipMaterializationDimensions);

    // Compute the sort order
    _sortOrder = new int[_dimensionNames.size()];
    // Add dimensions in the split order first
    int index = 0;
    for (int dimensionId : _dimensionsSplitOrder) {
      _sortOrder[index++] = dimensionId;
    }
    // Add dimensions that are not part of dimensionsSplitOrder or skipMaterializationForDimensions
    for (int i = 0; i < _numDimensions; i++) {
      if (!_dimensionsSplitOrder.contains(i) && !_skipMaterializationDimensions.contains(i)) {
        _sortOrder[index++] = i;
      }
    }
    // Add dimensions in the skipMaterializationForDimensions last
    // The reason for this is that, after sorting and replacing the value for dimensions not materialized to ALL, the
    // docs with same dimensions will be grouped together for aggregation
    for (int dimensionId : _skipMaterializationDimensions) {
      _sortOrder[index++] = dimensionId;
    }

    long start = System.currentTimeMillis();
    if (!_skipMaterializationDimensions.isEmpty() && _excludeSkipMaterializationDimensionsForStarTreeIndex) {
      // Remove the skip materialization dimensions
      removeSkipMaterializationDimensions();
      // Recursively construct the star tree
      constructStarTree(_rootNode, _numRawDocs, _numRawDocs + _numAggregatedDocs, 0);
    } else {
      // Sort the documents
      try (StarTreeDataTable dataTable = new StarTreeDataTable(new MMapBuffer(_dataFile, MMapMode.READ_WRITE),
          _dimensionSize, _metricSize, 0, _numRawDocs)) {
        dataTable.sort(0, _numRawDocs, _sortOrder);
        dataTable.flush();
      }
      // Recursively construct the star tree
      constructStarTree(_rootNode, 0, _numRawDocs, 0);
    }

    splitLeafNodesOnTimeColumn();

    // Create aggregate rows for all nodes in the tree
    createAggregatedDocForAllNodes();

    long end = System.currentTimeMillis();
    LOGGER.info("Took {}ms to build star tree index with {} raw documents and {} aggregated documents", (end - start),
        _numRawDocs, _numAggregatedDocs);
  }

  private void removeSkipMaterializationDimensions() throws IOException {
    try (StarTreeDataTable dataTable = new StarTreeDataTable(new MMapBuffer(_dataFile, MMapMode.READ_WRITE),
        _dimensionSize, _metricSize, 0, _numRawDocs)) {
      dataTable.sort(0, _numRawDocs, _sortOrder);
      dataTable.flush();
      Iterator<Pair<byte[], byte[]>> iterator = dataTable.iterator(0, _numRawDocs);
      DimensionBuffer currentDimensions = null;
      MetricBuffer currentMetrics = null;
      while (iterator.hasNext()) {
        Pair<byte[], byte[]> next = iterator.next();
        byte[] dimensionBytes = next.getLeft();
        byte[] metricBytes = next.getRight();
        DimensionBuffer dimensions = DimensionBuffer.fromBytes(dimensionBytes);
        MetricBuffer metrics = MetricBuffer.fromBytes(metricBytes, _schema.getMetricFieldSpecs());
        for (int i = 0; i < _numDimensions; i++) {
          if (_skipMaterializationDimensions.contains(i)) {
            dimensions.setDimension(i, StarTreeNode.ALL);
          }
        }

        if (currentDimensions == null) {
          currentDimensions = dimensions;
          currentMetrics = metrics;
        } else {
          if (dimensions.equals(currentDimensions)) {
            currentMetrics.aggregate(metrics);
          } else {
            appendToAggBuffer(currentDimensions, currentMetrics);
            currentDimensions = dimensions;
            currentMetrics = metrics;
          }
        }
      }
      appendToAggBuffer(currentDimensions, currentMetrics);
    }
    _dataOutputStream.flush();
  }

  private void createAggregatedDocForAllNodes() throws IOException {
    try (StarTreeDataTable dataTable = new StarTreeDataTable(new MMapBuffer(_dataFile, MMapMode.READ_ONLY),
        _dimensionSize, _metricSize, 0, _numRawDocs + _numAggregatedDocs)) {
      DimensionBuffer dimensions = new DimensionBuffer(_numDimensions);
      for (int i = 0; i < _numDimensions; i++) {
        dimensions.setDimension(i, StarTreeNode.ALL);
      }
      createAggregatedDocForAllNodesHelper(dataTable, _rootNode, dimensions);
    }
    _dataOutputStream.flush();
  }

  private MetricBuffer createAggregatedDocForAllNodesHelper(StarTreeDataTable dataTable, TreeNode node,
      DimensionBuffer dimensions) throws IOException {
    MetricBuffer aggregatedMetrics = null;
    if (node._children == null) {
      // Leaf node

      Iterator<Pair<byte[], byte[]>> iterator = dataTable.iterator(node._startDocId, node._endDocId);
      Pair<byte[], byte[]> first = iterator.next();
      aggregatedMetrics = MetricBuffer.fromBytes(first.getRight(), _schema.getMetricFieldSpecs());
      while (iterator.hasNext()) {
        Pair<byte[], byte[]> next = iterator.next();
        MetricBuffer metricBuffer = MetricBuffer.fromBytes(next.getRight(), _schema.getMetricFieldSpecs());
        aggregatedMetrics.aggregate(metricBuffer);
      }
    } else {
      // Non-leaf node

      int childDimensionId = node._childDimensionId;
      for (Map.Entry<Integer, TreeNode> entry : node._children.entrySet()) {
        int childDimensionValue = entry.getKey();
        TreeNode child = entry.getValue();
        dimensions.setDimension(childDimensionId, childDimensionValue);
        MetricBuffer childAggregatedMetrics = createAggregatedDocForAllNodesHelper(dataTable, child, dimensions);
        // Skip star node value when computing aggregate for the parent
        if (childDimensionValue != StarTreeNode.ALL) {
          if (aggregatedMetrics == null) {
            aggregatedMetrics = childAggregatedMetrics;
          } else {
            aggregatedMetrics.aggregate(childAggregatedMetrics);
          }
        }
      }
      dimensions.setDimension(childDimensionId, StarTreeNode.ALL);
    }
    node._aggregatedDocId = _numRawDocs + _numAggregatedDocs;
    appendToAggBuffer(dimensions, aggregatedMetrics);
    return aggregatedMetrics;
  }

  /**
   * Split the leaf nodes on time column if we have not split on time-column name yet, and time column is still
   * preserved (i.e. not replaced by StarTreeNode.all()).
   * <p>The method visits each leaf node does the following:
   * <ul>
   *   <li>Re-order the documents under the leaf node based on time column</li>
   *   <li>Create children nodes for each time value under this leaf node</li>
   * </ul>
   */
  private void splitLeafNodesOnTimeColumn() throws IOException {
    String timeColumnName = _schema.getTimeColumnName();
    if (timeColumnName != null) {
      int timeColumnId = _dimensionNames.indexOf(timeColumnName);
      if (!_skipMaterializationDimensions.contains(timeColumnId) && !_dimensionsSplitOrder.contains(timeColumnId)) {
        try (StarTreeDataTable dataTable = new StarTreeDataTable(new MMapBuffer(_dataFile, MMapMode.READ_WRITE),
            _dimensionSize, _metricSize, 0, _numRawDocs + _numAggregatedDocs)) {
          splitLeafNodesOnTimeColumnHelper(dataTable, _rootNode, 0, timeColumnId);
          dataTable.flush();
        }
      }
    }
  }

  private void splitLeafNodesOnTimeColumnHelper(StarTreeDataTable dataTable, TreeNode node, int level,
      int timeColumnId) {
    if (node._children == null) {
      // Leaf node

      int startDocId = node._startDocId;
      int endDocId = node._endDocId;
      dataTable.sort(startDocId, endDocId, getNewSortOrder(timeColumnId, level));
      Int2ObjectMap<IntPair> timeColumnRangeMap = dataTable.groupOnDimension(startDocId, endDocId, timeColumnId);
      node._childDimensionId = timeColumnId;
      Map<Integer, TreeNode> children = new HashMap<>(timeColumnRangeMap.size());
      node._children = children;
      for (Int2ObjectMap.Entry<IntPair> entry : timeColumnRangeMap.int2ObjectEntrySet()) {
        int timeValue = entry.getIntKey();
        IntPair range = entry.getValue();
        TreeNode child = new TreeNode();
        _numNodes++;
        children.put(timeValue, child);
        child._dimensionId = timeColumnId;
        child._dimensionValue = timeValue;
        child._startDocId = range.getLeft();
        child._endDocId = range.getRight();
      }
    } else {
      // Non-leaf node

      for (TreeNode child : node._children.values()) {
        splitLeafNodesOnTimeColumnHelper(dataTable, child, level + 1, timeColumnId);
      }
    }
  }

  /**
   * Move the value in the sort order to the new index, keep the order of other values the same.
   */
  private int[] getNewSortOrder(int value, int newIndex) {
    int length = _sortOrder.length;
    int[] newSortOrder = new int[length];
    int sortOrderIndex = 0;
    for (int i = 0; i < length; i++) {
      if (i == newIndex) {
        newSortOrder[i] = value;
      } else {
        if (_sortOrder[sortOrderIndex] == value) {
          sortOrderIndex++;
        }
        newSortOrder[i] = _sortOrder[sortOrderIndex++];
      }
    }
    return newSortOrder;
  }

  private Set<Integer> computeDefaultDimensionsToSkipMaterialization() {
    Set<Integer> skipDimensions = new HashSet<>();
    for (int i = 0; i < _numDimensions; i++) {
      if (_dimensionDictionaries.get(i).size() > _skipMaterializationCardinalityThreshold) {
        skipDimensions.add(i);
      }
    }
    return skipDimensions;
  }

  private List<Integer> computeDefaultSplitOrder() {
    List<Integer> defaultSplitOrder = new ArrayList<>();

    // Sort on all non-time dimensions that are not skipped in descending order
    Set<String> timeDimensions = new HashSet<>(_schema.getDateTimeNames());
    String timeColumnName = _schema.getTimeColumnName();
    if (timeColumnName != null) {
      timeDimensions.add(timeColumnName);
    }
    for (int i = 0; i < _numDimensions; i++) {
      if (!_skipMaterializationDimensions.contains(i) && !timeDimensions.contains(_dimensionNames.get(i))) {
        defaultSplitOrder.add(i);
      }
    }
    Collections.sort(defaultSplitOrder, new Comparator<Integer>() {
      @Override
      public int compare(Integer o1, Integer o2) {
        // Descending order
        return _dimensionDictionaries.get(o2).size() - _dimensionDictionaries.get(o1).size();
      }
    });

    return defaultSplitOrder;
  }

  private void constructStarTree(TreeNode node, int startDocId, int endDocId, int level) throws IOException {
    if (level == _dimensionsSplitOrder.size()) {
      return;
    }

    int splitDimensionId = _dimensionsSplitOrder.get(level);
    LOGGER.debug("Building tree at level: {} from startDoc: {} endDocId: {} splitting on dimension id: {}", level,
        startDocId, endDocId, splitDimensionId);

    int numDocs = endDocId - startDocId;
    Int2ObjectMap<IntPair> dimensionRangeMap;
    try (StarTreeDataTable dataTable = new StarTreeDataTable(
        new MMapBuffer(_dataFile, startDocId * _docSize, numDocs * _docSize, MMapMode.READ_ONLY), _dimensionSize,
        _metricSize, startDocId, endDocId)) {
      dimensionRangeMap = dataTable.groupOnDimension(startDocId, endDocId, splitDimensionId);
    }
    LOGGER.debug("Group stats:{}", dimensionRangeMap);

    node._childDimensionId = splitDimensionId;
    // Reserve one space for star node
    Map<Integer, TreeNode> children = new HashMap<>(dimensionRangeMap.size() + 1);
    node._children = children;
    for (Int2ObjectMap.Entry<IntPair> entry : dimensionRangeMap.int2ObjectEntrySet()) {
      int childDimensionValue = entry.getIntKey();
      TreeNode child = new TreeNode();
      _numNodes++;
      children.put(childDimensionValue, child);

      // The range pair value is the relative value to the start document id
      IntPair range = dimensionRangeMap.get(childDimensionValue);
      int childStartDocId = range.getLeft();
      child._startDocId = childStartDocId;
      int childEndDocId = range.getRight();
      child._endDocId = childEndDocId;

      if (childEndDocId - childStartDocId > _maxNumLeafRecords) {
        constructStarTree(child, childStartDocId, childEndDocId, level + 1);
      }
    }

    // Directly return if we don't need to create star-node
    if (_skipStarNodeCreationDimensions != null && _skipStarNodeCreationDimensions.contains(splitDimensionId)) {
      return;
    }

    // Create star node
    TreeNode starChild = new TreeNode();
    _numNodes++;
    children.put(StarTreeNode.ALL, starChild);
    starChild._dimensionId = splitDimensionId;
    starChild._dimensionValue = StarTreeNode.ALL;

    Iterator<Pair<DimensionBuffer, MetricBuffer>> iterator =
        getUniqueCombinations(startDocId, endDocId, splitDimensionId);
    int starChildStartDocId = _numRawDocs + _numAggregatedDocs;
    while (iterator.hasNext()) {
      Pair<DimensionBuffer, MetricBuffer> next = iterator.next();
      DimensionBuffer dimensions = next.getLeft();
      MetricBuffer metrics = next.getRight();
      appendToAggBuffer(dimensions, metrics);
    }
    _dataOutputStream.flush();
    int starChildEndDocId = _numRawDocs + _numAggregatedDocs;

    starChild._startDocId = starChildStartDocId;
    starChild._endDocId = starChildEndDocId;
    if (starChildEndDocId - starChildStartDocId > _maxNumLeafRecords) {
      constructStarTree(starChild, starChildStartDocId, starChildEndDocId, level + 1);
    }
  }

  /**
   * Get the unique combinations after removing a specified dimension.
   * <p>Here we assume the data file is already sorted.
   * <p>Aggregates the metrics for each unique combination.
   */
  private Iterator<Pair<DimensionBuffer, MetricBuffer>> getUniqueCombinations(final int startDocId, final int endDocId,
      int dimensionIdToRemove) throws IOException {
    LBufferAPI tempBuffer = null;
    int numDocs = endDocId - startDocId;
    long tempBufferSize = numDocs * _docSize;
    if (tempBufferSize > MMAP_SIZE_THRESHOLD) {
      // Create a temporary file and use MMapBuffer
      File tempFile = new File(_tempDir, startDocId + "_" + endDocId + ".unique.tmp");
      try (FileChannel src = new FileInputStream(_dataFile).getChannel();
          FileChannel dest = new FileOutputStream(tempFile).getChannel()) {
        dest.transferFrom(src, startDocId * _docSize, tempBufferSize);
      }
      tempBuffer = new MMapBuffer(tempFile, MMapMode.READ_WRITE);
    } else {
      // Use LBuffer (direct memory buffer)
      MMapBuffer dataBuffer = null;
      try {
        tempBuffer = new LBuffer(tempBufferSize);
        dataBuffer = new MMapBuffer(_dataFile, startDocId * _docSize, tempBufferSize, MMapMode.READ_ONLY);
        dataBuffer.copyTo(0, tempBuffer, 0, tempBufferSize);
      } catch (Exception e) {
        if (tempBuffer != null) {
          tempBuffer.release();
        }
        throw e;
      } finally {
        if (dataBuffer != null) {
          dataBuffer.close();
        }
      }
    }

    final StarTreeDataTable dataTable =
        new StarTreeDataTable(tempBuffer, _dimensionSize, _metricSize, startDocId, endDocId);
    _dataTablesToClose.add(dataTable);

    // Need to set skip materialization dimensions value to ALL before sorting
    if (!_skipMaterializationDimensions.isEmpty() && !_excludeSkipMaterializationDimensionsForStarTreeIndex) {
      for (int dimensionIdToSkip : _skipMaterializationDimensions) {
        dataTable.setDimensionValue(dimensionIdToSkip, StarTreeNode.ALL);
      }
    }
    dataTable.setDimensionValue(dimensionIdToRemove, StarTreeNode.ALL);
    dataTable.sort(startDocId, endDocId, _sortOrder);
    dataTable.flush();

    return new Iterator<Pair<DimensionBuffer, MetricBuffer>>() {
      private final Iterator<Pair<byte[], byte[]>> _iterator = dataTable.iterator(startDocId, endDocId);
      private DimensionBuffer _currentDimensions;
      private MetricBuffer _currentMetrics;
      boolean _hasNext = true;

      @Override
      public boolean hasNext() {
        return _hasNext;
      }

      @Override
      public Pair<DimensionBuffer, MetricBuffer> next() {
        while (_iterator.hasNext()) {
          Pair<byte[], byte[]> next = _iterator.next();
          DimensionBuffer dimensions = DimensionBuffer.fromBytes(next.getLeft());
          MetricBuffer metrics = MetricBuffer.fromBytes(next.getRight(), _schema.getMetricFieldSpecs());
          if (_currentDimensions == null) {
            _currentDimensions = dimensions;
            _currentMetrics = metrics;
          } else {
            if (dimensions.equals(_currentDimensions)) {
              _currentMetrics.aggregate(metrics);
            } else {
              ImmutablePair<DimensionBuffer, MetricBuffer> ret =
                  new ImmutablePair<>(_currentDimensions, _currentMetrics);
              _currentDimensions = dimensions;
              _currentMetrics = metrics;
              return ret;
            }
          }
        }
        _hasNext = false;
        closeDataTable(dataTable);
        return new ImmutablePair<>(_currentDimensions, _currentMetrics);
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Override
  public Iterator<GenericRow> iterator(final int startDocId, final int endDocId) throws IOException {
    int numDocs = endDocId - startDocId;
    final StarTreeDataTable dataTable =
        new StarTreeDataTable(new MMapBuffer(_dataFile, startDocId * _docSize, numDocs * _docSize, MMapMode.READ_ONLY),
            _dimensionSize, _metricSize, startDocId, endDocId);
    _dataTablesToClose.add(dataTable);

    return new Iterator<GenericRow>() {
      private final Iterator<Pair<byte[], byte[]>> _iterator = dataTable.iterator(startDocId, endDocId);

      @Override
      public boolean hasNext() {
        boolean hasNext = _iterator.hasNext();
        if (!hasNext) {
          closeDataTable(dataTable);
        }
        return hasNext;
      }

      @Override
      public GenericRow next() {
        Pair<byte[], byte[]> pair = _iterator.next();
        DimensionBuffer dimensions = DimensionBuffer.fromBytes(pair.getLeft());
        MetricBuffer metrics = MetricBuffer.fromBytes(pair.getRight(), _schema.getMetricFieldSpecs());
        return toGenericRow(dimensions, metrics);
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  private void closeDataTable(StarTreeDataTable dataTable) {
    try {
      dataTable.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    _dataTablesToClose.remove(dataTable);
  }

  private GenericRow toGenericRow(DimensionBuffer dimensions, MetricBuffer metrics) {
    GenericRow row = new GenericRow();
    Map<String, Object> map = new HashMap<>();
    for (int i = 0; i < _numDimensions; i++) {
      String dimensionName = _dimensionNames.get(i);
      int dictId = dimensions.getDimension(i);
      if (dictId == StarTreeNode.ALL) {
        map.put(dimensionName, _dimensionStarValues.get(i));
      } else {
        map.put(dimensionName, _dimensionDictionaries.get(i).inverse().get(dictId));
      }
    }
    for (int i = 0; i < _numMetrics; i++) {
      map.put(_metricNames.get(i), metrics.getValueConformToDataType(i));
    }
    row.init(map);
    return row;
  }

  @Override
  public void serializeTree(File starTreeFile, Map<String, ColumnIndexCreationInfo> indexCreationInfoMap)
      throws IOException {
    // Update the star tree with the segment dictionary
    updateTree(_rootNode, indexCreationInfoMap);

    // Serialize the star tree into a file
    serializeTree(starTreeFile);

    LOGGER.info("Finish serializing star tree into file: {}", starTreeFile);
  }

  private void updateTree(TreeNode node, Map<String, ColumnIndexCreationInfo> indexCreationInfoMap) {
    // Only need to update children map because the caller already updates the node
    Map<Integer, TreeNode> children = node._children;
    if (children != null) {
      Map<Integer, TreeNode> newChildren = new HashMap<>(children.size());
      node._children = newChildren;
      int childDimensionId = node._childDimensionId;
      BiMap<Integer, Object> dimensionDictionary = _dimensionDictionaries.get(childDimensionId).inverse();
      String childDimensionName = _dimensionNames.get(childDimensionId);
      Object segmentDictionary = indexCreationInfoMap.get(childDimensionName).getSortedUniqueElementsArray();
      for (Map.Entry<Integer, TreeNode> entry : children.entrySet()) {
        int childDimensionValue = entry.getKey();
        TreeNode childNode = entry.getValue();
        int dictId = StarTreeNode.ALL;
        // Only need to update the value for non-star node
        if (childDimensionValue != StarTreeNode.ALL) {
          dictId = indexOf(segmentDictionary, dimensionDictionary.get(childDimensionValue));
          childNode._dimensionValue = dictId;
        }
        newChildren.put(dictId, childNode);
        updateTree(childNode, indexCreationInfoMap);
      }
    }
  }

  /**
   * Helper method to binary-search the index of a given value in an array.
   */
  private static int indexOf(Object array, Object value) {
    if (array instanceof int[]) {
      return Arrays.binarySearch((int[]) array, (Integer) value);
    } else if (array instanceof long[]) {
      return Arrays.binarySearch((long[]) array, (Long) value);
    } else if (array instanceof float[]) {
      return Arrays.binarySearch((float[]) array, (Float) value);
    } else if (array instanceof double[]) {
      return Arrays.binarySearch((double[]) array, (Double) value);
    } else if (array instanceof String[]) {
      return Arrays.binarySearch((String[]) array, value);
    } else {
      throw new IllegalStateException();
    }
  }

  /**
   * Helper method to serialize the updated tree into a file.
   */
  private void serializeTree(File starTreeFile) throws IOException {
    int headerSizeInBytes = computeHeaderSizeInBytes();
    long totalSizeInBytes = headerSizeInBytes + _numNodes * OffHeapStarTreeNode.SERIALIZABLE_SIZE_IN_BYTES;

    MMapBuffer dataBuffer = new MMapBuffer(starTreeFile, 0, totalSizeInBytes, MMapMode.READ_WRITE);
    try {
      long offset = writeHeader(dataBuffer, headerSizeInBytes);
      Preconditions.checkState(offset == headerSizeInBytes, "Error writing Star Tree file, header size mis-match");

      writeNodes(dataBuffer, offset);
    } finally {
      dataBuffer.flush();
      dataBuffer.close();
    }
  }

  /**
   * Helper method to compute size of the header of the star tree in bytes.
   * <p>The header contains the following fields:
   * <ul>
   *   <li>Magic marker (long)</li>
   *   <li>Size of the header (int)</li>
   *   <li>Version (int)</li>
   *   <li>Number of dimensions (int)</li>
   *   <li>For each dimension, index of the dimension (int), number of bytes in the dimension string (int), and the byte
   *   array for the string</li>
   *   <li>Number of nodes in the tree (int)</li>
   * </ul>
   */
  private int computeHeaderSizeInBytes() {
    // Magic marker (8), version (4), size of header (4) and number of dimensions (4)
    int headerSizeInBytes = 20;

    for (String dimension : _dimensionNames) {
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
  private long writeHeader(MMapBuffer dataBuffer, int headerSizeInBytes) {
    long offset = 0L;

    dataBuffer.putLong(offset, OffHeapStarTree.MAGIC_MARKER);
    offset += V1Constants.Numbers.LONG_SIZE;

    dataBuffer.putInt(offset, OffHeapStarTree.VERSION);
    offset += V1Constants.Numbers.INTEGER_SIZE;

    dataBuffer.putInt(offset, headerSizeInBytes);
    offset += V1Constants.Numbers.INTEGER_SIZE;

    dataBuffer.putInt(offset, _numDimensions);
    offset += V1Constants.Numbers.INTEGER_SIZE;

    for (int i = 0; i < _numDimensions; i++) {
      String dimensionName = _dimensionNames.get(i);

      dataBuffer.putInt(offset, i);
      offset += V1Constants.Numbers.INTEGER_SIZE;

      byte[] dimensionBytes = dimensionName.getBytes(UTF_8);
      int dimensionLength = dimensionBytes.length;
      dataBuffer.putInt(offset, dimensionLength);
      offset += V1Constants.Numbers.INTEGER_SIZE;

      dataBuffer.readFrom(dimensionBytes, offset);
      offset += dimensionLength;
    }

    dataBuffer.putInt(offset, _numNodes);
    offset += V1Constants.Numbers.INTEGER_SIZE;

    return offset;
  }

  /**
   * Helper method to write the star tree nodes into the data buffer.
   */
  private void writeNodes(MMapBuffer dataBuffer, long offset) {
    int index = 0;
    Queue<TreeNode> queue = new LinkedList<>();
    queue.add(_rootNode);

    while (!queue.isEmpty()) {
      TreeNode node = queue.remove();

      if (node._children == null) {
        // Leaf node

        offset =
            writeNode(dataBuffer, node, offset, OffHeapStarTreeNode.INVALID_INDEX, OffHeapStarTreeNode.INVALID_INDEX);
      } else {
        // Non-leaf node

        // Get a list of children nodes sorted on the dimension value
        List<TreeNode> sortedChildren = new ArrayList<>(node._children.values());
        Collections.sort(sortedChildren, new Comparator<TreeNode>() {
          @Override
          public int compare(TreeNode o1, TreeNode o2) {
            return Integer.compare(o1._dimensionValue, o2._dimensionValue);
          }
        });

        int startChildrenIndex = index + queue.size() + 1;
        int endChildrenIndex = startChildrenIndex + sortedChildren.size() - 1;
        offset = writeNode(dataBuffer, node, offset, startChildrenIndex, endChildrenIndex);

        queue.addAll(sortedChildren);
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

    dataBuffer.putInt(offset, node._aggregatedDocId);
    offset += V1Constants.Numbers.INTEGER_SIZE;

    dataBuffer.putInt(offset, startChildrenIndex);
    offset += V1Constants.Numbers.INTEGER_SIZE;

    dataBuffer.putInt(offset, endChildrenIndex);
    offset += V1Constants.Numbers.INTEGER_SIZE;

    return offset;
  }

  @Override
  public int getTotalRawDocumentCount() {
    return _numRawDocs;
  }

  @Override
  public int getTotalAggregateDocumentCount() {
    return _numAggregatedDocs;
  }

  @Override
  public List<String> getDimensionsSplitOrder() {
    List<String> dimensionsSplitOrder = new ArrayList<>(_dimensionsSplitOrder.size());
    for (int dimensionId : _dimensionsSplitOrder) {
      dimensionsSplitOrder.add(_dimensionNames.get(dimensionId));
    }
    return dimensionsSplitOrder;
  }

  @Override
  public Set<String> getSkipMaterializationDimensions() {
    Set<String> skipMaterializationDimensions = new HashSet<>(_skipMaterializationDimensions.size());
    for (int dimensionId : _skipMaterializationDimensions) {
      skipMaterializationDimensions.add(_dimensionNames.get(dimensionId));
    }
    return skipMaterializationDimensions;
  }

  @Override
  public List<String> getDimensionNames() {
    return _dimensionNames;
  }

  @Override
  public List<BiMap<Object, Integer>> getDimensionDictionaries() {
    return _dimensionDictionaries;
  }

  @Override
  public void close() throws IOException {
    _dataOutputStream.close();
    for (StarTreeDataTable dataTable : _dataTablesToClose) {
      dataTable.close();
    }
    _dataTablesToClose.clear();
    FileUtils.deleteDirectory(_tempDir);
  }
}
