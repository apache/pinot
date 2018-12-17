/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.utils.Pairs.IntPair;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.segment.creator.ColumnIndexCreationInfo;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import com.linkedin.pinot.core.startree.StarTreeBuilderUtils.TreeNode;
import com.linkedin.pinot.core.startree.hll.HllUtil;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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

  // If the temporary buffer needed is larger than 500M, use MMAP, otherwise use DIRECT
  private static final long MMAP_SIZE_THRESHOLD = 500_000_000;

  private File _tempDir;
  private File _dataFile;
  private BufferedOutputStream _outputStream;

  private Schema _schema;
  private List<MetricFieldSpec> _metricFieldSpecs;
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

  private long _docSizeLong;
  private int[] _sortOrder;

  // Store data tables that need to be closed in close()
  private final Set<StarTreeDataTable> _dataTablesToClose = new HashSet<>();

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
    _outputStream = new BufferedOutputStream(new FileOutputStream(_dataFile));

    _schema = builderConfig.getSchema();
    _metricFieldSpecs = _schema.getMetricFieldSpecs();
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
        _dimensionDictionaries.add(HashBiMap.create());
      }
    }
    _dimensionSize = _numDimensions * Integer.BYTES;

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
    // NOTE: the order of _metricNames should be the same as _metricFieldSpecs
    for (MetricFieldSpec metricFieldSpec : _metricFieldSpecs) {
      _numMetrics++;
      _metricNames.add(metricFieldSpec.getName());
      _metricSize += metricFieldSpec.getFieldSize();
    }

    LOGGER.info("Dimension Names: {}", _dimensionNames);
    LOGGER.info("Metric Names: {}", _metricNames);

    _docSizeLong = _dimensionSize + _metricSize;

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
      dimensions.setDictId(i, dictId);
    }

    // Metrics
    Object[] metricValues = new Object[_numMetrics];
    for (int i = 0; i < _numMetrics; i++) {
      String metricName = _metricNames.get(i);
      Object metricValue = row.getValue(metricName);
      if (_metricFieldSpecs.get(i).getDerivedMetricType() == MetricFieldSpec.DerivedMetricType.HLL) {
        // Convert HLL field from string format to HyperLogLog
        metricValues[i] = HllUtil.convertStringToHll((String) metricValue);
      } else {
        // No conversion for standard data types
        metricValues[i] = metricValue;
      }
    }
    MetricBuffer metrics = new MetricBuffer(metricValues, _metricFieldSpecs);

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

  private void appendToBuffer(DimensionBuffer dimensions, MetricBuffer metrics) throws IOException {
    _outputStream.write(dimensions.toBytes(), 0, _dimensionSize);
    _outputStream.write(metrics.toBytes(_metricSize), 0, _metricSize);
  }

  @Override
  public void build() throws IOException {
    // From this point, all raw documents have been appended
    _outputStream.flush();

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
    // Add dimensions in the split order first
    List<Integer> sortOrderList = new ArrayList<>(_dimensionsSplitOrder);
    // Add dimensions that are not part of dimensionsSplitOrder or skipMaterializationForDimensions
    for (int i = 0; i < _numDimensions; i++) {
      if (!_dimensionsSplitOrder.contains(i) && !_skipMaterializationDimensions.contains(i)) {
        sortOrderList.add(i);
      }
    }
    int numDimensionsInSortOrder = sortOrderList.size();
    _sortOrder = new int[numDimensionsInSortOrder];
    for (int i = 0; i < numDimensionsInSortOrder; i++) {
      _sortOrder[i] = sortOrderList.get(i);
    }

    long start = System.currentTimeMillis();
    if (!_skipMaterializationDimensions.isEmpty() && _excludeSkipMaterializationDimensionsForStarTreeIndex) {
      // Remove the skip materialization dimensions
      removeSkipMaterializationDimensions();
      // Recursively construct the star tree
      constructStarTree(_rootNode, _numRawDocs, _numRawDocs + _numAggregatedDocs, 0);
    } else {
      // Sort the documents
      try (StarTreeDataTable dataTable = new StarTreeDataTable(
          PinotDataBuffer.mapFile(_dataFile, false, 0, _dataFile.length(), PinotDataBuffer.NATIVE_ORDER,
              "OffHeapStarTreeBuilder#build: data buffer"), _dimensionSize, _metricSize, 0)) {
        dataTable.sort(0, _numRawDocs, _sortOrder);
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
    try (StarTreeDataTable dataTable = new StarTreeDataTable(
        PinotDataBuffer.mapFile(_dataFile, false, 0, _dataFile.length(), PinotDataBuffer.NATIVE_ORDER,
            "OffHeapStarTreeBuilder#removeSkipMaterializationDimensions: data buffer"), _dimensionSize, _metricSize,
        0)) {
      dataTable.sort(0, _numRawDocs, _sortOrder);
      Iterator<Pair<byte[], byte[]>> iterator = dataTable.iterator(0, _numRawDocs);
      DimensionBuffer currentDimensions = null;
      MetricBuffer currentMetrics = null;
      while (iterator.hasNext()) {
        Pair<byte[], byte[]> next = iterator.next();
        byte[] dimensionBytes = next.getLeft();
        DimensionBuffer dimensions = DimensionBuffer.fromBytes(dimensionBytes);
        MetricBuffer metrics = MetricBuffer.fromBytes(next.getRight(), _metricFieldSpecs);
        for (int i = 0; i < _numDimensions; i++) {
          if (_skipMaterializationDimensions.contains(i)) {
            dimensions.setDictId(i, StarTreeNode.ALL);
          }
        }

        if (currentDimensions == null) {
          currentDimensions = dimensions;
          currentMetrics = metrics;
        } else {
          if (currentDimensions.hasSameBytes(dimensionBytes)) {
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
    _outputStream.flush();
  }

  private void createAggregatedDocForAllNodes() throws IOException {
    try (StarTreeDataTable dataTable = new StarTreeDataTable(
        PinotDataBuffer.mapFile(_dataFile, true, 0, _dataFile.length(), PinotDataBuffer.NATIVE_ORDER,
            "OffHeapStarTreeBuilder#createAggregatedDocForAllNodes: data buffer"), _dimensionSize, _metricSize, 0)) {
      DimensionBuffer dimensions = new DimensionBuffer(_numDimensions);
      for (int i = 0; i < _numDimensions; i++) {
        dimensions.setDictId(i, StarTreeNode.ALL);
      }
      createAggregatedDocForAllNodesHelper(dataTable, _rootNode, dimensions);
    }
    _outputStream.flush();
  }

  private MetricBuffer createAggregatedDocForAllNodesHelper(StarTreeDataTable dataTable, TreeNode node,
      DimensionBuffer dimensions) throws IOException {
    MetricBuffer aggregatedMetrics = null;
    if (node._children == null) {
      // Leaf node

      Iterator<Pair<byte[], byte[]>> iterator = dataTable.iterator(node._startDocId, node._endDocId);
      Pair<byte[], byte[]> first = iterator.next();
      aggregatedMetrics = MetricBuffer.fromBytes(first.getRight(), _metricFieldSpecs);
      while (iterator.hasNext()) {
        Pair<byte[], byte[]> next = iterator.next();
        MetricBuffer metricBuffer = MetricBuffer.fromBytes(next.getRight(), _metricFieldSpecs);
        aggregatedMetrics.aggregate(metricBuffer);
      }
    } else {
      // Non-leaf node

      int childDimensionId = node._childDimensionId;
      for (Map.Entry<Integer, TreeNode> entry : node._children.entrySet()) {
        int childDimensionValue = entry.getKey();
        TreeNode child = entry.getValue();
        dimensions.setDictId(childDimensionId, childDimensionValue);
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
      dimensions.setDictId(childDimensionId, StarTreeNode.ALL);
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
        try (StarTreeDataTable dataTable = new StarTreeDataTable(
            PinotDataBuffer.mapFile(_dataFile, false, 0, _dataFile.length(), PinotDataBuffer.NATIVE_ORDER,
                "OffHeapStarTreeBuilder#splitLeafNodesOnTimeColumn: data buffer"), _dimensionSize, _metricSize, 0)) {
          splitLeafNodesOnTimeColumnHelper(dataTable, _rootNode, 0, timeColumnId);
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
    defaultSplitOrder.sort((o1, o2) -> {
      // Descending order
      return _dimensionDictionaries.get(o2).size() - _dimensionDictionaries.get(o1).size();
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
        PinotDataBuffer.mapFile(_dataFile, true, startDocId * _docSizeLong, numDocs * _docSizeLong,
            PinotDataBuffer.NATIVE_ORDER, "OffHeapStarTreeBuilder#constructStarTree: data buffer"), _dimensionSize,
        _metricSize, startDocId)) {
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
    _outputStream.flush();
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
    long tempBufferSize = (endDocId - startDocId) * _docSizeLong;

    PinotDataBuffer tempBuffer;
    if (tempBufferSize > MMAP_SIZE_THRESHOLD) {
      // MMAP
      File tempFile = new File(_tempDir, startDocId + "_" + endDocId + ".unique.tmp");
      try (FileChannel src = new RandomAccessFile(_dataFile, "r").getChannel();
          FileChannel dest = new RandomAccessFile(tempFile, "rw").getChannel()) {
        com.linkedin.pinot.common.utils.FileUtils.transferBytes(src, startDocId * _docSizeLong, tempBufferSize, dest);
      }
      tempBuffer = PinotDataBuffer.mapFile(tempFile, false, 0, tempBufferSize, PinotDataBuffer.NATIVE_ORDER,
          "OffHeapStarTreeBuilder#getUniqueCombinations: temp buffer");
    } else {
      // DIRECT
      tempBuffer =
          PinotDataBuffer.loadFile(_dataFile, startDocId * _docSizeLong, tempBufferSize, PinotDataBuffer.NATIVE_ORDER,
              "OffHeapStarTreeBuilder#getUniqueCombinations: temp buffer");
    }

    final StarTreeDataTable dataTable = new StarTreeDataTable(tempBuffer, _dimensionSize, _metricSize, startDocId);
    _dataTablesToClose.add(dataTable);

    // Need to set skip materialization dimensions value to ALL before sorting
    if (!_skipMaterializationDimensions.isEmpty() && !_excludeSkipMaterializationDimensionsForStarTreeIndex) {
      for (int dimensionIdToSkip : _skipMaterializationDimensions) {
        dataTable.setDimensionValue(dimensionIdToSkip, StarTreeNode.ALL);
      }
    }
    dataTable.setDimensionValue(dimensionIdToRemove, StarTreeNode.ALL);
    dataTable.sort(startDocId, endDocId, _sortOrder);

    return new Iterator<Pair<DimensionBuffer, MetricBuffer>>() {
      final Iterator<Pair<byte[], byte[]>> _iterator = dataTable.iterator(startDocId, endDocId);
      DimensionBuffer _currentDimensions;
      MetricBuffer _currentMetrics;
      boolean _hasNext = true;

      @Override
      public boolean hasNext() {
        return _hasNext;
      }

      @Override
      public Pair<DimensionBuffer, MetricBuffer> next() {
        while (_iterator.hasNext()) {
          Pair<byte[], byte[]> next = _iterator.next();
          byte[] dimensionBytes = next.getLeft();
          MetricBuffer metrics = MetricBuffer.fromBytes(next.getRight(), _metricFieldSpecs);
          if (_currentDimensions == null) {
            _currentDimensions = DimensionBuffer.fromBytes(dimensionBytes);
            _currentMetrics = metrics;
          } else {
            if (_currentDimensions.hasSameBytes(dimensionBytes)) {
              _currentMetrics.aggregate(metrics);
            } else {
              ImmutablePair<DimensionBuffer, MetricBuffer> ret =
                  new ImmutablePair<>(_currentDimensions, _currentMetrics);
              _currentDimensions = DimensionBuffer.fromBytes(dimensionBytes);
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
    final StarTreeDataTable dataTable = new StarTreeDataTable(
        PinotDataBuffer.mapFile(_dataFile, true, startDocId * _docSizeLong, (endDocId - startDocId) * _docSizeLong,
            PinotDataBuffer.NATIVE_ORDER, "OffHeapStarTreeBuilder#iterator: data buffer"), _dimensionSize, _metricSize,
        startDocId);
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
        MetricBuffer metrics = MetricBuffer.fromBytes(pair.getRight(), _metricFieldSpecs);
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
      int dictId = dimensions.getDictId(i);
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
    StarTreeBuilderUtils.serializeTree(starTreeFile, _rootNode, _dimensionNames.toArray(new String[_numDimensions]),
        _numNodes);

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
    _outputStream.close();
    for (StarTreeDataTable dataTable : _dataTablesToClose) {
      dataTable.close();
    }
    _dataTablesToClose.clear();
    FileUtils.deleteDirectory(_tempDir);
  }
}
