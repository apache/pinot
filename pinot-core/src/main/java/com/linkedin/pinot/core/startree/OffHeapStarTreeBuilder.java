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
import com.google.common.collect.HashBiMap;
import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import xerial.larray.mmap.MMapBuffer;
import xerial.larray.mmap.MMapMode;


/**
 * Uses file to build the star tree. Each row is divided into dimension and metrics. Time is added to dimension list.
 * We use the split order to build the tree. By default, split order will be ranked depending on the cardinality
 * (descending order).
 * If not skipped or explicitly specified in split order, time column will be added as the last entry in split order
 * irrespective of its cardinality.
 * This is a recursive algorithm where we branch on one dimension at every level.
 *
 * <b>Psuedo algorithm</b>
 * <code>
 * build(){
 *   let table(0, N) consists of N input rows
 *   table.sort(0, N); // sort the table on all dimensions, according to split order
 *   constructTree(table, 0, N, 0);
 * }
 * constructTree(table, start, end, level) {
 *   splitDimensionName = dimensionsSplitOrder[level];
 *
 *   // get number of rows for each value in splitDimension
 *   groupByResult<splitDimensionValue, length> = table.groupBy(splitDimensionName);
 *
 *   // create non-star tree nodes
 *   int rangeStart = 0;
 *   for each (entry<splitDimensionValue, length> in groupByResult){
 *     if(entry.length > minThreshold){
 *       constructTree(table, rangeStart, rangeStart + entry.length, level + 1);
 *     }
 *     rangeStart = rangeStart + entry.length;
 *     updateStarTree() // add new child
 *   }
 *
 *   // create a star tree node
 *   aggregatedRows = table.uniqueAfterRemovingAttributeAndAggregateMetrics(start, end, splitDimensionName);
 *   for each (row in aggregatedRows) {
 *     table.add(row);
 *   }
 *   if(aggregateRows.size > minThreshold) {
 *     table.sort(end, end + aggregatedRows.size);
 *     constructStarTree(table, end, end + aggregatedRows.size, level + 1);
 *   }
 * }
 * </code>
 */
public class OffHeapStarTreeBuilder implements StarTreeBuilder {
  private static final int NUM_BYTES_IN_INT = Integer.SIZE / 8;

  private File _outDir;
  private File _dataFile;
  private DataOutputStream _dataBuffer;
  private MMapBuffer _mMapBuffer;

  private List<String> _dimensionsSplitOrder;
  private int _numDimensionsInSplitOrder;
  private Set<String> _skipStarNodeCreationForDimensions;
  private Set<String> _skipMaterializationForDimensions;
  private int _maxLeafRecords;
  private int _skipMaterializationCardinalityThreshold;
  private int _skipTimeColumnSplitThreshold;

  private StarTreeIndexNode _starTreeRootIndexNode = new StarTreeIndexNode();
  private List<String> _starTreeDimensionList = new ArrayList<>();
  private StarTree _starTree = new StarTree(_starTreeRootIndexNode, _starTreeDimensionList);
  private List<String> _dimensionNames = new ArrayList<>();
  private List<Object> _dimensionDefaultNullValueList = new ArrayList<>();
  private int _numDimensions;
  private int _dimensionSizeBytes;
  private String _timeColumnName;
  private List<String> _metricNames = new ArrayList<>();
  private List<Object> _metricDefaultNullValueList = new ArrayList<>();
  private List<DataType> _metricTypes = new ArrayList<>();
  private int _numMetrics;
  private int _metricSizeBytes;
  private int _totalSizeBytes;
  private Map<String, HashBiMap<Object, Integer>> _dictionaryMap = new HashMap<>();
  private int _rawRecordCount = 0;
  private int _aggRecordCount = 0;
  private int[] _sortOrder;

  // Reusable byte buffers.
  private byte[] _recordByteBuffer1;
  private byte[] _recordByteBuffer2;
  private byte[] _dimensionByteBuffer1;
  private byte[] _dimensionByteBuffer2;
  private byte[] _metricByteBuffer;

  /**
   * {@inheritDoc}
   *
   * @param builderConfig
   * @throws Exception
   */
  @Override
  public void init(StarTreeBuilderConfig builderConfig)
      throws Exception {
    // Initialize data file and data buffer.
    _outDir = builderConfig.getOutDir();
    if (_outDir == null) {
      _outDir = new File(System.getProperty("java.io.tmpdir"), V1Constants.STAR_TREE_INDEX_DIR + "_" + DateTime.now());
    }
    if (!_outDir.exists()) {
      Preconditions.checkState(_outDir.mkdirs());
    } else {
      Preconditions.checkState(_outDir.isDirectory());
    }
    _dataFile = new File(_outDir, "star-tree.buf");
    _dataBuffer = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(_dataFile)));

    // Initialize star tree config.
    _dimensionsSplitOrder = builderConfig.getDimensionsSplitOrder();
    _skipStarNodeCreationForDimensions = builderConfig.getSkipStarNodeCreationForDimensions();
    _skipMaterializationForDimensions = builderConfig.getSkipMaterializationForDimensions();
    _maxLeafRecords = builderConfig.getMaxLeafRecords();
    _skipMaterializationCardinalityThreshold = builderConfig.getSkipMaterializationCardinalityThreshold();
    _skipTimeColumnSplitThreshold = builderConfig.getSkipSplitOnTimeColumnThreshold();

    // Initialize column info.
    Schema schema = builderConfig.getSchema();
    // Dimension columns.
    for (DimensionFieldSpec dimensionFieldSpec : schema.getDimensionFieldSpecs()) {
      String dimensionName = dimensionFieldSpec.getName();
      _dimensionNames.add(dimensionName);
      _dimensionDefaultNullValueList.add(dimensionFieldSpec.getDefaultNullValue());
      HashBiMap<Object, Integer> dictionary = HashBiMap.create();
      _dictionaryMap.put(dimensionName, dictionary);
    }
    // Time column, treat time column as dimension column.
    _timeColumnName = schema.getTimeColumnName();
    if (_timeColumnName != null) {
      _dimensionNames.add(_timeColumnName);
      _dimensionDefaultNullValueList.add(schema.getTimeFieldSpec().getDefaultNullValue());
      HashBiMap<Object, Integer> dictionary = HashBiMap.create();
      _dictionaryMap.put(schema.getTimeColumnName(), dictionary);
    }
    _numDimensions = _dimensionNames.size();
    _dimensionSizeBytes = _numDimensions * NUM_BYTES_IN_INT;
    // Metric columns.
    _metricSizeBytes = 0;
    for (MetricFieldSpec metricFieldSpec : schema.getMetricFieldSpecs()) {
      _metricNames.add(metricFieldSpec.getName());
      _metricDefaultNullValueList.add(metricFieldSpec.getDefaultNullValue());
      DataType dataType = metricFieldSpec.getDataType();
      _metricTypes.add(dataType);
      _metricSizeBytes += dataType.size();
    }
    _numMetrics = _metricNames.size();

    // Total size in bytes.
    _totalSizeBytes = _dimensionSizeBytes + _metricSizeBytes;

    // Initialize reusable byte buffers.
    _recordByteBuffer1 = new byte[_totalSizeBytes];
    _recordByteBuffer2 = new byte[_totalSizeBytes];
    _dimensionByteBuffer1 = new byte[_dimensionSizeBytes];
    _dimensionByteBuffer2 = new byte[_dimensionSizeBytes];
    _metricByteBuffer = new byte[_metricSizeBytes];
  }

  /**
   * {@inheritDoc}
   *
   * @param row
   * @throws Exception
   */
  @Override
  public void append(GenericRow row)
      throws Exception {
    // Gather dimension columns and build dictionary.
    DimensionBuffer dimension = new DimensionBuffer(_numDimensions);
    for (int i = 0; i < _numDimensions; i++) {
      String dimensionName = _dimensionNames.get(i);
      Map<Object, Integer> dictionary = _dictionaryMap.get(dimensionName);
      Object dimensionValue = row.getValue(dimensionName);
      if (dimensionValue == null) {
        dimensionValue = _dimensionDefaultNullValueList.get(i);
      }
      Integer dimensionDictId = dictionary.get(dimensionValue);
      if (dimensionDictId == null) {
        dimensionDictId = dictionary.size();
        dictionary.put(dimensionValue, dimensionDictId);
      }
      dimension.setDimension(i, dimensionDictId);
    }

    // Gather metric columns.
    Number[] numbers = new Number[_numMetrics];
    for (int i = 0; i < _numMetrics; i++) {
      String metricName = _metricNames.get(i);
      Object metricValue = row.getValue(metricName);
      if (metricValue == null) {
        metricValue = _metricDefaultNullValueList.get(i);
      }
      numbers[i] = (Number) metricValue;
    }
    MetricBuffer metrics = new MetricBuffer(numbers);

    // Write record to buffer.
    appendToRawBuffer(dimension, metrics);
  }

  /**
   * Helper method to append a raw row to buffer.
   *
   * @param dimensionBuffer dimension buffer.
   * @param metricBuffer metric buffer.
   * @throws Exception
   */
  private void appendToRawBuffer(DimensionBuffer dimensionBuffer, MetricBuffer metricBuffer)
      throws Exception {
    appendToBuffer(dimensionBuffer, metricBuffer);
    _rawRecordCount++;
  }

  /**
   * Helper method to append an aggregate row to buffer.
   *
   * @param dimensionBuffer dimension buffer.
   * @param metricBuffer metric buffer.
   * @throws Exception
   */
  private void appendToAggBuffer(DimensionBuffer dimensionBuffer, MetricBuffer metricBuffer)
      throws Exception {
    appendToBuffer(dimensionBuffer, metricBuffer);
    _aggRecordCount++;
  }

  /**
   * Helper method to append a row to the buffer file.
   *
   * @param dimensionBuffer dimension buffer.
   * @param metricBuffer metric buffer.
   * @throws Exception
   */
  private void appendToBuffer(DimensionBuffer dimensionBuffer, MetricBuffer metricBuffer)
      throws Exception {
    for (int i = 0; i < _numDimensions; i++) {
      _dataBuffer.writeInt(dimensionBuffer.getDimension(i));
    }
    _dataBuffer.write(metricBuffer.toBytes(_metricSizeBytes, _metricTypes));
  }

  /**
   * {@inheritDoc}
   *
   * @throws Exception
   */
  @Override
  public void build()
      throws Exception {
    // Flush the data buffer to file and open a mmap buffer.
    _dataBuffer.flush();
    _mMapBuffer = new MMapBuffer(_dataFile, MMapMode.READ_WRITE);

    // Compute default skip materialization for dimensions set if not defined.
    if (_skipMaterializationForDimensions == null || _skipMaterializationForDimensions.isEmpty()) {
      _skipMaterializationForDimensions = computeDefaultSkipMaterializationForDimensions();
    }

    // Compute default dimensions split order list if not defined.
    if (_dimensionsSplitOrder == null || _dimensionsSplitOrder.isEmpty()) {
      // For default dimensions split order list, give preference to skip materialization for dimensions set.
      _dimensionsSplitOrder = computeDefaultDimensionsSplitOrder();
      _dimensionsSplitOrder.removeAll(_skipMaterializationForDimensions);
    } else {
      // For user-defined dimensions split order list, give preference to dimensions split order list.
      _skipMaterializationForDimensions.removeAll(_dimensionsSplitOrder);
    }
    _numDimensionsInSplitOrder = _dimensionsSplitOrder.size();
    Preconditions.checkState(_numDimensionsInSplitOrder != 0);

    // Compute the sort order according to the dimensions split order list.
    _sortOrder = new int[_numDimensions];
    int count = _numDimensionsInSplitOrder;
    for (int i = 0; i < _numDimensions; i++) {
      String dimensionName = _dimensionNames.get(i);
      int index = _dimensionsSplitOrder.indexOf(dimensionName);
      if (index >= 0) {
        _sortOrder[index] = i;
      } else {
        _sortOrder[count++] = i;
      }
    }

    // Sort all the data based on the sort order.
    sort(0, _rawRecordCount);
    _mMapBuffer.flush();
    _mMapBuffer.close();
    _mMapBuffer = new MMapBuffer(_dataFile, MMapMode.READ_ONLY);

    // Recursively construct the star tree.
    constructStarTree(_starTreeRootIndexNode, 0, _rawRecordCount, 0);

    // If time column exists and not in dimensions split order list or skip materialization for dimensions set, split
    // leaf nodes on time column.
    if (_timeColumnName != null && !_dimensionsSplitOrder.contains(_timeColumnName)
        && !_skipMaterializationForDimensions.contains(_timeColumnName)) {
      splitLeafNodesOnTimeColumn();
    }

    // Create aggregate row for all nodes recursively.
    DimensionBuffer dimensionBuffer = new DimensionBuffer(_numDimensions);
    for (int i = 0; i < _numDimensions; i++) {
      dimensionBuffer.setDimension(i, StarTreeIndexNode.ALL);
    }
    createAggregateDoc(_starTreeRootIndexNode, dimensionBuffer);
    _dataBuffer.flush();
    _dataBuffer.close();
    _mMapBuffer.close();
    _mMapBuffer = new MMapBuffer(_dataFile, MMapMode.READ_ONLY);
  }

  /**
   * Helper method to compute the default skip materialization for dimensions set according to the threshold.
   *
   * @return default skip materialization for dimensions set.
   */
  private Set<String> computeDefaultSkipMaterializationForDimensions() {
    Set<String> skipDimensions = new HashSet<>();
    for (String dimensionName : _dimensionNames) {
      if (_dictionaryMap.get(dimensionName).size() > _skipMaterializationCardinalityThreshold) {
        skipDimensions.add(dimensionName);
      }
    }
    return skipDimensions;
  }

  /**
   * Helper method to compute the default dimensions split order list according to the threshold.
   *
   * @return default dimensions split order list.
   */
  private List<String> computeDefaultDimensionsSplitOrder() {
    ArrayList<String> defaultSplitOrder = new ArrayList<>();
    // Exclude time column.
    for (String dimensionName : _dimensionNames) {
      if (!dimensionName.equals(_timeColumnName)
          && _dictionaryMap.get(dimensionName).size() <= _skipMaterializationCardinalityThreshold) {
        defaultSplitOrder.add(dimensionName);
      }
    }
    // Sort in descending order.
    Collections.sort(defaultSplitOrder, new Comparator<String>() {
      @Override
      public int compare(String o1, String o2) {
        return _dictionaryMap.get(o2).size() - _dictionaryMap.get(o1).size();
      }
    });
    return defaultSplitOrder;
  }

  /**
   * Helper method to sort the documents using the mmap buffer and according to the sort order.
   *
   * @param startDocId start document id.
   * @param endDocId end document id.
   * @throws Exception
   */
  private void sort(final int startDocId, int endDocId)
      throws Exception {
    List<Integer> recordIdList = new ArrayList<>();
    for (int i = startDocId; i < endDocId; i++) {
      recordIdList.add(i - startDocId);
    }

    // Sort the record id list according to sort order.
    Collections.sort(recordIdList, new Comparator<Integer>() {
      @Override
      public int compare(Integer docId1, Integer docId2) {
        int pos1 = (docId1 + startDocId) * _totalSizeBytes;
        int pos2 = (docId2 + startDocId) * _totalSizeBytes;
        _mMapBuffer.copyTo(pos1, _dimensionByteBuffer1, 0, _dimensionSizeBytes);
        _mMapBuffer.copyTo(pos2, _dimensionByteBuffer2, 0, _dimensionSizeBytes);
        IntBuffer intBuffer1 = ByteBuffer.wrap(_dimensionByteBuffer1).asIntBuffer();
        IntBuffer intBuffer2 = ByteBuffer.wrap(_dimensionByteBuffer2).asIntBuffer();

        // Compare according to sort order.
        for (int index : _sortOrder) {
          int v1 = intBuffer1.get(index);
          int v2 = intBuffer2.get(index);
          if (v1 != v2) {
            return v1 - v2;
          }
        }
        return 0;
      }
    });

    // Swap records in mmap buffer according to the sorted record id list.
    int length = endDocId - startDocId;

    // Record index to document id and document id to index mapping.
    int[] indexToDocId = new int[length];
    int[] docIdToIndex = new int[length];
    for (int i = 0; i < length; i++) {
      indexToDocId[i] = i;
      docIdToIndex[i] = i;
    }
    for (int i = startDocId; i < endDocId; i++) {
      // Find target document index.
      int offset = i - startDocId;
      int targetDocId = recordIdList.get(offset);
      int targetIndex = docIdToIndex[targetDocId];

      // Swap records if two indices do not match.
      if (offset != targetIndex) {
        int pos1 = i * _totalSizeBytes;
        int pos2 = (targetIndex + startDocId) * _totalSizeBytes;
        _mMapBuffer.copyTo(pos1, _recordByteBuffer1, 0, _totalSizeBytes);
        _mMapBuffer.copyTo(pos2, _recordByteBuffer2, 0, _totalSizeBytes);
        _mMapBuffer.readFrom(_recordByteBuffer1, 0, pos2, _totalSizeBytes);
        _mMapBuffer.readFrom(_recordByteBuffer2, 0, pos1, _totalSizeBytes);

        // Update two maps.
        int currentDocId = indexToDocId[offset];
        indexToDocId[offset] = targetDocId;
        docIdToIndex[targetDocId] = offset;
        indexToDocId[targetIndex] = currentDocId;
        docIdToIndex[currentDocId] = targetIndex;
      }
    }
  }

  /**
   * Helper method to construct the star tree recursively.
   *
   * @param node root node to split on.
   * @param startDocId start document id.
   * @param endDocId end document id.
   * @param level level of star tree.
   * @throws Exception
   */
  private void constructStarTree(StarTreeIndexNode node, int startDocId, int endDocId, int level)
      throws Exception {
    String splitDimensionName = _dimensionsSplitOrder.get(level);
    if (_starTreeDimensionList.size() == level) {
      _starTreeDimensionList.add(splitDimensionName);
    }
    node.setChildDimension(level);
    Map<Integer, StarTreeIndexNode> children = new HashMap<>();
    node.setChildren(children);

    // Group by dimension value and set children nodes.
    int splitDimensionIndex = _dimensionNames.indexOf(splitDimensionName);
    int prevValue = Integer.MIN_VALUE;
    int prevStartDocId = startDocId;
    // Use end doc id to handle the last group.
    for (int i = startDocId; i <= endDocId; i++) {
      int currentValue = 0;
      if (i < endDocId) {
        currentValue =
            Integer.reverseBytes(_mMapBuffer.getInt(i * _totalSizeBytes + splitDimensionIndex * NUM_BYTES_IN_INT));
      }

      // Find a new group.
      if ((i == endDocId) || (prevValue != Integer.MIN_VALUE && prevValue != currentValue)) {
        StarTreeIndexNode child = new StarTreeIndexNode();
        int numDocsInGroup = i - prevStartDocId;
        if ((numDocsInGroup > _maxLeafRecords) && (level != _numDimensionsInSplitOrder - 1)) {
          constructStarTree(child, prevStartDocId, i, level + 1);
        } else {
          child.setStartDocumentId(prevStartDocId);
          child.setEndDocumentId(i);
        }
        children.put(prevValue, child);
        prevStartDocId = i;
      }

      prevValue = currentValue;
    }

    // Return if star node does not need to be created.
    if (_skipStarNodeCreationForDimensions != null && _skipStarNodeCreationForDimensions.contains(splitDimensionName)) {
      return;
    }

    // Create star node
    StarTreeIndexNode starChild = new StarTreeIndexNode();
    children.put(StarTreeIndexNode.ALL, starChild);

    // Map from unique dimension buffer to metric buffer.
    Map<DimensionBuffer, MetricBuffer> aggregateResult = new HashMap<>();

    for (int i = startDocId; i < endDocId; i++) {
      // Compute dimension buffer with star node.
      _mMapBuffer.copyTo(i * _totalSizeBytes, _dimensionByteBuffer1, 0, _dimensionSizeBytes);
      DimensionBuffer dimensionBuffer = DimensionBuffer.fromBytes(_dimensionByteBuffer1);
      for (int j = 0; j < _numDimensions; j++) {
        String dimensionName = _dimensionNames.get(j);
        if (dimensionName.equals(splitDimensionName) || _skipMaterializationForDimensions.contains(dimensionName)) {
          dimensionBuffer.setDimension(j, StarTreeIndexNode.ALL);
        }
      }
      // Update metric buffer.
      _mMapBuffer.copyTo(i * _totalSizeBytes + _dimensionSizeBytes, _metricByteBuffer, 0, _metricSizeBytes);
      MetricBuffer metricBuffer = MetricBuffer.fromBytes(_metricByteBuffer, _metricTypes);
      MetricBuffer oldValue = aggregateResult.get(dimensionBuffer);
      if (oldValue == null) {
        aggregateResult.put(dimensionBuffer, metricBuffer);
      } else {
        oldValue.aggregate(metricBuffer, _metricTypes);
      }
    }

    // Append aggregate records and construct next level tree if necessary.
    int totalRecordCount = _rawRecordCount + _aggRecordCount;
    int numDocsInStarNode = aggregateResult.size();
    if ((numDocsInStarNode > _maxLeafRecords) && (level != _numDimensionsInSplitOrder - 1)) {
      // Sort the aggregate records.
      List<DimensionBuffer> dimensionBufferList = new ArrayList<>(aggregateResult.keySet());
      Collections.sort(dimensionBufferList, new Comparator<DimensionBuffer>() {
        @Override
        public int compare(DimensionBuffer o1, DimensionBuffer o2) {
          // Compare according to sort order.
          for (int index : _sortOrder) {
            int v1 = o1.getDimension(index);
            int v2 = o2.getDimension(index);
            if (v1 != v2) {
              return v1 - v2;
            }
          }
          return 0;
        }
      });
      for (DimensionBuffer dimensionBuffer : dimensionBufferList) {
        appendToAggBuffer(dimensionBuffer, aggregateResult.get(dimensionBuffer));
      }
      _dataBuffer.flush();
      _mMapBuffer.close();
      _mMapBuffer = new MMapBuffer(_dataFile, MMapMode.READ_ONLY);
      constructStarTree(starChild, totalRecordCount, totalRecordCount + numDocsInStarNode, level + 1);
    } else {
      for (Map.Entry<DimensionBuffer, MetricBuffer> entry : aggregateResult.entrySet()) {
        appendToAggBuffer(entry.getKey(), entry.getValue());
      }
      starChild.setStartDocumentId(totalRecordCount);
      starChild.setEndDocumentId(totalRecordCount + numDocsInStarNode);
      _dataBuffer.flush();
      _mMapBuffer.close();
      _mMapBuffer = new MMapBuffer(_dataFile, MMapMode.READ_ONLY);
    }
  }

  /**
   * Helper method to split each current leaf nodes on time column:
   *
   * @throws Exception
   */
  private void splitLeafNodesOnTimeColumn()
      throws Exception {
    int timeColumn = _starTreeDimensionList.size();
    int timeColumnIndex = _dimensionNames.indexOf(_timeColumnName);
    _starTreeDimensionList.add(_timeColumnName);
    _mMapBuffer.close();
    _mMapBuffer = new MMapBuffer(_dataFile, MMapMode.READ_WRITE);

    // Update sort order, make time column the first element to sort on.
    for (int i = 0; i < _numDimensions; i++) {
      if (_sortOrder[i] == timeColumnIndex) {
        System.arraycopy(_sortOrder, 0, _sortOrder, 1, i);
        _sortOrder[0] = timeColumnIndex;
      }
    }

    // Recursively split each leaf node on time column.
    splitOnTimeColumn(_starTreeRootIndexNode, timeColumn, timeColumnIndex);

    _mMapBuffer.flush();
    _mMapBuffer.close();
    _mMapBuffer = new MMapBuffer(_dataFile, MMapMode.READ_ONLY);
  }

  /**
   * Helper method to split current node on time column recursively.
   *
   * @param node star tree index node.
   * @param timeColumn int time column.
   * @param timeColumnIndex index of time column in all dimensions.
   */
  private void splitOnTimeColumn(StarTreeIndexNode node, int timeColumn, int timeColumnIndex)
      throws Exception {
    if (node.isLeaf()) {
      // For leaf node, split on time column as next level.
      int startDocId = node.getStartDocumentId();
      int endDocId = node.getEndDocumentId();

      // Do not split if number of records under the threshold.
      if (endDocId - startDocId < _skipTimeColumnSplitThreshold) {
        return;
      }

      // Sort records use the updated sort order.
      sort(startDocId, endDocId);

      // Group records and add them to children in next level.
      node.setChildDimension(timeColumn);
      Map<Integer, StarTreeIndexNode> children = new HashMap<>();
      node.setChildren(children);
      // Group by dimension value and set children nodes.
      int prevValue = Integer.MIN_VALUE;
      int prevStartDocId = startDocId;
      // Use end doc id to handle the last group.
      for (int i = startDocId; i <= endDocId; i++) {
        int currentValue = 0;
        if (i < endDocId) {
          currentValue =
              Integer.reverseBytes(_mMapBuffer.getInt(i * _totalSizeBytes + timeColumnIndex * NUM_BYTES_IN_INT));
        }

        // Find a new group.
        if ((i == endDocId) || (prevValue != Integer.MIN_VALUE && prevValue != currentValue)) {
          StarTreeIndexNode child = new StarTreeIndexNode();
          child.setStartDocumentId(prevStartDocId);
          child.setEndDocumentId(i);
          children.put(prevValue, child);
          prevStartDocId = i;
        }

        prevValue = currentValue;
      }
    } else {
      // For non leaf node, call this method on all it's child nodes.
      Map<Integer, StarTreeIndexNode> children = node.getChildren();
      for (StarTreeIndexNode child : children.values()) {
        splitOnTimeColumn(child, timeColumn, timeColumnIndex);
      }
    }
  }

  /**
   * Helper method to create aggregate document recursively.
   *
   * @param node star tree index node.
   * @param dimensionBuffer dimension buffer for this level.
   * @return aggregated metric buffer.
   * @throws Exception
   */
  private MetricBuffer createAggregateDoc(StarTreeIndexNode node, DimensionBuffer dimensionBuffer)
      throws Exception {
    MetricBuffer aggregatedMetricBuffer = null;

    if (node.isLeaf()) {
      // For leaf node, compute the aggregated metric buffer from the data.
      int startDocId = node.getStartDocumentId();
      int endDocId = node.getEndDocumentId();
      for (int i = startDocId; i < endDocId; i++) {
        _mMapBuffer.copyTo(i * _totalSizeBytes + _dimensionSizeBytes, _metricByteBuffer, 0, _metricSizeBytes);
        MetricBuffer metricBuffer = MetricBuffer.fromBytes(_metricByteBuffer, _metricTypes);
        if (aggregatedMetricBuffer == null) {
          aggregatedMetricBuffer = metricBuffer;
        } else {
          aggregatedMetricBuffer.aggregate(metricBuffer, _metricTypes);
        }
      }
      node.setAggregatedDocumentId(_rawRecordCount + _aggRecordCount);
      appendToAggBuffer(dimensionBuffer, aggregatedMetricBuffer);
    } else {
      // For non-leaf node, compute the aggregated metric buffer from the children.
      int childDimensionIndex = _dimensionNames.indexOf(_starTreeDimensionList.get(node.getChildDimension()));
      Map<Integer, StarTreeIndexNode> children = node.getChildren();
      boolean hasStarNode = children.containsKey(StarTreeIndexNode.ALL);
      for (Map.Entry<Integer, StarTreeIndexNode> entry : children.entrySet()) {
        int childDimensionValue = entry.getKey();
        StarTreeIndexNode child = entry.getValue();
        dimensionBuffer.setDimension(childDimensionIndex, childDimensionValue);
        MetricBuffer childMetricBuffer = createAggregateDoc(child, dimensionBuffer);
        if (hasStarNode) {
          // If star node exists, use its aggregated document id.
          if (childDimensionValue == StarTreeIndexNode.ALL) {
            aggregatedMetricBuffer = childMetricBuffer;
            node.setAggregatedDocumentId(child.getAggregatedDocumentId());
          }
        } else {
          // If star node does not exist, aggregate non-star nodes' metric buffer.
          if (childDimensionValue != StarTreeIndexNode.ALL) {
            if (aggregatedMetricBuffer == null) {
              aggregatedMetricBuffer = childMetricBuffer;
            } else {
              aggregatedMetricBuffer.aggregate(childMetricBuffer, _metricTypes);
            }
          }
        }
      }

      // If star node does not exist, append a new aggregate document.
      if (!hasStarNode) {
        node.setAggregatedDocumentId(_rawRecordCount + _aggRecordCount);
        appendToAggBuffer(dimensionBuffer, aggregatedMetricBuffer);
      }

      // Remove child dimension value from dimension buffer for back tracking.
      dimensionBuffer.setDimension(childDimensionIndex, StarTreeIndexNode.ALL);
    }

    return aggregatedMetricBuffer;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void cleanup() {
    FileUtils.deleteQuietly(_outDir);
  }

  /**
   * {@inheritDoc}
   *
   * @return
   */
  @Override
  public StarTree getTree() {
    return _starTree;
  }

  /**
   * {@inheritDoc}
   *
   * @param startDocId
   * @param endDocId
   * @return
   * @throws Exception
   */
  @Override
  public Iterator<GenericRow> iterator(final int startDocId, final int endDocId)
      throws Exception {
    return new Iterator<GenericRow>() {
      private int _currentDocId = startDocId;

      @Override
      public boolean hasNext() {
        return _currentDocId < endDocId;
      }

      @Override
      public GenericRow next() {
        _mMapBuffer.copyTo(_currentDocId * _totalSizeBytes, _dimensionByteBuffer1, 0, _dimensionSizeBytes);
        _mMapBuffer.copyTo(_currentDocId * _totalSizeBytes + _dimensionSizeBytes, _metricByteBuffer, 0,
            _metricSizeBytes);
        _currentDocId++;
        return toGenericRow(DimensionBuffer.fromBytes(_dimensionByteBuffer1),
            MetricBuffer.fromBytes(_metricByteBuffer, _metricTypes));
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  /**
   * Helper method to create generic row from dimension buffer and metric buffer.
   *
   * @param dimensionBuffer dimension buffer.
   * @param metricBuffer metric buffer.
   * @return created generic row.
   */
  private GenericRow toGenericRow(DimensionBuffer dimensionBuffer, MetricBuffer metricBuffer) {
    GenericRow genericRow = new GenericRow();
    Map<String, Object> map = new HashMap<>();
    for (int i = 0; i < _numDimensions; i++) {
      String dimensionName = _dimensionNames.get(i);
      int dimensionValue = dimensionBuffer.getDimension(i);
      Object dimensionObjectValue;
      if (dimensionValue == StarTreeIndexNode.ALL) {
        dimensionObjectValue = _dimensionDefaultNullValueList.get(i);
      } else {
        dimensionObjectValue = _dictionaryMap.get(dimensionName).inverse().get(dimensionValue);
      }
      map.put(dimensionName, dimensionObjectValue);
    }
    for (int i = 0; i < _numMetrics; i++) {
      String metricName = _metricNames.get(i);
      map.put(metricName, metricBuffer.get(i));
    }
    genericRow.init(map);
    return genericRow;
  }

  /**
   * {@inheritDoc}
   *
   * @return
   */
  @Override
  public int getTotalRawDocumentCount() {
    return _rawRecordCount;
  }

  /**
   * {@inheritDoc}
   *
   * @return
   */
  @Override
  public int getTotalAggregateDocumentCount() {
    return _aggRecordCount;
  }

  /**
   * {@inheritDoc}
   *
   * @return
   */
  @Override
  public int getMaxLeafRecords() {
    return _maxLeafRecords;
  }

  /**
   * {@inheritDoc}
   *
   * @return
   */
  @Override
  public List<String> getDimensionsSplitOrder() {
    return _dimensionsSplitOrder;
  }

  /**
   * {@inheritDoc}
   *
   * @return
   */
  @Override
  public Set<String> getSkipMaterializationForDimensions() {
    return _skipMaterializationForDimensions;
  }

  /**
   * {@inheritDoc}
   *
   * @return
   */
  @Override
  public Map<String, HashBiMap<Object, Integer>> getDictionaryMap() {
    return _dictionaryMap;
  }
}
