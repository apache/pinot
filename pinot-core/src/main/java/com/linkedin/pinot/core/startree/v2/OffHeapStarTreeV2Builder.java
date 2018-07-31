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

package com.linkedin.pinot.core.startree.v2;

import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.io.Closeable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.io.PrintWriter;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.nio.channels.FileChannel;
import org.apache.commons.lang3.tuple.Pair;
import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.utils.Pairs;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import com.linkedin.pinot.common.segment.ReadMode;
import org.apache.commons.lang3.tuple.ImmutablePair;
import com.linkedin.pinot.core.startree.StarTreeNode;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.core.startree.DimensionBuffer;
import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import org.apache.commons.configuration.PropertiesConfiguration;
import com.linkedin.pinot.core.data.readers.PinotSegmentColumnReader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.segment.index.loader.IndexLoadingConfig;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;


public class OffHeapStarTreeV2Builder extends StarTreeV2BaseClass implements StarTreeV2Builder, Closeable {

  // general stuff
  private long _fileSize;
  private long _tempFileSize;
  private int[] _sortOrder;
  private List<Long> _docSizeIndex;
  private List<Long> _tempDocSizeIndex;
  private static final long MMAP_SIZE_THRESHOLD = 500_000_000;



  // star tree.
  private File _dataFile;
  private File _tempDataFile;
  private File _metricOffsetFile;
  private File _tempMetricOffsetFile;

  private int _starTreeCount = 0;
  private int _aggregatedDocCount;
  private String _starTreeId = null;
  private BufferedOutputStream _outputStream;
  private BufferedOutputStream _tempOutputStream;
  private final Set<StarTreeV2DataTable> _dataTablesToClose = new HashSet<>();

  @Override
  public void init(File indexDir, StarTreeV2Config config) throws Exception {
    _fileSize = 0;
    _tempFileSize = 0;
    _aggregatedDocCount = 0;
    _dataFile = new File(indexDir, "star-tree.buf");
    _tempDataFile = new File(indexDir, "star-tree-temp.buf");
    _metricOffsetFile = new File(indexDir, "metric-offset-file.txt");
    _tempMetricOffsetFile = new File(indexDir, "temp-metric-offset-file.txt");


    _outputStream = new BufferedOutputStream(new FileOutputStream(_dataFile));
    _tempOutputStream = new BufferedOutputStream(new FileOutputStream(_tempDataFile));

    _tempDocSizeIndex = new ArrayList<>();
    _docSizeIndex = new ArrayList<>();

    _v3IndexLoadingConfig = new IndexLoadingConfig();
    _v3IndexLoadingConfig.setReadMode(ReadMode.mmap);
    _v3IndexLoadingConfig.setSegmentVersion(SegmentVersion.v3);

    _immutableSegment = ImmutableSegmentLoader.load(indexDir, _v3IndexLoadingConfig);
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
    _dimensionSize = _dimensionsCount * Integer.BYTES;

    // dimension split order.
    List<String> dimensionsSplitOrder = config.getDimensionsSplitOrder();
    _dimensionsSplitOrder = enumerateDimensions(_dimensionsName, dimensionsSplitOrder);
    List<String> dimensionsWithoutStarNode = config.getDimensionsWithoutStarNode();
    _dimensionsWithoutStarNode = enumerateDimensions(_dimensionsName, dimensionsWithoutStarNode);

    // metric
    _aggFunColumnPairsString = "";
    _metricsName = new HashSet<>();
    _metricsSpecMap = new HashMap<>();
    _aggFunColumnPairs = config.getMetric2aggFuncPairs();
    _aggFunColumnPairsCount = _aggFunColumnPairs.size();
    List<String> aggFunColumnPairsStringList = new ArrayList<>();
    for (AggregationFunctionColumnPair pair : _aggFunColumnPairs) {
      _metricsName.add(pair.getColumn());
      aggFunColumnPairsStringList.add(pair.getFunctionType().getName() + '_' + pair.getColumn());
    }
    _aggFunColumnPairsString = String.join(", ", aggFunColumnPairsStringList);
    _metricsCount = _metricsName.size();

    List<MetricFieldSpec> _metricsSpecList = _segmentMetadata.getSchema().getMetricFieldSpecs();
    for (MetricFieldSpec metric : _metricsSpecList) {
      if (_metricsName.contains(metric.getName())) {
        _metricsSpecMap.put(metric.getName(), metric);
      }
    }

    // other initialisation
    _starTreeId = StarTreeV2Constant.STAR_TREE + '_' + Integer.toString(_starTreeCount);
    _outDir = config.getOutDir();
    _maxNumLeafRecords = config.getMaxNumLeafRecords();
    _rootNode = new TreeNode();
    _rootNode._dimensionId = StarTreeV2Constant.STAR_NODE;
    _nodesCount++;
    _starTreeCount++;
    _aggregationFunctionFactory = new AggregationFunctionFactory();

    File metadataFile = StarTreeV2BaseClass.findFormatFile(indexDir, V1Constants.MetadataKeys.METADATA_FILE_NAME);
    _properties = new PropertiesConfiguration(metadataFile);
  }

  @Override
  public void build() throws IOException {

    _metricOffsetFile.createNewFile();
    _tempMetricOffsetFile.createNewFile();

    // storing dimension cardinality for calculating default sorting order.
    _dimensionsCardinality = new ArrayList<>();
    for (int i = 0; i < _dimensionsCount; i++) {
      String dimensionName = _dimensionsName.get(i);
      ImmutableDictionaryReader dictionary = _immutableSegment.getDictionary(dimensionName);
      _dimensionsCardinality.add(dictionary.length());
    }

    // gathering dimensions column reader
    List<PinotSegmentColumnReader> dimensionColumnReaders = new ArrayList<>();
    for (String name : _dimensionsName) {
      PinotSegmentColumnReader columnReader = new PinotSegmentColumnReader(_immutableSegment, name);
      dimensionColumnReaders.add(columnReader);
    }

    // gathering metric column reader
    Map<String, PinotSegmentColumnReader> metricColumnReaders = new HashMap<>();
    for (String metricName : _metricsSpecMap.keySet()) {
      PinotSegmentColumnReader columnReader = new PinotSegmentColumnReader(_immutableSegment, metricName);
      metricColumnReaders.put(metricName, columnReader);
    }

    // gathering dimensions data ( dictionary encoded id )
    for (int i = 0; i < _rawDocsCount; i++) {
      DimensionBuffer dimensions = new DimensionBuffer(_dimensionsCount);
      for (int j = 0; j < dimensionColumnReaders.size(); j++) {
        Integer dictId = dimensionColumnReaders.get(j).getDictionaryId(i);
        dimensions.setDictId(j, dictId);
      }

      Object[] metricValues = new Object[_aggFunColumnPairsCount];
      for (int j = 0; j < _aggFunColumnPairsCount; j++) {
        String metricName = _aggFunColumnPairs.get(j).getColumn();
        String aggfunc = _aggFunColumnPairs.get(j).getFunctionType().getName();
        if (aggfunc.equals(StarTreeV2Constant.AggregateFunctions.COUNT)) {
          metricValues[j] = 1L;
        } else {
          MetricFieldSpec metricFieldSpec = _metricsSpecMap.get(metricName);
          Object val = readHelper(metricColumnReaders.get(metricName), metricFieldSpec.getDataType(), i);
          metricValues[j] = val;
        }
      }
      AggregationFunctionColumnPairBuffer aggregationFunctionColumnPairBuffer = new AggregationFunctionColumnPairBuffer(metricValues, _aggFunColumnPairs);
      appendToRawBuffer(i, dimensions, aggregationFunctionColumnPairBuffer);
    }
    _tempOutputStream.flush();
    _tempDocSizeIndex.add(_tempFileSize);
    PrintWriter outFileObject = new PrintWriter(new BufferedWriter(new FileWriter(_tempMetricOffsetFile.getPath(), true)));
    outFileObject.println(Integer.toString(_rawDocsCount) + " " + Long.toString(_tempFileSize));
    outFileObject.close();

    computeDefaultSplitOrder(_dimensionsCardinality);

    _sortOrder = new int[_dimensionsCount];
    for (int i = 0; i < _dimensionsCount; i++) {
      _sortOrder[i] = _dimensionsSplitOrder.get(i);
    }


    // create pre-aggregated data for star tree
    Iterator<Pair<DimensionBuffer, AggregationFunctionColumnPairBuffer>> iterator = condenseData(_tempDataFile, 0, _rawDocsCount, _tempDocSizeIndex, StarTreeV2Constant.IS_RAW_DATA, -1);
    while (iterator.hasNext()) {
      Pair<DimensionBuffer, AggregationFunctionColumnPairBuffer> next = iterator.next();
      DimensionBuffer dimensions = next.getLeft();
      AggregationFunctionColumnPairBuffer aggregationFunctionColumnPairBuffer = next.getRight();
      _docSizeIndex.add(_fileSize);
      appendToAggBuffer(_aggregatedDocCount, dimensions, aggregationFunctionColumnPairBuffer);
    }
    _docSizeIndex.add(_fileSize);
    outFileObject = new PrintWriter(new BufferedWriter(new FileWriter(_metricOffsetFile.getPath(), true)));
    outFileObject.println(Integer.toString(_aggregatedDocCount) + " " + Long.toString(_fileSize));
    outFileObject.close();
    _outputStream.flush();


    // Recursively construct the star tree
    _rootNode._startDocId = 0;
    _rootNode._endDocId = _aggregatedDocCount;
    constructStarTree(_rootNode, 0, _aggregatedDocCount, 0);

    // create aggregated doc for all nodes.
    createAggregatedDocForAllNodes();

    return;
  }

  @Override
  public void serialize() throws Exception {

  }

  @Override
  public Map<String, String> getMetaData() {
    return null;
  }


  /**
   * Helper function to construct a star tree.
   */
  private void constructStarTree(TreeNode node, int startDocId, int endDocId, int level) throws IOException {
    if (level == _dimensionsSplitOrder.size()) {
      return;
    }

    int splitDimensionId = _dimensionsSplitOrder.get(level);
    Int2ObjectMap<Pairs.IntPair> dimensionRangeMap;
    try (StarTreeV2DataTable dataTable = new StarTreeV2DataTable(PinotDataBuffer.mapFile(_dataFile, true, _docSizeIndex.get(startDocId), _docSizeIndex.get(endDocId) - _docSizeIndex.get(startDocId), PinotDataBuffer.NATIVE_ORDER, "OffHeapStarTreeBuilder#constructStarTree: data buffer"), _dimensionSize, startDocId)) {
      dimensionRangeMap = dataTable.groupOnDimension(startDocId, endDocId, splitDimensionId, _docSizeIndex);
    }

    node._childDimensionId = splitDimensionId;

    // reserve one space for star node
    Map<Integer, TreeNode> children = new HashMap<>(dimensionRangeMap.size() + 1);

    node._children = children;
    for (Integer key : dimensionRangeMap.keySet()) {
      int childDimensionValue = key;
      Pairs.IntPair range = dimensionRangeMap.get(childDimensionValue);

      TreeNode child = new TreeNode();
      child._dimensionValue = key;
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

    // directly return if we don't need to create star-node
    if (_dimensionsWithoutStarNode != null && _dimensionsWithoutStarNode.contains(splitDimensionId)) {
      return;
    }

    // create a star node
    TreeNode starChild = new TreeNode();
    starChild._dimensionId = splitDimensionId;
    int starChildStartDocId = _aggregatedDocCount;
    starChild._startDocId = starChildStartDocId;
    starChild._dimensionValue = StarTreeV2Constant.STAR_NODE;
    children.put(StarTreeV2Constant.STAR_NODE, starChild);
    _nodesCount++;

    Iterator<Pair<DimensionBuffer, AggregationFunctionColumnPairBuffer>> iterator = condenseData(_dataFile, startDocId, endDocId, _docSizeIndex, !StarTreeV2Constant.IS_RAW_DATA, splitDimensionId);
    while (iterator.hasNext()) {
      Pair<DimensionBuffer, AggregationFunctionColumnPairBuffer> next = iterator.next();
      DimensionBuffer dimensions = next.getLeft();
      AggregationFunctionColumnPairBuffer aggregationFunctionColumnPairBuffer = next.getRight();
      appendToAggBuffer(_aggregatedDocCount, dimensions, aggregationFunctionColumnPairBuffer);
      _docSizeIndex.add(_fileSize);
    }
    _outputStream.flush();

    int starChildEndDocId = _aggregatedDocCount;
    starChild._endDocId = starChildEndDocId;

    if (starChildEndDocId - starChildStartDocId > _maxNumLeafRecords) {
      constructStarTree(starChild, starChildStartDocId, starChildEndDocId, level + 1);
    }
  }


  /**
   * function to condense documents according to sorted order.
   */
  protected Iterator<Pair<DimensionBuffer, AggregationFunctionColumnPairBuffer>> condenseData(File dataFile, int startDocId, int endDocId, List<Long> docSizeIndex, boolean isRawData, int dimensionIdToRemove) throws IOException {

    long tempBufferSize = docSizeIndex.get(endDocId) - docSizeIndex.get(startDocId);
    PinotDataBuffer tempBuffer;
    if (tempBufferSize > MMAP_SIZE_THRESHOLD) {

      File tempFile = new File(_outDir, startDocId + "_" + endDocId + ".unique.tmp");
      try (FileChannel src = new RandomAccessFile(dataFile, "r").getChannel();
          FileChannel dest = new RandomAccessFile(tempFile, "rw").getChannel()) {
        long numBytesTransferred = src.transferTo(docSizeIndex.get(startDocId), tempBufferSize, dest);
        Preconditions.checkState(numBytesTransferred == tempBufferSize, "Error transferring data from data file to temp file, transfer size mis-match");
        dest.force(false);
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }
      tempBuffer = PinotDataBuffer.mapFile(tempFile, false, docSizeIndex.get(startDocId), tempBufferSize, PinotDataBuffer.NATIVE_ORDER, "OffHeapStarTreeBuilder#getUniqueCombinations: temp buffer");

    } else {
      tempBuffer = PinotDataBuffer.loadFile(dataFile, docSizeIndex.get(startDocId), tempBufferSize, PinotDataBuffer.NATIVE_ORDER,"OffHeapStarTreeBuilder#getUniqueCombinations: temp buffer");
    }

    final StarTreeV2DataTable dataTable = new StarTreeV2DataTable(tempBuffer, _dimensionSize, startDocId);
    _dataTablesToClose.add(dataTable);

    if (!isRawData) {
      dataTable.setDimensionValue(startDocId, endDocId, dimensionIdToRemove, StarTreeV2Constant.STAR_NODE, docSizeIndex);
    }

    int[] sortedDocsId = dataTable.sort(startDocId, endDocId, _sortOrder, docSizeIndex);

    return new Iterator<Pair<DimensionBuffer, AggregationFunctionColumnPairBuffer>>() {
      final Iterator<Pair<byte[], byte[]>> _iterator = dataTable.iterator(startDocId, endDocId, docSizeIndex, sortedDocsId);

      DimensionBuffer _currentDimensions;
      AggregationFunctionColumnPairBuffer _currentMetrics;
      boolean _hasNext = true;

      @Override
      public boolean hasNext() {
        return _hasNext;
      }

      @Override
      public Pair<DimensionBuffer, AggregationFunctionColumnPairBuffer> next() {
        while (_iterator.hasNext()) {
          Pair<byte[], byte[]> next = _iterator.next();
          byte[] dimensionBytes = next.getLeft();
          AggregationFunctionColumnPairBuffer aggregationFunctionColumnPairBuffer = AggregationFunctionColumnPairBuffer.fromBytes(next.getRight(), _aggFunColumnPairs);

          if (_currentDimensions == null) {
            _currentDimensions = DimensionBuffer.fromBytes(dimensionBytes);
            _currentMetrics = aggregationFunctionColumnPairBuffer;
          } else {
            if (_currentDimensions.hasSameBytes(dimensionBytes)) {
              _currentMetrics.aggregate(aggregationFunctionColumnPairBuffer);
            } else {
              ImmutablePair<DimensionBuffer, AggregationFunctionColumnPairBuffer> ret = new ImmutablePair<>(_currentDimensions, _currentMetrics);
              _currentDimensions = DimensionBuffer.fromBytes(dimensionBytes);
              _currentMetrics = aggregationFunctionColumnPairBuffer;
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

  /**
   * Helper function to create aggregated document from children for a parent node.
   */
  private void createAggregatedDocForAllNodes() throws IOException {
    try (StarTreeV2DataTable dataTable = new StarTreeV2DataTable(
        PinotDataBuffer.mapFile(_dataFile, true, 0, _dataFile.length(), PinotDataBuffer.NATIVE_ORDER, "OffHeapStarV2TreeBuilder#createAggregatedDocForAllNodes: data buffer"), _dimensionSize, 0)) {

      DimensionBuffer dimensions = new DimensionBuffer(_dimensionsCount);
      for (int i = 0; i < _dimensionsCount; i++) {
        dimensions.setDictId(i, StarTreeV2Constant.STAR_NODE);
      }
      createAggregatedDocForAllNodesHelper(dataTable, _rootNode, dimensions);
    }
    _outputStream.flush();
  }

  /**
   * Helper function to create aggregated document from children for a parent node.
   */
  private AggregationFunctionColumnPairBuffer createAggregatedDocForAllNodesHelper(StarTreeV2DataTable dataTable, TreeNode node, DimensionBuffer dimensions) throws IOException {
    AggregationFunctionColumnPairBuffer aggregatedMetrics = null;

    int index = 0;
    int [] sortedDocIds = new int[node._endDocId - node._startDocId];
    for ( int i = node._startDocId; i < node._endDocId; i++) {
      sortedDocIds[index] = i;
      index++;
    }

    if (node._children == null) {
      Iterator<Pair<byte[], byte[]>> iterator = dataTable.iterator(node._startDocId, node._endDocId, _docSizeIndex, sortedDocIds);
      Pair<byte[], byte[]> first = iterator.next();
      aggregatedMetrics = AggregationFunctionColumnPairBuffer.fromBytes(first.getRight(), _aggFunColumnPairs);

      while (iterator.hasNext()) {
        Pair<byte[], byte[]> next = iterator.next();
        AggregationFunctionColumnPairBuffer metricBuffer = AggregationFunctionColumnPairBuffer.fromBytes(next.getRight(), _aggFunColumnPairs);
        aggregatedMetrics.aggregate(metricBuffer);
      }

    } else {
      int childDimensionId = node._childDimensionId;
      for (Map.Entry<Integer, TreeNode> entry : node._children.entrySet()) {
        int childDimensionValue = entry.getKey();
        TreeNode child = entry.getValue();
        dimensions.setDictId(childDimensionId, childDimensionValue);
        AggregationFunctionColumnPairBuffer childAggregatedMetrics = createAggregatedDocForAllNodesHelper(dataTable, child, dimensions);

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
    node._aggDataDocumentId = _aggregatedDocCount;
    appendToAggBuffer(_aggregatedDocCount, dimensions, aggregatedMetrics);

    return aggregatedMetrics;
  }

  /**
   * compute a defualt split order.
   */
  private void appendToRawBuffer(int docId, DimensionBuffer dimensions, AggregationFunctionColumnPairBuffer aggregationFunctionColumnPairBuffer) throws IOException {
    PrintWriter outFileObject = new PrintWriter(new BufferedWriter(new FileWriter(_tempMetricOffsetFile.getPath(), true)));
    outFileObject.println(Integer.toString(docId) + " " + Long.toString(_tempFileSize));
    outFileObject.close();

    int a = appendToBuffer(dimensions, aggregationFunctionColumnPairBuffer, _tempOutputStream);
    _tempDocSizeIndex.add(_tempFileSize);
    _tempFileSize += a;
  }

  /**
   * compute a defualt split order.
   */
  private void appendToAggBuffer(int docId, DimensionBuffer dimensions, AggregationFunctionColumnPairBuffer aggregationFunctionColumnPairBuffer) throws IOException {
    PrintWriter outFileObject = new PrintWriter(new BufferedWriter(new FileWriter(_metricOffsetFile.getPath(), true)));
    outFileObject.println(Integer.toString(docId) + " " + Long.toString(_fileSize));
    outFileObject.close();
    _aggregatedDocCount++;

    int a = appendToBuffer(dimensions, aggregationFunctionColumnPairBuffer, _outputStream);
    _fileSize += a;
  }

  private int  appendToBuffer(DimensionBuffer dimensions, AggregationFunctionColumnPairBuffer aggregationFunctionColumnPairBuffer, BufferedOutputStream outputStream)
      throws IOException {
    outputStream.write(dimensions.toBytes(), 0, _dimensionSize);
    outputStream.write(aggregationFunctionColumnPairBuffer.toBytes(), 0, aggregationFunctionColumnPairBuffer._totalBytes);

    return _dimensionSize + aggregationFunctionColumnPairBuffer._totalBytes;
  }

  @Override
  public void close() throws IOException {
    _outputStream.close();
  }

  private void closeDataTable(StarTreeV2DataTable dataTable) {
    try {
      dataTable.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    _dataTablesToClose.remove(dataTable);
  }
}
