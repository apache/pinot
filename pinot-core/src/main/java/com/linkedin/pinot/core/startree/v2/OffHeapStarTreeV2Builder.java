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

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.Pairs;
import com.linkedin.pinot.core.data.readers.PinotSegmentColumnReader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import com.linkedin.pinot.core.io.compression.ChunkCompressorFactory;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionType;
import com.linkedin.pinot.core.segment.creator.ForwardIndexCreator;
import com.linkedin.pinot.core.segment.creator.SingleValueForwardIndexCreator;
import com.linkedin.pinot.core.segment.creator.SingleValueRawIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.SegmentColumnarIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.creator.impl.fwd.SingleValueUnsortedForwardIndexCreator;
import com.linkedin.pinot.core.segment.index.loader.IndexLoadingConfig;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import com.linkedin.pinot.core.startree.DimensionBuffer;
import com.linkedin.pinot.core.startree.OffHeapStarTreeNode;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.LoggerFactory;


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
  private int _dimensionSize;
  private File _metricOffsetFile;
  private File _tempMetricOffsetFile;
  private List<AggregationFunction> _aggregationFunctions;

  private int _starTreeCount = 0;
  private int _aggregatedDocCount;
  private String _starTreeId = null;
  private BufferedOutputStream _outputStream;
  private BufferedOutputStream _tempOutputStream;
  private final Set<StarTreeV2DataTable> _dataTablesToClose = new HashSet<>();



  // using this dictionary map for debugging, can be removed later.
  Map<String, Dictionary> _dictionary = new HashMap<>();




  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(OffHeapStarTreeV2Builder.class);

  @Override
  public void init(File indexDir, StarTreeV2Config config) throws Exception {
    _fileSize = 0;
    _tempFileSize = 0;
    _aggregatedDocCount = 0;

    _outDir = config.getOutDir();
    _dataFile = new File(_outDir, "star-tree.buf");
    _tempDataFile = new File(_outDir, "star-tree-temp.buf");
    _metricOffsetFile = new File(_outDir, "metric-offset-file.txt");
    _tempMetricOffsetFile = new File(_outDir, "temp-metric-offset-file.txt");

    _metricOffsetFile.createNewFile();
    _tempMetricOffsetFile.createNewFile();
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
      aggFunColumnPairsStringList.add(pair.toColumnName());
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

    _maxNumLeafRecords = config.getMaxNumLeafRecords();
    _rootNode = new TreeNode();
    _rootNode._dimensionId = StarTreeV2Constant.STAR_NODE;
    _rootNode._dimensionValue = StarTreeV2Constant.STAR_NODE;
    _nodesCount++;
    _starTreeCount++;
    _aggregationFunctions = new ArrayList<>();

    File metadataFile = findFormatFile(indexDir, V1Constants.MetadataKeys.METADATA_FILE_NAME);
    _properties = new PropertiesConfiguration(metadataFile);
  }

  @Override
  public void build() throws Exception {

    // storing dimension cardinality for calculating default sorting order.
    _dimensionsCardinality = new ArrayList<>();
    for (int i = 0; i < _dimensionsCount; i++) {
      String dimensionName = _dimensionsName.get(i);
      ImmutableDictionaryReader dictionary = _immutableSegment.getDictionary(dimensionName);
      _dictionary.put(dimensionName ,dictionary);
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

    // collecting all aggFunColObjects
    for (AggregationFunctionColumnPair pair : _aggFunColumnPairs) {
      AggregationFunction function =
          AggregationFunctionFactory.getAggregationFunction(pair.getFunctionType().getName());

      _aggregationFunctions.add(function);
    }

    // gathering data ( dictionary encoded id )
    for (int i = 0; i < _rawDocsCount; i++) {
      DimensionBuffer dimensions = new DimensionBuffer(_dimensionsCount);
      for (int j = 0; j < dimensionColumnReaders.size(); j++) {
        Integer dictId = dimensionColumnReaders.get(j).getDictionaryId(i);
        dimensions.setDictId(j, dictId);
      }

      Object[] metricValues = new Object[_aggFunColumnPairsCount];
      for (int j = 0; j < _aggFunColumnPairsCount; j++) {
        AggregationFunctionColumnPair pair = _aggFunColumnPairs.get(j);
        String metricName = pair.getColumn();
        String aggfunc = pair.getFunctionType().getName();
        AggregationFunction function = _aggregationFunctions.get(j);
        if (aggfunc.equals(AggregationFunctionType.COUNT.getName())) {
          metricValues[j] = function.convert(1);
        } else {
          MetricFieldSpec metricFieldSpec = _metricsSpecMap.get(metricName);
          Object val = readHelper(metricColumnReaders.get(metricName), metricFieldSpec.getDataType(), i);
          metricValues[j] = function.convert(val);
        }
      }
      AggregationFunctionColumnPairBuffer aggregationFunctionColumnPairBuffer =
          new AggregationFunctionColumnPairBuffer(metricValues, _aggregationFunctions);
      appendToRawBuffer(i, dimensions, aggregationFunctionColumnPairBuffer);
    }
    _tempOutputStream.flush();
    _tempDocSizeIndex.add(_tempFileSize);
    PrintWriter outFileObject =
        new PrintWriter(new BufferedWriter(new FileWriter(_tempMetricOffsetFile.getPath(), true)));
    outFileObject.println(Integer.toString(_rawDocsCount) + " " + Long.toString(_tempFileSize));
    outFileObject.close();

    computeDefaultSplitOrder(_dimensionsCardinality);

    _sortOrder = new int[_dimensionsCount];
    for (int i = 0; i < _dimensionsCount; i++) {
      _sortOrder[i] = _dimensionsSplitOrder.get(i);
    }

    // create pre-aggregated data for star tree
    Iterator<Pair<DimensionBuffer, AggregationFunctionColumnPairBuffer>> iterator =
        condenseData(_tempDataFile, 0, _rawDocsCount, _tempDocSizeIndex, StarTreeV2Constant.IS_RAW_DATA, -1);
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

    // serialize the tree.
    serialize();

    FileUtils.deleteQuietly(_tempDataFile);
    FileUtils.deleteQuietly(_tempMetricOffsetFile);
  }

  /**
   * Helper function to serialize tree.
   */
  private void serialize() throws Exception {

    // updating segment metadata by adding star tree meta data.
    Map<String, String> metadata = getMetaData();
    for (String key : metadata.keySet()) {
      String value = metadata.get(key);
      _properties.setProperty(key, value);
    }
    _properties.setProperty(StarTreeV2Constant.STAR_TREE_V2_COUNT, _starTreeCount);
    _properties.save();

    createIndexes();
    serializeTree(new File(_outDir, _starTreeId));
    combineIndexesFiles(_starTreeCount - 1);
  }

  /**
   * Helper method to create star-tree meta data for saving to segment meta data
   */
  private Map<String, String> getMetaData() {
    Map<String, String> metadata = new HashMap<>();

    String starTreeDocsCount = _starTreeId + "_" + StarTreeV2Constant.StarTreeMetadata.STAR_TREE_DOCS_COUNT;
    metadata.put(starTreeDocsCount, Integer.toString(_aggregatedDocCount));

    String startTreeSplitOrder = _starTreeId + "_" + StarTreeV2Constant.StarTreeMetadata.STAR_TREE_SPLIT_ORDER;
    metadata.put(startTreeSplitOrder, _dimensionSplitOrderString);

    String withoutStarNode =
        _starTreeId + "_" + StarTreeV2Constant.StarTreeMetadata.STAR_TREE_SKIP_STAR_NODE_CREATION_FOR_DIMENSIONS;
    metadata.put(withoutStarNode, _dimensionWithoutStarNodeString);

    String startTreeMet2aggfuncPairs =
        _starTreeId + "_" + StarTreeV2Constant.StarTreeMetadata.STAR_TREE_AGG_FUN_COL_PAIR;
    metadata.put(startTreeMet2aggfuncPairs, _aggFunColumnPairsString);

    String maxNumLeafRecords = _starTreeId + "_" + StarTreeV2Constant.StarTreeMetadata.STAR_TREE_MAX_LEAF_RECORD;
    metadata.put(maxNumLeafRecords, Integer.toString(_maxNumLeafRecords));

    return metadata;
  }

  /**
   * Helper method to serialize the start tree into a file.
   */
  private void serializeTree(File starTreeFile) throws IOException {
    int headerSizeInBytes = computeHeaderSizeInBytes(_dimensionsName);
    long totalSizeInBytes = headerSizeInBytes + _nodesCount * OffHeapStarTreeNode.SERIALIZABLE_SIZE_IN_BYTES;

    try (PinotDataBuffer buffer = PinotDataBuffer.mapFile(starTreeFile, false, 0, totalSizeInBytes,
        ByteOrder.LITTLE_ENDIAN, "OffHeapStarTreeBuilder#serializeTree: star-tree buffer")) {
      long offset = writeHeader(buffer, headerSizeInBytes, _dimensionsCount, _dimensionsName, _nodesCount);
      Preconditions.checkState(offset == headerSizeInBytes, "Error writing Star Tree file, header size mis-match");
      writeNodes(buffer, offset, _rootNode);
    }
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
    try (StarTreeV2DataTable dataTable = new StarTreeV2DataTable(
        PinotDataBuffer.mapFile(_dataFile, true, _docSizeIndex.get(startDocId),
            _docSizeIndex.get(endDocId) - _docSizeIndex.get(startDocId), PinotDataBuffer.NATIVE_ORDER,
            "OffHeapStarTreeBuilder#constructStarTree: data buffer"), _dimensionSize, startDocId)) {
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
      int childEndDocId = range.getRight();

      child._startDocId = childStartDocId;
      child._endDocId = childEndDocId;
      children.put(childDimensionValue, child);
      if (childEndDocId - childStartDocId > _maxNumLeafRecords) {
        constructStarTree(child, childStartDocId, childEndDocId, level + 1);
      }
      _nodesCount++;
    }

    // directly return if we don't need to create star-node
    if ((_dimensionsWithoutStarNode != null && _dimensionsWithoutStarNode.contains(splitDimensionId))
        || dimensionRangeMap.size() == 1) {
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

    Iterator<Pair<DimensionBuffer, AggregationFunctionColumnPairBuffer>> iterator =
        condenseData(_dataFile, startDocId, endDocId, _docSizeIndex, !StarTreeV2Constant.IS_RAW_DATA, splitDimensionId);
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
   * Helper function to create indexes and index values.
   */
  private void createIndexes() throws Exception {

    List<ForwardIndexCreator> dimensionForwardIndexCreatorList = new ArrayList<>();
    List<ForwardIndexCreator> aggFunColumnPairForwardIndexCreatorList = new ArrayList<>();

    // 'SingleValueForwardIndexCreator' for dimensions.
    for (String dimensionName : _dimensionsName) {
      DimensionFieldSpec spec = _dimensionsSpecMap.get(dimensionName);
      int cardinality = _immutableSegment.getDictionary(dimensionName).length();
      dimensionForwardIndexCreatorList.add(
          new SingleValueUnsortedForwardIndexCreator(spec, _outDir, cardinality, _aggregatedDocCount,
              _aggregatedDocCount, false));
    }

    // 'SingleValueRawIndexCreator' for metrics
    int index = 0;
    for (AggregationFunctionColumnPair pair : _aggFunColumnPairs) {
      String columnName = pair.toColumnName();
      AggregationFunction function = _aggregationFunctions.get(index);
      index++;
      SingleValueRawIndexCreator rawIndexCreator = SegmentColumnarIndexCreator.getRawIndexCreatorForColumn(_outDir,
          ChunkCompressorFactory.CompressionType.PASS_THROUGH, columnName, function.getResultDataType(),
          _aggregatedDocCount, function.getResultMaxByteSize());
      aggFunColumnPairForwardIndexCreatorList.add(rawIndexCreator);
    }

    PinotDataBuffer tempBuffer;
    long tempBufferSize = _docSizeIndex.get(_aggregatedDocCount);

    if (tempBufferSize > MMAP_SIZE_THRESHOLD) {
      File tempFile = new File(_outDir, 0 + "_" + _aggregatedDocCount + ".unique.tmp");
      try (FileChannel src = new RandomAccessFile(_dataFile, "r").getChannel();
          FileChannel dest = new RandomAccessFile(tempFile, "rw").getChannel()) {
        long numBytesTransferred = src.transferTo(_docSizeIndex.get(0), tempBufferSize, dest);
        Preconditions.checkState(numBytesTransferred == tempBufferSize,
            "Error transferring data from data file to temp file, transfer size mis-match");
        dest.force(false);
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }
      tempBuffer =
          PinotDataBuffer.mapFile(tempFile, false, _docSizeIndex.get(0), tempBufferSize, PinotDataBuffer.NATIVE_ORDER,
              "OffHeapStarTreeBuilder#getUniqueCombinations: temp buffer");
    } else {
      tempBuffer =
          PinotDataBuffer.loadFile(_dataFile, _docSizeIndex.get(0), tempBufferSize, PinotDataBuffer.NATIVE_ORDER,
              "OffHeapStarTreeBuilder#getUniqueCombinations: temp buffer");
    }

    final StarTreeV2DataTable dataTable = new StarTreeV2DataTable(tempBuffer, _dimensionSize, 0);
    _dataTablesToClose.add(dataTable);

    int sortedDocsId[] = new int[_aggregatedDocCount];
    for (int i = 0; i < _aggregatedDocCount; i++) {
      sortedDocsId[i] = i;
    }

    final Iterator<Pair<byte[], byte[]>> _iterator =
        dataTable.iterator(0, _aggregatedDocCount, _docSizeIndex, sortedDocsId);

    int docId = 0;
    while (_iterator.hasNext()) {

      Pair<byte[], byte[]> next = _iterator.next();
      byte[] dimensionBytes = next.getLeft();

      // indexing dimensions.
      ByteBuffer buffer = ByteBuffer.wrap(dimensionBytes).order(PinotDataBuffer.NATIVE_ORDER);
      for (int j = 0; j < _dimensionsCount; j++) {
        int val = buffer.getInt();
        val = (val == StarTreeV2Constant.SKIP_VALUE) ? StarTreeV2Constant.VALID_INDEX_VALUE : val;
        ((SingleValueForwardIndexCreator) dimensionForwardIndexCreatorList.get(j)).index(docId, val);
      }

      // indexing AggFunColumn Pair data.
      AggregationFunctionColumnPairBuffer aggregationFunctionColumnPairBuffer =
          AggregationFunctionColumnPairBuffer.fromBytes(next.getRight(), _aggregationFunctions);
      Object[] values = aggregationFunctionColumnPairBuffer._values;
      for (int j = 0; j < _aggFunColumnPairsCount; j++) {
        AggregationFunction function = _aggregationFunctions.get(j);
        if (function.getResultDataType().equals(FieldSpec.DataType.BYTES)) {
          ((SingleValueRawIndexCreator) aggFunColumnPairForwardIndexCreatorList.get(j)).index(docId,
              function.serialize(values[j]));
        } else {
          ((SingleValueRawIndexCreator) aggFunColumnPairForwardIndexCreatorList.get(j)).index(docId, values[j]);
        }
      }
      docId++;
    }

    // closing all the opened index creator.
    for (int i = 0; i < dimensionForwardIndexCreatorList.size(); i++) {
      dimensionForwardIndexCreatorList.get(i).close();
    }

    for (int i = 0; i < aggFunColumnPairForwardIndexCreatorList.size(); i++) {
      aggFunColumnPairForwardIndexCreatorList.get(i).close();
    }

    return;
  }

  /**
   * Helper method to combine all the files to one
   */
  private void combineIndexesFiles(int starTreeId) throws Exception {
    StarTreeIndexesConverter converter = new StarTreeIndexesConverter();
    converter.convert(_outDir, starTreeId);

    return;
  }

  /**
   * function to condense documents according to sorted order.
   */
  protected Iterator<Pair<DimensionBuffer, AggregationFunctionColumnPairBuffer>> condenseData(File dataFile,
      int startDocId, int endDocId, List<Long> docSizeIndex, boolean isRawData, int dimensionIdToRemove)
      throws IOException {

    long tempBufferSize = docSizeIndex.get(endDocId) - docSizeIndex.get(startDocId);
    PinotDataBuffer tempBuffer;
    if (tempBufferSize > MMAP_SIZE_THRESHOLD) {

      File tempFile = new File(_outDir, startDocId + "_" + endDocId + ".unique.tmp");
      try (FileChannel src = new RandomAccessFile(dataFile, "r").getChannel();
          FileChannel dest = new RandomAccessFile(tempFile, "rw").getChannel()) {
        long numBytesTransferred = src.transferTo(docSizeIndex.get(startDocId), tempBufferSize, dest);
        Preconditions.checkState(numBytesTransferred == tempBufferSize,
            "Error transferring data from data file to temp file, transfer size mis-match");
        dest.force(false);
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }
      tempBuffer = PinotDataBuffer.mapFile(tempFile, false, docSizeIndex.get(startDocId), tempBufferSize,
          PinotDataBuffer.NATIVE_ORDER, "OffHeapStarTreeBuilder#getUniqueCombinations: temp buffer");
    } else {
      tempBuffer =
          PinotDataBuffer.loadFile(dataFile, docSizeIndex.get(startDocId), tempBufferSize, PinotDataBuffer.NATIVE_ORDER,
              "OffHeapStarTreeBuilder#getUniqueCombinations: temp buffer");
    }

    final StarTreeV2DataTable dataTable = new StarTreeV2DataTable(tempBuffer, _dimensionSize, startDocId);
    _dataTablesToClose.add(dataTable);

    if (!isRawData) {
      dataTable.setDimensionValue(startDocId, endDocId, dimensionIdToRemove, StarTreeV2Constant.STAR_NODE,
          docSizeIndex);
    }

    int[] sortedDocsId = dataTable.sort(startDocId, endDocId, _sortOrder, docSizeIndex);

    return new Iterator<Pair<DimensionBuffer, AggregationFunctionColumnPairBuffer>>() {
      final Iterator<Pair<byte[], byte[]>> _iterator =
          dataTable.iterator(startDocId, endDocId, docSizeIndex, sortedDocsId);

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
          AggregationFunctionColumnPairBuffer aggregationFunctionColumnPairBuffer = null;
          try {
            aggregationFunctionColumnPairBuffer =
                AggregationFunctionColumnPairBuffer.fromBytes(next.getRight(), _aggregationFunctions);
          } catch (IOException e) {
            LOGGER.info("failed in getting objects from bytes array");
          }

          if (_currentDimensions == null) {
            _currentDimensions = DimensionBuffer.fromBytes(dimensionBytes);
            _currentMetrics = aggregationFunctionColumnPairBuffer;
          } else {
            if (_currentDimensions.hasSameBytes(dimensionBytes)) {
              _currentMetrics.aggregate(aggregationFunctionColumnPairBuffer);
            } else {
              ImmutablePair<DimensionBuffer, AggregationFunctionColumnPairBuffer> ret =
                  new ImmutablePair<>(_currentDimensions, _currentMetrics);
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
        PinotDataBuffer.mapFile(_dataFile, true, 0, _dataFile.length(), PinotDataBuffer.NATIVE_ORDER,
            "OffHeapStarV2TreeBuilder#createAggregatedDocForAllNodes: data buffer"), _dimensionSize, 0)) {

      DimensionBuffer dimensions = new DimensionBuffer(_dimensionsCount);
      for (int i = 0; i < _dimensionsCount; i++) {
        dimensions.setDictId(i, StarTreeV2Constant.SKIP_VALUE);
      }
      createAggregatedDocForAllNodesHelper(dataTable, _rootNode, dimensions);
    }
    _outputStream.flush();
  }

  /**
   * Helper function to create aggregated document from children for a parent node.
   */
  private AggregationFunctionColumnPairBuffer createAggregatedDocForAllNodesHelper(StarTreeV2DataTable dataTable,
      TreeNode node, DimensionBuffer dimensions) throws IOException {
    AggregationFunctionColumnPairBuffer aggregatedMetrics = null;

    int index = 0;
    int[] sortedDocIds = new int[node._endDocId - node._startDocId];
    for (int i = node._startDocId; i < node._endDocId; i++) {
      sortedDocIds[index] = i;
      index++;
    }

    if (node._children == null) {
      Iterator<Pair<byte[], byte[]>> iterator =
          dataTable.iterator(node._startDocId, node._endDocId, _docSizeIndex, sortedDocIds);
      Pair<byte[], byte[]> first = iterator.next();
      aggregatedMetrics = AggregationFunctionColumnPairBuffer.fromBytes(first.getRight(), _aggregationFunctions);

      while (iterator.hasNext()) {
        Pair<byte[], byte[]> next = iterator.next();
        AggregationFunctionColumnPairBuffer metricBuffer =
            AggregationFunctionColumnPairBuffer.fromBytes(next.getRight(), _aggregationFunctions);
        aggregatedMetrics.aggregate(metricBuffer);
      }
      appendToAggBuffer(_aggregatedDocCount, dimensions, aggregatedMetrics);
      _docSizeIndex.add(_fileSize);

    } else {
      boolean hasStarChild = false;
      int starChildAggDocId = StarTreeV2Constant.STAR_NODE;
      int childDimensionId = node._childDimensionId;
      for (Map.Entry<Integer, TreeNode> entry : node._children.entrySet()) {
        int childDimensionValue = entry.getKey();
        TreeNode child = entry.getValue();
        dimensions.setDictId(childDimensionId, childDimensionValue);
        AggregationFunctionColumnPairBuffer childAggregatedMetrics =
            createAggregatedDocForAllNodesHelper(dataTable, child, dimensions);
        // Skip star node value when computing aggregate for the parent
        if (childDimensionValue != StarTreeV2Constant.STAR_NODE) {
          if (aggregatedMetrics == null) {
            aggregatedMetrics = childAggregatedMetrics;
          } else {
            aggregatedMetrics.aggregate(childAggregatedMetrics);
          }
        } else {
          hasStarChild = true;
          starChildAggDocId = entry.getValue()._aggDataDocumentId;
        }
      }
      dimensions.setDictId(childDimensionId, StarTreeV2Constant.STAR_NODE);

      // do  not create aggregated document for node with star child.
      if (hasStarChild) {
        node._aggDataDocumentId = starChildAggDocId;
      } else {
        appendToAggBuffer(_aggregatedDocCount, dimensions, aggregatedMetrics);
        _docSizeIndex.add(_fileSize);
      }

    }

    return aggregatedMetrics;
  }

  /**
   * compute a defualt split order.
   */
  private void appendToRawBuffer(int docId, DimensionBuffer dimensions,
      AggregationFunctionColumnPairBuffer aggregationFunctionColumnPairBuffer) throws IOException {
    PrintWriter outFileObject =
        new PrintWriter(new BufferedWriter(new FileWriter(_tempMetricOffsetFile.getPath(), true)));
    outFileObject.println(Integer.toString(docId) + " " + Long.toString(_tempFileSize));
    outFileObject.close();

    int a = appendToBuffer(dimensions, aggregationFunctionColumnPairBuffer, _tempOutputStream);
    _tempDocSizeIndex.add(_tempFileSize);
    _tempFileSize += a;
  }

  /**
   * compute a defualt split order.
   */
  private void appendToAggBuffer(int docId, DimensionBuffer dimensions,
      AggregationFunctionColumnPairBuffer aggregationFunctionColumnPairBuffer) throws IOException {
    PrintWriter outFileObject = new PrintWriter(new BufferedWriter(new FileWriter(_metricOffsetFile.getPath(), true)));
    outFileObject.println(Integer.toString(docId) + " " + Long.toString(_fileSize));
    outFileObject.close();
    _aggregatedDocCount++;

    int a = appendToBuffer(dimensions, aggregationFunctionColumnPairBuffer, _outputStream);
    _fileSize += a;
  }

  private int appendToBuffer(DimensionBuffer dimensions,
      AggregationFunctionColumnPairBuffer aggregationFunctionColumnPairBuffer, BufferedOutputStream outputStream)
      throws IOException {
    outputStream.write(dimensions.toBytes(), 0, _dimensionSize);
    outputStream.write(aggregationFunctionColumnPairBuffer.toBytes(), 0,
        aggregationFunctionColumnPairBuffer._totalBytes);

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
