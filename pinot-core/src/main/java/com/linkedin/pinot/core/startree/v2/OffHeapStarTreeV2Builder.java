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

import com.linkedin.pinot.core.startree.MetricBuffer;
import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.io.Closeable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.io.PrintWriter;
import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.nio.channels.FileChannel;
import org.apache.commons.lang3.tuple.Pair;
import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.segment.ReadMode;
import org.apache.commons.lang3.tuple.ImmutablePair;
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
  private int[] _tempSortOrder;
  private List<Long> docSizeIndex;
  private List<Long> _tempDocSizeIndex;
  private static final long MMAP_SIZE_THRESHOLD = 500_000_000;



  // star tree.
  private File _dataFile;
  private File _tempDataFile;
  private File _metricOffsetFile;
  private File _tempMetricOffsetFile;

  private int _starTreeCount = 0;
  private String _starTreeId = null;
  private BufferedOutputStream _outputStream;
  private BufferedOutputStream _tempOutputStream;
  private final Set<StarTreeV2DataTable> _dataTablesToClose = new HashSet<>();

  @Override
  public void init(File indexDir, StarTreeV2Config config) throws Exception {
    _fileSize = 0;
    _tempFileSize = 0;
    _dataFile = new File(indexDir, "star-tree.buf");
    _tempDataFile = new File(indexDir, "star-tree-temp.buf");
    _metricOffsetFile = new File(indexDir, "metric-offset-file.txt");
    _tempMetricOffsetFile = new File(indexDir, "temp-metric-offset-file.txt");


    _outputStream = new BufferedOutputStream(new FileOutputStream(_dataFile));
    _tempOutputStream = new BufferedOutputStream(new FileOutputStream(_tempDataFile));


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
      _metricsName.add(pair.getColumnName());
      aggFunColumnPairsStringList.add(pair.getFunctionType().getName() + '_' + pair.getColumnName());
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
        String metricName = _aggFunColumnPairs.get(j).getColumnName();
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

    PrintWriter outFileObject = new PrintWriter(new BufferedWriter(new FileWriter(_tempMetricOffsetFile.getPath(), true)));
    outFileObject.println(Integer.toString(_rawDocsCount) + " " + Long.toString(_tempFileSize));
    outFileObject.close();

    _tempOutputStream.flush();
    computeDefaultSplitOrder(_dimensionsCardinality);

    _tempSortOrder = new int[_dimensionsCount];
    for (int i = 0; i < _dimensionsCount; i++) {
      _tempSortOrder[i] = _dimensionsSplitOrder.get(i);
    }

    // Sort the documents
    try (StarTreeV2DataTable dataTable = new StarTreeV2DataTable(PinotDataBuffer.mapFile(_tempDataFile, false, 0, _tempDataFile.length(), PinotDataBuffer.NATIVE_ORDER, "OffHeapStarTreeV2Builder#build: data buffer"), _dimensionSize)) {

      _tempDocSizeIndex = new ArrayList<>();
      FileReader fileReader = new FileReader(_tempMetricOffsetFile);
      BufferedReader bufferedReader = new BufferedReader(fileReader);
      String line;
      while ((line = bufferedReader.readLine()) != null) {
        String s = line.toString();
        String[] parts = s.split(" ");
        _tempDocSizeIndex.add(Long.parseLong(parts[1]));
      }
      fileReader.close();

      int[] sortedDocsId = dataTable.sort(0, _rawDocsCount, _tempSortOrder, _tempDocSizeIndex);
      Iterator<Pair<DimensionBuffer, AggregationFunctionColumnPairBuffer>> iterator = condenseData(_tempDataFile, 0, _rawDocsCount, sortedDocsId, _tempDocSizeIndex);

      while (iterator.hasNext()) {
        Pair<DimensionBuffer, AggregationFunctionColumnPairBuffer> next = iterator.next();
        DimensionBuffer dimensions = next.getLeft();
        AggregationFunctionColumnPairBuffer metrics = next.getRight();
      }

    }

    // Recursively construct the star tree
    _rootNode._startDocId = 0;
 //   _rootNode._endDocId = _starTreeData.size();
//    constructStarTree(_rootNode, 0, _starTreeData.size(), 0);
//
//    // create aggregated doc for all nodes.
//    createAggregatedDocForAllNodes(_rootNode);

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
   * function to condense documents according to sorted order.
   */
  protected Iterator<Pair<DimensionBuffer, AggregationFunctionColumnPairBuffer>> condenseData(File dataFile, int startDocId, int endDocId, int[] sortedDocsId, List<Long> docSizeIndex) throws IOException {

    long tempBufferSize = docSizeIndex.get(endDocId);
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
      tempBuffer = PinotDataBuffer.mapFile(tempFile, false, 0, tempBufferSize, PinotDataBuffer.NATIVE_ORDER, "OffHeapStarTreeBuilder#getUniqueCombinations: temp buffer");

    } else {
      tempBuffer = PinotDataBuffer.loadFile(dataFile, docSizeIndex.get(startDocId), tempBufferSize, PinotDataBuffer.NATIVE_ORDER,"OffHeapStarTreeBuilder#getUniqueCombinations: temp buffer");
    }

    final StarTreeV2DataTable dataTable = new StarTreeV2DataTable(tempBuffer, _dimensionSize);
    _dataTablesToClose.add(dataTable);


    return new Iterator<Pair<DimensionBuffer, AggregationFunctionColumnPairBuffer>>() {
      final Iterator<Pair<byte[], byte[]>> _iterator = dataTable.iterator(startDocId, endDocId, docSizeIndex, sortedDocsId );

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
   * compute a defualt split order.
   */
  private void appendToRawBuffer(int docId, DimensionBuffer dimensions, AggregationFunctionColumnPairBuffer aggregationFunctionColumnPairBuffer) throws IOException {
    PrintWriter outFileObject = new PrintWriter(new BufferedWriter(new FileWriter(_tempMetricOffsetFile.getPath(), true)));
    outFileObject.println(Integer.toString(docId) + " " + Long.toString(_tempFileSize));
    outFileObject.close();
    int a = appendToBuffer(dimensions, aggregationFunctionColumnPairBuffer, _tempOutputStream);
    _tempFileSize += a;
  }

  /**
   * compute a defualt split order.
   */
  private void appendToAggBuffer(int docId, DimensionBuffer dimensions, AggregationFunctionColumnPairBuffer aggregationFunctionColumnPairBuffer) throws IOException {
    PrintWriter outFileObject = new PrintWriter(new BufferedWriter(new FileWriter(_metricOffsetFile.getPath(), true)));
    outFileObject.println(Integer.toString(docId) + " " + Long.toString(_fileSize));
    outFileObject.close();
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
