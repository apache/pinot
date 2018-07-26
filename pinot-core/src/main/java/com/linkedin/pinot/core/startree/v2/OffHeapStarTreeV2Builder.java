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
import java.util.List;
import java.util.HashMap;
import java.util.HashSet;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.io.PrintWriter;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.BufferedOutputStream;
import com.linkedin.pinot.common.segment.ReadMode;
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


public class OffHeapStarTreeV2Builder extends StarTreeV2BaseClass implements StarTreeV2Builder {

  // general stuff
  private int _fileSize;
  private int[] _sortOrder;
  private File _metricOffsetFile;


  // star tree.
  private File _dataFile;
  private int _starTreeCount = 0;
  private String _starTreeId = null;
  private BufferedOutputStream _outputStream;


  @Override
  public void init(File indexDir, StarTreeV2Config config) throws Exception {
    _fileSize = 0;
    _dataFile = new File(indexDir, "star-tree.buf");
    _metricOffsetFile = new File(indexDir, "metric-offset-file.txt");
    _outputStream = new BufferedOutputStream(new FileOutputStream(_dataFile));

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

    File metadataFile = StarTreeV2Util.findFormatFile(indexDir, V1Constants.MetadataKeys.METADATA_FILE_NAME);
    _properties = new PropertiesConfiguration(metadataFile);
  }

  @Override
  public void build() throws IOException {

    _metricOffsetFile.createNewFile();

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
        dimensions.setDictId(j,dictId);
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

    _sortOrder = new int[_dimensionsCount];
    for (int i = 0; i < _dimensionsCount; i++) {
      _sortOrder[i] = _dimensionsSplitOrder.get(i);
    }

    // Sort the documents
    try (StarTreeV2DataTable dataTable = new StarTreeV2DataTable(
        PinotDataBuffer.mapFile(_dataFile, false, 0, _dataFile.length(), PinotDataBuffer.NATIVE_ORDER,
            "OffHeapStarTreeV2Builder#build: data buffer"), _dimensionSize, 0, 0)) {
      //dataTable.sort(0, _rawDocsCount, _sortOrder);
    }


//    // sorting the data as per the sort order.
//    List<Record> rawSortedStarTreeData = OnHeapStarTreeV2BuilderHelper.sortStarTreeData(0, _rawDocsCount, _dimensionsSplitOrder, _rawStarTreeData);
//    _starTreeData = OnHeapStarTreeV2BuilderHelper.condenseData(rawSortedStarTreeData, _aggFunColumnPairs, StarTreeV2Constant.IS_RAW_DATA);

//    // Recursively construct the star tree
//    _rootNode._startDocId = 0;
//    _rootNode._endDocId = _starTreeData.size();
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
   * compute a defualt split order.
   */
  private void appendToRawBuffer(int docId, DimensionBuffer dimensions, AggregationFunctionColumnPairBuffer aggregationFunctionColumnPairBuffer) throws IOException {
    appendToBuffer(docId, dimensions, aggregationFunctionColumnPairBuffer);
  }

  /**
   * compute a defualt split order.
   */
  private void appendToAggBuffer(int docId, DimensionBuffer dimensions, AggregationFunctionColumnPairBuffer aggregationFunctionColumnPairBuffer) throws IOException {
    appendToBuffer(docId, dimensions, aggregationFunctionColumnPairBuffer);
  }

  /**
   * compute a defualt split order.
   */
  private void appendToBuffer(int docId, DimensionBuffer dimensions, AggregationFunctionColumnPairBuffer aggregationFunctionColumnPairBuffer) throws IOException {
    _outputStream.write(dimensions.toBytes(), 0, _dimensionSize);
    _outputStream.write(aggregationFunctionColumnPairBuffer.toBytes(), 0, aggregationFunctionColumnPairBuffer._totalBytes);
    PrintWriter outFileObject = new PrintWriter(new BufferedWriter(new FileWriter(_metricOffsetFile.getPath(), true)));
    outFileObject.println(Integer.toString(docId) + " " + Integer.toString(_fileSize));
    outFileObject.close();
    _fileSize += _dimensionSize + aggregationFunctionColumnPairBuffer._totalBytes;
  }
}
