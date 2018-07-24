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

package com.linkedin.pinot.core.startreeV2;

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.data.readers.PinotSegmentColumnReader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegment;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.index.loader.IndexLoadingConfig;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.io.IOException;
import java.io.FileOutputStream;
import java.io.BufferedOutputStream;
import java.util.Set;
import org.apache.commons.configuration.PropertiesConfiguration;


public class OffHeapStarTreeV2Builder implements StarTreeV2Builder{

  // segment
  private File _outDir;
  private int _rawDocsCount;
  private SegmentMetadata _segmentMetadata;
  private ImmutableSegment _immutableSegment;
  private PropertiesConfiguration _properties;
  private IndexLoadingConfig _v3IndexLoadingConfig;

  // star tree
  private File _dataFile;
  private BufferedOutputStream _outputStream;

  // Dimensions
  private int _dimensionSize;
  private int _dimensionsCount;
  private List<String> _dimensionsName;
  private String _dimensionSplitOrderString;
  private List<Integer> _dimensionsCardinalty;
  private List<Integer> _dimensionsSplitOrder;
  private List<Integer> _dimensionsWithoutStarNode;
  private Map<String, DimensionFieldSpec> _dimensionsSpecMap;

  // Metrics
  private int _metricsCount;
  private Set<String> _metricsName;
  private int _aggfunColumnPairsCount;
  private String _aggfunColumnPairsString;
  private List<AggfunColumnPair> _aggfunColumnPairs;
  private Map<String, MetricFieldSpec> _metricsSpecMap;

  // General
  private int _nodesCount;
  private TreeNode _rootNode;
  private int _maxNumLeafRecords;
  private AggregationFunctionFactory _aggregationFunctionFactory;

  // Star Tree
  private int _starTreeCount = 0;
  private String _starTreeId = null;


  @Override
  public void init(File indexDir, StarTreeV2Config config) throws Exception {
    _dataFile = new File(indexDir, "star-tree.buf");
    _outputStream = new BufferedOutputStream(new FileOutputStream(_dataFile));

    // segment
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
    _dimensionSize = _dimensionsCount * Integer.BYTES;

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
    _aggfunColumnPairsString = "";
    _metricsName = new HashSet<>();
    _metricsSpecMap = new HashMap<>();
    _aggfunColumnPairs = config.getMetric2aggFuncPairs();
    _aggfunColumnPairsCount = _aggfunColumnPairs.size();
    List<String> _aggfunColumnPairsStringList = new ArrayList<>();
    for (AggfunColumnPair pair : _aggfunColumnPairs) {
      _metricsName.add(pair.getColumnName());
      _aggfunColumnPairsStringList.add(pair.getAggregatefunction().getName() + '_' + pair.getColumnName());
    }
    _aggfunColumnPairsString = String.join(", ", _aggfunColumnPairsStringList);
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


    // storing dimension cardinality for calculating default sorting order.
    _dimensionsCardinalty = new ArrayList<>();
    for (int i = 0; i < _dimensionsCount; i++) {
      String dimensionName = _dimensionsName.get(i);
      ImmutableDictionaryReader dictionary = _immutableSegment.getDictionary(dimensionName);
      _dimensionsCardinalty.add(dictionary.length());
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

      int[] dimensionValues = new int[_dimensionsCount];
      for (int j = 0; j < dimensionColumnReaders.size(); j++) {
        Integer dictId = dimensionColumnReaders.get(j).getDictionaryId(i);
        dimensionValues[j] = dictId;
      }
      
      for (int j = 0; j < _aggfunColumnPairsCount; j++) {
        String metricName = _aggfunColumnPairs.get(j).getColumnName();
        String aggfunc = _aggfunColumnPairs.get(j).getAggregatefunction().getName();
        if (aggfunc.equals(StarTreeV2Constant.AggregateFunctions.COUNT)) {
          metricRawValues.add(1L);
        } else {
          MetricFieldSpec metricFieldSpec = _metricsSpecMap.get(metricName);
          Object val = OffHeapStarTreeV2BuilderHelper.readHelper(metricColumnReaders.get(metricName), metricFieldSpec.getDataType(), i);
          metricRawValues.add(val);
        }
      }



    }

  }

  @Override
  public void serialize() throws Exception {

  }

  @Override
  public Map<String, String> getMetaData() {
    return null;
  }
}
