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
import java.io.IOException;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.common.segment.StarTreeV2Metadata;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegment;


public class StarTreeV2DataSource {

  int _docsCount;
  File _indexDataFile;
  List<String> _met2aggfuncPairs;
  List<String> _dimensionsSplitOrder;
  ImmutableSegment _immutableSegment;
  SegmentMetadataImpl _segmentMetadataImpl;
  Map<String, Integer> _columnIndexInfoMap;
  AggregationFunctionFactory _aggregationFunctionFactory;
  Map<String, StarTreeV2MetricDataSource> _metricRawIndexReader;
  Map<String, StarTreeV2DimensionDataSource> _dimensionIndexReader;


  public StarTreeV2DataSource(ImmutableSegment immutableSegment, SegmentMetadataImpl segmentMetadataImpl, StarTreeV2Metadata metadata, Map<String, Integer> columnIndexInfoMap, File indexDataFile) {
    _indexDataFile = indexDataFile;
    _columnIndexInfoMap = columnIndexInfoMap;

    _docsCount = metadata.getDocsCount();
    _met2aggfuncPairs = metadata.getMet2AggfuncPairs();
    _dimensionsSplitOrder = metadata.getDimensionsSplitOrder();

    _immutableSegment = immutableSegment;
    _segmentMetadataImpl = segmentMetadataImpl;

    _dimensionIndexReader = new HashMap<>();
    _metricRawIndexReader = new HashMap<>();

    _aggregationFunctionFactory = new AggregationFunctionFactory();
  }

  public void loadDataSource(int starTreeId) throws IOException {

    for (String dimension: _dimensionsSplitOrder) {
      String a = "startree" + starTreeId + "." + dimension + ".start";
      String b = "startree" + starTreeId + "." + dimension + ".size";

      int start = _columnIndexInfoMap.get(a);
      int size = _columnIndexInfoMap.get(b);
      ColumnMetadata columnMetadata = _segmentMetadataImpl.getColumnMetadataFor(dimension);
      int maxNumberOfBits = columnMetadata.getBitsPerElement();
      StarTreeV2DimensionDataSource starTreeV2DimensionDataSource = new StarTreeV2DimensionDataSource(_indexDataFile, dimension, _immutableSegment, columnMetadata, _docsCount, start, size, maxNumberOfBits);
      _dimensionIndexReader.put(dimension, starTreeV2DimensionDataSource);
    }

    AggregationFunction function;
    for (String pair: _met2aggfuncPairs) {
      String a = "startree" + starTreeId + "." + pair + ".start";
      String b = "startree" + starTreeId + "." + pair + ".size";
      int start = _columnIndexInfoMap.get(a);
      int size = _columnIndexInfoMap.get(b);

      String parts[] = pair.split("_");
      if (parts[0].equals(StarTreeV2Constant.AggregateFunctions.COUNT)) {
        function = _aggregationFunctionFactory.getAggregationFunction(parts[0]);
      } else {
        function = _aggregationFunctionFactory.getAggregationFunction(parts[1]);
      }

      StarTreeV2MetricDataSource starTreeV2MetricDataSource = new StarTreeV2MetricDataSource(_indexDataFile, pair, _docsCount, start, size, function.getDatatype());
      _metricRawIndexReader.put(pair, starTreeV2MetricDataSource);
    }

    return;
  }

  public DataSource getDimensionForwardIndexReader(String column) {
    return _dimensionIndexReader.get(column);
  }

  public DataSource getMetricRawIndexReader(String column) {
    return _metricRawIndexReader.get(column);
  }
}
