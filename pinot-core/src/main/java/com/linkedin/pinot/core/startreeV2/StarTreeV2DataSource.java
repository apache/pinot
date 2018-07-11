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
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.common.segment.StarTreeV2Metadata;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.io.reader.impl.v1.FixedBitSingleValueReader;
import com.linkedin.pinot.core.io.reader.impl.v1.FixedByteChunkSingleValueReader;


public class StarTreeV2DataSource {

  int _docsCount;
  File _indexDataFile;
  DataSource _dataSource;
  List<String> _met2aggfuncPairs;
  List<String> _dimensionsSplitOrder;
  SegmentMetadataImpl _segmentMetadata;
  Map<String, Integer> _columnIndexInfoMap;
  Map<String, FixedBitSingleValueReader> _dimensionIndexReader;
  Map<String, FixedByteChunkSingleValueReader> _metricRawIndexReader;

  public StarTreeV2DataSource(SegmentMetadataImpl segmentMetadata, StarTreeV2Metadata metadata, Map<String, Integer> columnIndexInfoMap, File indexDataFile) {
    _indexDataFile = indexDataFile;
    _columnIndexInfoMap = columnIndexInfoMap;

    _docsCount = metadata.getDocsCount();
    _met2aggfuncPairs = metadata.getMet2AggfuncPairs();
    _dimensionsSplitOrder = metadata.getDimensionsSplitOrder();

    _segmentMetadata = segmentMetadata;
    _dataSource = new DataSource(_indexDataFile);

    _dimensionIndexReader = new HashMap<>();
    _metricRawIndexReader = new HashMap<>();
  }

  public void loadDataSource(int starTreeId) throws IOException {

    for (String dimension: _dimensionsSplitOrder) {
      String a = "startree" + starTreeId + "." + dimension + ".start";
      String b = "startree" + starTreeId + "." + dimension + ".size";

      int start = _columnIndexInfoMap.get(a);
      int size = _columnIndexInfoMap.get(b);
      ColumnMetadata columnMetadata = _segmentMetadata.getColumnMetadataFor(dimension);
      FixedBitSingleValueReader svFwdIndex = _dataSource.getDimensionDataSource(start, size, _docsCount, columnMetadata.getBitsPerElement());

      _dimensionIndexReader.put(dimension, svFwdIndex);
    }

    for (String pair: _met2aggfuncPairs) {
      String a = "startree" + starTreeId + "." + pair + ".start";
      String b = "startree" + starTreeId + "." + pair + ".size";
      int start = _columnIndexInfoMap.get(a);
      int size = _columnIndexInfoMap.get(b);
      FixedByteChunkSingleValueReader svRawIndex = _dataSource.getMetricDataSource(start, size);

      _metricRawIndexReader.put(pair, svRawIndex);
    }

    return;
  }

  public Map<String, FixedBitSingleValueReader> getDimensionForwardIndexReader() {
    return _dimensionIndexReader;
  }

  public Map<String, FixedByteChunkSingleValueReader> getMetricRawIndexReader() {
    return _metricRawIndexReader;
  }
}
