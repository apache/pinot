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

import java.io.File;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.nio.ByteOrder;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.channels.FileChannel;
import org.apache.commons.io.FileUtils;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.startree.StarTree;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.startree.OffHeapStarTree;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.common.segment.StarTreeV2Metadata;
import com.linkedin.pinot.core.segment.memory.PinotByteBuffer;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegment;


public class StarTreeV2DataSource {

  private int _docsCount;
  private File _indexDataFile;
  private List<String> _met2aggfuncPairs;
  private List<String> _dimensionsSplitOrder;
  private SegmentMetadataImpl _segmentMetadataImpl;
  private Map<String, Integer> _columnIndexInfoMap;
  private AggregationFunctionFactory _aggregationFunctionFactory;
  private Map<String, StarTreeV2DimensionDataSource> _dimensionIndexReader;
  private Map<String, StarTreeV2AggfunColumnPairDataSource> _metricRawIndexReader;

  private File _starTreeFile;
  private File _starTreeIndexMapFile;

  public StarTreeV2DataSource(SegmentMetadataImpl segmentMetadataImpl, StarTreeV2Metadata metadata, File indexDir) {

    _starTreeIndexMapFile = StarTreeV2Util.findFormatFile(indexDir, StarTreeV2Constant.STAR_TREE_V2_INDEX_MAP_FILE);
    _columnIndexInfoMap = OnHeapStarTreeV2LoaderHelper.readMetaData(_starTreeIndexMapFile);

    _starTreeFile = new File(indexDir, StarTreeV2Constant.STAR_TREE_V2_TEMP_FILE);
    _indexDataFile = StarTreeV2Util.findFormatFile(indexDir, StarTreeV2Constant.STAR_TREE_V2_COlUMN_FILE);
    ;

    _docsCount = metadata.getDocsCount();
    _met2aggfuncPairs = metadata.getMet2AggfuncPairs();
    _dimensionsSplitOrder = metadata.getDimensionsSplitOrder();

    _segmentMetadataImpl = segmentMetadataImpl;

    _dimensionIndexReader = new HashMap<>();
    _metricRawIndexReader = new HashMap<>();

    _aggregationFunctionFactory = new AggregationFunctionFactory();
  }

  public StarTree loadStarTree(int starTreeId) throws IOException {

    String sa = "startree" + starTreeId + ".root.start";
    String sb = "startree" + starTreeId + ".root.size";

    int start = _columnIndexInfoMap.get(sa);
    long size = _columnIndexInfoMap.get(sb);

    FileChannel src = new FileInputStream(_indexDataFile).getChannel();
    FileChannel dest = new FileOutputStream(_starTreeFile).getChannel();
    src.transferTo(start, size, dest);

    src.close();
    dest.close();

    StarTree s = new OffHeapStarTree(_starTreeFile, ReadMode.mmap);
    FileUtils.deleteQuietly(_starTreeFile);

    return s;
  }

  public void loadColumnsDataSource(int starTreeId, ImmutableSegment obj) throws IOException {

    for (String dimension : _dimensionsSplitOrder) {
      String a = "startree" + starTreeId + "." + dimension + ".start";
      String b = "startree" + starTreeId + "." + dimension + ".size";

      int start = _columnIndexInfoMap.get(a);
      long size = _columnIndexInfoMap.get(b);
      ColumnMetadata columnMetadata = _segmentMetadataImpl.getColumnMetadataFor(dimension);
      int maxNumberOfBits = columnMetadata.getBitsPerElement();
      PinotDataBuffer buffer =
          PinotDataBuffer.mapFile(_indexDataFile, false, start, size, ByteOrder.BIG_ENDIAN,
              "Star Tree V2");
      StarTreeV2DimensionDataSource starTreeV2DimensionDataSource =
          new StarTreeV2DimensionDataSource(buffer, dimension, obj, columnMetadata, _docsCount, maxNumberOfBits);
      _dimensionIndexReader.put(dimension, starTreeV2DimensionDataSource);
    }

    AggregationFunction function;
    for (String pair : _met2aggfuncPairs) {
      String a = "startree" + starTreeId + "." + pair + ".start";
      String b = "startree" + starTreeId + "." + pair + ".size";
      int start = _columnIndexInfoMap.get(a);
      long size = _columnIndexInfoMap.get(b);

      String parts[] = pair.split("_");
      function = _aggregationFunctionFactory.getAggregationFunction(parts[0]);

      PinotDataBuffer buffer;
      if (function.getDataType().equals(FieldSpec.DataType.BYTES)) {
        buffer = PinotByteBuffer.mapFile(_indexDataFile, false, start, size, ByteOrder.BIG_ENDIAN,
                "star tree v2");
      } else {
        buffer = PinotDataBuffer.mapFile(_indexDataFile, false, start, size, ByteOrder.BIG_ENDIAN,
                "star tree v2");
      }
      StarTreeV2AggfunColumnPairDataSource starTreeV2AggfunColumnPairDataSource =
          new StarTreeV2AggfunColumnPairDataSource(buffer, pair, _docsCount, function.getDataType());

      _metricRawIndexReader.put(pair, starTreeV2AggfunColumnPairDataSource);
    }

    return;
  }

  public Map<String, StarTreeV2DimensionDataSource> getDimensionForwardIndexReader() {
    return _dimensionIndexReader;
  }

  public Map<String, StarTreeV2AggfunColumnPairDataSource> getMetricRawIndexReader() {
    return _metricRawIndexReader;
  }
}
