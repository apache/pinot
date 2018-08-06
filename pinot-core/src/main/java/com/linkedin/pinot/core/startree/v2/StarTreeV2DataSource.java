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
import java.util.Set;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.nio.ByteOrder;
import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.channels.FileChannel;
import org.apache.commons.io.FileUtils;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.startree.StarTree;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.startree.OffHeapStarTree;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.memory.PinotByteBuffer;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegment;


public class StarTreeV2DataSource {

  private int _docsCount;
  private File _indexDataFile;
  private List<String> _dimensionsSplitOrder;
  private SegmentMetadataImpl _segmentMetadataImpl;
  private Map<String, Integer> _columnIndexInfoMap;
  private Set<AggregationFunctionColumnPair> _aggFunColPairs;
  private AggregationFunctionFactory _aggregationFunctionFactory;
  private Map<String, StarTreeV2DimensionDataSource> _dimensionIndexReader;
  private Map<String, StarTreeV2AggfunColumnPairDataSource> _metricRawIndexReader;

  private File _starTreeFile;

  public StarTreeV2DataSource(SegmentMetadataImpl segmentMetadataImpl, StarTreeV2Metadata metadata, File indexDir) {

    File _starTreeIndexMapFile =
        StarTreeV2BaseClass.findFormatFile(indexDir, StarTreeV2Constant.STAR_TREE_V2_INDEX_MAP_FILE);
    _columnIndexInfoMap = readMetaData(_starTreeIndexMapFile);

    _starTreeFile = new File(indexDir, StarTreeV2Constant.STAR_TREE_V2_TEMP_FILE);
    _indexDataFile = StarTreeV2BaseClass.findFormatFile(indexDir, StarTreeV2Constant.STAR_TREE_V2_COlUMN_FILE);
    ;

    _docsCount = metadata.getNumDocs();
    _aggFunColPairs = metadata.getAggregationFunctionColumnPairs();
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
          PinotDataBuffer.mapFile(_indexDataFile, false, start, size, ByteOrder.BIG_ENDIAN, "Star Tree V2");
      StarTreeV2DimensionDataSource starTreeV2DimensionDataSource =
          new StarTreeV2DimensionDataSource(buffer, dimension, obj, columnMetadata, _docsCount, maxNumberOfBits);
      _dimensionIndexReader.put(dimension, starTreeV2DimensionDataSource);
    }

    AggregationFunction function;
    for (AggregationFunctionColumnPair pair : _aggFunColPairs) {
      String column = pair.toColumnName();
      String a = "startree" + starTreeId + "." + column + ".start";
      String b = "startree" + starTreeId + "." + column + ".size";
      int start = _columnIndexInfoMap.get(a);
      long size = _columnIndexInfoMap.get(b);

      function = _aggregationFunctionFactory.getAggregationFunction(pair.getFunctionType().getName());

      PinotDataBuffer buffer;
      if (function.getResultDataType().equals(FieldSpec.DataType.BYTES)) {
        buffer = PinotByteBuffer.mapFile(_indexDataFile, false, start, size, ByteOrder.BIG_ENDIAN, "star tree v2");
      } else {
        buffer = PinotDataBuffer.mapFile(_indexDataFile, false, start, size, ByteOrder.BIG_ENDIAN, "star tree v2");
      }
      StarTreeV2AggfunColumnPairDataSource starTreeV2AggfunColumnPairDataSource =
          new StarTreeV2AggfunColumnPairDataSource(buffer, column, _docsCount, function.getResultDataType());

      _metricRawIndexReader.put(column, starTreeV2AggfunColumnPairDataSource);
    }
  }

  public Map<String, StarTreeV2DimensionDataSource> getDimensionForwardIndexReader() {
    return _dimensionIndexReader;
  }

  public Map<String, StarTreeV2AggfunColumnPairDataSource> getMetricRawIndexReader() {
    return _metricRawIndexReader;
  }

  /**
   * read meta data for star tree indexes.
   */
  private Map<String, Integer> readMetaData(File indexMapFile) {

    Map<String, Integer> metadata = new HashMap<>();
    try {
      FileReader fileReader = new FileReader(indexMapFile);
      BufferedReader bufferedReader = new BufferedReader(fileReader);
      String line;

      while ((line = bufferedReader.readLine()) != null) {
        String s = line.toString();
        String[] parts = s.split(":");
        metadata.put(parts[0], Integer.parseInt(parts[1]));
      }
      fileReader.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return metadata;
  }
}
