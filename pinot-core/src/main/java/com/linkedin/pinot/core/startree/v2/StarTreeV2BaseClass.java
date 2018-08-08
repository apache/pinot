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

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.data.readers.PinotSegmentColumnReader;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegment;
import com.linkedin.pinot.core.segment.index.loader.IndexLoadingConfig;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import com.linkedin.pinot.core.startree.OffHeapStarTree;
import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import org.apache.commons.configuration.PropertiesConfiguration;


public class StarTreeV2BaseClass {

  // Segment
  File _outDir;
  ImmutableSegment _immutableSegment;
  PropertiesConfiguration _properties;
  IndexLoadingConfig _v3IndexLoadingConfig;
  protected SegmentMetadata _segmentMetadata;

  // Dimensions
  int _dimensionsCount;
  List<String> _dimensionsName;
  String _dimensionSplitOrderString;
  List<Integer> _dimensionsCardinality;
  List<Integer> _dimensionsSplitOrder;
  String _dimensionWithoutStarNodeString;
  List<Integer> _dimensionsWithoutStarNode;
  Map<String, DimensionFieldSpec> _dimensionsSpecMap;

  // Metrics
  int _metricsCount;
  Set<String> _metricsName;
  int _aggFunColumnPairsCount;
  String _aggFunColumnPairsString;
  List<AggregationFunctionColumnPair> _aggFunColumnPairs;
  Map<String, MetricFieldSpec> _metricsSpecMap;

  // General
  int _nodesCount;
  int _rawDocsCount;
  TreeNode _rootNode;
  int _maxNumLeafRecords;

  protected final Charset UTF_8 = Charset.forName("UTF-8");

  /**
   * enumerate dimension set.
   */
  List<Integer> enumerateDimensions(List<String> dimensionNames, List<String> dimensionsOrder) {
    List<Integer> enumeratedDimensions = new ArrayList<>();
    if (dimensionsOrder != null) {
      for (String dimensionName : dimensionsOrder) {
        enumeratedDimensions.add(dimensionNames.indexOf(dimensionName));
      }
    }

    return enumeratedDimensions;
  }

  /**
   * compute a defualt split order.
   */
  void computeDefaultSplitOrder(List<Integer> dimensionCardinality) {

    if (_dimensionsSplitOrder.isEmpty() || _dimensionsSplitOrder == null) {

      for (int i = 0; i < _dimensionsCount; i++) {
        _dimensionsSplitOrder.add(i);
      }

      Collections.sort(_dimensionsSplitOrder, new Comparator<Integer>() {
        @Override
        public int compare(Integer o1, Integer o2) {
          return dimensionCardinality.get(o2) - dimensionCardinality.get(o1);
        }
      });
    }

    // creating a string variable for meta data (dimensionSplitOrderString)
    List<String> dimensionSplitOrderStringList = new ArrayList<>();
    for (int i = 0; i < _dimensionsSplitOrder.size(); i++) {
      dimensionSplitOrderStringList.add(_dimensionsName.get(_dimensionsSplitOrder.get(i)));
    }
    _dimensionSplitOrderString = String.join(",", dimensionSplitOrderStringList);

    // creating a string variable for meta data (dimensionWithoutStarNodeString)
    List<String> dimensionWithoutStarNodeStringList = new ArrayList<>();
    for (int i = 0; i < _dimensionsWithoutStarNode.size(); i++) {
      dimensionWithoutStarNodeStringList.add(_dimensionsName.get(_dimensionsWithoutStarNode.get(i)));
    }
    _dimensionWithoutStarNodeString = String.join(",", dimensionWithoutStarNodeStringList);
  }

  /**
   * compute a defualt split order.
   */
  Object readHelper(PinotSegmentColumnReader reader, FieldSpec.DataType dataType, int docId) {
    switch (dataType) {
      case INT:
        return reader.readInt(docId);
      case FLOAT:
        return reader.readFloat(docId);
      case LONG:
        return reader.readLong(docId);
      case DOUBLE:
        return reader.readDouble(docId);
      case STRING:
        return reader.readString(docId);
    }

    return null;
  }

  /**
   * Helper method to compute size of the header of the star tree in bytes.
   */
  int computeHeaderSizeInBytes(List<String> dimensionsName) {
    // Magic marker (8), version (4), size of header (4) and number of dimensions (4)
    int headerSizeInBytes = 20;

    for (String dimension : dimensionsName) {
      headerSizeInBytes += Integer.BYTES;  // For dimension index
      headerSizeInBytes += Integer.BYTES;  // For length of dimension name
      headerSizeInBytes += dimension.getBytes(UTF_8).length; // For dimension name
    }

    headerSizeInBytes += Integer.BYTES; // For number of nodes.
    return headerSizeInBytes;
  }

  /**
   * Helper method to write the header into the data buffer.
   */
  long writeHeader(PinotDataBuffer dataBuffer, int headerSizeInBytes, int dimensionCount, List<String> dimensionsName,
      int nodesCount) {
    long offset = 0L;
    dataBuffer.putLong(offset, OffHeapStarTree.MAGIC_MARKER);
    offset += Long.BYTES;

    dataBuffer.putInt(offset, OffHeapStarTree.VERSION);
    offset += Integer.BYTES;

    dataBuffer.putInt(offset, headerSizeInBytes);
    offset += Integer.BYTES;

    dataBuffer.putInt(offset, dimensionCount);
    offset += Integer.BYTES;

    for (int i = 0; i < dimensionCount; i++) {
      String dimensionName = dimensionsName.get(i);

      dataBuffer.putInt(offset, i);
      offset += Integer.BYTES;

      byte[] dimensionBytes = dimensionName.getBytes(UTF_8);
      int dimensionLength = dimensionBytes.length;
      dataBuffer.putInt(offset, dimensionLength);
      offset += Integer.BYTES;

      dataBuffer.readFrom(offset, dimensionBytes, 0, dimensionLength);
      offset += dimensionLength;
    }

    dataBuffer.putInt(offset, nodesCount);
    offset += Integer.BYTES;

    return offset;
  }

  /**
   * Helper method to write the star tree nodes into the data buffer.
   */
  void writeNodes(PinotDataBuffer dataBuffer, long offset, TreeNode rootNode) {
    int index = 0;
    Queue<TreeNode> queue = new LinkedList<>();
    queue.add(rootNode);

    while (!queue.isEmpty()) {
      TreeNode node = queue.remove();

      if (node._children == null) {
        offset =
            writeNode(dataBuffer, node, offset, StarTreeV2Constant.INVALID_INDEX, StarTreeV2Constant.INVALID_INDEX);
      } else {
        int startChildrenIndex = index + queue.size() + 1;
        int endChildrenIndex = startChildrenIndex + node._children.size() - 1;
        offset = writeNode(dataBuffer, node, offset, startChildrenIndex, endChildrenIndex);

        queue.addAll(node._children.values());
      }

      index++;
    }
  }

  /**
   * Helper method to write one node into the data buffer.
   */
  private long writeNode(PinotDataBuffer dataBuffer, TreeNode node, long offset, int startChildrenIndex,
      int endChildrenIndex) {
    dataBuffer.putInt(offset, node._dimensionId);
    offset += Integer.BYTES;

    dataBuffer.putInt(offset, node._dimensionValue);
    offset += Integer.BYTES;

    dataBuffer.putInt(offset, node._startDocId);
    offset += Integer.BYTES;

    dataBuffer.putInt(offset, node._endDocId);
    offset += Integer.BYTES;

    dataBuffer.putInt(offset, node._aggDataDocumentId);
    offset += Integer.BYTES;

    dataBuffer.putInt(offset, startChildrenIndex);
    offset += Integer.BYTES;

    dataBuffer.putInt(offset, endChildrenIndex);
    offset += Integer.BYTES;

    return offset;
  }

  static File findFormatFile(File indexDir, String fileName) {

    // Try to find v3 file first
    File v3Dir = new File(indexDir, "v3");
    File v3File = new File(v3Dir, fileName);
    if (v3File.exists()) {
      return v3File;
    }

    // If cannot find v3 file, try to find v1 file instead
    File v1File = new File(indexDir, fileName);
    if (v1File.exists()) {
      return v1File;
    }

    return null;
  }
}
