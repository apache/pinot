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
package com.linkedin.pinot.core.startree;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


/**
 * The {@code OffHeapStarTree} class implements the star-tree using off-heap memory.
 */
public class OffHeapStarTree implements StarTree {
  public static final long MAGIC_MARKER = 0xBADDA55B00DAD00DL;
  public static final int VERSION = 1;

  private final PinotDataBuffer _dataBuffer;
  private final OffHeapStarTreeNode _root;
  private final List<String> _dimensionNames;

  public OffHeapStarTree(PinotDataBuffer dataBuffer) {
    _dataBuffer = dataBuffer;

    long offset = 0L;
    Preconditions.checkState(MAGIC_MARKER == _dataBuffer.getLong(offset), "Invalid magic marker in Star Tree file");
    offset += Long.BYTES;

    Preconditions.checkState(VERSION == _dataBuffer.getInt(offset), "Invalid version in Star Tree file");
    offset += Integer.BYTES;

    int rootNodeOffset = _dataBuffer.getInt(offset);
    offset += Integer.BYTES;

    int numDimensions = _dataBuffer.getInt(offset);
    offset += Integer.BYTES;

    String[] dimensionNames = new String[numDimensions];
    for (int i = 0; i < numDimensions; i++) {
      // NOTE: this is for backward-compatibility. In old version, index might not be stored in order
      int dimensionId = _dataBuffer.getInt(offset);
      offset += Integer.BYTES;

      int numBytes = _dataBuffer.getInt(offset);
      offset += Integer.BYTES;
      byte[] bytes = new byte[numBytes];
      _dataBuffer.copyTo(offset, bytes);
      offset += numBytes;
      dimensionNames[dimensionId] = StringUtil.decodeUtf8(bytes);
    }
    _dimensionNames = Arrays.asList(dimensionNames);

    int numNodes = _dataBuffer.getInt(offset);
    offset += Integer.BYTES;
    Preconditions.checkState(offset == rootNodeOffset, "Error reading Star Tree file, header length mis-match");
    long bufferSize = _dataBuffer.size();
    Preconditions.checkState(offset + numNodes * OffHeapStarTreeNode.SERIALIZABLE_SIZE_IN_BYTES == bufferSize,
        "Error reading Star Tree file, buffer size mis-match");

    _root = new OffHeapStarTreeNode(_dataBuffer.view(rootNodeOffset, bufferSize), 0);
  }

  public OffHeapStarTree(File starTreeFile, ReadMode readMode) throws IOException {
    this(getDataBuffer(starTreeFile, readMode));
  }

  private static PinotDataBuffer getDataBuffer(File starTreeFile, ReadMode readMode) throws IOException {
    // Backward-compatible: star-tree file is always little-endian
    if (readMode == ReadMode.mmap) {
      return PinotDataBuffer.mapFile(starTreeFile, true, 0, starTreeFile.length(), ByteOrder.LITTLE_ENDIAN,
          "OffHeapStarTree");
    } else {
      return PinotDataBuffer.loadFile(starTreeFile, 0, starTreeFile.length(), ByteOrder.LITTLE_ENDIAN,
          "OffHeapStarTree");
    }
  }

  @Override
  public StarTreeNode getRoot() {
    return _root;
  }

  @Override
  public List<String> getDimensionNames() {
    return _dimensionNames;
  }

  @Override
  public void printTree(Map<String, Dictionary> dictionaryMap) {
    printTreeHelper(dictionaryMap, _root, 0);
  }

  /**
   * Helper method to print the tree.
   */
  private void printTreeHelper(Map<String, Dictionary> dictionaryMap, OffHeapStarTreeNode node, int level) {
    StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < level; i++) {
      stringBuilder.append("  ");
    }
    String dimensionName = "ALL";
    int dimensionId = node.getDimensionId();
    if (dimensionId != StarTreeNode.ALL) {
      dimensionName = _dimensionNames.get(dimensionId);
    }
    String dimensionValueString = "ALL";
    int dimensionValue = node.getDimensionValue();
    if (dimensionValue != StarTreeNode.ALL) {
      dimensionValueString = dictionaryMap.get(dimensionName).get(dimensionValue).toString();
    }

    // For leaf node, child dimension id is -1
    String childDimensionName = "null";
    int childDimensionId = node.getChildDimensionId();
    if (childDimensionId != -1) {
      childDimensionName = _dimensionNames.get(childDimensionId);
    }

    String formattedOutput = MoreObjects.toStringHelper(node)
        .add("level", level)
        .add("dimensionName", dimensionName)
        .add("dimensionValue", dimensionValueString)
        .add("childDimensionName", childDimensionName)
        .add("startDocId", node.getStartDocId())
        .add("endDocId", node.getEndDocId())
        .add("aggregatedDocId", node.getAggregatedDocId())
        .add("numChildren", node.getNumChildren())
        .toString();
    stringBuilder.append(formattedOutput);
    System.out.println(stringBuilder.toString());

    if (!node.isLeaf()) {
      Iterator<OffHeapStarTreeNode> childrenIterator = node.getChildrenIterator();
      while (childrenIterator.hasNext()) {
        printTreeHelper(dictionaryMap, childrenIterator.next(), level + 1);
      }
    }
  }

  @Override
  public void close() throws IOException {
    _dataBuffer.close();
  }
}
