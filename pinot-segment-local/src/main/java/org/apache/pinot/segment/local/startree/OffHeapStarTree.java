/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.local.startree;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.startree.StarTree;
import org.apache.pinot.segment.spi.index.startree.StarTreeNode;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * The {@code OffHeapStarTree} class implements the star-tree using off-heap memory.
 */
public class OffHeapStarTree implements StarTree {
  public static final long MAGIC_MARKER = 0xBADDA55B00DAD00DL;
  public static final int VERSION = 1;

  private final OffHeapStarTreeNode _root;
  private final List<String> _dimensionNames;

  public OffHeapStarTree(PinotDataBuffer dataBuffer) {
    long offset = 0L;
    Preconditions
        .checkState(MAGIC_MARKER == dataBuffer.getLong(offset), "Invalid magic marker in star-tree data buffer");
    offset += Long.BYTES;

    Preconditions.checkState(VERSION == dataBuffer.getInt(offset), "Invalid version in star-tree data buffer");
    offset += Integer.BYTES;

    int rootNodeOffset = dataBuffer.getInt(offset);
    offset += Integer.BYTES;

    int numDimensions = dataBuffer.getInt(offset);
    offset += Integer.BYTES;

    String[] dimensionNames = new String[numDimensions];
    for (int i = 0; i < numDimensions; i++) {
      // NOTE: this is for backward-compatibility. In old version, index might not be stored in order
      int dimensionId = dataBuffer.getInt(offset);
      offset += Integer.BYTES;

      int numBytes = dataBuffer.getInt(offset);
      offset += Integer.BYTES;
      byte[] bytes = new byte[numBytes];
      dataBuffer.copyTo(offset, bytes);
      offset += numBytes;
      dimensionNames[dimensionId] = new String(bytes, UTF_8);
    }
    _dimensionNames = Arrays.asList(dimensionNames);

    int numNodes = dataBuffer.getInt(offset);
    offset += Integer.BYTES;
    Preconditions.checkState(offset == rootNodeOffset, "Error loading star-tree, header length mis-match");
    long bufferSize = dataBuffer.size();
    Preconditions.checkState(offset + numNodes * OffHeapStarTreeNode.SERIALIZABLE_SIZE_IN_BYTES == bufferSize,
        "Error loading star-tree, buffer size mis-match");

    _root = new OffHeapStarTreeNode(dataBuffer.view(rootNodeOffset, bufferSize), 0);
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

    String formattedOutput = MoreObjects.toStringHelper(node).add("level", level).add("dimensionName", dimensionName)
        .add("dimensionValue", dimensionValueString).add("childDimensionName", childDimensionName)
        .add("startDocId", node.getStartDocId()).add("endDocId", node.getEndDocId())
        .add("aggregatedDocId", node.getAggregatedDocId()).add("numChildren", node.getNumChildren()).toString();
    stringBuilder.append(formattedOutput);
    System.out.println(stringBuilder.toString());

    if (!node.isLeaf()) {
      Iterator<OffHeapStarTreeNode> childrenIterator = node.getChildrenIterator();
      while (childrenIterator.hasNext()) {
        printTreeHelper(dictionaryMap, childrenIterator.next(), level + 1);
      }
    }
  }
}
