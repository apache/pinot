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
package com.linkedin.pinot.core.startree;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import xerial.larray.buffer.LBuffer;
import xerial.larray.buffer.LBufferAPI;
import xerial.larray.mmap.MMapBuffer;
import xerial.larray.mmap.MMapMode;


/**
 * LBuffer based implementation of star tree.
 */
public class OffHeapStarTree implements StarTree {
  public static final long MAGIC_MARKER = 0xBADDA55B00DAD00DL;
  public static final int VERSION = 1;

  private static final Charset UTF_8 = Charset.forName("UTF-8");
  private static final int DIMENSION_NAME_MAX_LENGTH = 4096;
  private static final int LOAD_FILE_BUFFER_SIZE = 10 * 1024 * 1024;

  private final LBufferAPI _dataBuffer;
  private final OffHeapStarTreeNode _root;
  private final List<String> _dimensionNames;

  /**
   * Constructor for the class.
   * - Reads in the header
   * - Loads/MMap's the OffHeapStarTreeNode array.
   */
  public OffHeapStarTree(File starTreeFile, ReadMode readMode) throws IOException {
    if (readMode.equals(ReadMode.mmap)) {
      _dataBuffer = new MMapBuffer(starTreeFile, MMapMode.READ_ONLY);
    } else {
      _dataBuffer = loadFrom(starTreeFile);
    }

    long offset = 0L;
    Preconditions.checkState(MAGIC_MARKER == _dataBuffer.getLong(offset), "Invalid magic marker in Star Tree file");
    offset += V1Constants.Numbers.LONG_SIZE;

    Preconditions.checkState(VERSION == _dataBuffer.getInt(offset), "Invalid version in Star Tree file");
    offset += V1Constants.Numbers.INTEGER_SIZE;

    int rootNodeOffset = _dataBuffer.getInt(offset);
    offset += V1Constants.Numbers.INTEGER_SIZE;

    int numDimensions = _dataBuffer.getInt(offset);
    offset += V1Constants.Numbers.INTEGER_SIZE;

    String[] dimensionNames = new String[numDimensions];
    byte[] dimensionNameBytes = new byte[DIMENSION_NAME_MAX_LENGTH];
    for (int i = 0; i < numDimensions; i++) {
      // NOTE: In old version, index might not be stored in order
      int dimensionId = _dataBuffer.getInt(offset);
      offset += V1Constants.Numbers.INTEGER_SIZE;

      int dimensionLength = _dataBuffer.getInt(offset);
      Preconditions.checkState(dimensionLength < DIMENSION_NAME_MAX_LENGTH);
      offset += V1Constants.Numbers.INTEGER_SIZE;

      _dataBuffer.copyTo((int) offset, dimensionNameBytes, 0, dimensionLength);
      offset += dimensionLength;

      String dimensionName = new String(dimensionNameBytes, 0, dimensionLength, UTF_8);
      dimensionNames[dimensionId] = dimensionName;
    }
    _dimensionNames = Arrays.asList(dimensionNames);

    int numNodes = _dataBuffer.getInt(offset);
    offset += V1Constants.Numbers.INTEGER_SIZE;
    Preconditions.checkState(offset == rootNodeOffset, "Error reading Star Tree file, header length mis-match");
    long fileLength = starTreeFile.length();
    Preconditions.checkState(offset + numNodes * OffHeapStarTreeNode.SERIALIZABLE_SIZE_IN_BYTES == fileLength,
        "Error reading Star Tree file, file length mis-match");

    _root = new OffHeapStarTreeNode(_dataBuffer.view(rootNodeOffset, fileLength), 0);
  }

  /**
   * Helper method to create an LBuffer from a given file.
   */
  private static LBuffer loadFrom(File file) throws IOException {
    try (FileChannel fileChannel = new FileInputStream(file).getChannel()) {
      ByteBuffer byteBuffer = ByteBuffer.allocate(LOAD_FILE_BUFFER_SIZE);
      LBuffer lBuffer = new LBuffer(file.length());

      long offset = 0;
      int numBytesRead;
      while ((numBytesRead = fileChannel.read(byteBuffer, offset)) > 0) {
        lBuffer.readFrom(byteBuffer.array(), 0, offset, numBytesRead);
        offset += numBytesRead;
      }
      return lBuffer;
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
    if (_dataBuffer instanceof MMapBuffer) {
      ((MMapBuffer) _dataBuffer).close();
    } else {
      _dataBuffer.release();
    }
  }
}
