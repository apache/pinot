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

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import xerial.larray.buffer.LBuffer;
import xerial.larray.buffer.LBufferAPI;
import xerial.larray.mmap.MMapBuffer;
import xerial.larray.mmap.MMapMode;


/**
 * LBuffer based implementation of star tree interface.
 */
public class StarTreeOffHeap implements StarTreeInterf {
  private static final Charset UTF_8 = Charset.forName("UTF-8");
  private static final int DIMENSION_NAME_MAX_LENGTH = 4096;
  private static final int LOAD_FILE_BUFFER_SIZE = 10 * 1024 * 1024;

  private final LBufferAPI _dataBuffer;
  private final StarTreeIndexNodeOffHeap _root;
  private final List<String> _dimensionNames;
  private final int _numNodes;

  /**
   * Constructor for the class.
   * - Reads in the header
   * - Loads/MMap's the StarTreeIndexNodeOffHeap array.
   */
  public StarTreeOffHeap(File starTreeFile, ReadMode readMode) throws IOException {
    if (readMode.equals(ReadMode.mmap)) {
      _dataBuffer = new MMapBuffer(starTreeFile, MMapMode.READ_ONLY);
    } else {
      _dataBuffer = loadFrom(starTreeFile);
    }

    int offset = 0;
    Preconditions.checkState(StarTreeSerDe.MAGIC_MARKER == _dataBuffer.getLong(offset),
        "Invalid magic marker in Star Tree file");
    offset += StarTreeSerDe.MAGIC_MARKER_SIZE_IN_BYTES;

    int version = _dataBuffer.getInt(offset);
    Preconditions.checkState(version == 1);
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

      _dataBuffer.copyTo(offset, dimensionNameBytes, 0, dimensionLength);
      offset += dimensionLength;

      String dimensionName = new String(dimensionNameBytes, 0, dimensionLength, UTF_8);
      dimensionNames[dimensionId] = dimensionName;
    }
    _dimensionNames = Arrays.asList(dimensionNames);

    _numNodes = _dataBuffer.getInt(offset);
    offset += V1Constants.Numbers.INTEGER_SIZE;
    Preconditions.checkState(offset == rootNodeOffset, "Error reading Star Tree file, header length mis-match");

    _root = new StarTreeIndexNodeOffHeap(_dataBuffer.view(rootNodeOffset, starTreeFile.length()), 0);
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
  public StarTreeIndexNodeInterf getRoot() {
    return _root;
  }

  @Override
  public StarTreeFormatVersion getVersion() {
    return StarTreeFormatVersion.OFF_HEAP;
  }

  @Override
  public int getNumNodes() {
    return _numNodes;
  }

  @Override
  public List<String> getDimensionNames() {
    return _dimensionNames;
  }

  @Override
  public void writeTree(File outputFile) throws IOException {
    throw new RuntimeException("Method 'writeTree' not implemented for class " + getClass().getName());
  }

  @Override
  public void printTree() {
    throw new RuntimeException("Method 'printTree' not implemented for class " + getClass().getName());
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
