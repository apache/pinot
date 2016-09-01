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
import com.google.common.collect.HashBiMap;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import xerial.larray.buffer.LBuffer;
import xerial.larray.buffer.LBufferAPI;
import xerial.larray.mmap.MMapBuffer;
import xerial.larray.mmap.MMapMode;


/**
 * LBuffer based implementation of star tree interface.
 */
public class StarTreeV2 implements StarTreeInterf {
  private static final long serialVersionUID = 1L;
  private static final int DIMENSION_NAME_MAX_LENGTH = 4096;
  private static final String UTF8 = "UTF-8";

  // Buffer size for buffered reading of on-disk file into byte buffer.
  private static final int BUFFER_SIZE = 10 * 1024 * 1024;

  // Off heap buffer were the star tree is loaded.
  LBufferAPI dataBuffer;

  StarTreeIndexNodeV2 root;

  // Offset of the root node in the file.
  private int rootNodeOffset;

  // Initial size of buffer to be allocated to read the star tree header.
  private static final long STAR_TREE_HEADER_READER_SIZE = 10 * 1024;

  int numNodes = 0;
  private HashBiMap<String, Integer> dimensionNameToIndexMap;
  private int version;

  /**
   * Constructor for the class.
   * - Reads in the header
   * - Loads/MMap's the StarTreeIndexNodeV2 array.
   *
   * @param starTreeFile
   * @param readMode
   * @throws IOException
   */
  public StarTreeV2(File starTreeFile, ReadMode readMode)
      throws IOException {
    int rootOffset = readHeader(starTreeFile);
    long size = starTreeFile.length() - rootOffset;

    if (readMode.equals(ReadMode.mmap)) {
      dataBuffer = new MMapBuffer(starTreeFile, rootOffset, size, MMapMode.READ_ONLY);
    } else {
      dataBuffer = loadFromFile(starTreeFile, rootOffset);
    }

    // Root node is the first one.
    root = new StarTreeIndexNodeV2(dataBuffer, 0);
  }

  /**
   * Read the header information from the star tree file, and populate the
   * following info:
   * - Version
   * - Dimension name to index map.
   * - Number of nodes
   * - Root offset.
   *
   * @throws UnsupportedEncodingException
   * @param starTreeFile
   */
  private int readHeader(File starTreeFile)
      throws IOException {
    int offset = 0;

    int size = (int) Math.min(starTreeFile.length(), STAR_TREE_HEADER_READER_SIZE);
    MMapBuffer dataBuffer = new MMapBuffer(starTreeFile, offset, size, MMapMode.READ_ONLY);

    Preconditions.checkState(StarTreeSerDe.MAGIC_MARKER == dataBuffer.getLong(offset),
        "Invalid magic marker in Star Tree file");
    offset += StarTreeSerDe.MAGIC_MARKER_SIZE_IN_BYTES;

    version = dataBuffer.getInt(offset);
    offset += V1Constants.Numbers.INTEGER_SIZE;
    Preconditions.checkState(version == 1);

    rootNodeOffset = dataBuffer.getInt(offset);
    offset += V1Constants.Numbers.INTEGER_SIZE;

    // If header size turns out to be larger than initially thought, then re-map with the correct size.
    if (rootNodeOffset > size) {
      dataBuffer.close();
      dataBuffer = new MMapBuffer(starTreeFile, 0, rootNodeOffset, MMapMode.READ_ONLY);
    }

    int numDimensions = dataBuffer.getInt(offset);
    offset += V1Constants.Numbers.INTEGER_SIZE;

    dimensionNameToIndexMap = HashBiMap.create(numDimensions);
    byte[] dimensionNameBytes = new byte[DIMENSION_NAME_MAX_LENGTH];

    for (int i = 0; i < numDimensions; i++) {
      int index = dataBuffer.getInt(offset);
      offset += V1Constants.Numbers.INTEGER_SIZE;

      int dimensionLength = dataBuffer.getInt(offset);
      offset += V1Constants.Numbers.INTEGER_SIZE;

      // Since we are re-using the same bytes for reading strings, assert we have allocated enough.
      Preconditions.checkState(dimensionLength < DIMENSION_NAME_MAX_LENGTH);

      // Ok to cast offset to int, as its value is too small at this point in the file.
      dataBuffer.copyTo((int) offset, dimensionNameBytes, 0, dimensionLength);
      offset += dimensionLength;

      String dimensionName = new String(dimensionNameBytes, 0, dimensionLength, UTF8);
      dimensionNameToIndexMap.put(dimensionName, index);
    }

    numNodes = dataBuffer.getInt(offset);
    offset += V1Constants.Numbers.INTEGER_SIZE;

    Preconditions.checkState((offset == rootNodeOffset), "Error reading Star Tree file, header length mis-match");

    dataBuffer.close();
    return offset;
  }

  /**
   * Helper method that loads the star tree into a LBuffer, from
   * the given file.
   *
   * @param starTreeFile
   * @param rootOffset
   * @return
   * @throws IOException
   */
  private static LBufferAPI loadFromFile(File starTreeFile, long rootOffset)
      throws IOException {
    FileChannel fileChannel = new FileInputStream(starTreeFile).getChannel();
    ByteBuffer byteBuffer = ByteBuffer.allocate(BUFFER_SIZE);

    long start = rootOffset;
    long end = starTreeFile.length();
    long destOffset = 0;
    LBufferAPI lBuffer = new LBuffer(end - start);

    int bytesRead = 0;
    while ((bytesRead = fileChannel.read(byteBuffer, start)) > 0) {
      lBuffer.readFrom(byteBuffer.array(), 0, destOffset, bytesRead);
      start += bytesRead;
      destOffset += bytesRead;
      byteBuffer.clear();
    }
    fileChannel.close();
    return lBuffer;
  }

  /**
   * {@inheritDoc}
   * @return
   */
  @Override
  public StarTreeIndexNodeInterf getRoot() {
    return root;
  }

  /**
   * {@inheritDoc}
   * @return
   */
  public StarTreeFormatVersion getVersion() {
    return StarTreeFormatVersion.V2;
  }

  @Override
  public int getNumNodes() {
    return numNodes;
  }

  /**
   * {@inheritDoc}
   * @return
   */
  @Override
  public HashBiMap<String, Integer> getDimensionNameToIndexMap() {
    return dimensionNameToIndexMap;
  }

  /**
   * {@inheritDoc}
   * @param outputFile
   * @throws IOException
   */
  @Override
  public void writeTree(File outputFile)
      throws IOException {
    throw new RuntimeException("Method 'writeTree' not implemented for class " + getClass().getName());
  }

  @Override
  public void printTree() {
    throw new RuntimeException("Method 'printTree' not implemented for class " + getClass().getName());
  }
}
