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
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import xerial.larray.buffer.LBuffer;
import xerial.larray.buffer.LBufferAPI;
import xerial.larray.mmap.MMapBuffer;
import xerial.larray.mmap.MMapMode;


/**
 * Utility class for serializing/de-serializing the StarTree
 * data structure.
 */
public class StarTreeSerDe {
  public static final long MAGIC_MARKER = 0xBADDA55B00DAD00DL;
  public static final int MAGIC_MARKER_SIZE_IN_BYTES = 8;

  private static final String UTF8 = "UTF-8";
  private static byte version = 1;

  /**
   * De-serializes a StarTree structure.
   */
  public static StarTreeInterf fromBytes(InputStream inputStream)
      throws IOException, ClassNotFoundException {

    BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
    StarTreeInterf.Version version = getStarTreeVersion(bufferedInputStream);

    switch (version) {
      case V1:
        return fromBytesV1(bufferedInputStream);

      case V2:
        return fromBytesV2(bufferedInputStream);

      default:
        throw new RuntimeException("StarTree version number not recognized: " + version);
    }
  }

  /**
   * Write the V0 version of StarTree to the output file.
   * @param starTree
   * @param outputFile
   * @throws IOException
   */
  public static void writeTreeV1(StarTreeInterf starTree, File outputFile)
      throws IOException {
    Preconditions.checkArgument(starTree.getVersion() == StarTreeInterf.Version.V1,
        "Cannot write V1 version of star tree from another version");
    starTree.writeTree(outputFile);
  }

  /**
   * Write the V1 version of StarTree to the output file.
   * @param starTree
   * @param outputFile
   * @throws IOException
   */
  public static void writeTreeV2(StarTreeInterf starTree, File outputFile)
      throws IOException {
    if (starTree.getVersion() == StarTreeInterf.Version.V1) {
      writeTreeV2FromV1((StarTree) starTree, outputFile);
    } else {
      starTree.writeTree(outputFile);
    }
  }

  /**
   * Utility method to StarTree version.
   * Presence of {@value #MAGIC_MARKER} indicates version V1, while its
   * absence indicates version V0.
   *
   * @param bufferedInputStream
   * @return
   * @throws IOException
   */
  public static StarTreeInterf.Version getStarTreeVersion(BufferedInputStream bufferedInputStream)
      throws IOException {
    byte[] magicBytes = new byte[MAGIC_MARKER_SIZE_IN_BYTES];

    bufferedInputStream.mark(MAGIC_MARKER_SIZE_IN_BYTES);
    bufferedInputStream.read(magicBytes, 0, MAGIC_MARKER_SIZE_IN_BYTES);
    bufferedInputStream.reset();

    LBufferAPI lBuffer = new LBuffer(MAGIC_MARKER_SIZE_IN_BYTES);
    lBuffer.readFrom(magicBytes, 0);
    long magicMarker = lBuffer.getLong(0);

    if (magicMarker == MAGIC_MARKER) {
      return StarTreeInterf.Version.V2;
    } else {
      return StarTreeInterf.Version.V1;
    }
  }

  /**
   * Given a StarTree in V1 format, serialize it into V2 format and write to the
   * given file.
   * @param starTree
   * @param outputFile
   */
  private static void writeTreeV2FromV1(StarTree starTree, File outputFile)
      throws IOException {
    int headerSizeInBytes = computeV2HeaderSizeInBytes(starTree);
    long totalSize = headerSizeInBytes + computeV2NodesSizeInBytes(starTree);

    MMapBuffer mappedByteBuffer = new MMapBuffer(outputFile, 0, totalSize, MMapMode.READ_WRITE);
    long offset = writeHeaderV2(starTree, headerSizeInBytes, mappedByteBuffer);

    // Ensure that the computed offset is the same as actual offset.
    Preconditions.checkState((offset == headerSizeInBytes), "Error writing Star Tree file, header size mis-match");

    // Write the actual star tree nodes in level order.
    writeNodesV2(starTree, mappedByteBuffer, offset);

    mappedByteBuffer.flush();
    mappedByteBuffer.close();
  }

  /**
   * Helper method to write the star tree nodes for Star Tree V2
   *
   * @param starTree
   * @param mappedByteBuffer
   * @param offset
   */
  private static void writeNodesV2(StarTree starTree, MMapBuffer mappedByteBuffer, long offset) {
    int index = 0;
    Queue<StarTreeIndexNode> queue = new LinkedList<>();
    StarTreeIndexNode root = (StarTreeIndexNode) starTree.getRoot();
    queue.add(root);

    while (!queue.isEmpty()) {
      StarTreeIndexNode node = queue.poll();
      List<StarTreeIndexNode> children = getSortedChildren(node); // Returns empty list instead of null.

      int numChildren = children.size();
      int startChildrenIndex = (numChildren != 0) ? (index + queue.size() + 1) : StarTreeIndexNodeV2.INVALID_INDEX;
      int endChildrenIndex =
          (numChildren != 0) ? (startChildrenIndex + numChildren - 1) : StarTreeIndexNodeV2.INVALID_INDEX;

      offset = writeOneV2Node(mappedByteBuffer, offset, node, startChildrenIndex, endChildrenIndex);
      for (StarTreeIndexNode child : children) {
        queue.add(child);
      }
      index++;
    }
  }

  /**
   * Helper method to write the Header information for Star Tree V2
   * - MAGIC_MARKER
   * - Version
   * - Header size
   * - Dimension Name to Index Map
   * - Number of nodes in the tree.
   *
   * @param starTree
   * @param headerSizeInBytes
   * @param mappedByteBuffer
   * @return
   * @throws UnsupportedEncodingException
   */
  private static long writeHeaderV2(StarTree starTree, int headerSizeInBytes, MMapBuffer mappedByteBuffer)
      throws UnsupportedEncodingException {
    long offset = 0;
    mappedByteBuffer.putLong(offset, MAGIC_MARKER);
    offset += V1Constants.Numbers.LONG_SIZE;

    mappedByteBuffer.putInt(offset, version);
    offset += V1Constants.Numbers.INTEGER_SIZE;

    mappedByteBuffer.putInt(offset, headerSizeInBytes);
    offset += V1Constants.Numbers.INTEGER_SIZE;

    HashBiMap<String, Integer> dimensionNameToIndexMap = starTree.getDimensionNameToIndexMap();

    mappedByteBuffer.putInt(offset, dimensionNameToIndexMap.size());
    offset += V1Constants.Numbers.INTEGER_SIZE;

    // Write the dimensionName to Index map
    for (Map.Entry<String, Integer> entry : dimensionNameToIndexMap.entrySet()) {
      String dimension = entry.getKey();
      int index = entry.getValue();

      mappedByteBuffer.putInt(offset, index);
      offset += V1Constants.Numbers.INTEGER_SIZE;

      int dimensionLength = dimension.length();
      mappedByteBuffer.putInt(offset, dimensionLength);
      offset += V1Constants.Numbers.INTEGER_SIZE;

      mappedByteBuffer.readFrom(dimension.getBytes(UTF8), offset);
      offset += dimensionLength;
    }

    mappedByteBuffer.putInt(offset, starTree.getNumNodes());
    offset += V1Constants.Numbers.INTEGER_SIZE;
    return offset;
  }

  /**
   * Helper method that returns a list of children for the given node, sorted based on
   * the dimension value.
   *
   * @param node
   * @return A list of sorted child nodes (empty if no children).
   */
  private static List<StarTreeIndexNode> getSortedChildren(StarTreeIndexNode node) {
    Map<Integer, StarTreeIndexNode> children = node.getChildren();
    if (children == null) {
      return Collections.EMPTY_LIST;
    }

    List<StarTreeIndexNode> sortedChildren = new ArrayList<>();
    sortedChildren.addAll(children.values());

    Collections.sort(sortedChildren, new Comparator<StarTreeIndexNode>() {
      @Override
      public int compare(StarTreeIndexNode node1, StarTreeIndexNode node2) {
        int v1 = node1.getDimensionValue();
        int v2 = node2.getDimensionValue();

        if (v1 < v2) {
          return -1;
        } else if (v1 > v2) {
          return v1;
        } else {
          return 0;
        }
      }
    });

    return sortedChildren;
  }

  /**
   * Helper method to write one StarTreeIndexNodeV2 into the mappedByteBuffer at the provided
   * offset.
   *
   * @param mappedByteBuffer
   * @param offset
   * @param node
   * @param startChildrenIndex
   * @param endChildrenIndex
   */
  private static long writeOneV2Node(MMapBuffer mappedByteBuffer, long offset, StarTreeIndexNode node,
      int startChildrenIndex, int endChildrenIndex) {
    mappedByteBuffer.putInt(offset, node.getDimensionName());
    offset += V1Constants.Numbers.INTEGER_SIZE;

    mappedByteBuffer.putInt(offset, node.getDimensionValue());
    offset += V1Constants.Numbers.INTEGER_SIZE;

    mappedByteBuffer.putInt(offset, node.getStartDocumentId());
    offset += V1Constants.Numbers.INTEGER_SIZE;

    mappedByteBuffer.putInt(offset, node.getEndDocumentId());
    offset += V1Constants.Numbers.INTEGER_SIZE;

    mappedByteBuffer.putInt(offset, node.getAggregatedDocumentId());
    offset += V1Constants.Numbers.INTEGER_SIZE;

    mappedByteBuffer.putInt(offset, startChildrenIndex);
    offset += V1Constants.Numbers.INTEGER_SIZE;

    mappedByteBuffer.putInt(offset, endChildrenIndex);
    offset += V1Constants.Numbers.INTEGER_SIZE;

    return offset;
  }

  /**
   * Helper method to compute size of tree in bytes, required to
   * store in V2 format. The size is computed as follows:
   * - Long (8 bytes) for magic marker
   * - Integer (4 bytes) for size of the header
   * - Integer (4 bytes) for version
   * - Integer (4 bytes) to store number of dimensions.
   * - Total size to store dimension name strings
   * - Integer (4 bytes) per dimension to store the index of the string
   * - Integer (4 bytes) to store total number of nodes in the tree.
   * @param starTree
   * @return
   */
  private static int computeV2HeaderSizeInBytes(StarTreeInterf starTree) {
    int size = 20; // magic marker, version, size of header and number of dimensions

    HashBiMap<String, Integer> dimensionNameToIndexMap = starTree.getDimensionNameToIndexMap();
    for (String dimension : dimensionNameToIndexMap.keySet()) {
      size += V1Constants.Numbers.INTEGER_SIZE; // For dimension index
      size += V1Constants.Numbers.INTEGER_SIZE; // For length of dimension name
      size += dimension.length(); // For dimension name
    }

    size += V1Constants.Numbers.INTEGER_SIZE; // For number of nodes.
    return size;
  }

  /**
   * Helper method to compute size of nodes of tree in bytes.
   * The size is computed as follows:
   * - Total number of nodes * size of one node
   *
   * @param starTree
   * @return
   */
  private static long computeV2NodesSizeInBytes(StarTreeInterf starTree) {
    return (starTree.getNumNodes() * StarTreeIndexNodeV2.getSerializableSize());
  }

  /**
   * Utility method that de-serializes bytes from inputStream
   * into the V0 version of star tree.
   *
   * @param inputStream
   * @return
   * @throws IOException
   * @throws ClassNotFoundException
   */
  private static StarTreeInterf fromBytesV1(InputStream inputStream)
      throws IOException, ClassNotFoundException {
    ObjectInputStream ois = new ObjectInputStream(inputStream);
    return (StarTree) ois.readObject();
  }

  /**
   * Utility method that de-serializes bytes from inputStream into
   * the V1 version of star tree.
   *
   * @param inputStream
   * @return
   */
  private static StarTreeInterf fromBytesV2(InputStream inputStream) {
    throw new RuntimeException("StarTree Version V2 does not support reading from bytes.");
  }

  /**
   *
   * @param starTreeFile  Star Tree index file
   * @param readMode Read mode MMAP or HEAP (direct memory), only applicable to StarTreeV2.
   * @return
   */
  public static StarTreeInterf fromFile(File starTreeFile, ReadMode readMode)
      throws IOException, ClassNotFoundException {

    InputStream inputStream = new FileInputStream(starTreeFile);
    BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
    StarTreeInterf.Version starTreeVersion = getStarTreeVersion(bufferedInputStream);

    if (starTreeVersion.equals(StarTreeInterf.Version.V1)) {
      return fromBytesV1(bufferedInputStream);
    } else if (starTreeVersion.equals(StarTreeInterf.Version.V2)) {
      return new StarTreeV2(starTreeFile, readMode);
    } else {
      throw new RuntimeException("Unrecognized version for Star Tree " + starTreeVersion);
    }
  }
}
