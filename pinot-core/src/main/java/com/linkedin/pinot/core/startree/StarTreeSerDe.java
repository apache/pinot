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
import com.linkedin.pinot.core.segment.store.SegmentDirectoryPaths;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xerial.larray.buffer.LBuffer;
import xerial.larray.buffer.LBufferAPI;
import xerial.larray.mmap.MMapBuffer;
import xerial.larray.mmap.MMapMode;


/**
 * Utility class for serializing/de-serializing the StarTree
 * data structure.
 */
public class StarTreeSerDe {
  private static final Logger LOGGER = LoggerFactory.getLogger(StarTreeSerDe.class);

  public static final long MAGIC_MARKER = 0xBADDA55B00DAD00DL;
  public static final int MAGIC_MARKER_SIZE_IN_BYTES = 8;
  private static final Charset UTF_8 = Charset.forName("UTF-8");
  private static final int VERSION = 1;

  /**
   * De-serializes a StarTree structure.
   */
  public static StarTreeInterf fromBytes(InputStream inputStream)
      throws IOException, ClassNotFoundException {

    BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
    StarTreeFormatVersion version = getStarTreeVersion(bufferedInputStream);

    switch (version) {
      case ON_HEAP:
        return fromBytesToOnHeapFormat(bufferedInputStream);

      case OFF_HEAP:
        return fromBytesToOffHeapFormat(bufferedInputStream);

      default:
        throw new RuntimeException("StarTree version number not recognized: " + version);
    }
  }

  /**
   * Write the on-heap version of StarTree to the output file.
   * @param starTree
   * @param outputFile
   * @throws IOException
   */
  public static void writeTreeOnHeapFormat(StarTreeInterf starTree, File outputFile)
      throws IOException {
    Preconditions.checkArgument(starTree.getVersion() == StarTreeFormatVersion.ON_HEAP,
        "Cannot write on-heap version of star tree from another version");
    starTree.writeTree(outputFile);
  }

  /**
   * Write the off-heap version of StarTree to the output file.
   * @param starTree
   * @param outputFile
   * @throws IOException
   */
  public static void writeTreeOffHeapFormat(StarTreeInterf starTree, File outputFile)
      throws IOException {
    if (starTree.getVersion() == StarTreeFormatVersion.ON_HEAP) {
      writeTreeOffHeapFromOnHeap((StarTree) starTree, outputFile);
    } else {
      starTree.writeTree(outputFile);
    }
  }

  /**
   * Utility method to StarTree version.
   * Presence of {@ref #MAGIC_MARKER} indicates on-heap format, while its
   * absence indicates on-heap format.
   *
   * @param bufferedInputStream
   * @return
   * @throws IOException
   */
  public static StarTreeFormatVersion getStarTreeVersion(BufferedInputStream bufferedInputStream)
      throws IOException {
    byte[] magicBytes = new byte[MAGIC_MARKER_SIZE_IN_BYTES];

    bufferedInputStream.mark(MAGIC_MARKER_SIZE_IN_BYTES);
    Preconditions.checkState(
        bufferedInputStream.read(magicBytes, 0, MAGIC_MARKER_SIZE_IN_BYTES) == MAGIC_MARKER_SIZE_IN_BYTES);
    bufferedInputStream.reset();

    LBufferAPI lBuffer = new LBuffer(MAGIC_MARKER_SIZE_IN_BYTES);
    lBuffer.readFrom(magicBytes, 0);
    long magicMarker = lBuffer.getLong(0);
    lBuffer.release();

    if (magicMarker == MAGIC_MARKER) {
      return StarTreeFormatVersion.OFF_HEAP;
    } else {
      return StarTreeFormatVersion.ON_HEAP;
    }
  }

  /**
   * Given a star tree file, return its version (on-heap or off-heap).
   * Assumes that the file is a valid star tree file.
   *
   * @param starTreeFile
   * @return
   * @throws IOException
   */
  public static StarTreeFormatVersion getStarTreeVersion(File starTreeFile)
      throws IOException {
    InputStream inputStream = new FileInputStream(starTreeFile);
    BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
    StarTreeFormatVersion version = getStarTreeVersion(bufferedInputStream);
    bufferedInputStream.close();
    return version;
  }

  /**
   * Given a StarTree in on-heap format, serialize it into OFF_HEAP format and write to the
   * given file.
   * @param starTree
   * @param outputFile
   */
  private static void writeTreeOffHeapFromOnHeap(StarTree starTree, File outputFile)
      throws IOException {
    int headerSizeInBytes = computeOffHeapHeaderSizeInBytes(starTree);
    long totalSize = headerSizeInBytes + computeOffHeapNodesSizeInBytes(starTree);

    MMapBuffer mappedByteBuffer = new MMapBuffer(outputFile, 0, totalSize, MMapMode.READ_WRITE);
    long offset = writeHeaderOffHeap(starTree, headerSizeInBytes, mappedByteBuffer);

    // Ensure that the computed offset is the same as actual offset.
    Preconditions.checkState((offset == headerSizeInBytes), "Error writing Star Tree file, header size mis-match");

    // Write the actual star tree nodes in level order.
    writeNodesOffHeap(starTree, mappedByteBuffer, offset);

    mappedByteBuffer.flush();
    mappedByteBuffer.close();
  }

  /**
   * Helper method to write the star tree nodes for Star Tree off-heap format
   *
   * @param starTree
   * @param mappedByteBuffer
   * @param offset
   */
  private static void writeNodesOffHeap(StarTree starTree, MMapBuffer mappedByteBuffer, long offset) {
    int index = 0;
    Queue<StarTreeIndexNode> queue = new LinkedList<>();
    StarTreeIndexNode root = (StarTreeIndexNode) starTree.getRoot();
    queue.add(root);

    while (!queue.isEmpty()) {
      StarTreeIndexNode node = queue.poll();
      List<StarTreeIndexNode> children = getSortedChildren(node); // Returns empty list instead of null.

      int numChildren = children.size();
      int startChildrenIndex = (numChildren != 0) ? (index + queue.size() + 1) : StarTreeIndexNodeOffHeap.INVALID_INDEX;
      int endChildrenIndex =
          (numChildren != 0) ? (startChildrenIndex + numChildren - 1) : StarTreeIndexNodeOffHeap.INVALID_INDEX;

      offset = writeOneOffHeapNode(mappedByteBuffer, offset, node, startChildrenIndex, endChildrenIndex);
      for (StarTreeIndexNode child : children) {
        queue.add(child);
      }
      index++;
    }
  }

  /**
   * Helper method to write the Header information for Star Tree off-heap format
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
  private static long writeHeaderOffHeap(StarTree starTree, int headerSizeInBytes, MMapBuffer mappedByteBuffer)
      throws UnsupportedEncodingException {
    long offset = 0;
    mappedByteBuffer.putLong(offset, MAGIC_MARKER);
    offset += V1Constants.Numbers.LONG_SIZE;

    mappedByteBuffer.putInt(offset, VERSION);
    offset += V1Constants.Numbers.INTEGER_SIZE;

    mappedByteBuffer.putInt(offset, headerSizeInBytes);
    offset += V1Constants.Numbers.INTEGER_SIZE;

    List<String> dimensionNames = starTree.getDimensionNames();
    int numDimensions = dimensionNames.size();

    mappedByteBuffer.putInt(offset, numDimensions);
    offset += V1Constants.Numbers.INTEGER_SIZE;

    // Write the dimensionName to Index map
    for (int i = 0; i < numDimensions; i++) {
      String dimensionName = dimensionNames.get(i);

      mappedByteBuffer.putInt(offset, i);
      offset += V1Constants.Numbers.INTEGER_SIZE;

      byte[] dimensionBytes = dimensionName.getBytes(UTF_8);
      int dimensionLength = dimensionBytes.length;
      mappedByteBuffer.putInt(offset, dimensionLength);
      offset += V1Constants.Numbers.INTEGER_SIZE;

      mappedByteBuffer.readFrom(dimensionBytes, offset);
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
   * Helper method to write one StarTreeIndexNodeOffHeap into the mappedByteBuffer at the provided
   * offset.
   *
   * @param mappedByteBuffer
   * @param offset
   * @param node
   * @param startChildrenIndex
   * @param endChildrenIndex
   */
  private static long writeOneOffHeapNode(MMapBuffer mappedByteBuffer, long offset, StarTreeIndexNode node,
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
   * store in off-heap format. The size is computed as follows:
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
  private static int computeOffHeapHeaderSizeInBytes(StarTreeInterf starTree) {
    int size = 20; // magic marker, version, size of header and number of dimensions

    List<String> dimensionNames = starTree.getDimensionNames();
    for (String dimension : dimensionNames) {
      size += V1Constants.Numbers.INTEGER_SIZE; // For dimension index
      size += V1Constants.Numbers.INTEGER_SIZE; // For length of dimension name
      size += dimension.getBytes(UTF_8).length; // For dimension name
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
  private static long computeOffHeapNodesSizeInBytes(StarTreeInterf starTree) {
    return (starTree.getNumNodes() * StarTreeIndexNodeOffHeap.getSerializableSize());
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
  private static StarTreeInterf fromBytesToOnHeapFormat(InputStream inputStream)
      throws IOException, ClassNotFoundException {
    ObjectInputStream ois = new ObjectInputStream(inputStream);
    return (StarTree) ois.readObject();
  }

  /**
   * Utility method that de-serializes bytes from inputStream into
   * the on-heap format of star tree.
   *
   * @param inputStream
   * @return
   */
  private static StarTreeInterf fromBytesToOffHeapFormat(InputStream inputStream) {
    throw new RuntimeException("StarTree Version off-heap does not support reading from bytes.");
  }

  /**
   *
   * @param starTreeFile  Star Tree index file
   * @param readMode Read mode MMAP or OFF-HEAP (direct memory), only applicable to StarTreeOffHeap.
   * @return
   */
  public static StarTreeInterf fromFile(File starTreeFile, ReadMode readMode)
      throws IOException, ClassNotFoundException {

    InputStream inputStream = new FileInputStream(starTreeFile);
    BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
    StarTreeFormatVersion starTreeVersion = getStarTreeVersion(bufferedInputStream);

    if (starTreeVersion.equals(StarTreeFormatVersion.ON_HEAP)) {
      return fromBytesToOnHeapFormat(bufferedInputStream);
    } else if (starTreeVersion.equals(StarTreeFormatVersion.OFF_HEAP)) {
      return new StarTreeOffHeap(starTreeFile, readMode);
    } else {
      throw new RuntimeException("Unrecognized version for Star Tree " + starTreeVersion);
    }
  }

  /**
   * Utility method to convert star tree from on-heap to off-heap format:
   * <p>- If star tree does not exist, or if actual version is the same as
   *   expected version, then no action is taken. </p>
   *
   * <p>- If actual version is on-heap and expected version is off-heap, then conversion from
   *   on-heap to off-heap is performed. Both on-heap and off-heap formats are also backed up.</p>
   *
   * <p>- If actual version is off-heap and expected version is on-heap, then on-heap is restored from
   *   backup version, if available, no-op otherwise. Note, there is no off-heap to on-heap
   *   conversion as of now.</p>
   *
   * @param indexDir
   * @param starTreeVersionToLoad
   * @throws IOException
   * @throws ClassNotFoundException
   */
  public static void convertStarTreeFormatIfNeeded(File indexDir, StarTreeFormatVersion starTreeVersionToLoad)
      throws IOException, ClassNotFoundException {
    File starTreeFile = SegmentDirectoryPaths.findStarTreeFile(indexDir);

    // If the star-tree file does not exist, this is not a star tree index, nothing to do here.
    if (starTreeFile == null || !starTreeFile.exists()) {
      LOGGER.debug("Skipping Star Tree format conversion, as star tree file {} does not exist in directory {}",
          V1Constants.STAR_TREE_INDEX_FILE, indexDir);
      return;
    }
    File parentDir = starTreeFile.getParentFile();

    StarTreeFormatVersion actualVersion = getStarTreeVersion(starTreeFile);
    if (actualVersion == StarTreeFormatVersion.ON_HEAP && starTreeVersionToLoad == StarTreeFormatVersion.OFF_HEAP) {
      LOGGER.info("Converting Star Tree from on-heap to off-heap format for {}", starTreeFile.getAbsolutePath());
      File starTreeOffHeapFile = new File(parentDir, V1Constants.STAR_TREE_OFF_HEAP_INDEX_FILE);
      if (starTreeOffHeapFile.exists()) {
        LOGGER.info("Replacing star tree on-heap format with off-heap format for {}", starTreeFile.getAbsolutePath());
        FileUtils.copyFile(starTreeOffHeapFile, starTreeFile);
      } else {
        StarTreeInterf starTreeOnHeap = fromFile(starTreeFile, ReadMode.heap); // OnHeap only supports HEAP mode.

        try {
          writeTreeOffHeapFormat(starTreeOnHeap, starTreeOffHeapFile);
          FileUtils.copyFile(starTreeFile, new File(parentDir, V1Constants.STAR_TREE_ON_HEAP_INDEX_FILE));
          FileUtils.copyFile(starTreeOffHeapFile, starTreeFile);
        } catch (Exception e) {
          LOGGER.warn("Exception caught while convert star tree on-heap to off-heap format for {}",
              starTreeFile.getAbsolutePath(), e);
        }
      }
    } else if (actualVersion == StarTreeFormatVersion.OFF_HEAP
        && starTreeVersionToLoad == StarTreeFormatVersion.ON_HEAP) {
      File starTreeOnHeapFile = new File(parentDir, V1Constants.STAR_TREE_ON_HEAP_INDEX_FILE);
      if (starTreeOnHeapFile.exists()) {
        try {
          FileUtils.copyFile(starTreeFile, new File(parentDir, V1Constants.STAR_TREE_OFF_HEAP_INDEX_FILE));
          FileUtils.copyFile(starTreeOnHeapFile, starTreeFile);
        } catch (Exception e) {
          LOGGER.warn("Exception caught while converting star tree off-heap to on-heap for {}",
              starTreeFile.getAbsolutePath(), e);
        }
      } else {
        LOGGER.info(
            "Could not replace star tree format off-heap to on-heap as {} does not exist, will load off-heap format",
            starTreeOnHeapFile.getAbsolutePath());
      }
    }
  }
}
