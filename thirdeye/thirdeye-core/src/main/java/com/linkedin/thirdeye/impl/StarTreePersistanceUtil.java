package com.linkedin.thirdeye.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricSpec;
import org.apache.commons.compress.utils.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.api.StarTreeRecord;

/**
 * A utility class to persist/restore contents of the star tree. This is a
 * temporary place for these methods. These methods should go into their
 * respective implementations.
 *
 * @author kgopalak
 *
 */
public class StarTreePersistanceUtil {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(StarTreePersistanceUtil.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final TypeReference FWD_INDEX_TYPE_REFERENCE = new TypeReference<Map<String, Map<String, Integer>>>() {
  };

  /**
   * Saves only the tree part without leaf records.
   *
   * @throws IOException
   */
  public static void saveTree(StarTree tree, String outputDir)
      throws IOException {
    new File(outputDir).mkdirs();
    // store the tree
    FileOutputStream fileOutputStream = new FileOutputStream(new File(outputDir
        + "/" + tree.getConfig().getCollection() + "-tree.bin"));
    ObjectOutputStream objectOutputStream = new ObjectOutputStream(
        fileOutputStream);
    objectOutputStream.writeObject(tree.getRoot());
    objectOutputStream.close();
  }

  /**
   * Format <code>
   * <leafNodeId>| <recordCount> | (<record>)*
   * </code>
   *
   * @param tree
   * @param outputDir
   * @throws IOException
   */
  public static void saveLeafDimensionData(StarTree tree, String outputDir)
      throws IOException {
    // get the leaf nodes
    LinkedList<StarTreeNode> leafNodes = new LinkedList<StarTreeNode>();
    StarTreeUtils.traverseAndGetLeafNodes(leafNodes, tree.getRoot());

    StarTreeConfig config = tree.getConfig();
    List<DimensionSpec> dimensionSpecs = config.getDimensions();
    // write to a serialized data
    for (StarTreeNode leafNode : leafNodes) {
      saveLeafNode(outputDir, dimensionSpecs, leafNode);
    }
  }

  /**
   * Save the contents of a leaf node under outputDir,
   *
   * @param outputDir
   * @param dimensionSpecs
   * @param leafNode
   * @throws IOException
   */
  public static void saveLeafNode(String outputDir,
      List<DimensionSpec> dimensionSpecs, StarTreeNode leafNode) throws IOException {

    Map<String, Map<String, Integer>> forwardIndex = leafNode.getRecordStore()
        .getForwardIndex();
    Iterable<StarTreeRecord> records = leafNode.getRecordStore();

    String nodeId = leafNode.getId().toString();

    saveLeafNode(outputDir, dimensionSpecs, forwardIndex, records, nodeId);
  }

  public static void saveLeafNode(String outputDir,
      List<DimensionSpec> dimensionSpecs,
      Map<String, Map<String, Integer>> forwardIndex,
      Iterable<StarTreeRecord> records, String nodeId) throws IOException,
          JsonGenerationException, JsonMappingException, FileNotFoundException {
    saveLeafNodeForwardIndex(outputDir, forwardIndex, nodeId);

    saveLeafNodeBuffer(outputDir, dimensionSpecs, forwardIndex, records, nodeId);
  }

  /**
   * Generate the buffer file for leaf, this needs the forward index to
   * serialize/deserialize. To deserialize the callers needs to provide the
   * number of dimensions
   */
  public static void saveLeafNodeBuffer(String outputDir,
      List<DimensionSpec> dimensionSpecs,
      Map<String, Map<String, Integer>> forwardIndex,
      Iterable<StarTreeRecord> records, String nodeId) throws IOException {
    // serialize buffer
    ByteArrayOutputStream bufferStream = new ByteArrayOutputStream();
    DataOutputStream bufferDataOutputStream = new DataOutputStream(bufferStream);
    for (StarTreeRecord record : records) {
      for (DimensionSpec dimensionSpec : dimensionSpecs) {
        String dimValue = record.getDimensionKey().getDimensionValue(dimensionSpecs, dimensionSpec.getName());
        int dimValueId = forwardIndex.get(dimensionSpec.getName()).get(dimValue);
        bufferDataOutputStream.writeInt(dimValueId);
      }
    }
    bufferDataOutputStream.flush();
    byte[] bufferBytes = bufferStream.toByteArray();

    // Write buffer
    File bufferFile = new File(outputDir, nodeId
        + StarTreeConstants.BUFFER_FILE_SUFFIX);
    if (!bufferFile.createNewFile()) {
      throw new IOException(bufferFile + " already exists");
    }
    IOUtils.copy(new ByteArrayInputStream(bufferBytes), new FileOutputStream(
        bufferFile));
  }

  /**
   * create the forward index under the specific directory. Forward index is
   * simply stored as json format for now.
   *
   * @param outputDir
   * @param forwardIndex
   * @param nodeId
   * @throws IOException
   */
  public static void saveLeafNodeForwardIndex(String outputDir,
      Map<String, Map<String, Integer>> forwardIndex, String nodeId)
      throws IOException {
    // serialize index
    byte[] indexBytes = OBJECT_MAPPER.writeValueAsBytes(forwardIndex);

    // Write index
    File indexFile = new File(outputDir, nodeId
        + StarTreeConstants.INDEX_FILE_SUFFIX);
    if (!indexFile.createNewFile()) {
      throw new IOException(indexFile + " already exists");
    }
    IOUtils.copy(new ByteArrayInputStream(indexBytes), new FileOutputStream(
        indexFile));
  }

  public static List<int[]> readLeafRecords(String dataDir, String nodeId,
      int numDimensions) throws IOException {
    File file = new File(dataDir, nodeId + StarTreeConstants.BUFFER_FILE_SUFFIX);
    FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel();
    ByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0,
        file.length());
    buffer.order(ByteOrder.BIG_ENDIAN);
    List<int[]> ret = new ArrayList<int[]>();
    while (buffer.hasRemaining()) {
      int[] arr = new int[numDimensions];
      for (int i = 0; i < numDimensions; i++) {
        arr[i] = buffer.getInt();
      }
      ret.add(arr);
    }
    fileChannel.close();
    return ret;
  }

  public static Map<int[], Map<Long, int[]>> readLeafRecords(String dataDir,
      String nodeId, int numDimensions, int numMetrics, int numTimeBuckets)
      throws IOException {
    Map<int[], Map<Long, int[]>> ret = new HashMap<int[], Map<Long, int[]>>();

    File file = new File(dataDir, nodeId + StarTreeConstants.BUFFER_FILE_SUFFIX);
    FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel();
    ByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0,
        file.length());
    buffer.order(ByteOrder.BIG_ENDIAN);

    while (buffer.hasRemaining()) {
      int[] dimArray = new int[numDimensions];

      for (int i = 0; i < numDimensions; i++) {
        dimArray[i] = buffer.getInt();
      }
      HashMap<Long, int[]> timeSeries = new HashMap<Long, int[]>();
      ret.put(dimArray, timeSeries);
      if (numTimeBuckets > 0) {
        for (int i = 0; i < numTimeBuckets; i++) {
          long timeWindow = buffer.getLong();
          int[] metricArray = new int[numMetrics];
          for (int j = 0; j < numMetrics; j++) {
            metricArray[j] = buffer.getInt();
          }
          timeSeries.put(timeWindow, metricArray);
        }
      }
    }
    fileChannel.close();
    return ret;
  }

  public static Map<int[], Map<Long, Number[]>> readLeafRecords(String dataDir,
      String nodeId, int numDimensions, int numMetrics,
      List<MetricSpec> metricSpecs, int numTimeBuckets) throws IOException {
    Map<int[], Map<Long, Number[]>> ret = new HashMap<int[], Map<Long, Number[]>>();

    File file = new File(dataDir, nodeId + StarTreeConstants.BUFFER_FILE_SUFFIX);
    FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel();
    ByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0,
        file.length());
    buffer.order(ByteOrder.BIG_ENDIAN);

    while (buffer.hasRemaining()) {
      int[] dimArray = new int[numDimensions];

      for (int i = 0; i < numDimensions; i++) {
        dimArray[i] = buffer.getInt();
      }
      HashMap<Long, Number[]> timeSeries = new HashMap<Long, Number[]>();
      ret.put(dimArray, timeSeries);
      if (numTimeBuckets > 0) {
        for (int i = 0; i < numTimeBuckets; i++) {
          long timeWindow = buffer.getLong();
          Number[] metricArray = new Number[numMetrics];
          for (int j = 0; j < numMetrics; j++) {
            metricArray[j] = NumberUtils.readFromBuffer(buffer, metricSpecs.get(j).getType());
          }
          timeSeries.put(timeWindow, metricArray);
        }
      }
    }
    fileChannel.close();
    return ret;
  }

  public static Map<String, Map<String, Integer>> readForwardIndex(
      String nodeId, String dataDir) throws IOException {
    String expectedName = nodeId + StarTreeConstants.INDEX_FILE_SUFFIX;
    File indexFile = new File(dataDir, expectedName);
    LOGGER.info("Reading forward index at:" + indexFile.getAbsolutePath());
    Map<String, Map<String, Integer>> forwardIndex = OBJECT_MAPPER.readValue(
        indexFile, FWD_INDEX_TYPE_REFERENCE);
    return forwardIndex;
  }

  public static StarTreeNode loadStarTree(InputStream is) throws Exception {
    ObjectInputStream objectInputStream = new ObjectInputStream(is);
    StarTreeNode starTreeRootNode = (StarTreeNode) objectInputStream
        .readObject();
    return starTreeRootNode;
  }

}
