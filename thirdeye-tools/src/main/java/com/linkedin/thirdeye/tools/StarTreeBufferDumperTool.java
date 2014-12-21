package com.linkedin.thirdeye.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.BiMap;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.impl.StarTreePersistanceUtil;
import com.linkedin.thirdeye.impl.StarTreeRecordStoreCircularBufferImpl;
import com.linkedin.thirdeye.impl.StarTreeRecordStoreFactoryCircularBufferImpl;
import com.linkedin.thirdeye.impl.StarTreeUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class StarTreeBufferDumperTool {
  public static void main(String[] args) throws Exception {
    String config = args[0];
    String pathToTreeBinary = args[1];
    String dataDirectory = args[2];

    JsonNode jsonNode = new ObjectMapper()
        .readTree(new FileInputStream(config));
    StarTreeConfig starTreeConfig = StarTreeConfig.fromJson(jsonNode, new File(dataDirectory).getParentFile());

    StarTreeNode starTreeRootNode = StarTreePersistanceUtil
        .loadStarTree(new FileInputStream(pathToTreeBinary));

    List<StarTreeNode> leafNodes = new LinkedList<StarTreeNode>();
    StarTreeUtils.traverseAndGetLeafNodes(leafNodes, starTreeRootNode);

    List<String> dimensionNames = starTreeConfig.getDimensionNames();

    String[] dimValues = new String[dimensionNames.size()];
    for (StarTreeNode node : leafNodes) {
      Map<String, Map<String, Integer>> forwardIndex = StarTreePersistanceUtil
          .readForwardIndex(node.getId().toString(), dataDirectory);
      Map<String, Map<Integer, String>> reverseIndex = StarTreeUtils
          .toReverseIndex(forwardIndex);
      List<int[]> leafRecords = StarTreePersistanceUtil.readLeafRecords(
          dataDirectory, node.getId().toString(), starTreeConfig
              .getDimensionNames().size());
      Arrays.fill(dimValues, "-");
      for (int i = 0; i < dimensionNames.size(); i++) {
        String name = dimensionNames.get(i);

        if (node.getAncestorDimensionValues().containsKey(name)) {
          dimValues[i] = node.getAncestorDimensionValues().get(name);
        }
        if (node.getDimensionName().equals(name)) {
          dimValues[i] = node.getDimensionValue();
        }
      }
      System.out.println(node.getId() + Arrays.toString(dimValues));
      for (int arr[] : leafRecords) {
        Arrays.fill(dimValues, "");
        for (int i = 0; i < dimensionNames.size(); i++) {
          String name = dimensionNames.get(i);
          dimValues[i] = reverseIndex.get(name).get(arr[i]);
        }
        System.out.println("\t");
        System.out.println(Arrays.toString(dimValues));
      }
    }
  }

  static void dumpOld(String[] args) throws Exception {
    if (args.length != 2) {
      throw new IllegalArgumentException("usage: config.json nodeId");
    }

    JsonNode jsonNode = new ObjectMapper()
        .readTree(new FileInputStream(args[0]));
    JsonNode rootDir = jsonNode.get("recordStoreFactoryConfig").get("rootDir");
    JsonNode timeBuckets = jsonNode.get("recordStoreFactoryConfig").get(
        "numTimeBuckets");

    StarTreeConfig config = StarTreeConfig.fromJson(jsonNode);

    File file = new File(rootDir.asText(), args[1]
        + StarTreeConstants.BUFFER_FILE_SUFFIX);

    FileChannel fileChannel = new RandomAccessFile(file, "r").getChannel();

    ByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0,
        file.length());

    PrintWriter printWriter = new PrintWriter(System.out);

    StarTreeRecordStoreCircularBufferImpl.dumpBuffer(buffer, printWriter,
        config.getDimensionNames(), config.getMetricNames(),
        timeBuckets.asInt());

    printWriter.flush();
  }
}
