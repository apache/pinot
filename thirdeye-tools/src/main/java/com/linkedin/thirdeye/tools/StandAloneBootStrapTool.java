package com.linkedin.thirdeye.tools;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.bootstrap.DimensionKey;
import com.linkedin.thirdeye.bootstrap.MetricSchema;
import com.linkedin.thirdeye.bootstrap.MetricTimeSeries;
import com.linkedin.thirdeye.bootstrap.MetricType;
import com.linkedin.thirdeye.bootstrap.startree.StarTreeJobUtils;
import com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase1.StarTreeBootstrapPhaseOneConfig;
import com.linkedin.thirdeye.bootstrap.util.CircularBufferUtil;
import com.linkedin.thirdeye.impl.StarTreePersistanceUtil;
import com.linkedin.thirdeye.impl.StarTreeRecordStreamAvroFileImpl;
import com.linkedin.thirdeye.impl.StarTreeUtils;

/**
 * Takes star tree + leaf records with empty time series and the input data.
 * Outputs the leaf records with time series
 * 
 * @author kgopalak
 * 
 */
public class StandAloneBootStrapTool {
  private static final Logger LOG = LoggerFactory
      .getLogger(StandAloneBootStrapTool.class);
  // turn it on when you want to debug. In general, debugging will be turned on
  // for some subset of data. It will be good to have this configurable.
  private static boolean debug = false;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static void main(String[] args) throws Exception {

    String configPath = null;
    String inputPath = null;
    String treeBinary = null;
    String leafDataInputDirectory = null;
    String leafDataOutputDirectory = null;
    if (args.length == 5) {
      configPath = args[0];
      inputPath = args[1];
      treeBinary = args[2];
      leafDataInputDirectory = args[3];
      leafDataOutputDirectory = args[4];
    } else {
      System.err.println();
      System.exit(-1);
    }
    // read config
    StarTreeBootstrapPhaseOneConfig config;
    config = OBJECT_MAPPER.readValue(new FileInputStream(new File(configPath)),
        StarTreeBootstrapPhaseOneConfig.class);
    List<String> dimensionNames = config.getDimensionNames();
    List<String> metricNames = config.getMetricNames();
    String timeColumnName = config.getTimeColumnName();
    int numTimeBuckets = config.getNumTimeBuckets();

    ArrayList<MetricType> types = Lists.newArrayList(MetricType.INT,
        MetricType.INT, MetricType.INT, MetricType.INT, MetricType.INT);

    MetricSchema schema = new MetricSchema(metricNames, types);
    // read the tree
    StarTreeNode starTreeRootNode = StarTreePersistanceUtil
        .loadStarTree(new FileInputStream(new File(treeBinary)));
    LinkedList<StarTreeNode> leafNodes = new LinkedList<StarTreeNode>();

    // Traverse tree and load the leaf data structures for each leaf, create
    // helper data structures
    StarTreeUtils.traverseAndGetLeafNodes(leafNodes, starTreeRootNode);
    LOG.info("Num leaf Nodes in star tree:" + leafNodes.size());
    HashMap<UUID, StarTreeNode> leafNodesMap = new HashMap<UUID, StarTreeNode>();
    HashMap<UUID, Map<String, Map<String, Integer>>> forwardIndexMap = new HashMap<UUID, Map<String, Map<String, Integer>>>();
    HashMap<UUID, List<int[]>> nodeIdToleafRecordsMap = new HashMap<UUID, List<int[]>>();
    for (StarTreeNode node : leafNodes) {
      UUID uuid = node.getId();
      Map<String, Map<String, Integer>> forwardIndex = StarTreePersistanceUtil
          .readForwardIndex(uuid.toString(), leafDataInputDirectory);
      List<int[]> leafRecords = StarTreePersistanceUtil.readLeafRecords(
          leafDataInputDirectory, uuid.toString(), dimensionNames.size());
      leafNodesMap.put(uuid, node);
      forwardIndexMap.put(uuid, forwardIndex);
      nodeIdToleafRecordsMap.put(uuid, leafRecords);
    }
    Map<DimensionKey, MetricTimeSeries> rawRecordMap = new HashMap<DimensionKey, MetricTimeSeries>();

    Map<DimensionKey, MetricTimeSeries> aggregatedLeafRecords = new HashMap<DimensionKey, MetricTimeSeries>();
    // process the input
    if (new File(inputPath).isFile()) {
      File avroFile = new File(inputPath);

      StarTreeRecordStreamAvroFileImpl recordStream = new StarTreeRecordStreamAvroFileImpl(
          avroFile, dimensionNames, metricNames, timeColumnName);
      Iterator<StarTreeRecord> iterator = recordStream.iterator();
      Map<UUID, StarTreeRecord> collector = new HashMap<UUID, StarTreeRecord>();
      int rowId = 0;
      while (iterator.hasNext()) {
        rowId = rowId + 1;
        if (rowId % 5000 == 0) {
          LOG.info("Processed {}", rowId);
        }
        StarTreeRecord record = iterator.next();
        String[] dimensionValues = new String[dimensionNames.size()];
        for (int i = 0; i < dimensionNames.size(); i++) {
          dimensionValues[i] = record.getDimensionValues().get(
              dimensionNames.get(i));
        }
        DimensionKey key = new DimensionKey(dimensionValues);
        MetricTimeSeries timeSeries = new MetricTimeSeries(schema);
        for (String name : metricNames) {
          timeSeries.set(record.getTime(), name,
              record.getMetricValues().get(name));
        }
        rawRecordMap.put(key, timeSeries);
        collector.clear();
        StarTreeJobUtils.collectRecords(starTreeRootNode, record, collector);
        LOG.info("processing {}", key);
        for (UUID uuid : collector.keySet()) {
          if (!leafNodesMap.containsKey(uuid)) {
            String msg = "Got a mapping to non existant leaf node:" + uuid
                + " - " + collector.get(uuid) + " input :" + record;
            LOG.error(msg);
            throw new RuntimeException(msg);
          }
          List<int[]> leafRecords = nodeIdToleafRecordsMap.get(uuid);
          Map<String, Map<String, Integer>> forwardIndex = forwardIndexMap
              .get(uuid);
          Map<String, Map<Integer, String>> reverseForwardIndex = StarTreeUtils
              .toReverseIndex(forwardIndex);
          int[] bestMatch = StarTreeJobUtils.findBestMatch(key, dimensionNames,
              leafRecords, forwardIndex);
          String[] matchedDimValues = StarTreeUtils.convertToStringValue(
              reverseForwardIndex, bestMatch, dimensionNames);
          DimensionKey matchedDimensionKey = new DimensionKey(matchedDimValues);
          LOG.info("Match: {} under {}", matchedDimensionKey,
              leafNodesMap.get(uuid).getPath());
          if (!aggregatedLeafRecords.containsKey(matchedDimensionKey)) {
            MetricTimeSeries value = new MetricTimeSeries(schema);
            aggregatedLeafRecords.put(matchedDimensionKey, value);
          }
          aggregatedLeafRecords.get(matchedDimensionKey).aggregate(timeSeries);
        }
      }

      new File(leafDataOutputDirectory).mkdirs();
      for (UUID nodeId : leafNodesMap.keySet()) {
        Map<String, Map<String, Integer>> forwardIndex = forwardIndexMap
            .get(nodeId);
        List<int[]> leafRecords = nodeIdToleafRecordsMap.get(nodeId);

        Map<String, Map<Integer, String>> reverseForwardIndex = StarTreeUtils
            .toReverseIndex(forwardIndex);
        // write the forward Index
        StarTreePersistanceUtil.saveLeafNodeForwardIndex(
            leafDataOutputDirectory, forwardIndex, nodeId.toString());
        // write the buffer file
        String fileName = leafDataOutputDirectory + "/" + nodeId
            + StarTreeConstants.BUFFER_FILE_SUFFIX;
        CircularBufferUtil.createLeafBufferFile(aggregatedLeafRecords,
            leafRecords, fileName, dimensionNames, metricNames, numTimeBuckets,
            reverseForwardIndex);
      }
      int[] metrics = new int[metricNames.size()];
      if (debug) {
        for (Entry<DimensionKey, MetricTimeSeries> entry : aggregatedLeafRecords
            .entrySet()) {
          DimensionKey key = entry.getKey();
          MetricTimeSeries timeSeries = entry.getValue();
          System.out.println(key);
          for (Long timeWindow : timeSeries.getTimeWindowSet()) {
            for (int j = 0; j < metricNames.size(); j++) {
              metrics[j] = timeSeries.get(timeWindow, metricNames.get(j))
                  .intValue();
            }
            System.out.println("\t\t\t" + timeWindow + " : "
                + Arrays.toString(metrics));
          }
        }
        // raw records
        for (Entry<DimensionKey, MetricTimeSeries> entry : rawRecordMap
            .entrySet()) {
          DimensionKey key = entry.getKey();
          MetricTimeSeries timeSeries = entry.getValue();
          System.out.println(key);
          for (Long timeWindow : timeSeries.getTimeWindowSet()) {
            for (int j = 0; j < metricNames.size(); j++) {
              metrics[j] = timeSeries.get(timeWindow, metricNames.get(j))
                  .intValue();
            }
            System.out.println("\t\t\t" + timeWindow + " : "
                + Arrays.toString(metrics));
          }
        }
        System.out.println();
      }
    }
  }
}
