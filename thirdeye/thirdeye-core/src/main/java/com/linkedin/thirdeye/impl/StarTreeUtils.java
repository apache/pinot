package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.StarTreeRecord;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Map.Entry;

public class StarTreeUtils {

  public static int getPartitionId(UUID nodeId, int numPartitions) {
    return (Integer.MAX_VALUE & nodeId.hashCode()) % numPartitions;
  }

  public static List<StarTreeQuery> filterQueries(StarTreeConfig config,
                                                  List<StarTreeQuery> queries,
                                                  Map<String, List<String>> filter)
  {
    List<StarTreeQuery> filteredQueries = new ArrayList<StarTreeQuery>(queries.size());

    for (StarTreeQuery query : queries)
    {
      boolean matches = true;

      for (Map.Entry<String, List<String>> entry : filter.entrySet())
      {
        String dimensionName = entry.getKey();
        String dimensionValue = query.getDimensionKey().getDimensionValue(config.getDimensions(), dimensionName);

        if (!entry.getValue().contains(StarTreeConstants.ALL) && !entry.getValue().contains(dimensionValue))
        {
          matches = false;
          break;
        }
      }

      if (matches)
      {
        filteredQueries.add(query);
      }
    }

    return filteredQueries;
  }

  public static List<StarTreeQuery> expandQueries(StarTree starTree,
                                                  StarTreeQuery baseQuery)
  {
    Set<String> dimensionsToExpand = new HashSet<String>();

    List<DimensionSpec> dimensionSpecs = starTree.getConfig().getDimensions();

    for (int i = 0; i < dimensionSpecs.size(); i++)
    {
      if (StarTreeConstants.ALL.equals(baseQuery.getDimensionKey().getDimensionValues()[i]))
      {
        dimensionsToExpand.add(dimensionSpecs.get(i).getName());
      }
    }

    List<StarTreeQuery> queries = new LinkedList<StarTreeQuery>();
    queries.add(baseQuery);

    // Expand "!" (all) dimension values into multiple queries
    for (String dimensionName : dimensionsToExpand)
    {
      // For each existing getAggregate, add a new one with these
      List<StarTreeQuery> expandedQueries = new ArrayList<StarTreeQuery>();
      for (StarTreeQuery query : queries)
      {
        Map<String, Collection<String>> dimensionValues = new HashMap<String, Collection<String>>(dimensionSpecs.size());

        for (int i = 0; i < dimensionSpecs.size(); i++)
        {
          dimensionValues.put(dimensionSpecs.get(i).getName(),
              Collections.singletonList(query.getDimensionKey().getDimensionValues()[i]));
        }

        Set<String> values = starTree.getDimensionValues(dimensionName, dimensionValues);

        for (String value : values)
        {
          // Create new key
          String[] newValues = new String[dimensionSpecs.size()];
          for (int i = 0; i < dimensionSpecs.size(); i++)
          {
            newValues[i] = dimensionSpecs.get(i).getName().equals(dimensionName)
                    ? value
                    : query.getDimensionKey().getDimensionValues()[i];
          }

          // Copy original getAggregate with new value
          expandedQueries.add(new StarTreeQueryImpl.Builder()
                                      .setDimensionKey(new DimensionKey(newValues))
                                      .setTimeRange(query.getTimeRange())
                                      .build(starTree.getConfig()));
        }
      }

      // Reset list of queries
      queries = expandedQueries;
    }

    return queries;
  }

  public static int printNode(PrintWriter printWriter, StarTreeNode node,
      int level) {
    int rawRecords = 0;
    StringBuilder sb = new StringBuilder();

    for (int i = 0; i < level; i++) {
      sb.append("\t");
    }

    sb.append("START:").append(node.getDimensionName()).append(":")
        .append(node.getDimensionValue()).append("(").append(node.getId())
        .append(")").append("(depth:").append(level).append(")");

    printWriter.println(sb.toString());

    if (!node.isLeaf()) {
      for (StarTreeNode child : node.getChildren()) {
        rawRecords += printNode(printWriter, child, level + 1);
      }
      rawRecords += printNode(printWriter, node.getOtherNode(), level + 1);
      printNode(printWriter, node.getStarNode(), level + 1);
    } else if (node.getRecordStore() != null) {
      rawRecords = node.getRecordStore().getRecordCountEstimate();
    }
    sb = new StringBuilder();
    for (int i = 0; i < level; i++) {
      sb.append("\t");
    }
    sb.append(String.format("END:%s count:%s", node.getDimensionName(),
        rawRecords));
    printWriter.println(sb.toString());
    return rawRecords;
  }

  /**
   * Traverses the star tree and computes all the leaf nodes. The leafNodes
   * structure is filled with all startreeNodes in the leaf.
   *
   * @param leafNodes
   * @param node
   */
  public static void traverseAndGetLeafNodes(Collection<StarTreeNode> leafNodes,
      StarTreeNode node) {
    if (node.isLeaf()) {
      leafNodes.add(node);
    } else {
      Collection<StarTreeNode> children = node.getChildren();
      for (StarTreeNode child : children) {
        traverseAndGetLeafNodes(leafNodes, child);
      }
      traverseAndGetLeafNodes(leafNodes, node.getOtherNode());
      traverseAndGetLeafNodes(leafNodes, node.getStarNode());
    }
  }

  /**
   *
   * @param forwardIndex
   * @return
   */
  public static Map<String, Map<Integer, String>> toReverseIndex(
      Map<String, Map<String, Integer>> forwardIndex) {
    Map<String, Map<Integer, String>> reverseForwardIndex = new HashMap<String, Map<Integer, String>>();
    for (String dimensionName : forwardIndex.keySet()) {
      Map<String, Integer> map = forwardIndex.get(dimensionName);
      reverseForwardIndex.put(dimensionName, new HashMap<Integer, String>());
      for (Entry<String, Integer> entry : map.entrySet()) {
        reverseForwardIndex.get(dimensionName).put(entry.getValue(),
            entry.getKey());
      }
    }
    return reverseForwardIndex;
  }

  public static String toDimensionString(StarTreeRecord record,
      List<DimensionSpec> dimensionSpecs) {
    return record.getDimensionKey().toString();
  }

  /**
   * converts the raw integer id to string representation using the reverse
   * forward Index
   *
   * @param reverseForwardIndex
   * @param leafRecord
   * @return
   */
  public static String[] convertToStringValue(
      Map<String, Map<Integer, String>> reverseForwardIndex, int[] leafRecord,
      List<String> dimensionNames) {
    String[] ret = new String[leafRecord.length];
    for (int i = 0; i < leafRecord.length; i++) {
      ret[i] = reverseForwardIndex.get(dimensionNames.get(i))
          .get(leafRecord[i]);
    }
    return ret;
  }


  public static int getDepth(StarTreeNode root) {
    if (root == null) {
      return 0;
    }

    int maxDepth = 0;
    List<Integer> depths = new ArrayList<Integer>();
    for (StarTreeNode child : root.getChildren()) {
      depths.add(getDepth(child));
    }
    depths.add(getDepth(root.getOtherNode()));
    depths.add(getDepth(root.getStarNode()));

    if (depths.size() != 0) {
      maxDepth = Collections.max(depths);
    }
    return 1 + maxDepth;
  }
}
