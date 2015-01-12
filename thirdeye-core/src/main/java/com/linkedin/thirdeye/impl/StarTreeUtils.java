package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.StarTreeRecord;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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

  public static StarTreeRecord merge(Collection<StarTreeRecord> records) {
    if (records.isEmpty()) {
      throw new IllegalArgumentException("Cannot merge empty set of records");
    }

    StarTreeRecordImpl.Builder builder = new StarTreeRecordImpl.Builder();

    Iterator<StarTreeRecord> itr = records.iterator();

    StarTreeRecord first = itr.next();
    builder.setDimensionValues(first.getDimensionValues());
    builder.setMetricValues(first.getMetricValues());
    builder.setMetricType(first.getMetricTypes());
    builder.setTime(first.getTime());

    while (itr.hasNext()) {
      StarTreeRecord record = itr.next();
      builder.updateMetricValues(record.getMetricValues());
      builder.updateDimensionValues(record.getDimensionValues());

      if (builder.getTime() != null
          && !builder.getTime().equals(record.getTime())) {
        throw new IllegalArgumentException(
            "Records with non-null time must all have same time to be merged");
      }
    }

    return builder.build();
  }

  public static List<StarTreeQuery> filterQueries(List<StarTreeQuery> queries,
      Map<String, List<String>> filter) {
    List<StarTreeQuery> filteredQueries = new ArrayList<StarTreeQuery>(
        queries.size());

    for (StarTreeQuery query : queries) {
      boolean matches = true;

      for (Map.Entry<String, List<String>> entry : filter.entrySet()) {
        if (!entry.getValue().contains(StarTreeConstants.ALL)
            && !entry.getValue().contains(
                query.getDimensionValues().get(entry.getKey()))) {
          matches = false;
          break;
        }
      }

      if (matches) {
        filteredQueries.add(query);
      }
    }

    return filteredQueries;
  }

  public static List<StarTreeQuery> expandQueries(StarTree starTree,
      StarTreeQuery baseQuery) {
    Set<String> dimensionsToExpand = new HashSet<String>();
    for (Map.Entry<String, String> entry : baseQuery.getDimensionValues()
        .entrySet()) {
      if (StarTreeConstants.ALL.equals(entry.getValue())) {
        dimensionsToExpand.add(entry.getKey());
      }
    }

    List<StarTreeQuery> queries = new LinkedList<StarTreeQuery>();
    queries.add(baseQuery);

    // Expand "!" (all) dimension values into multiple queries
    for (String dimensionName : dimensionsToExpand) {
      // For each existing getAggregate, add a new one with these
      List<StarTreeQuery> expandedQueries = new ArrayList<StarTreeQuery>();
      for (StarTreeQuery query : queries) {
        Set<String> values = starTree.getDimensionValues(dimensionName,
            query.getDimensionValues());

        for (String value : values) {
          // Copy original getAggregate with new value
          expandedQueries.add(new StarTreeQueryImpl.Builder()
              .setDimensionValues(query.getDimensionValues())
              .setTimeBuckets(query.getTimeBuckets())
              .setTimeRange(query.getTimeRange())
              .setDimensionValue(dimensionName, value).build());
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
      rawRecords = node.getRecordStore().getRecordCount();
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
   * Converts a StarTreeRecord to GenericRecord
   */
  public static GenericRecord toGenericRecord(StarTreeConfig config,
      Schema schema, StarTreeRecord record, GenericRecord reuse) {
    GenericRecord genericRecord;
    if (reuse != null) {
      genericRecord = reuse;
    } else {
      genericRecord = new GenericData.Record(schema);
    }

    // Dimensions
    for (Map.Entry<String, String> dimension : record.getDimensionValues()
        .entrySet()) {
      switch (getType(schema.getField(dimension.getKey()).schema())) {
      case INT:
        genericRecord.put(dimension.getKey(),
            Integer.valueOf(dimension.getValue()));
        break;
      case LONG:
        genericRecord.put(dimension.getKey(),
            Long.valueOf(dimension.getValue()));
        break;
      case FLOAT:
        genericRecord.put(dimension.getKey(),
            Float.valueOf(dimension.getValue()));
        break;
      case DOUBLE:
        genericRecord.put(dimension.getKey(),
            Double.valueOf(dimension.getValue()));
        break;
      case BOOLEAN:
        genericRecord.put(dimension.getKey(),
            Boolean.valueOf(dimension.getValue()));
        break;
      case STRING:
        genericRecord.put(dimension.getKey(), dimension.getValue());
        break;
      default:
        throw new IllegalStateException("Unsupported dimension type "
            + schema.getField(dimension.getKey()));
      }
    }

    // Metrics
    for (Map.Entry<String, Number> metric : record.getMetricValues()
        .entrySet()) {
      switch (getType(schema.getField(metric.getKey()).schema())) {
      case INT:
        genericRecord.put(metric.getKey(), metric.getValue());
        break;
      case LONG:
        genericRecord.put(metric.getKey(), metric.getValue().longValue());
        break;
      case FLOAT:
        genericRecord.put(metric.getKey(), metric.getValue().floatValue());
        break;
      case DOUBLE:
        genericRecord.put(metric.getKey(), metric.getValue().doubleValue());
        break;
      default:
        throw new IllegalStateException("Invalid metric schema type: "
            + schema.getField(metric.getKey()));
      }
    }

    // Time
    switch (getType(schema.getField(config.getTime().getColumnName()).schema())) {
    case INT:
      genericRecord
          .put(config.getTime().getColumnName(), record.getTime().intValue());
      break;
    case LONG:
      genericRecord.put(config.getTime().getColumnName(), record.getTime());
      break;
    default:
      throw new IllegalStateException("Invalid time schema type: "
          + schema.getField(config.getTime().getColumnName()));
    }

    // (Assume values we didn't touch are time, and fill in w/ 0, as these will
    // be unused)
    for (Schema.Field field : schema.getFields()) {
      if (!record.getDimensionValues().containsKey(field.name())
          && !record.getMetricValues().containsKey(field.name())
          && !config.getTime().getColumnName().equals(field.name())) {
        switch (getType(field.schema())) {
        case INT:
          genericRecord.put(field.name(), 0);
          break;
        case LONG:
          genericRecord.put(field.name(), 0L);
          break;
        default:
          throw new IllegalStateException("Invalid time schema type: "
              + field.schema().getType());
        }
      }
    }

    return genericRecord;
  }

  /**
   * Returns the type of a schema, handling ["null", {type}]-style optional
   * fields.
   */
  public static Schema.Type getType(Schema schema) {
    Schema.Type type = null;

    if (Schema.Type.UNION.equals(schema.getType())) {
      List<Schema> schemas = schema.getTypes();
      for (Schema s : schemas) {
        if (!Schema.Type.NULL.equals(s.getType())) {
          type = s.getType();
        }
      }
    } else {
      type = schema.getType();
    }

    if (type == null) {
      throw new IllegalStateException(
          "Could not unambiguously determine type of schema " + schema);
    }

    return type;
  }

  /**
   * Converts a GenericRecord to a StarTreeRecord
   */
  public static StarTreeRecord toStarTreeRecord(StarTreeConfig config,
      GenericRecord record) {
    StarTreeRecordImpl.Builder builder = new StarTreeRecordImpl.Builder();
    toStarTreeRecord(config, record, builder);
    return builder.build();
  }

  public static void toStarTreeRecord(StarTreeConfig config,
      GenericRecord record, StarTreeRecordImpl.Builder builder) {
    // Dimensions
    for (DimensionSpec dimensionSpec : config.getDimensions()) {
      Object dimensionValue = record.get(dimensionSpec.getName());
      if (dimensionValue == null) {
        throw new IllegalStateException("Record has no value for dimension "
            + dimensionSpec.getName());
      }
      builder.setDimensionValue(dimensionSpec.getName(), dimensionValue.toString());
    }

    // Metrics (n.b. null -> 0L)
    for (int i=0;i< config.getMetrics().size();i++) {
      String metricName  = config.getMetrics().get(i).getName();
      Object metricValue = record.get(metricName);
      if (metricValue == null) {
        metricValue = 0L;
      }
      builder.setMetricValue(metricName, ((Number) metricValue).intValue());
      builder.setMetricType(metricName, config.getMetrics().get(i).getType());
    }

    // Time
    Object time = record.get(config.getTime().getColumnName());
    if (time == null) {
      throw new IllegalStateException("Record does not have time column "
          + config.getTime().getColumnName() + ": " + record);
    }
    builder.setTime(((Number) time).longValue());
  }

  /**
   * Traverses the star tree and computes all the leaf nodes. The leafNodes
   * structure is filled with all startreeNodes in the leaf.
   * 
   * @param leafNodes
   * @param node
   */
  public static void traverseAndGetLeafNodes(List<StarTreeNode> leafNodes,
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
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    String delim = "";
    for (DimensionSpec spec : dimensionSpecs) {
      sb.append(delim).append(spec.getName()).append(":")
          .append(record.getDimensionValues().get(spec.getName()));
      delim = ",";
    }
    sb.append("]");
    return sb.toString();
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
}
