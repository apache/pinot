package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.StarTreeRecordThresholdFunction;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
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

public class StarTreeUtils
{
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static StarTree build(StarTreeConfig config, InputStream inputStream) throws IOException
  {
    JsonNode root = OBJECT_MAPPER.readTree(inputStream);
    StarTree starTree = new StarTreeImpl(config, buildNode(config, root));
    starTree.open();
    return starTree;
  }

  private static StarTreeNode buildNode(StarTreeConfig config, JsonNode jsonNode) throws IOException
  {
    if (jsonNode.isNull())
    {
      return null;
    }

    List<String> ancestorDimensionNames = new ArrayList<String>();
    for (JsonNode ancestorDimensionName : jsonNode.get("ancestorDimensionNames"))
    {
      ancestorDimensionNames.add(ancestorDimensionName.asText());
    }

    Map<String, StarTreeNode> children = new HashMap<String, StarTreeNode>();
    for (JsonNode child : jsonNode.get("children"))
    {
      children.put(child.get("dimensionValue").asText(), buildNode(config, child));
    }

    StarTreeNode otherNode = null;
    if (jsonNode.has("other"))
    {
      otherNode = buildNode(config, jsonNode.get("other"));
    }

    StarTreeNode starNode = null;
    if (jsonNode.has("star"))
    {
      starNode = buildNode(config, jsonNode.get("star"));
    }

    return new StarTreeNodeImpl(
            UUID.fromString(jsonNode.get("id").asText()),
            config.getThresholdFunction(),
            config.getRecordStoreFactory(),
            jsonNode.get("dimensionName").asText(),
            jsonNode.get("dimensionValue").asText(),
            ancestorDimensionNames,
            children,
            otherNode,
            starNode);
  }

  public static StarTreeRecord merge(Collection<StarTreeRecord> records)
  {
    if (records.isEmpty())
    {
      throw new IllegalArgumentException("Cannot merge empty set of records");
    }

    StarTreeRecordImpl.Builder builder = new StarTreeRecordImpl.Builder();

    Iterator<StarTreeRecord> itr = records.iterator();

    StarTreeRecord first = itr.next();
    builder.setDimensionValues(first.getDimensionValues());
    builder.setMetricValues(first.getMetricValues());
    builder.setTime(first.getTime());

    while (itr.hasNext())
    {
      StarTreeRecord record = itr.next();
      builder.updateMetricValues(record.getMetricValues());
      builder.updateDimensionValues(record.getDimensionValues());

      if (builder.getTime() != null && !builder.getTime().equals(record.getTime()))
      {
        throw new IllegalArgumentException("Records with non-null time must all have same time to be merged");
      }
    }

    return builder.build();
  }

  public static Set<String> getOtherValues(String dimensionName,
                                           Iterable<StarTreeRecord> records,
                                           StarTreeRecordThresholdFunction thresholdFunction)
  {
    Map<String, List<StarTreeRecord>> groupByValue = new HashMap<String, List<StarTreeRecord>>();

    // Group records by dimension value
    for (StarTreeRecord record : records)
    {
      String dimensionValue = record.getDimensionValues().get(dimensionName);
      List<StarTreeRecord> values = groupByValue.get(dimensionValue);
      if (values == null)
      {
        values = new LinkedList<StarTreeRecord>();
        groupByValue.put(dimensionValue, values);
      }
      values.add(record);
    }

    // Collect those who do not pass threshold
    Set<String> otherValues = new HashSet<String>();
    for (Map.Entry<String, List<StarTreeRecord>> entry : groupByValue.entrySet())
    {
      if (!thresholdFunction.passesThreshold(entry.getValue()))
      {
        otherValues.add(entry.getKey());
      }
    }

    return otherValues;
  }

  public static List<StarTreeQuery> expandQueries(StarTree starTree, StarTreeQuery baseQuery)
  {
    Set<String> dimensionsToExpand = new HashSet<String>();
    for (Map.Entry<String, String> entry : baseQuery.getDimensionValues().entrySet())
    {
      if (StarTreeConstants.ALL.equals(entry.getValue()))
      {
        dimensionsToExpand.add(entry.getKey());
      }
    }

    List<StarTreeQuery> queries = new LinkedList<StarTreeQuery>();
    queries.add(baseQuery);

    // Expand "!" (all) dimension values into multiple queries
    for (String dimensionName : dimensionsToExpand)
    {
      // Find all dimension values
      Set<String> values = starTree.getDimensionValues(dimensionName);

      // For each existing query, add a new one with these
      List<StarTreeQuery> expandedQueries = new ArrayList<StarTreeQuery>();
      for (StarTreeQuery query : queries)
      {
        for (String value : values)
        {
          // Copy original query with new value
          expandedQueries.add(
                  new StarTreeQueryImpl.Builder()
                          .setDimensionValues(query.getDimensionValues())
                          .setTimeBuckets(query.getTimeBuckets())
                          .setTimeRange(query.getTimeRange())
                          .setDimensionValue(dimensionName, value)
                          .build());
        }
      }

      // Reset list of queries
      queries = expandedQueries;
    }

    return queries;
  }
}
