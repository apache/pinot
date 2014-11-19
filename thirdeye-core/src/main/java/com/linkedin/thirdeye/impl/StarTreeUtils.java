package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.StarTreeRecordThresholdFunction;

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

public class StarTreeUtils
{
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
      // For each existing getAggregate, add a new one with these
      List<StarTreeQuery> expandedQueries = new ArrayList<StarTreeQuery>();
      for (StarTreeQuery query : queries)
      {
        Set<String> values = starTree.getDimensionValues(dimensionName, query.getDimensionValues());

        for (String value : values)
        {
          // Copy original getAggregate with new value
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

  public static void printNode(PrintWriter printWriter, StarTreeNode node, int level)
  {
    StringBuilder sb = new StringBuilder();

    for (int i = 0; i < level; i++)
    {
      sb.append("\t");
    }

    sb.append(node.getDimensionName())
      .append(":")
      .append(node.getDimensionValue())
      .append("(")
      .append(node.getId())
      .append(")");

    printWriter.println(sb.toString());

    if (!node.isLeaf())
    {
      for (StarTreeNode child : node.getChildren())
      {
        printNode(printWriter, child, level + 1);
      }
      printNode(printWriter, node.getOtherNode(), level + 1);
      printNode(printWriter, node.getStarNode(), level + 1);
    }
  }
}
