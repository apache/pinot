package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.StarTreeRecordThresholdFunction;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class StarTreeImpl implements StarTree
{
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final StarTreeRecordThresholdFunction thresholdFunction;
  private final int maxRecordStoreEntries;
  private final StarTreeConfig config;
  private final StarTreeNode root;

  public StarTreeImpl(StarTreeConfig config)
  {
    this(config, new StarTreeNodeImpl(
            UUID.randomUUID(),
            config.getThresholdFunction(),
            config.getRecordStoreFactory(),
            StarTreeConstants.STAR,
            StarTreeConstants.STAR,
            new ArrayList<String>(),
            new HashMap<String, StarTreeNode>(),
            null,
            null));
  }

  public StarTreeImpl(StarTreeConfig config, StarTreeNode root)
  {
    this.config = config;
    this.thresholdFunction = config.getThresholdFunction();
    this.maxRecordStoreEntries = config.getMaxRecordStoreEntries();
    this.root = root;
  }

  @Override
  public StarTreeNode getRoot()
  {
    return root;
  }

  @Override
  public StarTreeConfig getConfig()
  {
    return config;
  }

  @Override
  public void open() throws IOException
  {
    open(root);
  }

  private void open(StarTreeNode node) throws IOException
  {
    if (node == null)
    {
      return;
    }

    node.init();

    if (!node.isLeaf())
    {
      for (StarTreeNode child : node.getChildren())
      {
        open(child);
      }
      open(node.getOtherNode());
      open(node.getStarNode());
    }
  }

  @Override
  public StarTreeRecord search(StarTreeQuery query)
  {
    return search(root, query);
  }

  public StarTreeRecord search(StarTreeNode node, StarTreeQuery query)
  {
    if (node.isLeaf())
    {
      long[] sums = node.getRecordStore().getMetricSums(query, thresholdFunction);

      StarTreeRecordImpl.Builder result = new StarTreeRecordImpl.Builder();
      result.setDimensionValues(query.getDimensionValues());

      int idx = 0;
      for (String metricName : config.getMetricNames())
      {
        result.setMetricValue(metricName, sums[idx++]);
      }

      return result.build();
    }
    else
    {
      // Traverse
      StarTreeNode target = null;
      String queryDimensionValue = query.getDimensionValues().get(node.getChildDimensionName());
      if (StarTreeConstants.STAR.equals(queryDimensionValue))
      {
        target = node.getStarNode();
      }
      else if (StarTreeConstants.OTHER.equals(queryDimensionValue))
      {
        target = node.getOtherNode();
      }
      else
      {
        target = node.getChild(queryDimensionValue);
      }

      if (target == null)
      {
        target = node.getOtherNode();
      }

      return search(target, query);
    }
  }

  @Override
  public void add(StarTreeRecord record)
  {
    add(root, record);
  }

  private void add(StarTreeNode node, StarTreeRecord record)
  {
    if (node.isLeaf())
    {
      node.getRecordStore().update(record);

      // Split node on dimension with highest cardinality if we've spilled over, and have more dimensions to split on
      if (node.getRecordStore().size() > maxRecordStoreEntries
              && node.getAncestorDimensionNames().size() < record.getDimensionValues().size())
      {
        // Determine dimension cardinality
        Set<String> blacklist = new HashSet<String>();
        blacklist.addAll(node.getAncestorDimensionNames());
        blacklist.add(node.getDimensionName());
        String maxCardinalityDimensionName = node.getRecordStore().getMaxCardinalityDimension(blacklist);

        // Split if we found a valid dimension
        if (maxCardinalityDimensionName != null)
        {
          node.split(maxCardinalityDimensionName);
        }
      }
    }
    else
    {
      // Look for a specific dimension node
      StarTreeNode target = node.getChild(record.getDimensionValues().get(node.getChildDimensionName()));

      // If couldn't find one, use other node
      if (target == null)
      {
        target = node.getOtherNode();
      }

      // Add to this node
      add(target, record);

      // In addition to this, update the star node after relaxing dimension of level to "*"
      add(node.getStarNode(), record.relax(target.getDimensionName()));
    }
  }

  @Override
  public void close() throws IOException
  {
    close(root);
  }

  private void close(StarTreeNode node) throws IOException
  {
    if (node.isLeaf())
    {
      node.getRecordStore().close();
    }
    else
    {
      for (StarTreeNode child : node.getChildren())
      {
        close(child);
      }
      close(node.getOtherNode());
      close(node.getStarNode());
    }
  }

  @Override
  public String toString()
  {
    return toString(false);
  }

  @Override
  public String toString(boolean includeRecords)
  {
    try
    {
      return OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(toString(root, includeRecords));
    }
    catch (Exception e)
    {
      throw new IllegalStateException("Could not serialize tree", e);
    }
  }

  private Map<String, Object> toString(StarTreeNode node, boolean includeRecords)
  {
    if (node == null)
    {
      return null;
    }

    Map<String, Object> map = new HashMap<String, Object>();
    map.put("id", node.getId());
    map.put("dimensionName", node.getDimensionName());
    map.put("dimensionValue", node.getDimensionValue());
    map.put("ancestorDimensionNames", node.getAncestorDimensionNames());

    List<Map<String, Object>> children = new ArrayList<Map<String, Object>>();
    if (node.isLeaf())
    {
      map.put("recordStoreSize", node.getRecordStore().size());
      if (includeRecords)
      {
        List<String> records = new ArrayList<String>();
        for (StarTreeRecord record : node.getRecordStore())
        {
          records.add(record.toString());
        }
        map.put("records", records);
      }
    }
    else
    {
      for (StarTreeNode child : node.getChildren())
      {
        children.add(toString(child, includeRecords));
      }
    }
    map.put("children", children);
    map.put("other", toString(node.getOtherNode(), includeRecords));
    map.put("star", toString(node.getStarNode(), includeRecords));

    return map;
  }

  @Override
  public void save(OutputStream outputStream) throws IOException
  {
    outputStream.write(toString().getBytes());
  }

  @Override
  public Set<String> getDimensionValues(String dimensionName)
  {
    Set<String> collector = new HashSet<String>();
    getDimensionValues(root, dimensionName, collector);
    return collector;
  }

  public void getDimensionValues(StarTreeNode node,
                                 String dimensionName,
                                 Set<String> collector)
  {
    if (node.isLeaf())
    {
      Set<String> dimensionValues = node.getRecordStore().getDimensionValues(dimensionName);
      if (dimensionValues != null)
      {
        collector.addAll(dimensionValues);
      }
    }
    else if (dimensionName.equals(node.getDimensionName())
            && StarTreeConstants.OTHER.equals(node.getDimensionValue()))
    {
      collector.add(StarTreeConstants.OTHER);
    }
    else
    {
      // All children
      for (StarTreeNode child : node.getChildren())
      {
        getDimensionValues(child, dimensionName, collector);
      }

      // The other node (n.b. don't need star because those are just repeats)
      if (node.getOtherNode() != null)
      {
        getDimensionValues(node.getOtherNode(), dimensionName, collector);
      }
    }
  }

  @Override
  public Set<String> getOtherDimensionValues(String dimensionName)
  {
    Set<String> collector = new HashSet<String>();
    getOtherDimensionValues(root, dimensionName, collector);
    return collector;
  }

  private void getOtherDimensionValues(StarTreeNode node, String dimensionName, Set<String> collector)
  {
    if (node.isLeaf()) // we haven't split and determined "other" yet...
    {
      if (thresholdFunction != null)
      {
        collector.addAll(StarTreeUtils.getOtherValues(dimensionName, node.getRecordStore(), thresholdFunction));
      }
    }
    else if (dimensionName.equals(node.getDimensionName())
            && StarTreeConstants.OTHER.equals(node.getDimensionValue()))
    {
      // The other node is a sub-tree, so collect all dimension values under it
      getDimensionValues(node, dimensionName, collector);
    }
    else
    {
      // Traverse to find all sub-trees with dimensionName and "other"
      for (StarTreeNode child : node.getChildren())
      {
        getOtherDimensionValues(child, dimensionName, collector);
      }
      if (node.getOtherNode() != null)
      {
        getOtherDimensionValues(node.getOtherNode(), dimensionName, collector);
      }
    }
  }
}
