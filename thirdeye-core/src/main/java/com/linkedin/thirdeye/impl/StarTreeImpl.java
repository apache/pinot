package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.StarTreeRecordThresholdFunction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class StarTreeImpl implements StarTree
{
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

    node.init(config);

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
  public StarTreeRecord query(StarTreeQuery query)
  {
    StarTreeNode node = find(root, query);

    if (node == null)
    {
      throw new IllegalArgumentException("No star tree node for query " + query);
    }

    long[] sums = node.getRecordStore().getMetricSums(query);

    StarTreeRecordImpl.Builder result = new StarTreeRecordImpl.Builder();
    result.setDimensionValues(query.getDimensionValues());

    int idx = 0;
    for (String metricName : config.getMetricNames())
    {
      result.setMetricValue(metricName, sums[idx++]);
    }

    return result.build();
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

  @Override
  public StarTreeNode find(StarTreeQuery query)
  {
    return find(root, query);
  }

  private StarTreeNode find(StarTreeNode node, StarTreeQuery query)
  {
    if (node.isLeaf())
    {
      return node;
    }
    else
    {
      StarTreeNode target;

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

      return find(target, query);
    }
  }

  @Override
  public Collection<StarTreeNode> findAll(StarTreeQuery query)
  {
    Set<StarTreeNode> collector = new HashSet<StarTreeNode>();
    findAll(root, query, collector);
    return collector;
  }

  private void findAll(StarTreeNode node, StarTreeQuery query, Collection<StarTreeNode> collector)
  {
    if (node.isLeaf())
    {
      collector.add(node);
    }
    else
    {
      StarTreeNode target;

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

      findAll(target, query, collector);

      if (target != node.getStarNode())
      {
        findAll(node.getOtherNode(), query, collector);
      }
    }
  }
}
