package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeCallback;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.StarTreeRecordStoreFactory;
import com.linkedin.thirdeye.api.StarTreeStats;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StarTreeImpl implements StarTree {
  private static final Logger LOGGER = LoggerFactory.getLogger(StarTreeImpl.class);

  private final int maxRecordStoreEntries;
  private final StarTreeConfig config;
  private final StarTreeNode root;
  private final StarTreeRecordStoreFactory recordStoreFactory;
  private final File dataDir;
  private final MetricSchema metricSchema;

  public StarTreeImpl(StarTreeConfig config)
  {
    this(config, null);
  }

  public StarTreeImpl(StarTreeConfig config, File dataDir) {
    this(config, dataDir, new StarTreeNodeImpl(UUID.randomUUID(),
        StarTreeConstants.STAR, StarTreeConstants.STAR,
        new ArrayList<String>(), new HashMap<String, String>(),
        new HashMap<String, StarTreeNode>(), null, null));
  }

  public StarTreeImpl(StarTreeConfig config, File dataDir, StarTreeNode root) {
    this.config = config;
    this.maxRecordStoreEntries = config.getSplit().getThreshold();
    this.root = root;
    this.dataDir = dataDir;
    this.metricSchema = MetricSchema.fromMetricSpecs(config.getMetrics());

    try
    {
      this.recordStoreFactory
              = (StarTreeRecordStoreFactory) Class.forName(config.getRecordStoreFactoryClass()).newInstance();

      this.recordStoreFactory.init(dataDir,
                                   config,
                                   config.getRecordStoreFactoryConfig());
    }
    catch (Exception e)
    {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public StarTreeNode getRoot() {
    return root;
  }

  @Override
  public StarTreeConfig getConfig() {
    return config;
  }

  @Override
  public StarTreeRecordStoreFactory getRecordStoreFactory()
  {
    return recordStoreFactory;
  }

  @Override
  public void open() throws IOException {
    open(root);
  }

  private void open(StarTreeNode node) throws IOException {
    if (node == null) {
      return;
    }

    node.init(config, recordStoreFactory);

    if (!node.isLeaf()) {
      for (StarTreeNode child : node.getChildren()) {
        open(child);
      }
      open(node.getOtherNode());
      open(node.getStarNode());
    }
  }

  @Override
  public MetricTimeSeries getTimeSeries(StarTreeQuery query) {
    StarTreeNode node = find(root, query);

    if (node == null) {
      throw new IllegalArgumentException("No star tree node for query " + query);
    }
    return node.getRecordStore().getTimeSeries(query);
  }

  @Override
  public void add(StarTreeRecord record) {
    add(root, record);
  }

  private boolean shouldSplit(StarTreeNode node) {
    return !config.isFixed()
            && node.getRecordStore().getRecordCountEstimate() > maxRecordStoreEntries
            && node.getRecordStore().getRecordCount() > maxRecordStoreEntries
            && node.getAncestorDimensionNames().size() < config.getDimensions().size();
  }

  private void add(StarTreeNode node, StarTreeRecord record)
  {
    if (node.isLeaf())
    {
      node.getRecordStore().update(record);
      boolean valid = true;
      if (!node.getDimensionValue().equals(StarTreeConstants.STAR))
      {
        for (String name : node.getAncestorDimensionNames())
        {
          String recordValue = record.getDimensionKey().getDimensionValue(config.getDimensions(), name);
          if (!recordValue.equals(node.getAncestorDimensionValues().get(name)))
          {
            valid = false;
          }
        }

        String recordValue = record.getDimensionKey().getDimensionValue(config.getDimensions(), node.getDimensionName());
        if (!recordValue.equals(node.getDimensionValue()))
        {
          valid = false;
        }
      }
      if (valid)
      {
        if (LOGGER.isTraceEnabled())
        {
          LOGGER.trace(
              "Added record:{} to node:{}",
              StarTreeUtils.toDimensionString(record, config.getDimensions()),
              node.getPath());
        }
      } else
      {
        LOGGER.error(
            "INVALID: Added record:{} to node:{}",
            StarTreeUtils.toDimensionString(record, config.getDimensions()),
            node.getPath());

      }
      if (shouldSplit(node))
      {
        synchronized (node)
        {
          if (shouldSplit(node))
          {
            Set<String> blacklist = new HashSet<String>();
            blacklist.addAll(node.getAncestorDimensionNames());
            blacklist.add(node.getDimensionName());

            String splitDimensionName = null;
            if (config.getSplit().getOrder() == null)
            {
              // Pick highest cardinality dimension
              splitDimensionName = node.getRecordStore()
                                       .getMaxCardinalityDimension(blacklist);
            } else
            {
              // Pick next to split on from fixed order
              for (String dimensionName : config.getSplit().getOrder())
              {
                if (!blacklist.contains(dimensionName))
                {
                  splitDimensionName = dimensionName;
                  break;
                }
              }
            }

            // Split if we found a valid dimension
            if (splitDimensionName != null)
            {
              node.split(splitDimensionName);
            }
          }
        }
      }
    }
    else
    {
      // Look for a specific dimension node under this node
      String childDimensionName = node.getChildDimensionName();
      String childDimensionValue = record.getDimensionKey().getDimensionValue(config.getDimensions(), childDimensionName);
      StarTreeNode target = node.getChild(childDimensionValue);

      if (target == null)
      {
        if (config.isFixed())
        {
          // If couldn't find one, use other node
          target = node.getOtherNode();
          StarTreeRecord aliasOtherRecord = record.aliasOther(childDimensionName);

          // Add to this node
          add(target, aliasOtherRecord);
        }
        else
        {
          target = node.addChildNode(childDimensionValue);
          add(target, record);
        }
      }
      else
      {
        add(target, record);
      }

      // In addition to this, update the star node after relaxing dimension of
      // level to "*"
      add(node.getStarNode(), record.relax(childDimensionName));
    }
  }

  @Override
  public void close() throws IOException {
    recordStoreFactory.close();
    close(root);
  }

  private void close(StarTreeNode node) throws IOException {
    if (node.isLeaf()) {
      if (node.getRecordStore() != null) {
        node.getRecordStore().close();
      } else {
        throw new IllegalStateException("Cannot close null record store for leaf node "+node.getId());
      }
    } else {
      for (StarTreeNode child : node.getChildren()) {
        close(child);
      }
      close(node.getOtherNode());
      close(node.getStarNode());
    }
  }

  @Override
  public Set<String> getDimensionValues(String dimensionName,
      Map<String, String> fixedDimensions) {
    Set<String> collector = new HashSet<String>();
    getDimensionValues(root, dimensionName, fixedDimensions, collector);
    return collector;
  }

  public void getDimensionValues(StarTreeNode node, String dimensionName,
      Map<String, String> fixedDimensions, Set<String> collector) {
    if (node.isLeaf()) {
      Set<String> dimensionValues = node.getRecordStore().getDimensionValues(
          dimensionName);
      if (dimensionValues != null) {
        collector.addAll(dimensionValues);
      }
    } else if (dimensionName.equals(node.getDimensionName())
        && StarTreeConstants.OTHER.equals(node.getDimensionValue())) {
      collector.add(StarTreeConstants.OTHER);
    } else {
      // All children
      for (StarTreeNode child : node.getChildren()) {
        if (shouldTraverse(child, fixedDimensions)) {
          getDimensionValues(child, dimensionName, fixedDimensions, collector);
        }
      }

      // The other node (n.b. don't need star because those are just repeats)
      if (shouldTraverse(node.getOtherNode(), fixedDimensions)) {
        getDimensionValues(node.getOtherNode(), dimensionName, fixedDimensions,
            collector);
      }
    }
  }

  /**
   * Returns true if we should traverse to a child given a set of fixed
   * dimensions.
   *
   * <p>
   * That is, the dimension isn't fixed (null or star), or is fixed and value is
   * equal.
   * </p>
   */
  private boolean shouldTraverse(StarTreeNode child,
      Map<String, String> fixedDimensions) {
    if (fixedDimensions == null) {
      return true;
    }

    String fixedValue = fixedDimensions.get(child.getDimensionName());

    return fixedValue == null || fixedValue.equals(StarTreeConstants.ALL)
        || fixedValue.equals(StarTreeConstants.STAR)
        || fixedValue.equals(child.getDimensionValue());
  }

  @Override
  public StarTreeNode find(StarTreeQuery query) {
    return find(root, query);
  }

  private StarTreeNode find(StarTreeNode node, StarTreeQuery query) {
    if (node.isLeaf()) {
      return node;
    } else {
      StarTreeNode target;

      String queryDimensionValue = query.getDimensionKey().getDimensionValue(config.getDimensions(), node.getChildDimensionName());
      if (StarTreeConstants.STAR.equals(queryDimensionValue)) {
        target = node.getStarNode();
      } else if (StarTreeConstants.OTHER.equals(queryDimensionValue)) {
        target = node.getOtherNode();
      } else {
        target = node.getChild(queryDimensionValue);
      }

      if (target == null) {
        target = node.getOtherNode();
      }

      return find(target, query);
    }
  }

  @Override
  public Collection<StarTreeNode> findAll(StarTreeQuery query) {
    Set<StarTreeNode> collector = new HashSet<StarTreeNode>();
    findAll(root, query, collector);
    return collector;
  }

  private void findAll(StarTreeNode node, StarTreeQuery query,
      Collection<StarTreeNode> collector) {
    if (node.isLeaf()) {
      collector.add(node);
    } else {
      StarTreeNode target;

      String queryDimensionValue = query.getDimensionKey().getDimensionValue(config.getDimensions(), node.getChildDimensionName());
      if (StarTreeConstants.STAR.equals(queryDimensionValue)) {
        target = node.getStarNode();
      } else if (StarTreeConstants.OTHER.equals(queryDimensionValue)) {
        target = node.getOtherNode();
      } else {
        target = node.getChild(queryDimensionValue);
      }

      if (target == null) {
        target = node.getOtherNode();
      }

      findAll(target, query, collector);

      if (target != node.getStarNode()) {
        findAll(node.getStarNode(), query, collector);
      }
    }
  }

  @Override
  public StarTreeStats getStats() {
    StarTreeStats stats = new StarTreeStats();
    getStats(root, stats);
    return stats;
  }

  @Override
  public void eachLeaf(StarTreeCallback callback)
  {
    eachLeaf(root, callback);
  }

  private void eachLeaf(StarTreeNode node, StarTreeCallback callback)
  {
    if (node.isLeaf())
    {
      callback.call(node);
    }
    else
    {
      for (StarTreeNode child : node.getChildren())
      {
        eachLeaf(child, callback);
      }
      eachLeaf(node.getOtherNode(), callback);
      eachLeaf(node.getStarNode(), callback);
    }
  }

  @Override
  public void clear() throws IOException
  {
    eachLeaf(new StarTreeCallback()
    {
      @Override
      public void call(StarTreeNode node)
      {
        if (node.getRecordStore() != null)
        {
          node.getRecordStore().clear();
        }
      }
    });
  }

  public void getStats(StarTreeNode node, StarTreeStats stats)
  {
    if (node.isLeaf())
    {
      if (node.getRecordStore() == null)
      {
        throw new IllegalStateException("Node " + node.getId() + " does not have record store. Has tree been opened?");
      }
      stats.countRecords(node.getRecordStore().getRecordCountEstimate());
      stats.updateMinTime(node.getRecordStore().getMinTime());
      stats.updateMaxTime(node.getRecordStore().getMaxTime());
      stats.countNode();
      stats.countLeaf();
    }
    else
    {
      stats.countNode();

      for (StarTreeNode child : node.getChildren())
      {
        getStats(child, stats);
      }
      getStats(node.getOtherNode(), stats);
      getStats(node.getStarNode(), stats);
    }
  }

  @Override
  public boolean equals(Object o)
  {
    if (!(o instanceof StarTree))
    {
      return false;
    }

    StarTree starTree = (StarTree) o;

    return root.getId().equals(starTree.getRoot().getId());
  }

  @Override
  public int hashCode()
  {
    return root.hashCode();
  }
}
