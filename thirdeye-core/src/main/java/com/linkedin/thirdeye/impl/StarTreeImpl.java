package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.StarTree;
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
  private static final Logger LOG = LoggerFactory.getLogger(StarTreeImpl.class);

  private final int maxRecordStoreEntries;
  private final StarTreeConfig config;
  private final StarTreeNode root;
  private final StarTreeRecordStoreFactory recordStoreFactory;
  private final File dataDir;

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
  public StarTreeRecord getAggregate(StarTreeQuery query) {
    StarTreeNode node = find(root, query);

    if (node == null) {
      throw new IllegalArgumentException("No star tree node for query " + query);
    }

    Number[] sums = node.getRecordStore().getMetricSums(query);

    StarTreeRecordImpl.Builder result = new StarTreeRecordImpl.Builder();

    // Set dimension values (aliasing to other as appropriate)
    for (Map.Entry<String, String> entry : query.getDimensionValues()
        .entrySet()) {
      if (node.getDimensionName().equals(entry.getKey())) {
        String dimensionValue = node.getDimensionValue().equals(
            StarTreeConstants.OTHER) ? StarTreeConstants.OTHER : entry
            .getValue();
        result.setDimensionValue(entry.getKey(), dimensionValue);
      } else if (node.getAncestorDimensionValues().containsKey(entry.getKey())) {
        String dimensionValue = node.getAncestorDimensionValues().get(
            entry.getKey());
        result
            .setDimensionValue(
                entry.getKey(),
                StarTreeConstants.OTHER.equals(dimensionValue) ? StarTreeConstants.OTHER
                    : entry.getValue());
      } else {
        result.setDimensionValue(entry.getKey(), entry.getValue());
      }
    }

    int idx = 0;
    for (int i=0;i< config.getMetrics().size();i++) {
      String metricName = config.getMetrics().get(i).getName();
      result.setMetricValue(metricName, sums[idx++]);
     result.setMetricType(metricName,config.getMetrics().get(i).getType());
      
    }

    return result.build();
  }

  @Override
  public List<StarTreeRecord> getTimeSeries(StarTreeQuery query) {
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
    return node.getRecordStore().getRecordCountEstimate() > maxRecordStoreEntries
        && node.getRecordStore().getRecordCount() > maxRecordStoreEntries
        && node.getAncestorDimensionNames().size() < config.getDimensions()
            .size();
  }

  private void add(StarTreeNode node, StarTreeRecord record) {
    if (node.isLeaf()) {
      node.getRecordStore().update(record);
      boolean valid = true;
      if (!node.getDimensionValue().equals(StarTreeConstants.STAR)) {
        for (String name : node.getAncestorDimensionNames()) {
          if (!record.getDimensionValues().get(name)
              .equals(node.getAncestorDimensionValues().get(name))) {
            valid = false;
          }
        }
        if (!record.getDimensionValues().get(node.getDimensionName())
            .equals(node.getDimensionValue())) {
          valid = false;
        }
      }
      if (valid) {
        LOG.info(
            "Added record:{} to node:{}",
            StarTreeUtils.toDimensionString(record, config.getDimensions()),
            node.getPath());
      } else {
        LOG.error(
            "INVALID: Added record:{} to node:{}",
            StarTreeUtils.toDimensionString(record, config.getDimensions()),
            node.getPath());

      }
      if (shouldSplit(node)) {
        synchronized (node) {
          if (shouldSplit(node)) {
            Set<String> blacklist = new HashSet<String>();
            blacklist.addAll(node.getAncestorDimensionNames());
            blacklist.add(node.getDimensionName());

            String splitDimensionName = null;
            if (config.getSplit().getOrder() == null) {
              // Pick highest cardinality dimension
              splitDimensionName = node.getRecordStore()
                  .getMaxCardinalityDimension(blacklist);
            } else {
              // Pick next to split on from fixed order
              for (String dimensionName : config.getSplit().getOrder()) {
                if (!blacklist.contains(dimensionName)) {
                  splitDimensionName = dimensionName;
                  break;
                }
              }
            }

            // Split if we found a valid dimension
            if (splitDimensionName != null) {
              node.split(splitDimensionName);
            }
          }
        }
      }
    } else {
      // Look for a specific dimension node under this node
      String childDimensionName = node.getChildDimensionName();
      String childDimensionValue = record.getDimensionValues().get(
          childDimensionName);
      StarTreeNode target = node.getChild(childDimensionValue);
      // if the child does not exist, either map it to OTHER node or create a
      // new child for this value
      if (target == null) {
        // TODO: based on the mode either create a node or map to this to other
        // node.
        boolean mapToOtherNode = false;
        if (mapToOtherNode) {
          // If couldn't find one, use other node
          target = node.getOtherNode();
          // TODO: change the dimensionValue in the record to other.
          String otherDimensionNames = childDimensionName;
          StarTreeRecord aliasOtherRecord = record
              .aliasOther(otherDimensionNames);
          // Add to this node
          add(target, aliasOtherRecord);
        } else {
          target = node.addChildNode(childDimensionValue);

          add(target, record);
        }
      } else {
        add(target, record);
      }
      // In addition to this, update the star node after relaxing dimension of
      // level to "*"
      add(node.getStarNode(), record.relax(childDimensionName));
    }
  }

  @Override
  public void close() throws IOException {
    close(root);
  }

  private void close(StarTreeNode node) throws IOException {
    if (node.isLeaf()) {
      node.getRecordStore().close();
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

      String queryDimensionValue = query.getDimensionValues().get(
          node.getChildDimensionName());
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

      String queryDimensionValue = query.getDimensionValues().get(
          node.getChildDimensionName());
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
    StarTreeStats stats = new StarTreeStats(config.getDimensions(),
        config.getMetrics(),
        config.getTime().getColumnName(),
        config.getTime().getBucket().getSize(),
        config.getTime().getBucket().getUnit());
    getStats(root, stats);
    return stats;
  }

  public void getStats(StarTreeNode node, StarTreeStats stats) {
    if (node.isLeaf()) {
      stats.countRecords(node.getRecordStore().getRecordCount());
      stats.countBytes(node.getRecordStore().getByteCount());
      stats.countNode();
      stats.countLeaf();
      stats.updateMinTime(node.getRecordStore().getMinTime());
      stats.updateMaxTime(node.getRecordStore().getMaxTime());
    } else {
      stats.countNode();

      for (StarTreeNode child : node.getChildren()) {
        getStats(child, stats);
      }
      getStats(node.getOtherNode(), stats);
      getStats(node.getStarNode(), stats);
    }
  }
}
