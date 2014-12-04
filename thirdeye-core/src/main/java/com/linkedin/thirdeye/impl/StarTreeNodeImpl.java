package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.StarTreeRecordStore;
import com.linkedin.thirdeye.api.StarTreeRecordStoreFactory;
import com.linkedin.thirdeye.api.StarTreeRecordThresholdFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class StarTreeNodeImpl implements StarTreeNode
{
  private static final long serialVersionUID = -403250971215465050L;

  private static final Logger LOG = LoggerFactory.getLogger(StarTreeNodeImpl.class);

  private transient StarTreeRecordThresholdFunction thresholdFunction;
  private transient StarTreeRecordStoreFactory recordStoreFactory;
  private transient StarTreeConfig config;
  private transient StarTreeRecordStore recordStore;
  private transient final Object sync;

  private final UUID nodeId;
  private final String dimensionName;
  private final String dimensionValue;
  private final Map<String, StarTreeNode> children;
  private final List<String> ancestorDimensionNames;
  private final Map<String, String> ancestorDimensionValues;

  private StarTreeNode otherNode;
  private StarTreeNode starNode;
  private String childDimensionName;

  public StarTreeNodeImpl(UUID nodeId,
                          StarTreeRecordThresholdFunction thresholdFunction,
                          StarTreeRecordStoreFactory recordStoreFactory,
                          String dimensionName,
                          String dimensionValue,
                          List<String> ancestorDimensionNames,
                          Map<String, String> ancestorDimensionValues,
                          Map<String, StarTreeNode> children,
                          StarTreeNode otherNode,
                          StarTreeNode starNode)
  {
    this.nodeId = nodeId;
    this.thresholdFunction = thresholdFunction;
    this.recordStoreFactory = recordStoreFactory;
    this.dimensionName = dimensionName;
    this.dimensionValue = dimensionValue;
    this.ancestorDimensionNames = ancestorDimensionNames;
    this.ancestorDimensionValues = ancestorDimensionValues;
    this.children = children;
    this.otherNode = otherNode;
    this.starNode = starNode;
    this.sync = new Object();
  }

  @Override
  public UUID getId()
  {
    return nodeId;
  }

  @Override
  public void init(StarTreeConfig config)
  {
    this.config = config;
    this.thresholdFunction = config.getThresholdFunction();

    if (this.recordStore == null && this.children.isEmpty()) // only init record stores for leaves
    {
      try
      {
        this.recordStoreFactory = config.getRecordStoreFactory();
        this.recordStore = recordStoreFactory.createRecordStore(nodeId);
        this.recordStore.open();
      }
      catch (Exception e)
      {
        throw new IllegalStateException(e);
      }
    }
  }

  @Override
  public boolean isLeaf()
  {
    return children.isEmpty();
  }

  @Override
  public Collection<StarTreeNode> getChildren()
  {
    return children.values();
  }

  @Override
  public StarTreeNode getChild(String dimensionValue)
  {
    if (children.isEmpty())
    {
      throw new UnsupportedOperationException("Cannot get child of leaf node");
    }

    return children.get(dimensionValue);
  }

  @Override
  public StarTreeNode getOtherNode()
  {
    return otherNode;
  }

  @Override
  public StarTreeNode getStarNode()
  {
    return starNode;
  }

  @Override
  public StarTreeRecordStore getRecordStore()
  {
    return recordStore;
  }

  @Override
  public void setRecordStore(StarTreeRecordStore recordStore) throws IOException
  {
    if (this.recordStore != null)
    {
      this.recordStore.close();
    }
    this.recordStore = recordStore;
  }

  @Override
  public String getDimensionName()
  {
    return dimensionName; // immutable
  }

  @Override
  public String getChildDimensionName()
  {
    return childDimensionName;
  }

  @Override
  public List<String> getAncestorDimensionNames()
  {
    return ancestorDimensionNames;
  }

  @Override
  public Map<String, String> getAncestorDimensionValues()
  {
    return ancestorDimensionValues;
  }

  @Override
  public String getDimensionValue()
  {
    return dimensionValue; // immutable
  }

  @Override
  public void split(String splitDimensionName)
  {
    synchronized (sync)
    {
      // Check
      if (!children.isEmpty())
      {
        return;
      }
      else if (recordStore == null)
      {
        throw new IllegalStateException("Splitting a node with null record store on dimension " + splitDimensionName);
      }

      // Log split info
      StringBuilder nodeName = new StringBuilder();
      for (String name : ancestorDimensionNames)
      {
        String value = ancestorDimensionValues.get(name);
        nodeName.append("(")
                .append(name)
                .append(":")
                .append(value)
                .append(").");
      }
      nodeName.append("(").append(dimensionName).append(":").append(dimensionValue).append(")");
      LOG.info("Splitting {} on dimension {} (record={})",
               nodeName.toString(), splitDimensionName, recordStore.getRecordCount());

      // Ancestor dimension names now contain the current node's dimension name
      List<String> nextAncestorDimensionNames = new ArrayList<String>();
      Map<String, String> nextAncestorDimensionValues = new HashMap<String, String>();
      if (!StarTreeConstants.STAR.equals(dimensionName))
      {
        nextAncestorDimensionNames.addAll(ancestorDimensionNames);
        nextAncestorDimensionNames.add(dimensionName);
        nextAncestorDimensionValues.putAll(ancestorDimensionValues);
        nextAncestorDimensionValues.put(dimensionName, dimensionValue);
      }

      // Group all records by the dimension value on which we're splitting
      Map<String, List<StarTreeRecord>> groupedRecords = new HashMap<String, List<StarTreeRecord>>();
      for (StarTreeRecord record : recordStore)
      {
        String dimensionValue = record.getDimensionValues().get(splitDimensionName);
        List<StarTreeRecord> records = groupedRecords.get(dimensionValue);
        if (records == null)
        {
          records = new ArrayList<StarTreeRecord>();
          groupedRecords.put(dimensionValue, records);
        }
        records.add(record);
      }

      // Apply function to see which records pass threshold and which don't
      Set<String> passingValues;
      if (thresholdFunction == null)
      {
        passingValues = new HashSet<String>(groupedRecords.keySet());
      }
      else
      {
        passingValues = thresholdFunction.apply(groupedRecords);
      }

      LOG.info("Passing dimension values for split on {}", splitDimensionName);
      for (String passingValue : passingValues)
      {
        LOG.info("\t{}", passingValue);
      }

      // Add star node
      starNode = new StarTreeNodeImpl(
              UUID.randomUUID(),
              thresholdFunction,
              recordStoreFactory,
              splitDimensionName,
              StarTreeConstants.STAR,
              nextAncestorDimensionNames,
              nextAncestorDimensionValues,
              new HashMap<String, StarTreeNode>(),
              null,
              null);
      starNode.init(config);

      // Add other node
      otherNode = new StarTreeNodeImpl(
              UUID.randomUUID(),
              thresholdFunction,
              recordStoreFactory,
              splitDimensionName,
              StarTreeConstants.OTHER,
              nextAncestorDimensionNames,
              nextAncestorDimensionValues,
              new HashMap<String, StarTreeNode>(),
              null,
              null);
      otherNode.init(config);

      // Add children nodes who passed
      for (String dimensionValue : passingValues)
      {
        StarTreeNode child = new StarTreeNodeImpl(
                UUID.randomUUID(),
                thresholdFunction,
                recordStoreFactory,
                splitDimensionName,
                dimensionValue,
                nextAncestorDimensionNames,
                nextAncestorDimensionValues,
                new HashMap<String, StarTreeNode>(),
                null,
                null);
        child.init(config);
        children.put(dimensionValue, child);
      }

      // Now, add all records from the leaf node's store
      for (Map.Entry<String, List<StarTreeRecord>> entry : groupedRecords.entrySet())
      {
        String dimensionValue = entry.getKey();
        List<StarTreeRecord> records = entry.getValue();

        // To the appropriate specific or "other" node
        StarTreeNode child = children.get(dimensionValue);
        if (child == null) // other
        {
          child = otherNode;
        }

        // Add records and compute aggregate
        for (StarTreeRecord record : records)
        {
          StarTreeRecord recordForUpdate = record;

          if (child == otherNode)
          {
            recordForUpdate = record.aliasOther(splitDimensionName);
          }

          child.getRecordStore().update(recordForUpdate);

          starNode.getRecordStore().update(recordForUpdate.relax(splitDimensionName));
        }
      }

      // Clear this node's record store
      recordStore.clear();

      // Set the children dimension name
      childDimensionName = splitDimensionName;
    }
  }
}
