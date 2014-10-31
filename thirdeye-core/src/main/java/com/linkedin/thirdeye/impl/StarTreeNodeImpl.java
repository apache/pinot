package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.StarTreeRecordStore;
import com.linkedin.thirdeye.api.StarTreeRecordStoreFactory;
import com.linkedin.thirdeye.api.StarTreeRecordThresholdFunction;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StarTreeNodeImpl implements StarTreeNode
{
  private static final Logger LOG = Logger.getLogger(StarTreeNodeImpl.class);

  private transient final StarTreeRecordThresholdFunction thresholdFunction;
  private transient final StarTreeRecordStoreFactory recordStoreFactory;
  private transient final ReadWriteLock lock;

  private transient StarTreeRecordStore recordStore;

  private final UUID nodeId;
  private final String dimensionName;
  private final String dimensionValue;
  private final Map<String, StarTreeNode> children;
  private final List<String> ancestorDimensionNames;

  private StarTreeNode otherNode;
  private StarTreeNode starNode;
  private String childDimensionName;

  public StarTreeNodeImpl(UUID nodeId,
                          StarTreeRecordThresholdFunction thresholdFunction,
                          StarTreeRecordStoreFactory recordStoreFactory,
                          String dimensionName,
                          String dimensionValue,
                          List<String> ancestorDimensionNames,
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
    this.children = children;
    this.lock = new ReentrantReadWriteLock(true);
    this.otherNode = otherNode;
    this.starNode = starNode;
  }

  @Override
  public UUID getId()
  {
    return nodeId;
  }

  @Override
  public void init()
  {
    recordStore = recordStoreFactory.createRecordStore(nodeId);
  }

  @Override
  public boolean isLeaf()
  {
    lock.readLock().lock();
    try
    {
      return children.isEmpty();
    }
    finally
    {
      lock.readLock().unlock();
    }
  }

  @Override
  public Collection<StarTreeNode> getChildren()
  {
    lock.readLock().lock();
    try
    {
      return children.values();
    }
    finally
    {
      lock.readLock().unlock();
    }
  }

  @Override
  public StarTreeNode getChild(String dimensionValue)
  {
    lock.readLock().lock();
    try
    {
      if (children.isEmpty())
      {
        throw new UnsupportedOperationException("Cannot get child of leaf node");
      }

      return children.get(dimensionValue);
    }
    finally
    {
      lock.readLock().unlock();
    }
  }

  @Override
  public StarTreeNode getOtherNode()
  {
    lock.readLock().lock();
    try
    {
      return otherNode;
    }
    finally
    {
      lock.readLock().unlock();
    }
  }

  @Override
  public StarTreeNode getStarNode()
  {
    lock.readLock().lock();
    try
    {
      return starNode;
    }
    finally
    {
      lock.readLock().unlock();
    }
  }

  @Override
  public StarTreeRecordStore getRecordStore()
  {
    lock.readLock().lock();
    try
    {
      return recordStore;
    }
    finally
    {
      lock.readLock().unlock();
    }
  }

  @Override
  public String getDimensionName()
  {
    return dimensionName; // immutable
  }

  @Override
  public String getChildDimensionName()
  {
    lock.readLock().lock();
    try
    {
      return childDimensionName;
    }
    finally
    {
      lock.readLock().unlock();
    }
  }

  @Override
  public List<String> getAncestorDimensionNames()
  {
    lock.readLock().lock();
    try
    {
      return ancestorDimensionNames;
    }
    finally
    {
      lock.readLock().unlock();
    }
  }

  @Override
  public String getDimensionValue()
  {
    return dimensionValue; // immutable
  }

  @Override
  public void split(String splitDimensionName)
  {
    lock.writeLock().lock();
    try
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

      // Split info
      StringBuilder nodeName = new StringBuilder();
      if (!ancestorDimensionNames.isEmpty())
      {
        nodeName.append(StarTreeConstants.STAR).append(".");
      }
      for (String ancestorName : ancestorDimensionNames)
      {
        nodeName.append(ancestorName).append(".");
      }
      nodeName.append(dimensionName);
      LOG.info("Splitting node " + nodeName.toString() + ":" + dimensionValue
                       + " on dimension " + splitDimensionName + " (records=" + recordStore.size() + ")");

      // Ancestor dimension names now contain the current node's dimension name
      List<String> nextAncestorDimensionNames = new ArrayList<String>();
      if (!StarTreeConstants.STAR.equals(dimensionName))
      {
        nextAncestorDimensionNames.addAll(ancestorDimensionNames);
        nextAncestorDimensionNames.add(dimensionName);
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
      Map<String, Boolean> passingDimensionValues = new HashMap<String, Boolean>();
      for (Map.Entry<String, List<StarTreeRecord>> entry : groupedRecords.entrySet())
      {
        if (thresholdFunction == null)
        {
          passingDimensionValues.put(entry.getKey(), true);
        }
        else
        {
          passingDimensionValues.put(entry.getKey(), thresholdFunction.passesThreshold(entry.getValue()));
        }
      }

      // Add star node
      starNode = new StarTreeNodeImpl(
              UUID.randomUUID(),
              thresholdFunction,
              recordStoreFactory,
              splitDimensionName,
              StarTreeConstants.STAR,
              nextAncestorDimensionNames,
              new HashMap<String, StarTreeNode>(),
              null,
              null);
      starNode.init();

      // Add other node
      otherNode = new StarTreeNodeImpl(
              UUID.randomUUID(),
              thresholdFunction,
              recordStoreFactory,
              splitDimensionName,
              StarTreeConstants.OTHER,
              nextAncestorDimensionNames,
              new HashMap<String, StarTreeNode>(),
              null,
              null);
      otherNode.init();

      // Add children nodes who passed
      for (Map.Entry<String, Boolean> entry : passingDimensionValues.entrySet())
      {
        String dimensionValue = entry.getKey();
        Boolean passesThreshold = entry.getValue();

        if (passesThreshold)
        {
          StarTreeNode child = new StarTreeNodeImpl(
                  UUID.randomUUID(),
                  thresholdFunction,
                  recordStoreFactory,
                  splitDimensionName,
                  dimensionValue,
                  nextAncestorDimensionNames,
                  new HashMap<String, StarTreeNode>(),
                  null,
                  null);
          child.init();
          children.put(entry.getKey(), child);
        }
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
    finally
    {
      lock.writeLock().unlock();
    }
  }
}
