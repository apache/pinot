package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.StarTreeRecordStore;
import com.linkedin.thirdeye.api.StarTreeRecordStoreFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class StarTreeNodeImpl implements StarTreeNode {
  private static final long serialVersionUID = -403250971215465050L;

  private static final Logger LOGGER = LoggerFactory
      .getLogger(StarTreeNodeImpl.class);

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

  private List<String> nextAncestorDimensionNames;

  private Map<String, String> nextAncestorDimensionValues;

  private String splitDimensionName;

  public StarTreeNodeImpl(UUID nodeId,
                          String dimensionName,
                          String dimensionValue,
                          List<String> ancestorDimensionNames,
                          Map<String, String> ancestorDimensionValues,
                          Map<String, StarTreeNode> children,
                          StarTreeNode otherNode,
                          StarTreeNode starNode)
  {
    this.nodeId = nodeId;
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
  public UUID getId() {
    return nodeId;
  }

  @Override
  public void init(StarTreeConfig config, StarTreeRecordStoreFactory recordStoreFactory) {
    this.config = config;

    if (this.recordStore == null && this.children.isEmpty()) // only init record
                                                             // stores for
                                                             // leaves
    {
      try {
        this.recordStoreFactory = recordStoreFactory;
        this.recordStore = recordStoreFactory.createRecordStore(nodeId);
        this.recordStore.open();
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }
  }

  @Override
  public boolean isLeaf() {
    return children.isEmpty();
  }

  @Override
  public Collection<StarTreeNode> getChildren() {
    return children.values();
  }

  @Override
  public StarTreeNode getChild(String dimensionValue) {
    if (children.isEmpty()) {
      throw new UnsupportedOperationException("Cannot get child of leaf node");
    }

    return children.get(dimensionValue);
  }

  @Override
  public StarTreeNode getOtherNode() {
    return otherNode;
  }

  @Override
  public StarTreeNode getStarNode() {
    return starNode;
  }

  @Override
  public StarTreeRecordStore getRecordStore() {
    return recordStore;
  }

  @Override
  public void setRecordStore(StarTreeRecordStore recordStore)
      throws IOException {
    if (this.recordStore != null) {
      this.recordStore.close();
    }
    this.recordStore = recordStore;
  }

  @Override
  public String getDimensionName() {
    return dimensionName; // immutable
  }

  @Override
  public String getChildDimensionName() {
    return childDimensionName;
  }

  @Override
  public List<String> getAncestorDimensionNames() {
    return ancestorDimensionNames;
  }

  @Override
  public Map<String, String> getAncestorDimensionValues() {
    return ancestorDimensionValues;
  }

  @Override
  public String getDimensionValue() {
    return dimensionValue; // immutable
  }

  @Override
  public void split(String dimensionToSplitOn) {
    this.splitDimensionName = dimensionToSplitOn;
    synchronized (sync) {
      // Check
      if (!children.isEmpty()) {
        return;
      } else if (recordStore == null) {
        throw new IllegalStateException(
            "Splitting a node with null record store on dimension "
                + splitDimensionName);
      }

      // Log split info
      StringBuilder nodeName = new StringBuilder();
      for (String name : ancestorDimensionNames) {
        String value = ancestorDimensionValues.get(name);
        nodeName.append("(").append(name).append(":").append(value)
            .append(").");
      }
      nodeName.append("(").append(dimensionName).append(":")
          .append(dimensionValue).append(")");
      LOGGER.info("Splitting {} on dimension {} (record={})", nodeName.toString(),
          splitDimensionName, recordStore.getRecordCount());

      nextAncestorDimensionNames = new ArrayList<String>();
      nextAncestorDimensionValues = new HashMap<String, String>();
      if (!StarTreeConstants.STAR.equals(dimensionName)) {
        nextAncestorDimensionNames.addAll(ancestorDimensionNames);
        nextAncestorDimensionNames.add(dimensionName);
        nextAncestorDimensionValues.putAll(ancestorDimensionValues);
        nextAncestorDimensionValues.put(dimensionName, dimensionValue);
      }

      // Group all records by the dimension value on which we're splitting
      Map<String, List<StarTreeRecord>> groupedRecords = new HashMap<String, List<StarTreeRecord>>();
      for (StarTreeRecord record : recordStore) {
        String dimensionValue = record.getDimensionKey().getDimensionValue(config.getDimensions(), splitDimensionName);
        List<StarTreeRecord> records = groupedRecords.get(dimensionValue);
        if (records == null) {
          records = new ArrayList<StarTreeRecord>();
          groupedRecords.put(dimensionValue, records);
        }
        records.add(record);
      }

      // Add star node
      starNode = new StarTreeNodeImpl(UUID.randomUUID(),
          splitDimensionName, StarTreeConstants.STAR,
          nextAncestorDimensionNames, nextAncestorDimensionValues,
          new HashMap<String, StarTreeNode>(), null, null);
      starNode.init(config, recordStoreFactory);

      // Add other node
      otherNode = new StarTreeNodeImpl(UUID.randomUUID(),
          splitDimensionName, StarTreeConstants.OTHER,
          nextAncestorDimensionNames, nextAncestorDimensionValues,
          new HashMap<String, StarTreeNode>(), null, null);
      otherNode.init(config, recordStoreFactory);
      // Add children nodes who passed
      for (String dimensionValue : groupedRecords.keySet()) {
        // Skip other nodes (forces operations on these to go to special other
        // node)
        if (StarTreeConstants.OTHER.equals(dimensionValue)) {
          continue;
        }

        addChildNode(dimensionValue);
      }

      // Now, add all records from the leaf node's store
      for (Map.Entry<String, List<StarTreeRecord>> entry : groupedRecords
          .entrySet()) {
        String dimensionValue = entry.getKey();
        List<StarTreeRecord> records = entry.getValue();

        // To the appropriate specific or "other" node
        StarTreeNode child = children.get(dimensionValue);
        if (child == null) // other
        {
          child = otherNode;
        }

        // Add records and compute aggregate
        for (StarTreeRecord record : records) {
          StarTreeRecord recordForUpdate = record;

          if (child == otherNode) {
            recordForUpdate = record.aliasOther(splitDimensionName);
          }

          child.getRecordStore().update(recordForUpdate);

          starNode.getRecordStore().update(
              recordForUpdate.relax(splitDimensionName));
        }
      }

      // Clear this node's record store
      recordStore.clear();

      // Set the children dimension name
      childDimensionName = splitDimensionName;
    }
  }

  @Override
  public StarTreeNode addChildNode(String dimensionValue) {
    if (children.containsKey(dimensionValue)) {
      return children.get(dimensionValue);
    }
    if(dimensionValue.equals(StarTreeConstants.OTHER)){
      return otherNode;
    }
    if(dimensionValue.equals(StarTreeConstants.STAR)){
      return starNode;
    }
    StarTreeNode child = new StarTreeNodeImpl(UUID.randomUUID(),
        splitDimensionName,
        dimensionValue, nextAncestorDimensionNames,
        nextAncestorDimensionValues, new HashMap<String, StarTreeNode>(), null,
        null);
    child.init(config, recordStoreFactory);
    children.put(dimensionValue, child);
    return child;
  }

  /**
   * To string format of the path
   */
  @Override
  public String getPath(){
    StringBuilder sb = new StringBuilder();
    for(String name:ancestorDimensionNames){
      sb.append("/").append("(").append(name).append(":").append(ancestorDimensionValues.get(name)).append(")");
    }
    sb.append("/").append(dimensionName).append(":").append(dimensionValue);
    return sb.toString();
  }
}
