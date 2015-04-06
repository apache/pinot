package com.linkedin.thirdeye.healthcheck;

import com.codahale.metrics.health.HealthCheck;
import com.google.common.base.Joiner;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.api.TimeRange;
import com.linkedin.thirdeye.impl.StarTreeUtils;
import com.linkedin.thirdeye.impl.storage.DimensionIndexEntry;
import com.linkedin.thirdeye.impl.storage.MetricIndexEntry;
import com.linkedin.thirdeye.impl.storage.StorageUtils;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class CollectionConsistencyHealthCheck extends HealthCheck
{
  public static final String NAME = "collectionConsistencyCheck";

  private static final Joiner PATH_JOINER = Joiner.on(File.separator);

  private final File rootDir;
  private final StarTreeManager manager;

  public CollectionConsistencyHealthCheck(File rootDir, StarTreeManager manager)
  {
    this.rootDir = rootDir;
    this.manager = manager;
  }

  @Override
  protected Result check() throws Exception
  {
    for (String collection : manager.getCollections())
    {
      for (StarTree starTree : manager.getStarTrees(collection).values())
      {
        // Get leaf nodes
        Set<StarTreeNode> leafNodes = new HashSet<StarTreeNode>();
        StarTreeUtils.traverseAndGetLeafNodes(leafNodes, starTree.getRoot());
        Map<UUID, NodeStats> allNodeStats = new HashMap<UUID, NodeStats>();
        for (StarTreeNode leafNode : leafNodes)
        {
          allNodeStats.put(leafNode.getId(), new NodeStats());
        }

        // Check dimension stores
        File dimensionStoreDir = new File(PATH_JOINER.join(
                rootDir, collection, StarTreeConstants.DATA_DIR_PREFIX, StarTreeConstants.DIMENSION_STORE));
        File[] dimensionIndexFiles = dimensionStoreDir.listFiles(INDEX_FILE_FILTER);
        if (dimensionIndexFiles != null)
        {
          for (File dimensionIndexFile : dimensionIndexFiles)
          {
            List<DimensionIndexEntry> indexEntries = StorageUtils.readDimensionIndex(dimensionIndexFile);

            for (DimensionIndexEntry indexEntry : indexEntries)
            {
              NodeStats nodeStats = allNodeStats.get(indexEntry.getNodeId());

              // Check node in index exists
              if (nodeStats == null)
              {
                throw new IllegalStateException("Found node in dimension index which does not exist in tree: " +
                        "nodeId=" + indexEntry.getNodeId() +
                        "; indexFileId=" + indexEntry.getFileId());
              }

              nodeStats.incrementDimensionIndexCount();
            }
          }
        }

        // Check metric stores
        File metricStoreDir = new File(PATH_JOINER.join(
                rootDir, collection, StarTreeConstants.DATA_DIR_PREFIX, StarTreeConstants.METRIC_STORE));
        File[] metricIndexFiles = metricStoreDir.listFiles(INDEX_FILE_FILTER);
        if (metricIndexFiles != null)
        {
          for (File metricIndexFile : metricIndexFiles)
          {
            List<MetricIndexEntry> indexEntries = StorageUtils.readMetricIndex(metricIndexFile);

            for (MetricIndexEntry indexEntry : indexEntries)
            {
              NodeStats nodeStats = allNodeStats.get(indexEntry.getNodeId());

              // Check node in index exists
              if (nodeStats == null)
              {
                throw new IllegalStateException("Found node in metric index which does not exist in tree: " +
                        "nodeId=" + indexEntry.getNodeId() +
                        "; indexFileId=" + indexEntry.getFileId());
              }

              nodeStats.incrementMetricIndexCount();

              nodeStats.addTimeRange(indexEntry.getTimeRange());
            }
          }
        }

        Integer metricIndexCount = null;

        for (StarTreeNode leafNode : leafNodes)
        {
          NodeStats nodeStats = allNodeStats.get(leafNode.getId());
          if (nodeStats == null)
          {
            throw new IllegalStateException("No node stats for leaf " + leafNode.getId());
          }

          if (metricIndexCount == null)
          {
            metricIndexCount = nodeStats.getMetricIndexCount();
          }

          // Check there is one dimension store for each node
          if (nodeStats.getDimensionIndexCount() != 1)
          {
            throw new IllegalStateException("There must be one and only one dimension index for node " + leafNode.getId());
          }

          // Check all nodes have the same number of metric segments
          if (metricIndexCount != nodeStats.getMetricIndexCount())
          {
            throw new IllegalStateException("There are " + nodeStats.getMetricIndexCount()
                    + " metric index entries for node " + leafNode.getId()
                    + ", but expected " + metricIndexCount
                    + ". This probably indicates some segments were lost");
          }

          if (leafNode.getRecordStore().getRecordCountEstimate() > 0)
          {
            // Check the record store max time is the same as that in index
            if (!leafNode.getRecordStore().getMaxTime().equals(nodeStats.getMaxTimeInIndex()))
            {
              throw new IllegalStateException("Record store max time differs from that in index: "
                      + leafNode.getRecordStore().getMaxTime()
                      + " vs " + nodeStats.getMaxTimeInIndex()
                      + " for node " + leafNode.getId());
            }

            // Check the record store min time is the same as that in index
            if (!leafNode.getRecordStore().getMinTime().equals(nodeStats.getMinTimeInIndex()))
            {
              throw new IllegalStateException("Record store min time differs from that in index: "
                      + leafNode.getRecordStore().getMinTime()
                      + " vs " + nodeStats.getMinTimeInIndex()
                      + " for node " + leafNode.getId());
            }
          }
        }
      }
    }

    return Result.healthy();
  }

  private static class NodeStats
  {
    private int dimensionIndexCount;
    private int metricIndexCount;
    private Set<TimeRange> timeRanges = new HashSet<TimeRange>();

    public void incrementDimensionIndexCount()
    {
      dimensionIndexCount++;
    }

    public void incrementMetricIndexCount()
    {
      metricIndexCount++;
    }

    public void addTimeRange(TimeRange timeRange)
    {
      timeRanges.add(timeRange);
    }

    public int getMetricIndexCount()
    {
      return metricIndexCount;
    }

    public int getDimensionIndexCount()
    {
      return dimensionIndexCount;
    }

    public Set<TimeRange> getTimeRanges()
    {
      return timeRanges;
    }

    public Long getMinTimeInIndex()
    {
      if (timeRanges.isEmpty())
      {
        return null;
      }

      List<TimeRange> sortedRanges = new ArrayList<TimeRange>(timeRanges);
      Collections.sort(sortedRanges);
      return sortedRanges.get(0).getStart();
    }

    public Long getMaxTimeInIndex()
    {
      if (timeRanges.isEmpty())
      {
        return null;
      }

      List<TimeRange> sortedRanges = new ArrayList<TimeRange>(timeRanges);
      Collections.sort(sortedRanges);
      return sortedRanges.get(sortedRanges.size() - 1).getEnd();
    }
  }

  private static final FileFilter INDEX_FILE_FILTER = new FileFilter()
  {
    @Override
    public boolean accept(File pathname)
    {
      return pathname.getName().endsWith(StarTreeConstants.INDEX_FILE_SUFFIX);
    }
  };
}
