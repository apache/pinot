package com.linkedin.thirdeye.healthcheck;

import com.codahale.metrics.health.HealthCheck;
import com.google.common.base.Joiner;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.impl.StarTreeUtils;
import com.linkedin.thirdeye.impl.storage.DimensionIndexEntry;
import com.linkedin.thirdeye.impl.storage.MetricIndexEntry;
import com.linkedin.thirdeye.impl.storage.StorageUtils;

import java.io.File;
import java.io.FileFilter;
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
      // Get leaf nodes
      Set<StarTreeNode> leafNodes = new HashSet<StarTreeNode>();
      StarTreeUtils.traverseAndGetLeafNodes(leafNodes, manager.getStarTree(collection).getRoot());
      Map<UUID, NodeStats> allNodeStats = new HashMap<UUID, NodeStats>();
      for (StarTreeNode leafNode : leafNodes)
      {
        allNodeStats.put(leafNode.getId(), new NodeStats());
      }

      // Check dimension stores
      File dimensionStoreDir = new File(PATH_JOINER.join(
              rootDir, collection, StarTreeConstants.DATA_DIR_NAME, StarTreeConstants.DIMENSION_STORE));
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
              rootDir, collection, StarTreeConstants.DATA_DIR_NAME, StarTreeConstants.METRIC_STORE));
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
          }
        }
      }

      Integer metricIndexCount = null;

      for (Map.Entry<UUID, NodeStats> entry : allNodeStats.entrySet())
      {
        // Check there is one dimension store for each node
        if (entry.getValue().getDimensionIndexCount() != 1)
        {
          throw new IllegalStateException("There must be one and only one dimension index for node " + entry.getKey());
        }

        // Check all nodes have the same number of metric segments
        if (metricIndexCount == null)
        {
          metricIndexCount = entry.getValue().getMetricIndexCount();
        }
        if (metricIndexCount != entry.getValue().getMetricIndexCount())
        {
          throw new IllegalStateException("There are " + entry.getValue().getMetricIndexCount()
                                                  + " metric index entries for node " + entry.getKey()
                                                  + ", but expected " + metricIndexCount
                                                  + ". This probably indicates some segments were lost");
        }
      }
    }

    return Result.healthy();
  }

  private static class NodeStats
  {
    private int dimensionIndexCount;
    private int metricIndexCount;

    public void incrementDimensionIndexCount()
    {
      dimensionIndexCount++;
    }

    public void incrementMetricIndexCount()
    {
      metricIndexCount++;
    }

    public int getMetricIndexCount()
    {
      return metricIndexCount;
    }

    public int getDimensionIndexCount()
    {
      return dimensionIndexCount;
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
