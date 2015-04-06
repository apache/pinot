package com.linkedin.thirdeye.healthcheck;

import java.io.File;
import java.io.FileFilter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.codahale.metrics.health.HealthCheck;
import com.google.common.base.Joiner;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.impl.StarTreeUtils;
import com.linkedin.thirdeye.impl.storage.DimensionIndexEntry;
import com.linkedin.thirdeye.impl.storage.StorageUtils;

public class StarTreeHealthCheck extends HealthCheck {

  public static final String NAME = "starTreeHealthCheck";

  private static final Joiner PATH_JOINER = Joiner.on(File.separator);

  private final File rootDir;
  private final StarTreeManager manager;

  public StarTreeHealthCheck(File rootDir, StarTreeManager manager)
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
        Map<UUID, Integer> nodeCount = new HashMap<UUID, Integer>();
        Set<StarTreeNode> leafNodes = new HashSet<StarTreeNode>();
        StarTreeUtils.traverseAndGetLeafNodes(leafNodes, starTree.getRoot());

        for (StarTreeNode leafNode : leafNodes)
        {
          nodeCount.put(leafNode.getId(), 0);
        }

        // Check dimension stores for extra entries
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

              Integer count = nodeCount.get(indexEntry.getNodeId());

              if (count == null)
              {
                throw new IllegalStateException("Found node in dimension index which does not exist in tree: " +
                        "nodeId=" + indexEntry.getNodeId() +
                        "; indexFileId=" + indexEntry.getFileId());
              }
              else
              {
                nodeCount.put(indexEntry.getNodeId(), count + 1);
              }
            }
          }
        }

        // Ensure every leaf node has exactly 1 entry in the dimension indexes
        for (StarTreeNode leafNode : leafNodes)
        {
          if (nodeCount.get(leafNode.getId()) != 1)
          {
            throw new IllegalStateException("There must be one and only one dimension index for node " + leafNode.getId());
          }
        }

      }
    }

    return Result.healthy();

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
