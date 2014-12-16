package com.linkedin.thirdeye.tools;

import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.impl.StarTreeUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class StarTreePartitionTool
{
  private static final Logger LOG = LoggerFactory.getLogger(StarTreePartitionTool.class);

  public static void main(String[] args) throws Exception
  {
    int numPartitions = Integer.valueOf(args[0]);
    File inputDir = new File(args[1]);
    File outputDir = new File(args[2]);

    File inputDataDir = new File(inputDir, StarTreeConstants.DATA_DIR_NAME);

    // Get all buffers (from which to extract node id)
    File[] files = inputDataDir.listFiles(new FileFilter()
    {
      @Override
      public boolean accept(File pathname)
      {
        return pathname.getName().endsWith(StarTreeConstants.BUFFER_FILE_SUFFIX); // just look at buffers
      }
    });
    if (files == null)
    {
      throw new IllegalStateException("No files in " + inputDir);
    }

    // Group nodes by partition id
    Map<Integer, Set<UUID>> partitionMap = new HashMap<Integer, Set<UUID>>();
    for (File file : files)
    {
      UUID nodeId = UUID.fromString(
              file.getName().substring(0, file.getName().indexOf(StarTreeConstants.BUFFER_FILE_SUFFIX)));
      int partitionId = StarTreeUtils.getPartitionId(nodeId, numPartitions);
      Set<UUID> nodeIds = partitionMap.get(partitionId);
      if (nodeIds == null)
      {
        nodeIds = new HashSet<UUID>();
        partitionMap.put(partitionId, nodeIds);
      }
      nodeIds.add(nodeId);
    }

    // Copy to partition output dir
    FileUtils.forceMkdir(outputDir);
    for (Map.Entry<Integer, Set<UUID>> entry : partitionMap.entrySet())
    {
      LOG.info("Creating partition {}", entry.getKey());

      File partitionDir = new File(outputDir, Integer.toString(entry.getKey()));
      File collectionDir = new File(partitionDir, inputDir.getName());
      File outputDataDir = new File(collectionDir, StarTreeConstants.DATA_DIR_NAME);

      FileUtils.forceMkdir(collectionDir);
      FileUtils.forceMkdir(outputDataDir);

      // Tree
      File treeSrc = new File(inputDir, StarTreeConstants.TREE_FILE_NAME);
      File treeDst = new File(collectionDir, StarTreeConstants.TREE_FILE_NAME);
      FileUtils.copyFile(treeSrc, treeDst);
      LOG.info("Copied tree to partition {}", entry.getKey());

      // Config
      File configSrc = new File(inputDir, StarTreeConstants.CONFIG_FILE_NAME);
      File configDst = new File(collectionDir, StarTreeConstants.CONFIG_FILE_NAME);
      FileUtils.copyFile(configSrc, configDst);
      LOG.info("Copied config to partition {}", entry.getKey());

      // Data
      for (UUID nodeId : entry.getValue())
      {
        // Copy buffer
        File bufferSrc = new File(inputDataDir, nodeId + StarTreeConstants.BUFFER_FILE_SUFFIX);
        File bufferDst = new File(outputDataDir, nodeId + StarTreeConstants.BUFFER_FILE_SUFFIX);
        FileUtils.copyFile(bufferSrc, bufferDst);

        // Copy index
        File indexSrc = new File(inputDataDir, nodeId + StarTreeConstants.INDEX_FILE_SUFFIX);
        File indexDst = new File(outputDataDir, nodeId + StarTreeConstants.INDEX_FILE_SUFFIX);
        FileUtils.copyFile(indexSrc, indexDst);

        LOG.info("\tCopied {} to partition {}", nodeId, entry.getKey());
      }
    }
  }
}
