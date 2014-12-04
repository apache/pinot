package com.linkedin.thirdeye.bootstrap.startree;

import com.linkedin.thirdeye.api.StarTreeNode;
import org.I0Itec.zkclient.DataUpdater;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.model.IdealState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class StarTreeHelixTool implements Runnable
{
  private static final Logger LOG = LoggerFactory.getLogger(StarTreeHelixTool.class);
  private static final String LEAF_MAP_PATH = "/LEAF_MAP";

  private final String zkAddress;
  private final String clusterName;
  private final String collection;
  private final File treeFile;
  private final int numPartitions;

  public StarTreeHelixTool(String zkAddress,
                           String clusterName,
                           String collection,
                           File treeFile,
                           int numPartitions)
  {
    this.zkAddress = zkAddress;
    this.clusterName = clusterName;
    this.collection = collection;
    this.treeFile = treeFile;
    this.numPartitions = numPartitions;
  }

  @Override
  public void run()
  {
    try
    {
      // Read tree structure for collection
      if (!treeFile.exists())
      {
        throw new IllegalArgumentException(treeFile.getAbsolutePath() + " does not exist");
      }
      ObjectInputStream is = new ObjectInputStream(new FileInputStream(treeFile));
      StarTreeNode root = (StarTreeNode) is.readObject();
      LOG.info("Read {}", treeFile);

      // Connect to helix
      HelixManager helixManager
              = HelixManagerFactory.getZKHelixManager(clusterName, "ADMIN", InstanceType.ADMINISTRATOR, zkAddress);
      helixManager.connect();
      LOG.info("Connected to Helix {}:{}", zkAddress, clusterName);

      // Get ideal state for collection
      IdealState idealState
              = helixManager.getClusterManagmentTool()
                            .getResourceIdealState(helixManager.getClusterName(), collection);
      if (idealState == null)
      {
        // Create ideal state
        helixManager.getClusterManagmentTool()
                    .addResource(clusterName,
                                 collection,
                                 numPartitions,
                                 StateModelDefId.OnlineOffline.stringify(),
                                 IdealState.RebalanceMode.SEMI_AUTO.name());
        idealState = helixManager.getClusterManagmentTool()
                                 .getResourceIdealState(helixManager.getClusterName(), collection);
        LOG.info("Created ideal state for collection {} with {} partitions", collection, numPartitions);
      }
      else if (idealState.getNumPartitions() != numPartitions)
      {
        throw new IllegalStateException("Ideal state exists with different number partitions " + idealState.getNumPartitions());
      }
      else
      {
        LOG.warn("Ideal state exists for {} with same number of partitions - reusing it", collection);
      }

      // Collect all leaf IDs
      Set<UUID> leafIds = new HashSet<UUID>();
      collectLeafIds(root, leafIds);

      // Map these to partitions
      final Map<Integer, List<String>> partitionToLeaves = new HashMap<Integer, List<String>>();
      for (UUID leafId : leafIds)
      {
        int partitionId = (Integer.MAX_VALUE & leafId.hashCode()) % idealState.getNumPartitions();
        List<String> ids = partitionToLeaves.get(partitionId);
        if (ids == null)
        {
          ids = new ArrayList<String>();
          partitionToLeaves.put(partitionId, ids);
        }
        ids.add(leafId.toString());

        LOG.info("Mapped {} -> {}", leafId, partitionId);
      }

      // Write that mapping to property store
      String propertyStorePath = LEAF_MAP_PATH + File.separator + collection;
      helixManager.getHelixPropertyStore()
                  .update(propertyStorePath,
                          new DataUpdater<ZNRecord>()
                          {
                            @Override
                            public ZNRecord update(ZNRecord currentData) // always overwrite
                            {
                              ZNRecord data = new ZNRecord(collection);

                              for (Map.Entry<Integer, List<String>> entry : partitionToLeaves.entrySet())
                              {
                                data.setListField(Integer.toString(entry.getKey()), entry.getValue());
                              }

                              return data;
                            }
                          }, AccessOption.PERSISTENT);
      LOG.info("Wrote partition mapping to {} property store", propertyStorePath);
    }
    catch (Exception e)
    {
      throw new RuntimeException(e);
    }
  }

  private void collectLeafIds(StarTreeNode node, Set<UUID> collector)
  {
    if (node.isLeaf())
    {
      collector.add(node.getId());
    }
    else
    {
      for (StarTreeNode child : node.getChildren())
      {
        collectLeafIds(child, collector);
      }
      collectLeafIds(node.getOtherNode(), collector);
      collectLeafIds(node.getStarNode(), collector);
    }
  }

  public static void main(String[] args) throws Exception
  {
    if (args.length != 5)
    {
      throw new Exception("usage: zkAddress clusterName collection treeFile numPartitions");
    }

    String zkAddress = args[0];
    String clusterName = args[1];
    String collection = args[2];
    File treeFile = new File(args[3]);
    int numPartitions = Integer.valueOf(args[4]);

    new StarTreeHelixTool(zkAddress, clusterName, collection, treeFile, numPartitions).run();
  }
}
