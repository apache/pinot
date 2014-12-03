package com.linkedin.thirdeye.task;

import com.google.common.collect.ImmutableMultimap;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeNode;
import io.dropwizard.servlets.tasks.Task;
import org.I0Itec.zkclient.DataUpdater;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.IdealState;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class ThirdEyePartitionTask extends Task
{
  private static final String COLLECTION = "collection";
  private static final String LEAF_MAP_PATH = "/LEAF_MAP";

  private final HelixManager helixManager;
  private final File rootDir;

  public ThirdEyePartitionTask(HelixManager helixManager, File rootDir)
  {
    super("partition");
    this.helixManager = helixManager;
    this.rootDir = rootDir;
  }

  @Override
  public void execute(ImmutableMultimap<String, String> params,
                      PrintWriter printWriter) throws Exception
  {
    // Get collection
    Collection<String> collectionParam = params.get(COLLECTION);
    if (collectionParam == null || collectionParam.isEmpty())
    {
      throw new IllegalArgumentException("Must provide collection");
    }
    final String collection = collectionParam.iterator().next();

    // Get collection directory
    File collectionDir = new File(rootDir, collection);
    if (!collectionDir.exists())
    {
      throw new IllegalArgumentException(collectionDir.getAbsolutePath() + " does not exist");
    }

    // Read tree structure for collection
    File treeFile = new File(collectionDir, StarTreeConstants.TREE_FILE_NAME);
    if (!treeFile.exists())
    {
      throw new IllegalArgumentException(treeFile.getAbsolutePath() + " does not exist");
    }
    ObjectInputStream is = new ObjectInputStream(new FileInputStream(treeFile));
    StarTreeNode root = (StarTreeNode) is.readObject();

    // Get ideal state for collection
    IdealState idealState
            = helixManager.getClusterManagmentTool()
                          .getResourceIdealState(helixManager.getClusterName(), collection);
    if (idealState == null)
    {
      throw new IllegalArgumentException("No ideal state for collection " + collection);
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

      printWriter.println(leafId + " -> " + partitionId);
      printWriter.flush();
    }

    // Write that mapping to property store
    helixManager.getHelixPropertyStore().update(LEAF_MAP_PATH + File.separator + collection,
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
}
