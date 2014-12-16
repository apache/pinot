package com.linkedin.thirdeye.cluster;

import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.impl.StarTreeRecordStoreBlackHoleImpl;
import com.linkedin.thirdeye.impl.StarTreeUtils;
import com.linkedin.thirdeye.util.ThirdEyeTarUtils;
import org.apache.commons.io.FileUtils;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.TransitionHandler;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

@StateModelInfo(states = "{'OFFLINE', 'ONLINE'}", initialState = "OFFLINE")
public class ThirdEyeTransitionHandler extends TransitionHandler
{
  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeTransitionHandler.class);

  private final PartitionId partitionId;
  private final StarTreeManager starTreeManager;
  private final URI archiveSource;
  private final File rootDir;

  public ThirdEyeTransitionHandler(PartitionId partitionId,
                                   StarTreeManager starTreeManager,
                                   URI archiveSource,
                                   File rootDir)
  {
    this.partitionId = partitionId;
    this.starTreeManager = starTreeManager;
    this.archiveSource = archiveSource;
    this.rootDir = rootDir;
  }

  @Transition(from = "OFFLINE", to = "ONLINE")
  public void fromOfflineToOnline(Message message, NotificationContext context) throws Exception
  {
    LOG.info("BEGIN\t{}: OFFLINE -> ONLINE", message.getPartitionId());
    String collection = message.getResourceName();
    int partitionId = Integer.valueOf(PartitionId.stripResourceId(message.getPartitionName()));

    String skipBootstrap
            = context.getManager()
                     .getConfigAccessor()
                     .get(new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER)
                                  .forCluster(context.getManager().getClusterName()).build(), "skipBootstrap");

    // Copy data here if not present
    if (archiveSource != null && (skipBootstrap == null || !Boolean.valueOf(skipBootstrap)))
    {
      String archiveName = String.format("%s_%d.tgz", collection, partitionId);

      File collectionDir = new File(rootDir, collection);
      File dataDir = new File(collectionDir, StarTreeConstants.DATA_DIR_NAME);

      FileUtils.forceMkdir(collectionDir);
      FileUtils.forceMkdir(dataDir);

      synchronized (archiveSource) // avoid galloping herd
      {
        // Build filter (in order to not overwrite existing files
        Set<String> overwriteFilter = new HashSet<String>();
        File[] collectionDirFiles = collectionDir.listFiles();
        if (collectionDirFiles != null)
        {
          for (File file : collectionDirFiles)
          {
            overwriteFilter.add(file.getName());
          }
        }
        File[] dataDirFiles = dataDir.listFiles();
        if (dataDirFiles != null)
        {
          for (File file : dataDirFiles)
          {
            overwriteFilter.add(file.getName());
          }
        }

        // Download archive from external source
        InputStream inputStream = URI.create(archiveSource.toString() + "/" + archiveName).toURL().openStream();
        ThirdEyeTarUtils.extractGzippedTarArchive(inputStream, collectionDir, overwriteFilter, null);
        inputStream.close();
        LOG.info("Downloaded and extracted archive {} into {}", archiveName, collectionDir);
      }
    }

    // Lazily instantiate tree
    starTreeManager.stub(rootDir, collection); // NOP if exists
    StarTree starTree = starTreeManager.getStarTree(collection);

    // Get the leaf IDs for this partition
    Set<UUID> targetIds = getLeafIds(collection, partitionId, context);

    // Swap black holes for record stores
    enableRecordStores(starTree.getRoot(), starTree.getConfig(), targetIds);

    LOG.info("END\t{}: OFFLINE -> ONLINE", message.getPartitionId());
  }

  @Transition(from = "ONLINE", to = "OFFLINE")
  public void fromOnlineToOffline(Message message, NotificationContext context) throws Exception
  {
    LOG.info("BEGIN\t{}: ONLINE -> OFFLINE", message.getPartitionId());
    String collection = message.getResourceName();

    // Get star tree
    StarTree starTree = starTreeManager.getStarTree(collection);
    if (starTree == null)
    {
      throw new IllegalStateException("No star tree for " + collection + " but was in ONLINE state");
    }

    // Get the leaf IDs for this partition
    int partitionId = Integer.valueOf(PartitionId.stripResourceId(message.getPartitionName()));
    Set<UUID> targetIds = getLeafIds(collection, partitionId, context);

    // Swap record stores w/ black hole for hosted partitions
    disableRecordStores(starTree.getRoot(), starTree.getConfig(), targetIds);

    LOG.info("END\t{}: ONLINE -> OFFLINE", message.getPartitionId());
  }

  @Transition(from = "OFFLINE", to = "DROPPED")
  public void fromOfflineToDropped(Message message, NotificationContext context) throws Exception
  {
    LOG.info("BEGIN\t{}: OFFLINE -> DROPPED", message.getPartitionId());
    String collection = message.getResourceName();

    // Remove star tree from in-memory
    starTreeManager.close(collection);
    starTreeManager.remove(collection);
    starTreeManager.removeConfig(collection);

    // Get the leaf IDs for this partition
    int partitionId = Integer.valueOf(PartitionId.stripResourceId(message.getPartitionName()));
    Set<UUID> targetIds = getLeafIds(collection, partitionId, context);

    // Drop all data files
    File collectionDir = new File(rootDir, collection);
    File dataDir = new File(collectionDir, StarTreeConstants.DATA_DIR_NAME);
    File[] dataFiles = dataDir.listFiles();
    if (dataFiles != null)
    {
      for (File dataFile : dataFiles)
      {
        UUID id = UUID.fromString(dataFile.getName().substring(0, dataFile.getName().indexOf(".")));
        if (targetIds.contains(id))
        {
          if (dataFile.delete())
          {
            LOG.info("Deleted {} when dropping {}", dataFile, message.getPartitionId());
          }
          else
          {
            LOG.warn("Failed to delete {} when dropping {}", dataFile, message.getPartitionId());
          }
        }
      }
    }

    LOG.info("END\t{}: OFFLINE -> DROPPED", message.getPartitionId());
  }

  @Override
  public void reset()
  {
    LOG.info("Reset partition {}", partitionId);
  }

  private Set<UUID> getLeafIds(String collection, int partitionId, NotificationContext context)
  {
    Set<UUID> collector = new HashSet<UUID>();
    StarTree starTree = starTreeManager.getStarTree(collection);
    if (starTree == null)
    {
      throw new IllegalStateException("No star tree for collection " + collection);
    }
    getLeafIds(starTree.getRoot(), partitionId, getNumPartitions(collection, context), collector);
    return collector;
  }

  private void getLeafIds(StarTreeNode node, int partitionId, int numPartitions, Set<UUID> collector)
  {
    if (node.isLeaf())
    {
      if (partitionId == StarTreeUtils.getPartitionId(node.getId(), numPartitions))
      {
        collector.add(node.getId());
      }
    }
    else
    {
      for (StarTreeNode child : node.getChildren())
      {
        getLeafIds(child, partitionId, numPartitions, collector);
      }
      getLeafIds(node.getOtherNode(), partitionId, numPartitions, collector);
      getLeafIds(node.getStarNode(), partitionId, numPartitions, collector);
    }
  }

  private void enableRecordStores(StarTreeNode node, StarTreeConfig config, Set<UUID> targetIds) throws IOException
  {
    if (node.isLeaf())
    {
      if (targetIds.contains(node.getId()))
      {
        node.setRecordStore(null); // will close previous
        node.init(config); // will create and open new store
      }
    }
    else
    {
      for (StarTreeNode child : node.getChildren())
      {
        enableRecordStores(child, config, targetIds);
      }
      enableRecordStores(node.getOtherNode(), config, targetIds);
      enableRecordStores(node.getStarNode(), config, targetIds);
    }
  }

  private void disableRecordStores(StarTreeNode node, StarTreeConfig config, Set<UUID> targetIds) throws IOException
  {
    if (node.isLeaf())
    {
      if (targetIds.contains(node.getId()))
      {
        // will close previous, and new is just a stubbed implementation
        node.setRecordStore(new StarTreeRecordStoreBlackHoleImpl(config.getDimensionNames(), config.getMetricNames()));
      }
    }
    else
    {
      for (StarTreeNode child : node.getChildren())
      {
        enableRecordStores(child, config, targetIds);
      }
      enableRecordStores(node.getOtherNode(), config, targetIds);
      enableRecordStores(node.getStarNode(), config, targetIds);
    }
  }

  private int getNumPartitions(String collection, NotificationContext context)
  {
    IdealState idealState
            = context.getManager()
                     .getClusterManagmentTool()
                     .getResourceIdealState(context.getManager().getClusterName(), collection);

    if (idealState == null)
    {
      throw new IllegalStateException("No ideal state for " + collection);
    }

    return idealState.getNumPartitions();
  }
}
