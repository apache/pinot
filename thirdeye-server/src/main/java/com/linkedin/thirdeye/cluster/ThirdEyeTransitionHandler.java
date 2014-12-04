package com.linkedin.thirdeye.cluster;

import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.data.ThirdEyeExternalDataSource;
import com.linkedin.thirdeye.impl.StarTreeRecordStoreBlackHoleImpl;
import com.linkedin.thirdeye.util.ThirdEyeTarUtils;
import org.apache.commons.io.FileUtils;
import org.apache.helix.NotificationContext;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.TransitionHandler;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

@StateModelInfo(states = "{'OFFLINE', 'ONLINE'}", initialState = "OFFLINE")
public class ThirdEyeTransitionHandler extends TransitionHandler
{
  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeTransitionHandler.class);

  private final PartitionId partitionId;
  private final StarTreeManager starTreeManager;
  private final ThirdEyeExternalDataSource externalDataSource;
  private final File rootDir;
  private final File tmpDir;

  public ThirdEyeTransitionHandler(PartitionId partitionId,
                                   StarTreeManager starTreeManager,
                                   ThirdEyeExternalDataSource externalDataSource,
                                   File rootDir,
                                   File tmpDir)
  {
    this.partitionId = partitionId;
    this.starTreeManager = starTreeManager;
    this.externalDataSource = externalDataSource;
    this.rootDir = rootDir;
    this.tmpDir = tmpDir;
  }

  @Transition(from = "OFFLINE", to = "ONLINE")
  public void fromOfflineToOnline(Message message, NotificationContext context) throws Exception
  {
    LOG.info("BEGIN\t{}: OFFLINE -> ONLINE", message.getPartitionId());
    String collection = message.getResourceName();
    int partitionId = Integer.valueOf(PartitionId.stripResourceId(message.getPartitionName()));

    // Copy data here if not present
    if (externalDataSource != null)
    {
      String archiveName = String.format("%s_%d.tgz", collection, partitionId);

      File collectionDir = new File(rootDir, collection);
      File dataDir = new File(collectionDir, StarTreeConstants.DATA_DIR_NAME);
      File tmpFile = new File(tmpDir, archiveName);

      FileUtils.forceMkdir(collectionDir);
      FileUtils.forceMkdir(dataDir);

      if (tmpFile.exists())
      {
        FileUtils.forceDelete(tmpFile);
      }

      synchronized (externalDataSource) // avoid galloping herd
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
        OutputStream outputStream = new FileOutputStream(tmpFile);
        externalDataSource.copy(URI.create("/" + archiveName), outputStream);
        outputStream.flush();
        outputStream.close();
        LOG.info("Downloaded archive {}", archiveName);

        // Extract into data directory
        InputStream inputStream = new FileInputStream(tmpFile);
        ThirdEyeTarUtils.extractGzippedTarArchive(inputStream, collectionDir, overwriteFilter, null);
        LOG.info("Extracted archive {} into {}", archiveName, collectionDir);

        // Delete tmp file
        FileUtils.forceDelete(tmpFile);
        LOG.info("Deleted {}", tmpFile);
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
    LOG.info("END\t{}: OFFLINE -> DROPPED", message.getPartitionId());
  }

  @Override
  public void reset()
  {
    LOG.info("Reset partition {}", partitionId);
  }

  private Set<UUID> getLeafIds(String collection, int partitionId, NotificationContext context)
  {
    ZNRecord data = context.getManager().getHelixPropertyStore().get("/LEAF_MAP/" + collection, new Stat(), 0);

    List<String> ids = data.getListField(String.valueOf(partitionId));
    if (ids == null)
    {
      throw new IllegalStateException("No IDs for partition " + partitionId);
    }

    Set<UUID> leafIds = new HashSet<UUID>();
    for (String id : ids)
    {
      leafIds.add(UUID.fromString(id));
    }

    return leafIds;
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
}
