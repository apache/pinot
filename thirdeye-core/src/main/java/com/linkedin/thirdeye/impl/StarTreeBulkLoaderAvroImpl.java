package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeBulkLoader;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.StarTreeRecordStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class StarTreeBulkLoaderAvroImpl implements StarTreeBulkLoader
{
  private static final Logger LOG = LoggerFactory.getLogger(StarTreeBulkLoaderAvroImpl.class);
  private static final String BACKUP_DIR = "backups";
  private static final String DONE_DIR = "done";
  private static final String DATA_DIR = "data";
  private static final String AVRO_FILE_SUFFIX = ".avro";
  private static final String BUFFER_FILE_SUFFIX = ".buf";

  private final ExecutorService executorService;
  private final PrintWriter printWriter;

  public StarTreeBulkLoaderAvroImpl(ExecutorService executorService)
  {
    this(executorService, null);
  }

  public StarTreeBulkLoaderAvroImpl(ExecutorService executorService, PrintWriter printWriter)
  {
    this.executorService = executorService;
    this.printWriter = printWriter;
  }

  @Override
  public void bulkLoad(final StarTree starTree,
                       final File rootDir,
                       final File tmpDir) throws IOException
  {
    // Backup directory
    final File backupsDir = new File(tmpDir, BACKUP_DIR);
    if (backupsDir.mkdir())
    {
      info("Created " + backupsDir);
    }

    // Done directory
    final File doneDir = new File(tmpDir, DONE_DIR);
    if (doneDir.mkdir())
    {
      info("Created " + doneDir);
    }

    // Check data directory
    final File dataDir = new File(tmpDir, DATA_DIR);
    if (!dataDir.exists())
    {
      throw new IllegalStateException("No data directory " + dataDir);
    }

    // Get mapping of node ID to record store
    final Map<UUID, StarTreeRecordStore> recordStores = new HashMap<UUID, StarTreeRecordStore>();
    collectRecordStores(starTree.getRoot(), recordStores);

    // Restore any backups (should have deleted if successful)
    File[] backupFiles = backupsDir.listFiles();
    Set<UUID> restoredNodeIds = new HashSet<UUID>();
    if (backupFiles != null)
    {
      for (File backupFile : backupFiles)
      {
        File primaryFile = new File(rootDir, backupFile.getName());

        UUID nodeId = UUID.fromString(
                primaryFile.getName().substring(0, primaryFile.getName().indexOf(BUFFER_FILE_SUFFIX)));

        File doneFile = new File(doneDir, nodeId.toString());

        // No longer consider this done if it was
        if (doneFile.exists())
        {
          if (doneFile.delete())
          {
            info("Removed done file " + doneFile + " while restoring node " + nodeId);
          }
          else
          {
            throw new IllegalStateException("A done file exists and could not be removed when attempting restore " + doneFile);
          }
        }

        // Restore the backup
        if (primaryFile.delete())
        {
          restoredNodeIds.add(nodeId);
          copyFile(backupFile, primaryFile);
          info("Restored " + primaryFile + " from backup " + backupFile);
        }
        else
        {
          throw new IllegalStateException("Could not delete " + primaryFile + " to restore backup " + backupFile);
        }
      }
    }

    // Remove any nodes that we've already processed
    String[] doneNodeIds = doneDir.list();
    if (doneNodeIds != null)
    {
      for (String doneNodeId : doneNodeIds)
      {
        UUID uuid = UUID.fromString(doneNodeId);

        if (!restoredNodeIds.contains(uuid)) // if we restored, don't consider this one done
        {
          recordStores.remove(uuid);
        }
      }
    }

    // Process all files in data directory
    File[] dataFiles = dataDir.listFiles();
    if (dataFiles != null)
    {
      final int numWorkers = Runtime.getRuntime().availableProcessors();
      final Queue<File> fileQueue = new ConcurrentLinkedQueue<File>(Arrays.asList(dataFiles));
      final CountDownLatch latch = new CountDownLatch(numWorkers);

      // Run a thread for each core
      for (int i = 0; i < numWorkers; i++)
      {
        executorService.submit(new Runnable()
        {
          @Override
          public void run()
          {
            File dataFile = null;

            try
            {
              while ((dataFile = fileQueue.poll()) != null)
              {
                // Parse node id from file name
                UUID nodeId = UUID.fromString(
                        dataFile.getName().substring(0, dataFile.getName().indexOf(AVRO_FILE_SUFFIX)));

                // Get record store
                StarTreeRecordStore recordStore = recordStores.get(nodeId);
                if (recordStore == null)
                {
                  warn("Skipping node " + nodeId);
                  continue;
                }

                // Backup current buffer
                byte[] backup = recordStore.encode();
                File backupFile = new File(backupsDir, nodeId + BUFFER_FILE_SUFFIX);
                FileOutputStream outputStream = new FileOutputStream(backupFile);
                outputStream.write(backup);
                outputStream.flush();
                outputStream.close();

                // Load the Avro data from this file into the record store
                Iterable<StarTreeRecord> records
                        = new StarTreeRecordStreamAvroFileImpl(dataFile,
                                                               starTree.getConfig().getDimensionNames(),
                                                               starTree.getConfig().getMetricNames(),
                                                               starTree.getConfig().getTimeColumnName());
                for (StarTreeRecord record : records)
                {
                  recordStore.update(record);
                }

                // Mark success
                File doneFile = new File(doneDir, nodeId.toString());
                if (doneFile.createNewFile())
                {
                  info("Loaded " + nodeId);
                }

                // Remove backup
                if (!backupFile.delete())
                {
                  warn("Could not delete backup file " + backupFile);
                }
              }
            }
            catch (Exception e)
            {
              throw new RuntimeException(e);
            }
            finally
            {
              latch.countDown();
            }
          }
        });
      }

      // Wait till workers are done
      try
      {
        latch.await();
      }
      catch (InterruptedException e)
      {
        throw new IOException(e);
      }

      info("Done with bulk load on " + starTree.getConfig().getCollection());
    }
  }

  /**
   * Traverses tree to construct node ID -> record store mapping
   */
  private void collectRecordStores(StarTreeNode node, Map<UUID, StarTreeRecordStore> collector)
  {
    if (node.isLeaf())
    {
      collector.put(node.getId(), node.getRecordStore());
    }
    else
    {
      for (StarTreeNode child : node.getChildren())
      {
        collectRecordStores(child, collector);
      }
      collectRecordStores(node.getOtherNode(), collector);
      collectRecordStores(node.getStarNode(), collector);
    }
  }

  private void info(String message)
  {
    LOG.info(message);

    if (printWriter != null)
    {
      printWriter.println(message);
      printWriter.flush();
    }
  }

  private void warn(String message)
  {
    LOG.warn(message);

    if (printWriter != null)
    {
      printWriter.println(message);
      printWriter.flush();
    }
  }

  private static void copyFile(File src, File dst) throws IOException
  {
    FileChannel srcChannel = new FileInputStream(src).getChannel();
    FileChannel dstChannel = new FileOutputStream(dst).getChannel();

    try
    {
      dstChannel.transferFrom(srcChannel, 0, srcChannel.size());
    }
    finally
    {
      srcChannel.close();
      dstChannel.close();
    }
  }
}
