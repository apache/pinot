package com.linkedin.thirdeye.impl.storage;

import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.impl.TarGzCompressionUtils;
import com.linkedin.thirdeye.impl.TarUtils;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DataUpdateManager
{
  private static final Logger LOG = LoggerFactory.getLogger(DataUpdateManager.class);

  private final File rootDir;
  private final ConcurrentMap<String, Lock> collectionLocks;

  public DataUpdateManager(File rootDir)
  {
    this.rootDir = rootDir;
    this.collectionLocks = new ConcurrentHashMap<String, Lock>();
  }

  public void deleteCollection(String collection) throws Exception
  {
    File collectionDir = new File(rootDir, collection);

    if (!collectionDir.isAbsolute())
    {
      throw new IllegalArgumentException("Collection dir cannot be relative " + collectionDir);
    }

    FileUtils.forceDelete(collectionDir);
  }

  public void deleteData(String collection,
                         String schedule,
                         DateTime minTime,
                         DateTime maxTime) throws Exception
  {
    Lock lock = collectionLocks.get(collection);
    if (lock == null)
    {
      collectionLocks.putIfAbsent(collection, new ReentrantLock());
      lock = collectionLocks.get(collection);
    }

    lock.lock();
    LOG.info("Locked collection {} using lock {} for data delete", collection, lock);
    try
    {
      // Find files prefixed with the parameters (i.e. not including treeId)
      final String dataDirPrefix = StorageUtils.getDataDirPrefix(schedule, minTime, maxTime);
      File collectionDir = new File(rootDir, collection);
      File[] matchingDirs = collectionDir.listFiles(new FilenameFilter()
      {
        @Override
        public boolean accept(File dir, String name)
        {
          return name.startsWith(dataDirPrefix);
        }
      });

      if (matchingDirs == null || matchingDirs.length == 0)
      {
        throw new FileNotFoundException("No directory with prefix " + dataDirPrefix);
      }

      for (File dataDir : matchingDirs)
      {
        FileUtils.forceDelete(dataDir); // n.b. will trigger watch on collection dir
        LOG.info("Deleted {}", dataDir);
      }
    }
    finally
    {
      lock.unlock();
      LOG.info("Unlocked collection {} using lock {} for data delete", collection, lock);
    }
  }

  public void updateData(String collection,
                         String schedule,
                         DateTime minTime,
                         DateTime maxTime,
                         byte[] data) throws Exception
  {
    Lock lock = collectionLocks.get(collection);
    if (lock == null)
    {
      collectionLocks.putIfAbsent(collection, new ReentrantLock());
      lock = collectionLocks.get(collection);
    }

    lock.lock();
    LOG.info("Locked collection {} using lock {} for data update", collection, lock);
    try
    {
      File collectionDir = new File(rootDir, collection);
      if (!collectionDir.exists())
      {
        FileUtils.forceMkdir(collectionDir);
        LOG.info("Created {}", collectionDir);
      }

      if (schedule.contains("_"))
      {
        throw new IOException("schedule cannot contain '_'");
      }

      String loadId = "load_" + UUID.randomUUID();
      File tmpDir = new File(new File(rootDir, collection), loadId);

      try
      {
        // Extract into tmp dir
        FileUtils.forceMkdir(tmpDir);
        File tarGzFile = new File(tmpDir, "data.tar.gz");
        IOUtils.write(data, new FileOutputStream(tarGzFile));
        TarGzCompressionUtils.unTar(tarGzFile, tmpDir);
        LOG.info("Extracted data into {}", tmpDir);

        // Read tree to get ID
        File tmpTreeFile = new File(tmpDir, StarTreeConstants.TREE_FILE_NAME);
        ObjectInputStream treeStream = new ObjectInputStream(new FileInputStream(tmpTreeFile));
        StarTreeNode rootNode = (StarTreeNode) treeStream.readObject();
        String treeId = rootNode.getId().toString();
        LOG.info("Tree ID for {} is {}", loadId, treeId);

        // Move into data dir
        File dataDir = new File(collectionDir, StorageUtils.getDataDirName(treeId, schedule, minTime, maxTime));
        if (dataDir.exists()) {
          throw new Exception("Data is already uploaded for timerange:" + minTime + " to "
              + maxTime + ". Please delete the existing data for this range and try again");
        }
        FileUtils.forceMkdir(dataDir);
        StorageUtils.moveAllFiles(tmpDir, dataDir);
        LOG.info("Moved files from {} to {}", tmpDir, dataDir);

        // Touch data dir to trigger watch service
        if (!dataDir.setLastModified(System.currentTimeMillis()))
        {
          LOG.warn("setLastModified on dataDir failed - watch service will not be triggered!");
        }
      }
      finally
      {
        FileUtils.forceDelete(tmpDir);
        LOG.info("Deleted tmp dir {}", tmpDir);
      }
    }
    finally
    {
      lock.unlock();
      LOG.info("Unlocked collection {} using lock {} for data update", collection, lock);
    }
  }
}
