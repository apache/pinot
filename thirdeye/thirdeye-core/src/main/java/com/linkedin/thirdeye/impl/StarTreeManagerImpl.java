package com.linkedin.thirdeye.impl;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.impl.storage.StorageUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class StarTreeManagerImpl implements StarTreeManager
{
  private static final Logger LOG = LoggerFactory.getLogger(StarTreeManagerImpl.class);
  private static final long REFRESH_WAIT_SLEEP_MILLIS = 1000;
  private static final long REFRESH_WAIT_TIMEOUT_MILLIS = 30000;

  private final ConcurrentMap<String, StarTreeConfig> configs;
  private final ConcurrentMap<String, ConcurrentMap<File, StarTree>> trees;
  private final Set<String> openCollections;

  public StarTreeManagerImpl()
  {
    this.configs = new ConcurrentHashMap<String, StarTreeConfig>();
    this.trees = new ConcurrentHashMap<String, ConcurrentMap<File, StarTree>>();
    this.openCollections = new HashSet<String>();
  }

  @Override
  public Set<String> getCollections()
  {
    return openCollections;
  }

  @Override
  public StarTreeConfig getConfig(String collection)
  {
    return configs.get(collection);
  }

  @Override
  public Map<File, StarTree> getStarTrees(String collection)
  {
    return trees.get(collection);
  }

  @Override
  public void restore(File rootDir, String collection) throws Exception
  {
    synchronized (trees)
    {
      if (!trees.containsKey(collection))
      {
        LOG.info("Creating new startree for {}", collection);
        trees.put(collection, new ConcurrentHashMap<File, StarTree>());

        File collectionDir = new File(rootDir, collection);

        // Data dirs
        File[] dataDirs = collectionDir.listFiles(new FilenameFilter()
        {
          @Override
          public boolean accept(File dir, String name)
          {
            return name.startsWith(StorageUtils.getDataDirPrefix());
          }
        });

        if (dataDirs == null)
        {
          throw new IllegalArgumentException("No data dirs for collection " + collection);
        }

        // Read config
        File configFile = new File(collectionDir, StarTreeConstants.CONFIG_FILE_NAME);
        StarTreeConfig config = StarTreeConfig.decode(new FileInputStream(configFile));
        configs.put(collection, config);

        for (File dataDir : dataDirs)
        {
          // Read tree structure
          File treeFile = new File(dataDir, StarTreeConstants.TREE_FILE_NAME);
          ObjectInputStream inputStream = new ObjectInputStream(new FileInputStream(treeFile));
          StarTreeNode root = (StarTreeNode) inputStream.readObject();

          // Create tree
          StarTree starTree = new StarTreeImpl(config, dataDir, root);
          trees.get(collection).put(dataDir, starTree);
          starTree.open();
          openCollections.add(collection);
          LOG.info("Opened tree {} for collection {}", starTree.getRoot(), collection);
        }

        // Register watch on collection dir
        DataRefreshWatcher refreshWatcher = new DataRefreshWatcher(config);
        refreshWatcher.register(Paths.get(collectionDir.getAbsolutePath()));
        Thread watcherThread = new Thread(refreshWatcher);
        watcherThread.setDaemon(true);
        watcherThread.start();
      }
    }
  }

  @Override
  public void close(String collection) throws IOException
  {
    synchronized (trees)
    {
      Map<File, StarTree> starTrees = trees.remove(collection);
      if (starTrees != null)
      {
        for (StarTree starTree : starTrees.values())
        {
          starTree.close();
        }
        LOG.info("Closed trees for collection {}", collection);
      }

      openCollections.remove(collection);
    }
  }

  private class DataRefreshWatcher implements Runnable
  {
    private final StarTreeConfig config;
    private final WatchService watchService;
    private final ConcurrentMap<WatchKey, Path> keys;

    DataRefreshWatcher(StarTreeConfig config) throws IOException
    {
      this.config = config;
      this.watchService = FileSystems.getDefault().newWatchService();
      this.keys = new ConcurrentHashMap<WatchKey, Path>();
    }

    void register(Path dir) throws IOException
    {
      WatchKey key = dir.register(watchService, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
      keys.put(key, dir);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void run()
    {
      for (;;)
      {
        WatchKey key = null;
        try
        {
          try
          {
            key = watchService.take();
          }
          catch (InterruptedException e)
          {
            continue;
          }

          Path dir = keys.get(key);
          if (dir == null)
          {
            LOG.error("WatchKey not recognized: {}", key);
            continue;
          }

          for (WatchEvent<?> event : key.pollEvents())
          {
            if (event.kind() == OVERFLOW)
            {
              LOG.info("Received a overflow event");
              continue;
            }

            WatchEvent<Path> ev = (WatchEvent<Path>) event;
            Path path = dir.resolve(ev.context());
            File file = path.toFile();

            LOG.info("{} {}", ev.kind(), path);

            if (file.getName().startsWith(StorageUtils.getDataDirPrefix()))
            {
              StorageUtils.waitForModifications(file, REFRESH_WAIT_SLEEP_MILLIS, REFRESH_WAIT_TIMEOUT_MILLIS);

              synchronized (trees)
              {
                // Read tree structure
                File treeFile = new File(file, StarTreeConstants.TREE_FILE_NAME);
                ObjectInputStream inputStream = new ObjectInputStream(new FileInputStream(treeFile));
                StarTreeNode root = (StarTreeNode) inputStream.readObject();

                Map<File, StarTree> existingTrees = trees.get(config.getCollection());
                if (existingTrees == null)
                {
                  LOG.error("There is a watch on collection {} but no open trees!", config.getCollection());
                }
                else
                {
                  // If tree is already open, close it
                  StarTree existingTree = existingTrees.get(file);
                  if (existingTree != null)
                  {
                    existingTree.close();
                    LOG.info("Closed existing tree {} in {}", existingTree.getRoot().getId(), file);
                  }

                  // Create tree
                  try
                  {
                    StarTree starTree = new StarTreeImpl(config, file, root);
                    starTree.open();
                    trees.get(config.getCollection()).put(file, starTree);
                    LOG.info("Opened tree {} from {}", starTree.getRoot().getId(), file);
                  }
                  catch (Exception e)
                  {
                    // n.b. there may be partial data i.e. during a push; another watch will be fired later
                    if (LOG.isDebugEnabled())
                    {
                      LOG.debug("{}", e);
                    }
                  }
                }
              }
            }
          }
        }
        catch (Exception e)
        {
          LOG.error("{}", e);
        }

        if (key != null)
        {
          boolean valid = key.reset();
          if (!valid)
          {
            keys.remove(key);
            if (keys.isEmpty())
            {
              break;
            }
          }
        }
      }

      try
      {
        watchService.close();
      }
      catch (IOException e)
      {
        LOG.warn("Failed to close watcher service ",e);
      }
    }
  }
}
