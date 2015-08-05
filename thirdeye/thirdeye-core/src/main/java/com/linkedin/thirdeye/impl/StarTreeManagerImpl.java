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
import com.linkedin.thirdeye.impl.storage.IndexMetadata;
import com.linkedin.thirdeye.impl.storage.StorageUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class StarTreeManagerImpl implements StarTreeManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(StarTreeManagerImpl.class);
  private static final long REFRESH_WAIT_SLEEP_MILLIS = 1000;
  private static final long REFRESH_WAIT_TIMEOUT_MILLIS = 30000;

  private final ConcurrentMap<String, StarTreeConfig> configs;
  private final ConcurrentMap<String, ConcurrentMap<File, StarTree>> trees;
  private final ConcurrentMap<UUID, IndexMetadata> allIndexMetadata;
  private final ConcurrentMap<String, StarTree> mutableTrees;
  private final Set<String> openCollections;
  private final ConcurrentMap<String, Long> maxDataTime;

  public StarTreeManagerImpl() {
    this.configs = new ConcurrentHashMap<String, StarTreeConfig>();
    this.trees = new ConcurrentHashMap<String, ConcurrentMap<File, StarTree>>();
    this.allIndexMetadata = new ConcurrentHashMap<>();
    this.mutableTrees = new ConcurrentHashMap<>();
    this.openCollections = new HashSet<String>();
    this.maxDataTime = new ConcurrentHashMap<>();
  }

  @Override
  public Set<String> getCollections() {
    return openCollections;
  }

  @Override
  public StarTreeConfig getConfig(String collection) {
    return configs.get(collection);
  }

  @Override
  public StarTree getMutableStarTree(String collection) {
    return mutableTrees.get(collection);
  }

  @Override
  public Long getMaxDataTime(String collection) {
    return maxDataTime.get(collection);
  }

  @Override
  public Map<File, StarTree> getStarTrees(String collection) {
    return trees.get(collection);
  }

  @Override
  public IndexMetadata getIndexMetadata(UUID treeId) {
    return allIndexMetadata.get(treeId);
  }

  @Override
  public void restore(File rootDir, String collection) throws Exception {
    synchronized (trees) {
      if (!trees.containsKey(collection)) {
        LOGGER.info("Creating new startree for {}", collection);
        trees.put(collection, new ConcurrentHashMap<File, StarTree>());

        File collectionDir = new File(rootDir, collection);

        // Data dirs
        File[] dataDirs = collectionDir.listFiles(new FilenameFilter() {
          @Override
          public boolean accept(File dir, String name) {
            return name.startsWith(StorageUtils.getDataDirPrefix());
          }
        });

        if (dataDirs == null) {
          throw new IllegalArgumentException("No data dirs for collection " + collection);
        }

        // Read config
        File configFile = new File(collectionDir, StarTreeConstants.CONFIG_FILE_NAME);
        StarTreeConfig config = StarTreeConfig.decode(new FileInputStream(configFile));
        configs.put(collection, config);

        long maxDataTimeMillis = 0;
        for (File dataDir : dataDirs) {
          // Read tree structure
          File treeFile = new File(dataDir, StarTreeConstants.TREE_FILE_NAME);
          ObjectInputStream inputStream = new ObjectInputStream(new FileInputStream(treeFile));
          StarTreeNode root = (StarTreeNode) inputStream.readObject();
          inputStream.close();

          // Create tree
          StarTree starTree = new StarTreeImpl(config, dataDir, root);
          trees.get(collection).put(dataDir, starTree);
          starTree.open();
          LOGGER.info("Opened tree {} for collection {}", dataDir.getName(), collection);

          // Read index metadata
          InputStream indexMetadataFile = new FileInputStream(new File(dataDir, StarTreeConstants.METADATA_FILE_NAME));
          Properties indexMetadataProps = new Properties();
          indexMetadataProps.load(indexMetadataFile);
          indexMetadataFile.close();
          IndexMetadata indexMetadata = IndexMetadata.fromProperties(indexMetadataProps);
          allIndexMetadata.put(root.getId(), indexMetadata);

          if (indexMetadata.getMaxDataTimeMillis() > maxDataTimeMillis) {
            maxDataTimeMillis = indexMetadata.getMaxDataTimeMillis();
          }
        }
        maxDataTime.put(collection, maxDataTimeMillis);

        // Create mutable in-memory tree
        StarTreeConfig inMemoryConfig = new StarTreeConfig(config.getCollection(),
            StarTreeRecordStoreFactoryHashMapImpl.class.getCanonicalName(),
            new Properties(),
            config.getAnomalyDetectionFunctionClass(),
            config.getAnomalyDetectionFunctionConfig(),
            config.getAnomalyHandlerClass(),
            config.getAnomalyHandlerConfig(),
            config.getAnomalyDetectionMode(),
            config.getDimensions(),
            config.getMetrics(),
            config.getTime(),
            config.getJoinSpec(),
            config.getRollup(),
            config.getSplit(),
            false);
        final StarTree mutableTree = new StarTreeImpl(inMemoryConfig);
        mutableTree.open();
        mutableTrees.put(collection, mutableTree);

        openCollections.add(collection);
        LOGGER.info("Opened {} trees for collection {}", dataDirs.length, collection);

        // Register watch on collection dir
        DataRefreshWatcher refreshWatcher = new DataRefreshWatcher(config);
        refreshWatcher.register(Paths.get(collectionDir.getAbsolutePath()));
        Thread watcherThread = new Thread(refreshWatcher);
        watcherThread.setDaemon(true);
        watcherThread.start();
        LOGGER.info("Started watcher on {}", collectionDir.getAbsolutePath());
      }
    }
  }

  @Override
  public void close(String collection) throws IOException {
    synchronized (trees) {
      Map<File, StarTree> starTrees = trees.remove(collection);
      if (starTrees != null) {
        for (StarTree starTree : starTrees.values()) {
          starTree.close();
        }
        LOGGER.info("Closed trees for collection {}", collection);
      }

      openCollections.remove(collection);
    }
  }

  private class DataRefreshWatcher implements Runnable {
    private final StarTreeConfig config;
    private final WatchService watchService;
    private final ConcurrentMap<WatchKey, Path> keys;

    DataRefreshWatcher(StarTreeConfig config) throws IOException {
      this.config = config;
      this.watchService = FileSystems.getDefault().newWatchService();
      this.keys = new ConcurrentHashMap<WatchKey, Path>();
    }

    void register(Path dir) throws IOException {
      WatchKey key = dir.register(watchService, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
      keys.put(key, dir);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void run() {
      for (;;) {
        WatchKey key = null;
        try {
          try {
            key = watchService.take();
          } catch (InterruptedException e) {
            continue;
          }

          Path dir = keys.get(key);
          if (dir == null) {
            LOGGER.error("WatchKey not recognized: {}", key);
            continue;
          }

          for (WatchEvent<?> event : key.pollEvents()) {
            if (event.kind() == OVERFLOW) {
              LOGGER.info("Received an overflow event");
              for (Entry<String, ConcurrentMap<File, StarTree>> collectionEntry : trees.entrySet()) {
                for (Entry<File, StarTree> mapEntry : collectionEntry.getValue().entrySet()) {
                  if (!mapEntry.getKey().exists()) {
                    collectionEntry.getValue().remove(mapEntry.getKey());
                  }
                }
              }
              continue;
            }

            WatchEvent<Path> ev = (WatchEvent<Path>) event;
            Path path = dir.resolve(ev.context());
            File file = path.toFile();

            LOGGER.info("{} {}", ev.kind(), path);

            if (file.getName().startsWith(StorageUtils.getDataDirPrefix()) && ev.kind().equals(ENTRY_DELETE)) {
              for (Entry<String, ConcurrentMap<File, StarTree>> entry : trees.entrySet()) {
                entry.getValue().get(path.toFile()).close();
                entry.getValue().remove(path.toFile());
              }
            } else if (file.getName().startsWith(StorageUtils.getDataDirPrefix())) {
              StorageUtils.waitForModifications(file, REFRESH_WAIT_SLEEP_MILLIS, REFRESH_WAIT_TIMEOUT_MILLIS);

              synchronized (trees) {
                // Read tree structure
                File treeFile = new File(file, StarTreeConstants.TREE_FILE_NAME);
                ObjectInputStream inputStream = new ObjectInputStream(new FileInputStream(treeFile));
                StarTreeNode root = (StarTreeNode) inputStream.readObject();
                inputStream.close();

                // Read index metadata
                InputStream indexMetadataFile = new FileInputStream(new File(file, StarTreeConstants.METADATA_FILE_NAME));
                Properties indexMetadataProps = new Properties();
                indexMetadataProps.load(indexMetadataFile);
                indexMetadataFile.close();
                IndexMetadata indexMetadata = IndexMetadata.fromProperties(indexMetadataProps);
                allIndexMetadata.put(root.getId(), indexMetadata);

                Long previous = maxDataTime.putIfAbsent(config.getCollection(), indexMetadata.getMaxDataTimeMillis());
                if (previous != null) {
                  if (indexMetadata.getMaxDataTimeMillis() > previous) {
                    maxDataTime.put(config.getCollection(), indexMetadata.getMaxDataTimeMillis());
                  }
                }

                Map<File, StarTree> existingTrees = trees.get(config.getCollection());
                if (existingTrees == null) {
                  LOGGER.error("There is a watch on collection {} but no open trees!", config.getCollection());
                } else {
                  // If tree is already open, close it
                  StarTree existingTree = existingTrees.get(file);
                  if (existingTree != null) {
                    existingTree.close();
                    LOGGER.info("Closed existing tree {} in {}", existingTree.getRoot().getId(), file);
                  }

                  // Create tree
                  try {
                    StarTree starTree = new StarTreeImpl(config, file, root);
                    starTree.open();
                    trees.get(config.getCollection()).put(file, starTree);
                    LOGGER.info("Opened tree {} from {}", starTree.getRoot().getId(), file);
                  } catch (Exception e) {
                    // n.b. there may be partial data i.e. during a push; another watch will be fired later
                    if (LOGGER.isDebugEnabled()) {
                      LOGGER.debug("Error while watching collection directory", e);
                    }
                  }
                }
              }
            }


          }
        } catch (Exception e) {
          LOGGER.error("Error while watching collection directory", e);
        }

        if (key != null) {
          boolean valid = key.reset();
          if (!valid) {
            keys.remove(key);
            if (keys.isEmpty()) {
              break;
            }
          }
        }
      }

      try {
        watchService.close();
      } catch (IOException e) {
        LOGGER.warn("Failed to close watcher service ", e);
      }
    }
  }
}
