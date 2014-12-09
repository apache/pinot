package com.linkedin.thirdeye.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.api.StarTreeRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class StarTreeManagerImpl implements StarTreeManager
{
  private static final Logger LOG = LoggerFactory.getLogger(StarTreeManagerImpl.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final int DEFAULT_LOAD_QUEUE_SIZE = 1024;

  private final ConcurrentMap<String, StarTreeConfig> configs;
  private final ConcurrentMap<String, StarTree> trees;
  private final ExecutorService executorService;

  public StarTreeManagerImpl(ExecutorService executorService)
  {
    this.configs = new ConcurrentHashMap<String, StarTreeConfig>();
    this.trees = new ConcurrentHashMap<String, StarTree>();
    this.executorService = executorService;
  }

  @Override
  public Set<String> getCollections()
  {
    return configs.keySet();
  }

  @Override
  public void registerConfig(String collection, StarTreeConfig config)
  {
    configs.putIfAbsent(collection, config);
  }

  @Override
  public StarTreeConfig getConfig(String collection)
  {
    return configs.get(collection);
  }

  @Override
  public void removeConfig(String collection)
  {
    configs.remove(collection);
  }

  @Override
  public StarTree getStarTree(String collection)
  {
    return trees.get(collection);
  }

  @Override
  public void load(final String collection, final Iterable<StarTreeRecord> records) throws IOException
  {
    StarTreeConfig config = configs.get(collection);
    if (config == null)
    {
      throw new IllegalArgumentException("Cannot build for collection with no config: " + collection);
    }

    final StarTree tree = getStarTree(collection);

    // Multiple threads to load
    final BlockingQueue<StarTreeRecord> recordQueue = new ArrayBlockingQueue<StarTreeRecord>(DEFAULT_LOAD_QUEUE_SIZE);
    final AtomicInteger numLoaded = new AtomicInteger(0);
    final int numWorkers = Runtime.getRuntime().availableProcessors();
    final CountDownLatch latch = new CountDownLatch(numWorkers);
    for (int i = 0; i < Runtime.getRuntime().availableProcessors(); i++)
    {
      executorService.submit(new Runnable()
      {
        @Override
        public void run()
        {
          try
          {
            StarTreeRecord record;
            while (!((record = recordQueue.take()) instanceof StarTreeRecordEndMarker))
            {
              tree.add(record);
              int n = numLoaded.incrementAndGet();
              if (n % 5000 == 0)
              {
                LOG.info(n + " records loaded into " + collection);
              }
            }
            latch.countDown();
          }
          catch (InterruptedException e)
          {
            throw new RuntimeException(e);
          }
        }
      });
    }

    try
    {
      // Populate queue
      for (StarTreeRecord record : records)
      {
        recordQueue.put(record);
      }

      // Done populating; put in poison pills
      for (int i = 0; i < Runtime.getRuntime().availableProcessors(); i++)
      {
        recordQueue.put(new StarTreeRecordEndMarker());
      }

      latch.await();

      LOG.info("Loaded {} records into startree for collection {}", numLoaded.get(), collection);
    }
    catch (InterruptedException e)
    {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void restore(File rootDir, String collection) throws Exception
  {
    synchronized (trees)
    {
      LOG.info("Creating new startree for {}", collection);

      File collectionDir = new File(rootDir, collection);

      // Read tree structure
      File treeFile = new File(collectionDir, StarTreeConstants.TREE_FILE_NAME);
      ObjectInputStream inputStream = new ObjectInputStream(new FileInputStream(treeFile));
      StarTreeNode root = (StarTreeNode) inputStream.readObject();

      // Read config
      File configFile = new File(collectionDir, StarTreeConstants.CONFIG_FILE_NAME);
      StarTreeConfig config = StarTreeConfig.fromJson(OBJECT_MAPPER.readTree(new FileInputStream(configFile)), rootDir);

      // Create tree
      StarTree starTree = new StarTreeImpl(config, root);
      StarTree previous = trees.put(collection, starTree);
      if (previous != null)
      {
        previous.close();
      }

      // Store config
      configs.put(collection, config);
    }
  }

  @Override
  public void stub(File rootDir, String collection) throws Exception
  {
    synchronized (trees)
    {
      StarTree starTree = trees.get(collection);
      if (starTree == null)
      {
        restore(rootDir, collection);
        starTree = trees.get(collection);
        stubRecordStores(starTree.getRoot(), starTree.getConfig());
      }
    }
  }

  private void stubRecordStores(StarTreeNode node, StarTreeConfig config) throws IOException
  {
    if (node.isLeaf())
    {
      node.setRecordStore(new StarTreeRecordStoreBlackHoleImpl(config.getDimensionNames(), config.getMetricNames()));
    }
    else
    {
      for (StarTreeNode child : node.getChildren())
      {
        stubRecordStores(child, config);
      }
      stubRecordStores(node.getOtherNode(), config);
      stubRecordStores(node.getStarNode(), config);
    }
  }

  @Override
  public void remove(String collection) throws IOException
  {
    synchronized (trees)
    {
      StarTree starTree = trees.remove(collection);
      if (starTree != null)
      {
        LOG.info("Closing startree for {}", collection);
        starTree.close();
      }
    }
  }

  @Override
  public void create(String collection) throws IOException
  {
    synchronized (trees)
    {
      StarTree starTree = trees.get(collection);
      if (starTree == null)
      {
        StarTreeConfig config = configs.get(collection);
        if (config == null)
        {
          throw new IllegalStateException("No config exists for collection " + collection);
        }

        starTree = new StarTreeImpl(config);
        trees.put(collection, starTree);
      }
    }
  }

  /**
   * Uses to indicate that end of record stream has been reached
   */
  private static class StarTreeRecordEndMarker extends StarTreeRecordImpl
  {
    StarTreeRecordEndMarker()
    {
      super(null, null, null); // Okay because we will never access these values
    }
  }

  @Override
  public void open(String collection) throws IOException
  {
    synchronized (trees)
    {
      StarTree starTree = trees.get(collection);
      if (starTree == null)
      {
        throw new IllegalArgumentException("No star tree for collection " + collection);
      }
      starTree.open();
      LOG.info("Opened tree for collection {}", collection);
    }
  }

  @Override
  public void close(String collection) throws IOException
  {
    synchronized (trees)
    {
      StarTree starTree = trees.get(collection);
      if (starTree != null)
      {
        starTree.close();
        LOG.info("Closed tree for collection {}", collection);
      }
    }
  }
}
