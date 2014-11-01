package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.StarTreeRecordStoreFactory;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class StarTreeManagerImpl implements StarTreeManager
{
  private static final Logger LOG = Logger.getLogger(StarTreeManagerImpl.class);
  private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;
  private static final int DEFAULT_LOAD_QUEUE_SIZE = 1024;

  private final ConcurrentMap<String, StarTreeConfig> configs;
  private final ConcurrentMap<String, StarTree> trees;
  private final ConcurrentMap<String, StarTreeRecordStoreFactory> recordStoreFactories;
  private final ExecutorService executorService;

  public StarTreeManagerImpl(ExecutorService executorService)
  {
    this.configs = new ConcurrentHashMap<String, StarTreeConfig>();
    this.trees = new ConcurrentHashMap<String, StarTree>();
    this.recordStoreFactories = new ConcurrentHashMap<String, StarTreeRecordStoreFactory>();
    this.executorService = executorService;
  }

  @Override
  public Set<String> getCollections()
  {
    return configs.keySet();
  }

  @Override
  public void registerRecordStoreFactory(String collection,
                                         List<String> dimensionNames,
                                         List<String> metricNames,
                                         URI rootUri)
  {
    StarTreeRecordStoreFactory recordStoreFactory
            = new StarTreeRecordStoreByteBufferImpl.Factory(dimensionNames, metricNames, DEFAULT_BUFFER_SIZE, true);
    recordStoreFactories.put(collection, recordStoreFactory);
  }

  @Override
  public StarTreeRecordStoreFactory getRecordStoreFactory(String collection)
  {
    return recordStoreFactories.get(collection);
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

    // Initialize tree
    final StarTree tree;
    synchronized (trees)
    {
      StarTree previousTree = trees.get(collection);
      if (previousTree == null)
      {
        previousTree = new StarTreeImpl(config);
        previousTree.open();
        trees.put(collection, previousTree);
      }
      tree = previousTree;
    }

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
    }
    catch (InterruptedException e)
    {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void remove(String collection) throws IOException
  {
    StarTree starTree = trees.remove(collection);
    if (starTree != null)
    {
      starTree.close();
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
}
