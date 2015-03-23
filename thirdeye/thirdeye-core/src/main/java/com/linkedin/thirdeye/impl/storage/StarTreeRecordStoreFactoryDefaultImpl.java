package com.linkedin.thirdeye.impl.storage;

import static java.nio.file.StandardWatchEventKinds.*;

import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeRecordStore;
import com.linkedin.thirdeye.api.StarTreeRecordStoreFactory;
import com.linkedin.thirdeye.api.TimeRange;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Creates {@link StarTreeRecordStoreDefaultImpl}
 *
 * <p>
 *   This factory assumes the following directory structure:
 *   <pre>
 *     rootDir/
 *        collection/
 *          config.yml
 *          tree.bin
 *          data/
 *            dimensionStore/
 *              fileId.buf
 *              fileId.idx
 *              fileId.dict
 *            metricStore/
 *              fileId.buf
 *              fileId.idx
 *   </pre>
 * </p>
 *
 * <p>
 *   "someId" here is a unique identifier for the file, not
 * </p>
 *
 * <p>
 *   The .dict file will contain the term to id mapping for dimension values by name.
 * </p>
 *
 * <p>
 *  The .buf file stores the raw data in sorted dimension order. The dimension key at index
 *  i in dimensionStore/fileId.buf corresponds to the metric time-series at index i in
 *  metricStore/fileId.buf.
 * </p>
 *
 * <p>
 *   The dimensionStore/*.idx file contains entries in the following format:
 *   <pre>
 *      leafId fileId dictStartOffset length bufStartOffset length
 *   </pre>
 * </p>
 *
 * <p>
 *   The metricStore/*.idx file contains entries in the following format:
 *   <pre>
 *      leafId fileId startOffset length startTime endTime
 *   </pre>
 *   This points to the region in the metricStore/*.buf file that has metric data corresponding to the leafId.
 * </p>
 */
public class StarTreeRecordStoreFactoryDefaultImpl implements StarTreeRecordStoreFactory
{
  public static String PROP_METRIC_STORE_MUTABLE = "metricStoreMutable";

  private static final Logger LOG = LoggerFactory.getLogger(StarTreeRecordStoreFactoryDefaultImpl.class);

  private final Object sync = new Object();

  // nodeId to dimension index
  private final Map<UUID, DimensionIndexEntry> dimensionIndex = new HashMap<UUID, DimensionIndexEntry>();

  // nodeId to metric index
  private final Map<UUID, List<MetricIndexEntry>> metricIndex = new HashMap<UUID, List<MetricIndexEntry>>();

  // nodeId to metric store
  private final Map<UUID, MetricStore> metricStores = new HashMap<UUID, MetricStore>();

  // nodeId to metric store listener
  private final Map<UUID, MetricStoreListener> metricStoreListeners = new HashMap<UUID, MetricStoreListener>();

  // fileId to buffer
  private final Map<UUID, ByteBuffer> dictionarySegments = new HashMap<UUID, ByteBuffer>();

  // fileId to buffer
  private final Map<UUID, ByteBuffer> dimensionSegments = new HashMap<UUID, ByteBuffer>();

  // fileId to buffer
  private final Map<UUID, ByteBuffer> metricSegments = new HashMap<UUID, ByteBuffer>();

  // index fileId to dimension index entries
  private final Map<UUID, Set<DimensionIndexEntry>> dimensionIndexByFile = new HashMap<UUID, Set<DimensionIndexEntry>>();

  // index fileId to metric index entries
  private final Map<UUID, Set<MetricIndexEntry>> metricIndexByFile = new HashMap<UUID, Set<MetricIndexEntry>>();

  private File rootDir;
  private boolean isInit;
  private StarTreeConfig starTreeConfig;
  private boolean metricStoreMutable;

  @Override
  public void init(File rootDir, StarTreeConfig starTreeConfig, Properties recordStoreConfig) throws IOException
  {
    synchronized (sync)
    {
      if (isInit)
      {
        return;
      }

      this.rootDir = rootDir;
      this.isInit = true;
      this.starTreeConfig = starTreeConfig;

      if (recordStoreConfig != null)
      {
        Object metricStoreMutableProp = recordStoreConfig.get(PROP_METRIC_STORE_MUTABLE);

        if (metricStoreMutableProp != null)
        {
          if (metricStoreMutableProp instanceof String)
          {
            this.metricStoreMutable = Boolean.valueOf((String) metricStoreMutableProp);
          }
          else
          {
            this.metricStoreMutable = (Boolean) metricStoreMutableProp;
          }
        }
      }

      File dimensionStore = new File(rootDir, StarTreeConstants.DIMENSION_STORE);
      FileUtils.forceMkdir(dimensionStore);
      File[] dimensionIndexFiles = dimensionStore.listFiles(INDEX_FILE_FILTER);
      if (dimensionIndexFiles != null)
      {
        for (File indexFile : dimensionIndexFiles)
        {
          loadDimensionIndex(indexFile);
        }
      }

      File metricStore = new File(rootDir, StarTreeConstants.METRIC_STORE);
      FileUtils.forceMkdir(metricStore);
      File[] metricIndexFiles = metricStore.listFiles(INDEX_FILE_FILTER);
      if (metricIndexFiles != null)
      {
        for (File indexFile : metricIndexFiles)
        {
          loadMetricIndex(indexFile);
        }
      }

      loadDimensionBuffers(dimensionIndex.values());

      for (List<MetricIndexEntry> entryGroup : metricIndex.values())
      {
        loadMetricBuffers(entryGroup);
      }

      if (!metricStoreMutable)
      {
        MetricStoreRefreshWatcher refreshWatcher = new MetricStoreRefreshWatcher();

        Path metricPath = FileSystems.getDefault().getPath(rootDir.getAbsolutePath(), StarTreeConstants.METRIC_STORE);
        refreshWatcher.register(metricPath);
        LOG.info("Registered watch on {}", metricPath);

        Thread watcherThread = new Thread(refreshWatcher);
        watcherThread.setDaemon(true);
        watcherThread.start();
        LOG.info("Started file system watcher in {}", rootDir);
      }
    }
  }

  @Override
  public StarTreeRecordStore createRecordStore(UUID nodeId) throws IOException
  {
    synchronized (sync)
    {
      // Dimension store
      DimensionIndexEntry dimensionIndexEntry = dimensionIndex.get(nodeId);
      if (dimensionIndexEntry == null)
      {
        throw new IllegalArgumentException("No dimension index entry for " + nodeId);
      }
      DimensionDictionary dictionary = getDictionary(dimensionIndexEntry);
      ByteBuffer dimensionBuffer = getDimensionBuffer(dimensionIndexEntry);
      DimensionStore dimensionStore = new DimensionStoreImmutableImpl(starTreeConfig, dimensionBuffer, dictionary);

      // Metric store
      ConcurrentMap<TimeRange, List<ByteBuffer>> metricBuffers = new ConcurrentHashMap<TimeRange, List<ByteBuffer>>();
      List<MetricIndexEntry> metricIndexEntries = metricIndex.get(nodeId);
      if (metricIndexEntries != null)
      {
        for (MetricIndexEntry indexEntry : metricIndexEntries)
        {
          List<ByteBuffer> bufferList = metricBuffers.get(indexEntry.getTimeRange());
          if (bufferList == null)
          {
            bufferList = new CopyOnWriteArrayList<ByteBuffer>();
            metricBuffers.put(indexEntry.getTimeRange(), bufferList);
          }
          bufferList.add(getMetricBuffer(indexEntry));
        }
      }

      MetricStore metricStore;
      if (metricStoreMutable)
      {
        metricStore = new MetricStoreMutableImpl(starTreeConfig);
      }
      else
      {
        MetricStoreImmutableImpl immutableStore = new MetricStoreImmutableImpl(starTreeConfig, metricBuffers);
        metricStoreListeners.put(nodeId, immutableStore);
        metricStore = immutableStore;
      }
      metricStores.put(nodeId, metricStore);

      return new StarTreeRecordStoreDefaultImpl(starTreeConfig, dimensionStore, metricStore);
    }
  }

  private void loadDimensionIndex(File indexFile) throws IOException
  {
    List<DimensionIndexEntry> entries = StorageUtils.readDimensionIndex(indexFile);

    UUID fileId = UUID.fromString(indexFile.getName().substring(0, indexFile.getName().lastIndexOf(StarTreeConstants.INDEX_FILE_SUFFIX)));

    dimensionIndexByFile.put(fileId, new HashSet<DimensionIndexEntry>());

    for (DimensionIndexEntry entry : entries)
    {
      dimensionIndex.put(entry.getNodeId(), entry);
      dimensionIndexByFile.get(fileId).add(entry);
    }

    LOG.info("Loaded dimension index {}", indexFile);
  }

  private void loadMetricIndex(File indexFile) throws IOException
  {
    List<MetricIndexEntry> entries = StorageUtils.readMetricIndex(indexFile);

    UUID fileId = getFileId(indexFile.getName(), StarTreeConstants.INDEX_FILE_SUFFIX);

    metricIndexByFile.put(fileId, new HashSet<MetricIndexEntry>());

    for (MetricIndexEntry entry : entries)
    {
      List<MetricIndexEntry> nodeEntries = metricIndex.get(entry.getNodeId());
      if (nodeEntries == null)
      {
        nodeEntries = new ArrayList<MetricIndexEntry>();
        metricIndex.put(entry.getNodeId(), nodeEntries);
      }
      nodeEntries.add(entry);
      metricIndexByFile.get(fileId).add(entry);
    }

    LOG.info("Loaded metric index {}", indexFile);
  }

  private void loadDimensionBuffers(Collection<DimensionIndexEntry> indexEntries) throws IOException
  {
    File dimensionStore = new File(rootDir, StarTreeConstants.DIMENSION_STORE);
    for (DimensionIndexEntry indexEntry : indexEntries)
    {
      if (!dimensionSegments.containsKey(indexEntry.getFileId()))
      {
        File bufferFile = new File(dimensionStore, indexEntry.getFileId().toString() + StarTreeConstants.BUFFER_FILE_SUFFIX);
        dimensionSegments.put(indexEntry.getFileId(), mapBuffer(bufferFile));
        LOG.info("Loaded buffer file {}", bufferFile);
      }

      if (!dictionarySegments.containsKey(indexEntry.getFileId()))
      {
        File bufferFile = new File(dimensionStore, indexEntry.getFileId().toString() + StarTreeConstants.DICT_FILE_SUFFIX);
        dictionarySegments.put(indexEntry.getFileId(), mapBuffer(bufferFile));
        LOG.info("Loaded buffer file {}", bufferFile);
      }
    }
  }

  private void loadMetricBuffers(Collection<MetricIndexEntry> indexEntries) throws IOException
  {
    File metricStore = new File(rootDir, StarTreeConstants.METRIC_STORE);
    for (MetricIndexEntry indexEntry : indexEntries)
    {
      if (!metricSegments.containsKey(indexEntry.getFileId()))
      {
        File bufferFile = new File(metricStore, indexEntry.getFileId().toString() + StarTreeConstants.BUFFER_FILE_SUFFIX);
        metricSegments.put(indexEntry.getFileId(), mapBuffer(bufferFile));
        LOG.info("Loaded buffer file {}", bufferFile);
      }
    }
  }

  private DimensionDictionary getDictionary(DimensionIndexEntry indexEntry) throws IOException
  {
    ByteBuffer dictionaryBuffer = dictionarySegments.get(indexEntry.getFileId());
    if (dictionaryBuffer == null)
    {
      throw new IllegalStateException("No mapped buffer for file " + indexEntry.getFileId() + StarTreeConstants.DICT_FILE_SUFFIX);
    }

    dictionaryBuffer.rewind();
    dictionaryBuffer.position(indexEntry.getDictionaryStartOffset());
    byte[] dictionaryBytes = new byte[indexEntry.getDictionaryLength()];
    dictionaryBuffer.get(dictionaryBytes);
    ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(dictionaryBytes));

    DimensionDictionary dictionary;
    try
    {
      dictionary = (DimensionDictionary) ois.readObject();
    }
    catch (ClassNotFoundException e)
    {
      throw new IOException(e);
    }

    return dictionary;
  }

  private ByteBuffer getDimensionBuffer(DimensionIndexEntry indexEntry) throws IOException
  {
    ByteBuffer dimensionBuffer = dimensionSegments.get(indexEntry.getFileId());
    if (dimensionBuffer == null)
    {
      throw new IllegalStateException("No mapped buffer for file " + indexEntry.getFileId() + StarTreeConstants.BUFFER_FILE_SUFFIX);
    }

    dimensionBuffer.rewind();
    dimensionBuffer.position(indexEntry.getBufferStartOffset());

    ByteBuffer slicedBuffer = dimensionBuffer.slice();
    slicedBuffer.limit(indexEntry.getBufferLength());

    return slicedBuffer;
  }

  private ByteBuffer getMetricBuffer(MetricIndexEntry indexEntry) throws IOException
  {
    ByteBuffer metricBuffer = metricSegments.get(indexEntry.getFileId());
    if (metricBuffer == null)
    {
      throw new IllegalStateException("No mapped buffer for file " + indexEntry.getFileId() + StarTreeConstants.BUFFER_FILE_SUFFIX);
    }

    metricBuffer.rewind();
    metricBuffer.position(indexEntry.getStartOffset());

    ByteBuffer slicedBuffer = metricBuffer.slice();
    slicedBuffer.limit(indexEntry.getLength());

    return slicedBuffer;
  }

  private static ByteBuffer mapBuffer(File bufferFile) throws IOException
  {
    FileChannel channel = new RandomAccessFile(bufferFile, "r").getChannel();
    ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, bufferFile.length());
    buffer.order(ByteOrder.BIG_ENDIAN);
    return buffer;
  }

  private static final FileFilter INDEX_FILE_FILTER = new FileFilter()
  {
    @Override
    public boolean accept(File file)
    {
      return file.getName().endsWith(StarTreeConstants.INDEX_FILE_SUFFIX);
    }
  };

  private class MetricStoreRefreshWatcher implements Runnable
  {
    private final WatchService watchService;
    private final Map<WatchKey, Path> keys;

    MetricStoreRefreshWatcher() throws IOException
    {
      this.watchService = FileSystems.getDefault().newWatchService();
      this.keys = new HashMap<WatchKey, Path>();
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
        WatchKey key;
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

        synchronized (sync)
        {
          for (WatchEvent<?> event : key.pollEvents())
          {
            WatchEvent<Path> ev = (WatchEvent<Path>) event;
            Path path = dir.resolve(ev.context());
            File file = path.toFile();

            if (LOG.isDebugEnabled())
            {
              LOG.debug("{} {}", ev.kind(), path);
            }

            if (file.getName().endsWith(StarTreeConstants.INDEX_FILE_SUFFIX))
            {
              UUID fileId = getFileId(path.toFile().getName(), StarTreeConstants.INDEX_FILE_SUFFIX);

              // Clear existing index / metric stores for this file (always)
              Set<MetricIndexEntry> indexEntries = metricIndexByFile.remove(fileId);
              if (indexEntries != null)
              {
                for (MetricIndexEntry indexEntry : indexEntries)
                {
                  List<MetricIndexEntry> indexEntriesByNode = metricIndex.get(indexEntry.getNodeId());
                  if (indexEntriesByNode != null)
                  {
                    indexEntriesByNode.remove(indexEntry);
                  }

                  MetricStoreListener metricStoreListener = metricStoreListeners.get(indexEntry.getNodeId());
                  if (metricStoreListener != null)
                  {
                    metricStoreListener.notifyDelete(indexEntry.getTimeRange());
                  }
                }
              }

              if (ENTRY_CREATE.equals(event.kind()) || ENTRY_MODIFY.equals(event.kind()))
              {
                try
                {
                  waitForWriteComplete(file);
                  loadMetricIndex(file);

                  // Notify create if buffer exists too
                  File bufferFile = new File(path.toFile().getParent(), fileId + StarTreeConstants.BUFFER_FILE_SUFFIX);
                  if (bufferFile.exists())
                  {
                    waitForWriteComplete(bufferFile);
                    for (MetricIndexEntry indexEntry : metricIndexByFile.get(fileId))
                    {
                      MetricStoreListener metricStoreListener = metricStoreListeners.get(indexEntry.getNodeId());
                      if (metricStoreListener != null)
                      {
                        metricStoreListener.notifyCreate(indexEntry.getTimeRange(), getMetricBuffer(indexEntry));
                      }
                    }
                    LOG.info("Notified of creation of metric index and buffer for {}", fileId);
                  }
                }
                catch (Exception e)
                {
                  LOG.warn("Error loading index file {}", path, e);
                }
              }
              else if (ENTRY_DELETE.equals(event.kind()))
              {
                LOG.info("Deleted metric index for file {}", fileId);
              }

            }
            else if (file.getName().endsWith(StarTreeConstants.BUFFER_FILE_SUFFIX))
            {
              UUID fileId = getFileId(path.toFile().getName(), StarTreeConstants.BUFFER_FILE_SUFFIX);

              if (ENTRY_CREATE.equals(event.kind()) || ENTRY_MODIFY.equals(event.kind()))
              {
                try
                {
                  waitForWriteComplete(file);
                  ByteBuffer buffer = mapBuffer(file);
                  metricSegments.put(fileId, buffer);
                  LOG.info("Loaded buffer file {}: {}", file, buffer);

                  // Touch index file to trigger another event
                  File indexFile = new File(path.toFile().getParent(), fileId + StarTreeConstants.INDEX_FILE_SUFFIX);
                  if (indexFile.exists())
                  {
                    indexFile.setLastModified(System.currentTimeMillis());
                  }
                }
                catch (Exception e)
                {
                  LOG.warn("Error loading buffer file {}", file, e);
                }
              }
              else if (ENTRY_DELETE.equals(event.kind()))
              {
                ByteBuffer buffer = metricSegments.remove(fileId);
                if (buffer != null)
                {
                  LOG.info("Removed existing buffer file {}", fileId);
                }
              }
            }
            else
            {
              LOG.warn("Unrecognized file type {}", path);
            }
          }
        }

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
  }

  private static void waitForWriteComplete(File file) throws InterruptedException
  {
    long startTime = System.currentTimeMillis();
    long fileSize;
    do
    {
      fileSize = file.length();
      Thread.sleep(100); // wait for some writes
    }
    while (fileSize < file.length() && System.currentTimeMillis() - startTime < 60000);
  }

  private static UUID getFileId(String fileName, String suffix)
  {
    return UUID.fromString(fileName.substring(0, fileName.lastIndexOf(suffix)));
  }
}
