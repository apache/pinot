package com.linkedin.thirdeye.impl.storage;

import static java.nio.file.StandardWatchEventKinds.*;

import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeRecordStore;
import com.linkedin.thirdeye.api.StarTreeRecordStoreFactory;
import com.linkedin.thirdeye.api.TimeRange;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.input.CountingInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
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

/**
 * Creates {@link StarTreeRecordStoreFixedImpl}
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
public class StarTreeRecordStoreFactoryFixedImpl implements StarTreeRecordStoreFactory
{
  private static final Logger LOG = LoggerFactory.getLogger(StarTreeRecordStoreFactoryFixedImpl.class);

  private static final String INDEX_SUFFIX = ".idx";
  private static final String BUFFER_SUFFIX = ".buf";
  private static final String DICT_SUFFIX = ".dict";
  private static final String DIMENSION_STORE = "dimensionStore";
  private static final String METRIC_STORE = "metricStore";

  private final Object sync = new Object();

  // nodeId to dimension index
  private final Map<UUID, DimensionIndexEntry> dimensionIndex = new HashMap<UUID, DimensionIndexEntry>();

  // nodeId to metric index
  private final Map<UUID, List<MetricIndexEntry>> metricIndex = new HashMap<UUID, List<MetricIndexEntry>>();

  // nodeId to metric store
  private final Map<UUID, MetricStore> metricStores = new HashMap<UUID, MetricStore>();

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

      File dimensionStore = new File(rootDir, DIMENSION_STORE);
      FileUtils.forceMkdir(dimensionStore);
      File[] dimensionIndexFiles = dimensionStore.listFiles(INDEX_FILE_FILTER);
      if (dimensionIndexFiles != null)
      {
        for (File indexFile : dimensionIndexFiles)
        {
          loadDimensionIndex(indexFile);
        }
      }

      File metricStore = new File(rootDir, METRIC_STORE);
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

      RefreshWatcher refreshWatcher = new RefreshWatcher();

      Path dimensionPath = FileSystems.getDefault().getPath(rootDir.getAbsolutePath(), DIMENSION_STORE);
      refreshWatcher.register(dimensionPath);
      LOG.info("Registered watch on {}", dimensionPath);

      Path metricPath = FileSystems.getDefault().getPath(rootDir.getAbsolutePath(), METRIC_STORE);
      refreshWatcher.register(metricPath);
      LOG.info("Registered watch on {}", metricPath);

      Thread watcherThread = new Thread(refreshWatcher);
      watcherThread.setDaemon(true);
      watcherThread.start();
      LOG.info("Started file system watcher in {}", rootDir);
    }
  }

  @Override
  public StarTreeRecordStore createRecordStore(UUID nodeId) throws IOException
  {
    synchronized (sync)
    {
      DimensionIndexEntry dimensionIndexEntry = dimensionIndex.get(nodeId);
      if (dimensionIndexEntry == null)
      {
        throw new IllegalArgumentException("No dimension index entry for " + nodeId);
      }

      List<MetricIndexEntry> metricIndexEntries = metricIndex.get(nodeId);
      if (metricIndexEntries == null)
      {
        throw new IllegalArgumentException("No metric index entries for " + nodeId);
      }

      DimensionDictionary dictionary = getDictionary(dimensionIndexEntry);

      ByteBuffer dimensionBuffer = getDimensionBuffer(dimensionIndexEntry);

      Map<TimeRange, ByteBuffer> metricBuffers = new HashMap<TimeRange, ByteBuffer>();
      for (MetricIndexEntry indexEntry : metricIndexEntries)
      {
        metricBuffers.put(indexEntry.getTimeRange(), getMetricBuffer(indexEntry));
      }

      DimensionStore dimensionStore = new DimensionStore(starTreeConfig, dimensionBuffer, dictionary);

      MetricStore metricStore = new MetricStore(starTreeConfig, metricBuffers);

      metricStores.put(nodeId, metricStore);

      return new StarTreeRecordStoreFixedImpl(starTreeConfig, dimensionStore, metricStore);
    }
  }

  private void loadDimensionIndex(File indexFile) throws IOException
  {
    List<Object> entries = readObjectFile(indexFile);

    UUID fileId = UUID.fromString(indexFile.getName().substring(0, indexFile.getName().lastIndexOf(INDEX_SUFFIX)));

    dimensionIndexByFile.put(fileId, new HashSet<DimensionIndexEntry>());

    for (Object o : entries)
    {
      DimensionIndexEntry e = (DimensionIndexEntry) o;
      dimensionIndex.put(e.getNodeId(), e);
      dimensionIndexByFile.get(fileId).add(e);
    }

    LOG.info("Loaded dimension index {}", indexFile);
  }

  private void loadMetricIndex(File indexFile) throws IOException
  {
    List<Object> entries = readObjectFile(indexFile);

    UUID fileId = getFileId(indexFile.getName(), INDEX_SUFFIX);

    metricIndexByFile.put(fileId, new HashSet<MetricIndexEntry>());

    for (Object o : entries)
    {
      MetricIndexEntry e = (MetricIndexEntry) o;
      List<MetricIndexEntry> nodeEntries = metricIndex.get(e.getNodeId());
      if (nodeEntries == null)
      {
        nodeEntries = new ArrayList<MetricIndexEntry>();
        metricIndex.put(e.getNodeId(), nodeEntries);
      }
      nodeEntries.add(e);
      metricIndexByFile.get(fileId).add(e);
    }

    LOG.info("Loaded metric index {}", indexFile);
  }

  private void loadDimensionBuffers(Collection<DimensionIndexEntry> indexEntries) throws IOException
  {
    File dimensionStore = new File(rootDir, DIMENSION_STORE);
    for (DimensionIndexEntry indexEntry : indexEntries)
    {
      if (!dimensionSegments.containsKey(indexEntry.getFileId()))
      {
        File bufferFile = new File(dimensionStore, indexEntry.getFileId().toString() + BUFFER_SUFFIX);
        dimensionSegments.put(indexEntry.getFileId(), mapBuffer(bufferFile));
        LOG.info("Loaded buffer file {}", bufferFile);
      }

      if (!dictionarySegments.containsKey(indexEntry.getFileId()))
      {
        File bufferFile = new File(dimensionStore, indexEntry.getFileId().toString() + DICT_SUFFIX);
        dictionarySegments.put(indexEntry.getFileId(), mapBuffer(bufferFile));
        LOG.info("Loaded buffer file {}", bufferFile);
      }
    }
  }

  private void loadMetricBuffers(Collection<MetricIndexEntry> indexEntries) throws IOException
  {
    File metricStore = new File(rootDir, METRIC_STORE);
    for (MetricIndexEntry indexEntry : indexEntries)
    {
      if (!metricSegments.containsKey(indexEntry.getFileId()))
      {
        File bufferFile = new File(metricStore, indexEntry.getFileId().toString() + BUFFER_SUFFIX);
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
      throw new IllegalStateException("No mapped buffer for file " + indexEntry.getFileId() + DICT_SUFFIX);
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
      throw new IllegalStateException("No mapped buffer for file " + indexEntry.getFileId() + BUFFER_SUFFIX);
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
      throw new IllegalStateException("No mapped buffer for file " + indexEntry.getFileId() + BUFFER_SUFFIX);
    }

    metricBuffer.rewind();
    metricBuffer.position(indexEntry.getStartOffset());

    ByteBuffer slicedBuffer = metricBuffer.slice();
    slicedBuffer.limit(indexEntry.getLength());

    return slicedBuffer;
  }

  private static List<Object> readObjectFile(File objectFile) throws IOException
  {
    long fileLength = objectFile.length();

    FileInputStream fis = new FileInputStream(objectFile);
    CountingInputStream cis = new CountingInputStream(fis);
    ObjectInputStream ois = new ObjectInputStream(cis);

    List<Object> objects = new ArrayList<Object>();

    try
    {
      while (cis.getByteCount() < fileLength)
      {
        objects.add(ois.readObject());
      }
    }
    catch (ClassNotFoundException e)
    {
      throw new IOException(e);
    }
    finally
    {
      ois.close();
    }

    return objects;
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
      return file.getName().endsWith(INDEX_SUFFIX);
    }
  };

  private class RefreshWatcher implements Runnable
  {
    private final WatchService watchService;
    private final Map<WatchKey, Path> keys;

    RefreshWatcher() throws IOException
    {
      this.watchService = FileSystems.getDefault().newWatchService();
      this.keys = new HashMap<WatchKey, Path>();
    }

    void register(Path dir) throws IOException
    {
      WatchKey key = dir.register(watchService, ENTRY_CREATE, ENTRY_DELETE);
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

        for (WatchEvent<?> event : key.pollEvents())
        {
          WatchEvent<Path> ev = (WatchEvent<Path>) event;
          Path name = ev.context();
          Path child = dir.resolve(name);
          String fileName = name.toFile().getName();
          String storeName = child.getParent().toFile().getName();

          if (METRIC_STORE.equals(storeName)
                  && fileName.endsWith(INDEX_SUFFIX) && ENTRY_CREATE.equals(event.kind()))
          {
            UUID fileId = getFileId(fileName, INDEX_SUFFIX);

            // Load index, buffer, and notify all metric stores of the new segment
            synchronized (sync)
            {
              try
              {
                loadMetricIndex(child.toFile());

                Set<MetricIndexEntry> indexEntries = metricIndexByFile.get(fileId);

                loadMetricBuffers(indexEntries);

                for (MetricIndexEntry indexEntry : indexEntries)
                {
                  MetricStore metricStore = metricStores.get(indexEntry.getNodeId());
                  if (metricStore != null)
                  {
                    metricStore.notifyCreate(indexEntry.getTimeRange(), getMetricBuffer(indexEntry));
                  }
                }
              }
              catch (IOException e)
              {
                LOG.error("Error loading metric index", e);
              }
            }

            LOG.info("Loaded metric index {}", fileId);
          }
          else if (METRIC_STORE.equals(storeName)
                  && fileName.endsWith(INDEX_SUFFIX) && ENTRY_DELETE.equals(event.kind()))
          {
            UUID fileId = getFileId(fileName, INDEX_SUFFIX);

            // Remove index entries from old file, and notify metric stores segment is no longer valid
            synchronized (sync)
            {
              Set<MetricIndexEntry> indexEntries = metricIndexByFile.remove(fileId);

              if (indexEntries != null)
              {
                for (MetricIndexEntry indexEntry : indexEntries)
                {
                  metricSegments.remove(indexEntry.getFileId());
                }

                for (List<MetricIndexEntry> nodeEntries : metricIndex.values())
                {
                  nodeEntries.removeAll(indexEntries);
                }

                for (MetricIndexEntry indexEntry : indexEntries)
                {
                  MetricStore metricStore = metricStores.get(indexEntry.getNodeId());
                  if (metricStore != null)
                  {
                    metricStore.notifyDelete(indexEntry.getTimeRange());
                  }
                }
              }

              LOG.info("Removed metric index {}", fileId);
            }
          }
          else if (METRIC_STORE.equals(storeName)
                  && fileName.endsWith(BUFFER_SUFFIX) && ENTRY_DELETE.equals(event.kind()))
          {
            UUID fileId = getFileId(fileName, BUFFER_SUFFIX);

            LOG.info("Removed metric buffer {}", fileId);
          }
          else if (METRIC_STORE.equals(storeName)
                  && fileName.endsWith(BUFFER_SUFFIX) && ENTRY_CREATE.equals(event.kind()))
          {
            UUID fileId = getFileId(fileName, BUFFER_SUFFIX);

            LOG.info("Added metric buffer {}", fileId);
          }
          else
          {
            LOG.warn("Unrecognized event {}: {}", event.kind().name(), child);
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

  private static UUID getFileId(String fileName, String suffix)
  {
    return UUID.fromString(fileName.substring(0, fileName.lastIndexOf(suffix)));
  }
}
