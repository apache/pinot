package com.linkedin.thirdeye.impl.storage;

import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeRecordStore;
import com.linkedin.thirdeye.api.StarTreeRecordStoreFactory;
import com.linkedin.thirdeye.api.TimeRange;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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
  private static final String DATA_DIR = "data";
  private static final String DIMENSION_STORE = "dimensionStore";
  private static final String METRIC_STORE = "metricStore";

  private final Object sync = new Object();

  // nodeId to dimension index
  private final Map<UUID, DimensionIndexEntry> dimensionIndex = new HashMap<UUID, DimensionIndexEntry>();

  // nodeId to metric index
  private final Map<UUID, List<MetricIndexEntry>> metricIndex = new HashMap<UUID, List<MetricIndexEntry>>();

  // fileId to buffer
  private final Map<UUID, ByteBuffer> dictionarySegments = new HashMap<UUID, ByteBuffer>();

  // fileId to buffer
  private final Map<UUID, ByteBuffer> dimensionSegments = new HashMap<UUID, ByteBuffer>();

  // fileId to buffer
  private final Map<UUID, ByteBuffer> metricSegments = new HashMap<UUID, ByteBuffer>();

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

      this.isInit = true;
      this.starTreeConfig = starTreeConfig;

      // Load dimension index
      File dimensionStore = new File(rootDir, DIMENSION_STORE);
      File[] dimensionIndexFiles = dimensionStore.listFiles(INDEX_FILE_FILTER);
      if (dimensionIndexFiles != null)
      {
        for (File indexFile : dimensionIndexFiles)
        {
          List<Object> entries = readObjectFile(indexFile);
          for (Object o : entries)
          {
            DimensionIndexEntry e = (DimensionIndexEntry) o;
            dimensionIndex.put(e.getNodeId(), e);
          }

          LOG.info("Loaded dimension index {}", indexFile);
        }
      }

      // Load metric index
      File metricStore = new File(rootDir, METRIC_STORE);
      File[] metricIndexFiles = metricStore.listFiles(INDEX_FILE_FILTER);
      if (metricIndexFiles != null)
      {
        for (File indexFile : metricIndexFiles)
        {
          List<Object> entries = readObjectFile(indexFile);
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
          }

          LOG.info("Loaded metric index {}", indexFile);
        }
      }

      // Load dimension buffers and dictionaries
      for (DimensionIndexEntry indexEntry : dimensionIndex.values())
      {
        if (!dimensionSegments.containsKey(indexEntry.getFileId()))
        {
          File bufferFile = new File(dimensionStore, indexEntry.getFileId().toString() + BUFFER_SUFFIX);
          dimensionSegments.put(indexEntry.getFileId(), mapBuffer(bufferFile));
        }

        if (!dictionarySegments.containsKey(indexEntry.getFileId()))
        {
          File bufferFile = new File(dimensionStore, indexEntry.getFileId().toString() + DICT_SUFFIX);
          dictionarySegments.put(indexEntry.getFileId(), mapBuffer(bufferFile));
        }
      }

      // Load metric buffers
      for (List<MetricIndexEntry> indexEntries : metricIndex.values())
      {
        for (MetricIndexEntry indexEntry : indexEntries)
        {
          File bufferFile = new File(metricStore, indexEntry.getFileId().toString() + BUFFER_SUFFIX);
          metricSegments.put(indexEntry.getFileId(), mapBuffer(bufferFile));
        }
      }
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

      ByteBuffer dimensionBuffer = dimensionSegments.get(dimensionIndexEntry.getFileId());
      if (dimensionBuffer == null)
      {
        throw new IllegalStateException("No mapped buffer for file " + dimensionIndexEntry.getFileId() + BUFFER_SUFFIX);
      }

      ByteBuffer dictionaryBuffer = dictionarySegments.get(dimensionIndexEntry.getFileId());
      if (dictionaryBuffer == null)
      {
        throw new IllegalStateException("No mapped buffer for file " + dimensionIndexEntry.getFileId() + DICT_SUFFIX);
      }

      // Dictionary
      dictionaryBuffer.rewind();
      dictionaryBuffer.position(dimensionIndexEntry.getDictionaryStartOffset());
      byte[] dictionaryBytes = new byte[dimensionIndexEntry.getDictionaryLength()];
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

      // Dimensions
      dimensionBuffer.rewind();
      dimensionBuffer.position(dimensionIndexEntry.getBufferStartOffset());
      dimensionBuffer = dimensionBuffer.slice();
      dimensionBuffer.limit(dimensionIndexEntry.getBufferLength());
      DimensionStore dimensionStore = new DimensionStore(starTreeConfig, dimensionBuffer, dictionary);

      // Metrics
      Map<TimeRange, ByteBuffer> metricBuffers = new HashMap<TimeRange, ByteBuffer>();
      for (MetricIndexEntry indexEntry : metricIndexEntries)
      {
        ByteBuffer metricBuffer = metricSegments.get(indexEntry.getFileId());
        if (metricBuffer == null)
        {
          throw new IllegalStateException("No mapped buffer for file " + indexEntry.getFileId() + BUFFER_SUFFIX);
        }

        // Create projection
        metricBuffer.rewind();
        metricBuffer.position(indexEntry.getStartOffset());
        metricBuffer = metricBuffer.slice();
        metricBuffer.limit(indexEntry.getLength());

        metricBuffers.put(indexEntry.getTimeRange(), metricBuffer);
      }
      MetricStore metricStore = new MetricStore(starTreeConfig, metricBuffers);

      return new StarTreeRecordStoreFixedImpl(starTreeConfig, dimensionStore, metricStore);
    }
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
}
