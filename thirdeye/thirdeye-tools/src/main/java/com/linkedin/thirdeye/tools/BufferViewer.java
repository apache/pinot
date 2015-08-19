package com.linkedin.thirdeye.tools;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.TimeRange;
import com.linkedin.thirdeye.impl.NumberUtils;
import com.linkedin.thirdeye.impl.storage.DimensionDictionary;
import com.linkedin.thirdeye.impl.storage.DimensionIndexEntry;
import com.linkedin.thirdeye.impl.storage.DimensionStore;
import com.linkedin.thirdeye.impl.storage.DimensionStoreImmutableImpl;
import com.linkedin.thirdeye.impl.storage.MetricIndexEntry;
import com.linkedin.thirdeye.impl.storage.MetricStore;
import com.linkedin.thirdeye.impl.storage.MetricStoreImmutableImpl;
import com.linkedin.thirdeye.impl.storage.StorageUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class BufferViewer
{
  public static void main(String[] args) throws Exception
  {
    Options options = new Options();
    options.addOption("excludeZeroValues", false, "Should exclude rows for which all metric values are zero");
    CommandLine commandLine = new GnuParser().parse(options, args);
    args = commandLine.getArgs();

    boolean excludeZeroValues = commandLine.hasOption("excludeZeroValues");

    if (args.length != 2)
    {
      throw new IllegalArgumentException("usage: collectionDir nodeId");
    }

    UUID nodeId = UUID.fromString(args[1]);

    // Read dimension index

    File dataDir = new File(args[0], StarTreeConstants.DATA_DIR_PREFIX);

    File dimensionStoreDir = new File(dataDir, StarTreeConstants.DIMENSION_STORE);

    File[] dimensionIndexFiles = dimensionStoreDir.listFiles(new FilenameFilter()
    {
      @Override
      public boolean accept(File dir, String name)
      {
        return name.endsWith(StarTreeConstants.INDEX_FILE_SUFFIX);
      }
    });

    if (dimensionIndexFiles == null)
    {
      throw new IllegalStateException("No index files in " + dimensionStoreDir);
    }

    List<DimensionIndexEntry> nodeDimensionIndexEntries = new ArrayList<DimensionIndexEntry>();
    for (File dimensionIndexFile : dimensionIndexFiles)
    {
      List<DimensionIndexEntry> dimensionIndexEntries = StorageUtils.readDimensionIndex(dimensionIndexFile);

      for (DimensionIndexEntry indexEntry : dimensionIndexEntries)
      {
        if (indexEntry.getNodeId().equals(nodeId))
        {
          nodeDimensionIndexEntries.add(indexEntry);
        }
      }
    }

    if (nodeDimensionIndexEntries.size() != 1)
    {
      throw new IllegalStateException("There must be exactly one dimension index entry per node. " +
                                          "There are " + nodeDimensionIndexEntries.size());
    }

    DimensionIndexEntry nodeDimensionIndexEntry = nodeDimensionIndexEntries.get(0);

    // Read star tree config
    StarTreeConfig starTreeConfig = StarTreeConfig.decode(
        new FileInputStream(new File(args[0], StarTreeConstants.CONFIG_FILE_NAME)));

    // Read dictionary
    DimensionDictionary dimensionDictionary = getDictionary(
        nodeDimensionIndexEntry, new File(dimensionStoreDir, nodeDimensionIndexEntry.getFileId().toString() + StarTreeConstants.DICT_FILE_SUFFIX));

    // Read dimension buffers
    ByteBuffer dimensionBuffer = getDimensionBuffer(
        nodeDimensionIndexEntry, new File(dimensionStoreDir, nodeDimensionIndexEntry.getFileId().toString() + StarTreeConstants.BUFFER_FILE_SUFFIX));
    DimensionStore dimensionStore
        = new DimensionStoreImmutableImpl(starTreeConfig, dimensionBuffer, dimensionDictionary);

    // Read metric index

    File metricStoreDir = new File(dataDir, StarTreeConstants.METRIC_STORE);

    File[] metricIndexFiles = metricStoreDir.listFiles(new FilenameFilter()
    {
      @Override
      public boolean accept(File dir, String name)
      {
        return name.endsWith(StarTreeConstants.INDEX_FILE_SUFFIX);
      }
    });

    if (metricIndexFiles == null)
    {
      throw new IllegalStateException("No index files in " + metricStoreDir);
    }

    List<MetricIndexEntry> nodeMetricIndexEntries = new ArrayList<MetricIndexEntry>();
    Set<UUID> fileIds = new HashSet<UUID>();
    for (File metricIndexFile : metricIndexFiles)
    {
      List<MetricIndexEntry> metricIndexEntries = StorageUtils.readMetricIndex(metricIndexFile);

      for (MetricIndexEntry metricIndexEntry : metricIndexEntries)
      {
        if (metricIndexEntry.getNodeId().equals(nodeId))
        {
          nodeMetricIndexEntries.add(metricIndexEntry);
          fileIds.add(metricIndexEntry.getFileId());
        }
      }
    }

    if (nodeMetricIndexEntries.isEmpty())
    {
      throw new IllegalStateException("No metric index entries for " + nodeId);
    }

    // Read metric buffer files

    Map<UUID, ByteBuffer> metricBuffers = new HashMap<UUID, ByteBuffer>();

    for (UUID fileId : fileIds)
    {
      File bufferFile = new File(metricStoreDir, fileId + StarTreeConstants.BUFFER_FILE_SUFFIX);
      metricBuffers.put(fileId, mapBuffer(bufferFile));
    }

    ConcurrentMap<TimeRange, List<ByteBuffer>> projectedBuffers = new ConcurrentHashMap<TimeRange, List<ByteBuffer>>();

    for (MetricIndexEntry indexEntry : nodeMetricIndexEntries)
    {
      ByteBuffer metricBuffer = getMetricBuffer(indexEntry, metricBuffers);

      List<ByteBuffer> buffers = projectedBuffers.get(indexEntry.getTimeRange());
      if (buffers == null)
      {
        buffers = new ArrayList<ByteBuffer>();
        projectedBuffers.put(indexEntry.getTimeRange(), buffers);
      }

      buffers.add(metricBuffer);
    }

    MetricStore metricStore = new MetricStoreImmutableImpl(starTreeConfig, projectedBuffers);

    // Dump buffer

    for (DimensionKey dimensionKey : dimensionStore.getDimensionKeys())
    {
      MetricTimeSeries timeSeries
          = metricStore.getTimeSeries(new ArrayList(dimensionStore.findMatchingKeys(dimensionKey).values()), null);

      List<Long> times = new ArrayList<Long>(timeSeries.getTimeWindowSet());
      Collections.sort(times);

      Number[] current = new Number[starTreeConfig.getMetrics().size()];
      for (Long time : times)
      {
        boolean nonZero = false;
        for (int i = 0; i < starTreeConfig.getMetrics().size(); i++)
        {
          current[i] = timeSeries.get(time, starTreeConfig.getMetrics().get(i).getName());
          if (!NumberUtils.isZero(current[i], starTreeConfig.getMetrics().get(i).getType()))
          {
            nonZero = true;
          }
        }

        if (!excludeZeroValues || nonZero)
        {
          System.out.print(dimensionKey + "\t@" + time);
          for (Number value : current)
          {
            System.out.print("\t" + value);
          }
          System.out.println();
        }
      }
    }
  }

  private static ByteBuffer mapBuffer(File bufferFile) throws IOException
  {
    FileChannel channel = new RandomAccessFile(bufferFile, "r").getChannel();
    ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, bufferFile.length());
    buffer.order(ByteOrder.BIG_ENDIAN);
    return buffer;
  }

  private static DimensionDictionary getDictionary(DimensionIndexEntry indexEntry, File dictionaryFile) throws IOException
  {
    ByteBuffer dictionaryBuffer = mapBuffer(dictionaryFile);

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

  private static ByteBuffer getDimensionBuffer(DimensionIndexEntry indexEntry, File bufferFile) throws IOException
  {
    ByteBuffer dimensionBuffer = mapBuffer(bufferFile);

    dimensionBuffer.rewind();
    dimensionBuffer.position(indexEntry.getBufferStartOffset());

    ByteBuffer slicedBuffer = dimensionBuffer.slice();
    slicedBuffer.limit(indexEntry.getBufferLength());

    return slicedBuffer;
  }

  private static ByteBuffer getMetricBuffer(MetricIndexEntry indexEntry, Map<UUID, ByteBuffer> metricBuffers) throws IOException
  {
    ByteBuffer metricBuffer = metricBuffers.get(indexEntry.getFileId());
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
}
