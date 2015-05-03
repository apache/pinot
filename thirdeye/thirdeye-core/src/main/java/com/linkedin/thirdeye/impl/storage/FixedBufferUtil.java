package com.linkedin.thirdeye.impl.storage;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.TimeRange;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;

public class FixedBufferUtil
{
  private static final Logger LOG = LoggerFactory.getLogger(FixedBufferUtil.class);

  /**
   * Creates dictionary, dimension, metric files for an individual leaf in following structure:
   *
   *  dataDir/
   *    dimensionStore/
   *      nodeId
   *    metricStore/
   *      nodeId
   *    dictStore/
   *      nodeId
   */
  public static void createLeafBufferFiles(File outputDir,
                                           String nodeId,
                                           StarTreeConfig config,
                                           Map<DimensionKey, MetricTimeSeries> records,
                                           DimensionDictionary dictionary) throws IOException

  {
    // Dictionary
    File dictDir = new File(outputDir, StarTreeConstants.DICT_STORE);
    FileUtils.forceMkdir(dictDir);
    File dictFile = new File(dictDir, nodeId);
    FileOutputStream fos = new FileOutputStream(dictFile);
    ObjectOutputStream oos = new ObjectOutputStream(fos);
    oos.writeObject(dictionary);
    oos.flush();
    oos.close();

    // Sort dimensions
    List<DimensionKey> dimensionKeys = new ArrayList<DimensionKey>(records.keySet());
    Collections.sort(dimensionKeys);

    // Dimensions
    int dimensionBufferSize = dimensionKeys.size() * config.getDimensions().size() * Integer.SIZE / 8;
    File dimensionDir = new File(outputDir, StarTreeConstants.DIMENSION_STORE);
    File dimensionFile = new File(dimensionDir, nodeId);
    if (LOG.isDebugEnabled())
    {
      LOG.debug("Dimension buffer for node {}: bytes={}, numKeys={}, numDimensions={}, file={}",
               nodeId, dimensionBufferSize, dimensionKeys.size(), config.getDimensions().size(), dimensionFile);
    }
    ByteBuffer dimensionBuffer = ByteBuffer.allocate(dimensionBufferSize);
    for (DimensionKey dimensionKey : dimensionKeys)
    {
      StorageUtils.addToDimensionStore(config, dimensionBuffer, dimensionKey, dictionary);
    }
    dimensionBuffer.flip();
    FileUtils.forceMkdir(dimensionDir);
    FileChannel dimensionFileChannel = new RandomAccessFile(dimensionFile, "rw").getChannel();
    dimensionFileChannel.write(dimensionBuffer);
    dimensionFileChannel.close();

    // Metrics

    long minTime = -1;
    long maxTime = -1;
    for (DimensionKey dimensionKey : dimensionKeys)
    {
      MetricTimeSeries timeSeries = records.get(dimensionKey);
      for (Long time : timeSeries.getTimeWindowSet())
      {
        if (minTime == -1 || time < minTime)
        {
          minTime = time;
        }

        if (maxTime == -1 || time > maxTime)
        {
          maxTime = time;
        }
      }
    }

    TimeRange timeRange = new TimeRange(minTime, maxTime);

    // Metrics

    ByteBuffer metricBuffer;
    if (minTime == -1 || maxTime == -1)
    {
      metricBuffer = ByteBuffer.allocate(0);
    }
    else
    {
      int metricSize = MetricSchema.fromMetricSpecs(config.getMetrics()).getRowSizeInBytes();
      metricBuffer = ByteBuffer.allocate(dimensionKeys.size() * timeRange.totalBuckets() * (Long.SIZE / 8 + metricSize));

      for (DimensionKey dimensionKey : dimensionKeys)
      {
        MetricTimeSeries timeSeries = records.get(dimensionKey);

        // Ensure that each time bucket is represented
        for (long i = minTime; i <= maxTime; i++)
        {
          for (MetricSpec metricSpec : config.getMetrics())
          {
            timeSeries.increment(i, metricSpec.getName(), 0);
          }
        }

        StorageUtils.addToMetricStore(config, metricBuffer, timeSeries);
      }
    }

    metricBuffer.flip();
    File metricDir = new File(outputDir, StarTreeConstants.METRIC_STORE);
    FileUtils.forceMkdir(metricDir);
    FileChannel metricFile = new RandomAccessFile(
            new File(metricDir, nodeId + "_" + timeRange.getStart() + ":" + timeRange.getEnd()), "rw").getChannel();
    metricFile.write(metricBuffer);
    metricFile.close();
  }

  public static void combineDataFiles(InputStream starTree, File inputDir, File outputDir) throws IOException
  {
    UUID fileId = UUID.randomUUID();

    File dimensionStore = new File(outputDir, StarTreeConstants.DIMENSION_STORE);
    File metricStore = new File(outputDir, StarTreeConstants.METRIC_STORE);

    if (!outputDir.exists()) {
      FileUtils.forceMkdir(outputDir);
    }
    FileUtils.forceMkdir(dimensionStore);
    FileUtils.forceMkdir(metricStore);

    // Tree
    File starTreeFile = new File(outputDir, StarTreeConstants.TREE_FILE_NAME);
    FileUtils.copyInputStreamToFile(starTree, starTreeFile);

    // Dictionaries
    File combinedDictionaryFile = new File(dimensionStore, fileId + StarTreeConstants.DICT_FILE_SUFFIX);
    Map<UUID, List<Long>> dictionaryMetadata = combineFiles(new File(inputDir, StarTreeConstants.DICT_STORE), combinedDictionaryFile, false);

    // Dimensions
    File combinedDimensionFile = new File(dimensionStore, fileId + StarTreeConstants.BUFFER_FILE_SUFFIX);
    Map<UUID, List<Long>> dimensionMetadata = combineFiles(new File(inputDir, StarTreeConstants.DIMENSION_STORE), combinedDimensionFile, false);

    // Metrics
    File combinedMetricFile = new File(metricStore, fileId + StarTreeConstants.BUFFER_FILE_SUFFIX);
    Map<UUID, List<Long>> metricMetadata = combineFiles(new File(inputDir, StarTreeConstants.METRIC_STORE), combinedMetricFile, true);

    // Dimension index
    List<DimensionIndexEntry> dimensionIndexEntries = new ArrayList<DimensionIndexEntry>();
    for (Map.Entry<UUID, List<Long>> entry : dimensionMetadata.entrySet())
    {
      UUID nodeId = entry.getKey();

      List<Long> dictionaryPosition = dictionaryMetadata.get(nodeId);
      if (dictionaryPosition == null)
      {
        throw new IllegalStateException("No dictionary for node " + nodeId);
      }

      int dictionaryStartOffset = dictionaryPosition.get(0).intValue();
      int dictionaryLength = dictionaryPosition.get(1).intValue();
      int bufferStartOffset = entry.getValue().get(0).intValue();
      int bufferLength = entry.getValue().get(1).intValue();

      dimensionIndexEntries.add(new DimensionIndexEntry(
              nodeId, fileId, dictionaryStartOffset, dictionaryLength, bufferStartOffset, bufferLength));
    }

    File dimensionIndexFile = new File(dimensionStore, fileId + StarTreeConstants.INDEX_FILE_SUFFIX);
    writeObjects(dimensionIndexEntries, dimensionIndexFile);

    // Metric index
    List<MetricIndexEntry> metricIndexEntries = new ArrayList<MetricIndexEntry>();
    for (Map.Entry<UUID, List<Long>> entry : metricMetadata.entrySet())
    {
      UUID nodeId = entry.getKey();
      int startOffset = entry.getValue().get(0).intValue();
      int length = entry.getValue().get(1).intValue();
      long minTime = entry.getValue().get(2);
      long maxTime = entry.getValue().get(3);
      metricIndexEntries.add(new MetricIndexEntry(nodeId, fileId, startOffset, length, new TimeRange(minTime, maxTime)));
    }

    if (!metricIndexEntries.isEmpty())
    {
      File metricIndexFile = new File(metricStore, fileId + StarTreeConstants.INDEX_FILE_SUFFIX);
      writeObjects(metricIndexEntries, metricIndexFile);
    }
  }

  private static Map<UUID, List<Long>> combineFiles(File inputDir, File outputFile, boolean hasTime) throws IOException
  {
    Map<UUID, List<Long>> metadata = new HashMap<UUID, List<Long>>();

    File[] files = inputDir.listFiles();

    if (files != null)
    {
      int bufferSize = 0;
      for (File file : files)
      {
        bufferSize += file.length();
      }

      ByteBuffer buffer = ByteBuffer.allocate(bufferSize);

      for (File file : files)
      {
        List<Long> nodeMetadata = new ArrayList<Long>(4);

        byte[] bytes = FileUtils.readFileToByteArray(file);

        nodeMetadata.add((long) buffer.position());
        nodeMetadata.add((long) bytes.length);

        UUID nodeId;
        if (hasTime)
        {
          String[] tokens = file.getName().split("_");
          nodeId = UUID.fromString(tokens[0]);
          String[] timeTokens = tokens[1].split(":");
          nodeMetadata.add(Long.valueOf(timeTokens[0]));
          nodeMetadata.add(Long.valueOf(timeTokens[1]));
        }
        else
        {
          nodeId = UUID.fromString(file.getName());
        }

        metadata.put(nodeId, nodeMetadata);

        buffer.put(bytes);
      }

      buffer.flip();

      FileChannel fileChannel = new RandomAccessFile(outputFile, "rw").getChannel();
      fileChannel.write(buffer);
      fileChannel.close();
    }

    return metadata;
  }

  private static void writeObjects(List objects, File outputFile) throws IOException
  {
    FileOutputStream fos = new FileOutputStream(outputFile);
    ObjectOutputStream oos = new ObjectOutputStream(fos);

    for (Object o : objects)
    {
      oos.writeObject(o);
    }

    oos.flush();
    oos.close();
  }

  public static void writeMetadata(IndexMetadata indexMetadata, File outputDir) throws IOException
  {
    File indexMetadataFile =
        new File(outputDir, StarTreeConstants.METADATA_FILE_NAME);

    List<IndexMetadata> indexMetadataList = new ArrayList<IndexMetadata>();
    indexMetadataList.add(indexMetadata);

    writeObjects(indexMetadataList, indexMetadataFile);

  }
}
