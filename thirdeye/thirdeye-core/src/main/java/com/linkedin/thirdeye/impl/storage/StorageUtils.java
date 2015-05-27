package com.linkedin.thirdeye.impl.storage;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.impl.NumberUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.input.CountingInputStream;
import org.joda.time.DateTime;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class StorageUtils
{
  /** Adds a dimension combination to the end of a dimension store */
  public static void addToDimensionStore(StarTreeConfig config,
                                         ByteBuffer buffer,
                                         DimensionKey dimensionKey,
                                         DimensionDictionary dictionary)
  {
    for (int i = 0; i < config.getDimensions().size(); i++)
    {
      String dimensionName = config.getDimensions().get(i).getName();
      String dimensionValue = dimensionKey.getDimensionValues()[i];
      Integer valueId = dictionary.getValueId(dimensionName, dimensionValue);
      buffer.putInt(valueId);
    }
  }

  /** Adds a metric time series to the end of a metric store */
  public static void addToMetricStore(StarTreeConfig config,
                                      ByteBuffer buffer,
                                      MetricTimeSeries timeSeries)
  {
    try
    {
      List<Long> times = new ArrayList<Long>(timeSeries.getTimeWindowSet());
      Collections.sort(times);
      for (Long time : times)
      {
        buffer.putLong(time);
        for (MetricSpec metricSpec : config.getMetrics())
        {
          Number value = timeSeries.get(time, metricSpec.getName());
          NumberUtils.addToBuffer(buffer, value, metricSpec.getType());
        }
      }
    }
    catch (Exception e)
    {
      throw new IllegalStateException("Buffer: " + buffer, e);
    }
  }

  public static String getDataDirName(String treeId, String schedule, DateTime minTime, DateTime maxTime)
  {
    return StarTreeConstants.DATA_DIR_PREFIX
        + "_" + schedule
        + "_" + StarTreeConstants.DATE_TIME_FORMATTER.print(minTime)
        + "_" + (maxTime == null ? "LATEST" : StarTreeConstants.DATE_TIME_FORMATTER.print(maxTime))
        + "_" + treeId;
  }

  public static String getDataDirPrefix()
  {
    return StarTreeConstants.DATA_DIR_PREFIX;
  }

  public static String getDataDirPrefix(String schedule, DateTime minTime, DateTime maxTime)
  {
    return StarTreeConstants.DATA_DIR_PREFIX
        + "_" + schedule
        + "_" + StarTreeConstants.DATE_TIME_FORMATTER.print(minTime)
        + "_" + (maxTime == null ? "LATEST" : StarTreeConstants.DATE_TIME_FORMATTER.print(maxTime));
  }

  public static void prefixFilesWithTime(File dir,
                                         String schedule,
                                         DateTime minTime,
                                         DateTime maxTime) throws IOException
  {
    File[] files = dir.listFiles();

    if (files != null)
    {
      for (File file : files)
      {
        String minTimeComponent = StarTreeConstants.DATE_TIME_FORMATTER.print(minTime);
        String maxTimeComponent = StarTreeConstants.DATE_TIME_FORMATTER.print(maxTime);
        File renamed = new File(
                file.getParent(), schedule + "_" + minTimeComponent + "_" + maxTimeComponent + "_" + file.getName());
        FileUtils.moveFile(file, renamed);
      }
    }
  }

  public static File findLatestDataDir(File collectionDir)
  {
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
      return null;
    }

    Arrays.sort(dataDirs, new Comparator<File>()
    {
      @Override
      public int compare(File f1, File f2)
      {
        String[] f1Tokens = f1.getName().split("_");
        String[] f2Tokens = f2.getName().split("_");

        if ("LATEST".equals(f1Tokens[3]))
        {
          return -1;
        }
        else if ("LATEST".equals(f2Tokens[3]))
        {
          return 1;
        }

        DateTime f1MaxTime = StarTreeConstants.DATE_TIME_FORMATTER.parseDateTime(f1Tokens[3]);
        DateTime f2MaxTime = StarTreeConstants.DATE_TIME_FORMATTER.parseDateTime(f2Tokens[3]);

        return (int) (f1MaxTime.getMillis() - f2MaxTime.getMillis());
      }
    });

    return dataDirs[dataDirs.length - 1];
  }

  public static void moveAllFiles(File srcDataDir, File dstDataDir) throws IOException
  {
    // Tree
    File srcTreeFile = new File(srcDataDir, StarTreeConstants.TREE_FILE_NAME);
    File dstTreeFile = new File(dstDataDir, StarTreeConstants.TREE_FILE_NAME);
    if (!dstTreeFile.exists())
    {
      FileUtils.moveFile(srcTreeFile, dstTreeFile);
    }

    // Metadata
    File srcMetadataFile = new File(srcDataDir, StarTreeConstants.METADATA_FILE_NAME);
    File dstMetadataFile = new File(dstDataDir, StarTreeConstants.METADATA_FILE_NAME);
    if (!dstMetadataFile.exists())
    {
      FileUtils.moveFile(srcMetadataFile, dstMetadataFile);
    }

    // Config
    File srcConfigFile = new File(srcDataDir, StarTreeConstants.CONFIG_FILE_NAME);
    File dstConfigFile = new File(dstDataDir, StarTreeConstants.CONFIG_FILE_NAME);
    if (!dstConfigFile.exists())
    {
      FileUtils.moveFile(srcConfigFile, dstConfigFile);
    }

    // Dimensions
    File[] dimensionFiles = new File(srcDataDir, StarTreeConstants.DIMENSION_STORE).listFiles();
    File dstDimensionStore = new File(dstDataDir, StarTreeConstants.DIMENSION_STORE);
    if (dimensionFiles != null)
    {
      for (File file : dimensionFiles)
      {
        FileUtils.moveFile(file, new File(dstDimensionStore, file.getName()));
      }
    }

    // Metrics
    File[] metricFiles = new File(srcDataDir, StarTreeConstants.METRIC_STORE).listFiles();
    File dstMetricStore = new File(dstDataDir, StarTreeConstants.METRIC_STORE);
    if (metricFiles != null)
    {
      for (File file : metricFiles)
      {
        FileUtils.moveFile(file, new File(dstMetricStore, file.getName()));
      }
    }
  }

  public static List<MetricIndexEntry> readMetricIndex(File indexFile) throws IOException
  {
    List<Object> objects = readObjectFile(indexFile);

    List<MetricIndexEntry> indexEntries = new ArrayList<MetricIndexEntry>(objects.size());

    for (Object o : objects)
    {
      indexEntries.add((MetricIndexEntry) o);
    }

    return indexEntries;
  }

  public static List<DimensionIndexEntry> readDimensionIndex(File indexFile) throws IOException
  {
    List<Object> objects = readObjectFile(indexFile);

    List<DimensionIndexEntry> indexEntries = new ArrayList<DimensionIndexEntry>(objects.size());

    for (Object o : objects)
    {
      indexEntries.add((DimensionIndexEntry) o);
    }

    return indexEntries;
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

  /** @return true if file was not modified in sleepMillis before timeoutMillis */
  public static boolean waitForModifications(File file, long sleepMillis, long timeoutMillis)
      throws InterruptedException
  {
    long startTimeMillis = System.currentTimeMillis();
    long lastModified = file.lastModified();

    do
    {
      Thread.sleep(sleepMillis);

      long currentLastModified = file.lastModified();
      if (lastModified == currentLastModified)
      {
        return true;
      }

      lastModified = currentLastModified;
    }
    while (System.currentTimeMillis() - startTimeMillis < timeoutMillis);

    return false;
  }
}
