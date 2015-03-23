package com.linkedin.thirdeye.impl.storage;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.impl.NumberUtils;
import org.apache.commons.io.input.CountingInputStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
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
}
