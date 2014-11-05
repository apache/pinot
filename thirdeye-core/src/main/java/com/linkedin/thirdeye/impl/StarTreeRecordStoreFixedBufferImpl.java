package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.StarTreeRecordStore;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class StarTreeRecordStoreFixedBufferImpl implements StarTreeRecordStore
{
  private static Comparator<int[]> DIMENSION_COMBINATION_COMPARATOR = new Comparator<int[]>()
  {
    @Override
    public int compare(int[] a1, int[] a2)
    {
      for (int i = 0; i < a1.length; i++)
      {
        if (a1[i] != a2[i])
        {
          return a1[i] - a2[i];
        }
      }
      return 0;
    }
  };

  private final File file;
  private final List<String> dimensionNames;
  private final List<String> metricNames;
  private final Map<String, Map<String, Integer>> forwardIndex;
  private final Map<String, Map<Integer, String>> reverseIndex;
  private List<Long> timeBuckets;
  private final int entrySize;

  private MappedByteBuffer buffer;
  private int size;

  public StarTreeRecordStoreFixedBufferImpl(File file,
                                            List<String> dimensionNames,
                                            List<String> metricNames,
                                            Map<String, Map<String, Integer>> forwardIndex)
  {
    this.file = file;
    this.dimensionNames = dimensionNames;
    this.metricNames = metricNames;
    this.forwardIndex = forwardIndex;
    this.reverseIndex = new HashMap<String, Map<Integer, String>>();
    this.timeBuckets = new LinkedList<Long>();

    for (Map.Entry<String, Map<String, Integer>> e1 : forwardIndex.entrySet())
    {
      reverseIndex.put(e1.getKey(), new HashMap<Integer, String>());
      for (Map.Entry<String, Integer> e2 : e1.getValue().entrySet())
      {
        reverseIndex.get(e1.getKey()).put(e2.getValue(), e2.getKey());
      }
    }

    this.entrySize = dimensionNames.size() * Integer.SIZE / 8 + (metricNames.size() + 1) * Long.SIZE / 8;
  }

  @Override
  public void update(StarTreeRecord record)
  {
    int[] targetDimensions = new int[dimensionNames.size()];
    for (int i = 0; i < dimensionNames.size(); i++)
    {
      String dimensionName = dimensionNames.get(i);
      String dimensionValue = record.getDimensionValues().get(dimensionName);
      int valueId = forwardIndex.get(dimensionName).get(dimensionValue);
      targetDimensions[i] = valueId;
    }

    int idx = search(buffer, record.getTime(), targetDimensions);

    if (idx < 0)
    {
      throw new IllegalArgumentException("No bucket for record " + record);
    }

    buffer.position(idx);

    // Read / throw away dimension values / time
    buffer.getLong();
    for (String dimensionName : dimensionNames)
    {
      buffer.getInt();
    }

    buffer.mark();

    // Read metric values
    long[] metricValues = new long[metricNames.size()];
    for (int i = 0; i < metricNames.size(); i++)
    {
      metricValues[i] = buffer.getLong() + record.getMetricValues().get(metricNames.get(i));
    }

    buffer.reset();

    // Re-write metric values
    for (long metricValue : metricValues)
    {
      buffer.putLong(metricValue);
    }
  }

  @Override
  public Iterator<StarTreeRecord> iterator()
  {
    List<StarTreeRecord> records = new ArrayList<StarTreeRecord>();

    buffer.rewind();

    while (buffer.position() < buffer.limit())
    {
      StarTreeRecordImpl.Builder builder = new StarTreeRecordImpl.Builder();
      builder.setTime(buffer.getLong());

      for (String dimensionName : dimensionNames)
      {
        Map<Integer, String> valueIds = reverseIndex.get(dimensionName);
        int valueId = buffer.getInt();
        String value = valueIds.get(valueId);
        builder.setDimensionValue(dimensionName, value);
      }

      for (String metricName : metricNames)
      {
        long metricValue = buffer.getLong();
        builder.setMetricValue(metricName, metricValue);
      }

      records.add(builder.build());
    }

    return records.iterator();
  }

  @Override
  public void clear()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void open() throws IOException
  {
    FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel();
    buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, file.length());
    buffer.rewind();

    // Check buffer sort order
    int[] lastDimensions = new int[dimensionNames.size()];
    int[] currentDimensions = new int[dimensionNames.size()];
    Long lastTime = null;
    Long currentTime = null;
    while (buffer.position() < buffer.limit())
    {
      // Get dimensions / time
      currentTime = buffer.getLong();
      for (int i = 0; i < dimensionNames.size(); i++)
      {
        currentDimensions[i] = buffer.getInt();
      }

      // Check ordering
      if (lastTime != null) // i.e. not first
      {
        int compareResult = !lastTime.equals(currentTime)
                ? (int) (lastTime - currentTime)
                : DIMENSION_COMBINATION_COMPARATOR.compare(lastDimensions, currentDimensions);

        if (compareResult >= 0)
        {
          throw new IllegalStateException(
                  "Buffer is not sorted by time then dimensions: " +
                          "last=" + lastTime + ":" + Arrays.toString(lastDimensions) +
                          "; current=" + currentTime + ":" + Arrays.toString(currentDimensions));
        }
      }

      // Read through the metrics to position at next record
      for (String metricName : metricNames)
      {
        buffer.getLong();
      }

      // Add time bucket to list of all
      if (lastTime == null || lastTime < currentTime)
      {
        timeBuckets.add(currentTime);
      }

      // Reset last time
      System.arraycopy(currentDimensions, 0, lastDimensions, 0, currentDimensions.length);
      lastTime = currentTime;

      // Update size
      size++;
    }
  }

  @Override
  public void close() throws IOException
  {
    buffer.force();
  }

  @Override
  public int size()
  {
    return size;
  }

  @Override
  public int getCardinality(String dimensionName)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getMaxCardinalityDimension()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getMaxCardinalityDimension(Collection<String> blacklist)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<String> getDimensionValues(String dimensionName)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public long[] getMetricSums(StarTreeQuery query)
  {
    long[] sums = new long[metricNames.size()];

    int[] targetDimensions = new int[dimensionNames.size()];
    for (int i = 0; i < dimensionNames.size(); i++)
    {
      String dimensionName = dimensionNames.get(i);
      String dimensionValue = query.getDimensionValues().get(dimensionName);
      int valueId = forwardIndex.get(dimensionName).get(dimensionValue);
      targetDimensions[i] = valueId;
    }

    if (query.getTimeBuckets() != null)
    {
      for (Long bucket : query.getTimeBuckets())
      {
        int idx = search(buffer, bucket, targetDimensions);
        if (idx >= 0)
        {
          buffer.position(idx);
          updateSums(buffer, sums);
        }
      }
    }
    else if (query.getTimeRange() != null)
    {
      for (long bucket = query.getTimeRange().getKey(); bucket <= query.getTimeRange().getValue(); bucket++)
      {
        int idx = search(buffer, bucket, targetDimensions);
        if (idx >= 0)
        {
          buffer.position(idx);
          updateSums(buffer, sums);
        }
      }
    }
    else // Everything
    {
      for (long bucket : timeBuckets)
      {
        int idx = search(buffer, bucket, targetDimensions);
        if (idx >= 0)
        {
          buffer.position(idx);
          updateSums(buffer, sums);
        }
      }
    }

    return sums;
  }

  private void updateSums(ByteBuffer buffer, long[] sums)
  {
    buffer.getLong();

    for (String dimensionName : dimensionNames)
    {
      buffer.getInt();
    }

    for (int i = 0; i < metricNames.size(); i++)
    {
      sums[i] += buffer.getLong();
    }
  }

  private int search(ByteBuffer buffer, long targetTime, int[] targetDimensions)
  {
    buffer.rewind();

    int[] currentDimensions = new int[dimensionNames.size()];

    while (buffer.position() < buffer.limit())
    {
      int idx = ((buffer.limit() + buffer.position()) / entrySize / 2) * entrySize;

      buffer.mark();
      buffer.position(idx);

      long currentTime = buffer.getLong();
      for (int i = 0; i < currentDimensions.length; i++)
      {
        currentDimensions[i] = buffer.getInt();
      }

      buffer.reset();

      int compareResult = targetTime != currentTime
              ? (int) (targetTime - currentTime)
              : DIMENSION_COMBINATION_COMPARATOR.compare(targetDimensions, currentDimensions);

      if (compareResult == 0)
      {
        return idx;
      }
      else if (compareResult < 0)
      {
        buffer.limit(idx);
      }
      else
      {
        buffer.position(idx + entrySize);
      }
    }

    return -1;
  }
}
