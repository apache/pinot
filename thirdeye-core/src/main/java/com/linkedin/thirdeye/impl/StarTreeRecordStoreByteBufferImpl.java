package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.StarTreeRecordStore;
import com.linkedin.thirdeye.api.StarTreeRecordThresholdFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class StarTreeRecordStoreByteBufferImpl implements StarTreeRecordStore
{
  private static final Logger LOG = LoggerFactory.getLogger(StarTreeRecordStoreByteBufferImpl.class);
  private static final int STAR_VALUE = 0;

  private final UUID nodeId;
  private final List<String> dimensionNames;
  private final List<String> metricNames;
  private final int bufferSize;
  private final boolean useDirect;
  private final double targetCompressionRatio;
  private final List<ByteBuffer> buffers;
  private final AtomicInteger nextValueId;
  private final AtomicInteger size;
  private final int entrySize;
  private final Object sync;

  private final Map<String, Map<String, Integer>> forwardDimensionValueIndex;
  private final Map<Integer, String> reverseDimensionValueIndex;

  public StarTreeRecordStoreByteBufferImpl(UUID nodeId,
                                           List<String> dimensionNames,
                                           List<String> metricNames,
                                           int bufferSize,
                                           boolean useDirect,
                                           double targetCompressionRatio)
  {
    this.nodeId = nodeId;
    this.dimensionNames = dimensionNames;
    this.metricNames = metricNames;
    this.bufferSize = bufferSize;
    this.useDirect = useDirect;
    this.targetCompressionRatio = targetCompressionRatio;
    this.buffers = new ArrayList<ByteBuffer>();
    this.nextValueId = new AtomicInteger(1);
    this.sync = new Object();
    this.size = new AtomicInteger(0);

    this.entrySize =
            dimensionNames.size() * (Integer.SIZE / 8) +
                    metricNames.size() * (Long.SIZE / 8) +
                    Long.SIZE / 8; // time

    this.forwardDimensionValueIndex = new HashMap<String, Map<String, Integer>>();
    this.reverseDimensionValueIndex = new HashMap<Integer, String>();
    this.reverseDimensionValueIndex.put(STAR_VALUE, StarTreeConstants.STAR);
  }

  @Override
  public void update(StarTreeRecord record)
  {
    synchronized (sync)
    {
      ByteBuffer buffer = getBuffer();
      buffer.position(buffer.limit());
      buffer.limit(buffer.position() + entrySize);
      putRecord(buffer, record);
      size.incrementAndGet();
    }
  }

  @Override
  public Iterator<StarTreeRecord> iterator()
  {
    synchronized (sync)
    {
      List<StarTreeRecord> records = new LinkedList<StarTreeRecord>();
      for (ByteBuffer buffer : buffers)
      {
        buffer.rewind();
        while (buffer.position() < buffer.limit())
        {
          records.add(getRecord(buffer));
        }
      }
      return records.iterator();
    }
  }

  @Override
  public void clear()
  {
    synchronized (sync)
    {
      buffers.clear();
      forwardDimensionValueIndex.clear();
      reverseDimensionValueIndex.clear();
      nextValueId.set(1);
      size.set(0);
    }
  }

  @Override
  public void open() throws IOException
  {
    // Do nothing
  }

  @Override
  public void close() throws IOException
  {
    // Do nothing
  }

  @Override
  public int size()
  {
    return size.get();
  }

  @Override
  public int getCardinality(String dimensionName)
  {
    synchronized (sync)
    {
      Map<String, Integer> valueIds = forwardDimensionValueIndex.get(dimensionName);
      if (valueIds == null)
      {
        return 0;
      }
      return valueIds.size();
    }
  }

  @Override
  public String getMaxCardinalityDimension()
  {
    return getMaxCardinalityDimension(null);
  }

  @Override
  public String getMaxCardinalityDimension(Collection<String> blacklist)
  {
    synchronized (sync)
    {
      String maxDimension = null;
      int maxCardinality = 0;

      for (String dimensionName : dimensionNames)
      {
        int cardinality = getCardinality(dimensionName);
        if (cardinality > maxCardinality && (blacklist == null || !blacklist.contains(dimensionName)))
        {
          maxCardinality = cardinality;
          maxDimension = dimensionName;
        }
      }

      return maxDimension;
    }
  }

  @Override
  public Set<String> getDimensionValues(String dimensionName)
  {
    synchronized (sync)
    {
      Map<String, Integer> valueIds = forwardDimensionValueIndex.get(dimensionName);
      return valueIds == null ? null : valueIds.keySet();
    }
  }

  @Override
  public long[] getMetricSums(StarTreeQuery query, StarTreeRecordThresholdFunction thresholdFunction)
  {
    synchronized (sync)
    {
      long[] sums = new long[metricNames.size()];

      for (ByteBuffer buffer : buffers)
      {
        buffer.rewind();
        while (buffer.position() < buffer.limit())
        {
          boolean matches = true;

          // Dimensions
          for (String dimensionName : dimensionNames)
          {
            int valueId = buffer.getInt();
            String recordValue = reverseDimensionValueIndex.get(valueId);
            String queryValue = query.getDimensionValues().get(dimensionName);

            if (!StarTreeConstants.STAR.equals(queryValue) && !queryValue.equals(recordValue))
            {
              matches = false;
            }
          }

          // Check time
          long time = buffer.getLong();
          if (query.getTimeBuckets() != null && !query.getTimeBuckets().contains(time))
          {
            matches = false;
          }
          else if (query.getTimeRange() != null
                  && (time < query.getTimeRange().getKey() || time > query.getTimeRange().getValue()))
          {
            matches = false;
          }

          // Aggregate while advancing cursor
          for (int i = 0; i < metricNames.size(); i++)
          {
            long value = buffer.getLong();
            if (matches)
            {
              sums[i] += value;
            }
          }
        }
      }

      return sums;
    }
  }

  private ByteBuffer createBuffer()
  {
    ByteBuffer buffer;
    if (useDirect)
    {
      buffer = ByteBuffer.allocateDirect(bufferSize);
    }
    else
    {
      buffer = ByteBuffer.allocate(bufferSize);
    }
    buffer.limit(0);
    return buffer;
  }

  private ByteBuffer getBuffer()
  {
    if (buffers.isEmpty())
    {
      buffers.add(createBuffer());
    }

    ByteBuffer buffer = buffers.get(buffers.size() - 1);

    if (buffer.limit() + entrySize > buffer.capacity())
    {
      int oldLimit = buffer.limit();
      compressBuffer(buffer);
      int newLimit = buffer.limit();
      double compressionRatio = (1.0 * newLimit) / oldLimit;

      LOG.info(String.format("Compressed buffer to %.02f (oldLimit=%d, newLimit=%d)", compressionRatio, oldLimit, newLimit));

      if (compressionRatio > targetCompressionRatio)
      {
        buffer = createBuffer();
        buffers.add(buffer);
        LOG.info(String.format("Created new buffer (total=%d)", buffers.size()));
      }
    }

    return buffer;
  }

  private void putRecord(ByteBuffer buffer, StarTreeRecord record)
  {
    for (String dimensionName : dimensionNames)
    {
      String dimensionValue = record.getDimensionValues().get(dimensionName);

      if (StarTreeConstants.STAR.equals(dimensionValue))
      {
        buffer.putInt(STAR_VALUE);
      }
      else
      {
        Map<String, Integer> valueIds = forwardDimensionValueIndex.get(dimensionName);
        if (valueIds == null)
        {
          valueIds = new HashMap<String, Integer>();
          forwardDimensionValueIndex.put(dimensionName, valueIds);
        }

        Integer valueId = valueIds.get(dimensionValue);
        if (valueId == null)
        {
          valueId = nextValueId.getAndIncrement();
          valueIds.put(dimensionValue, valueId);
          reverseDimensionValueIndex.put(valueId, dimensionValue);
        }

        buffer.putInt(valueId);
      }
    }

    buffer.putLong(record.getTime() == null ? -1L : record.getTime());

    for (String metricName : metricNames)
    {
      buffer.putLong(record.getMetricValues().get(metricName));
    }
  }

  private StarTreeRecord getRecord(ByteBuffer buffer)
  {
    StarTreeRecordImpl.Builder builder = new StarTreeRecordImpl.Builder();

    for (String dimensionName : dimensionNames)
    {
      String dimensionValue = reverseDimensionValueIndex.get(buffer.getInt());
      builder.setDimensionValue(dimensionName, dimensionValue);
    }

    long time = buffer.getLong();
    builder.setTime(time == -1 ? null : time);

    for (String metricName : metricNames)
    {
      builder.setMetricValue(metricName, buffer.getLong());
    }

    return builder.build();
  }

  private Map<String, List<StarTreeRecord>> getGroupedRecords(ByteBuffer buffer)
  {
    Map<String, List<StarTreeRecord>> groupedRecords = new HashMap<String, List<StarTreeRecord>>();
    buffer.rewind();
    while (buffer.position() < buffer.limit())
    {
      StarTreeRecord record = getRecord(buffer);
      List<StarTreeRecord> group = groupedRecords.get(record.getKey());
      if (group == null)
      {
        group = new ArrayList<StarTreeRecord>();
        groupedRecords.put(record.getKey(), group);
      }
      group.add(record);
    }
    return groupedRecords;
  }

  /**
   * Replaces entries in the buffer which share the same dimension + time combination with an aggregate.
   */
  protected void compressBuffer(ByteBuffer buffer)
  {
    Map<String, List<StarTreeRecord>> groupedRecords = getGroupedRecords(buffer);

    buffer.rewind();

    for (List<StarTreeRecord> group : groupedRecords.values())
    {
      StarTreeRecord mergedRecord = StarTreeUtils.merge(group);
      putRecord(buffer, mergedRecord);
    }

    buffer.limit(buffer.position());
  }
}
