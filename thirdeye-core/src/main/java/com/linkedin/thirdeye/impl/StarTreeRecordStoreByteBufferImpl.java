package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.StarTreeRecordStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
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
  private final double targetLoadFactor;
  private final AtomicInteger nextValueId;
  private final AtomicInteger size;
  private final int entrySize;
  private final Object sync;

  private final Map<String, Map<String, Integer>> forwardIndex;
  private final Map<String, Map<Integer, String>> reverseIndex;

  private ByteBuffer buffer;

  public StarTreeRecordStoreByteBufferImpl(UUID nodeId,
                                           List<String> dimensionNames,
                                           List<String> metricNames,
                                           int bufferSize,
                                           boolean useDirect,
                                           double targetLoadFactor)
  {
    this.nodeId = nodeId;
    this.dimensionNames = dimensionNames;
    this.metricNames = metricNames;
    this.bufferSize = bufferSize;
    this.useDirect = useDirect;
    this.targetLoadFactor = targetLoadFactor;
    this.nextValueId = new AtomicInteger(1);
    this.sync = new Object();
    this.size = new AtomicInteger(0);

    this.entrySize =
            dimensionNames.size() * (Integer.SIZE / 8) +
                    metricNames.size() * (Long.SIZE / 8) +
                    Long.SIZE / 8; // time

    this.forwardIndex = new HashMap<String, Map<String, Integer>>();
    this.reverseIndex = new HashMap<String, Map<Integer, String>>();

    for (String dimensionName : dimensionNames)
    {
      Map<String, Integer> forward = new HashMap<String, Integer>();
      forward.put(StarTreeConstants.STAR, StarTreeConstants.STAR_VALUE);
      forward.put(StarTreeConstants.OTHER, StarTreeConstants.OTHER_VALUE);
      forwardIndex.put(dimensionName, forward);

      Map<Integer, String> reverse = new HashMap<Integer, String>();
      reverse.put(StarTreeConstants.STAR_VALUE, StarTreeConstants.STAR);
      reverse.put(StarTreeConstants.OTHER_VALUE, StarTreeConstants.OTHER);
      reverseIndex.put(dimensionName, reverse);
    }
  }

  @Override
  public int getEntrySize()
  {
    return entrySize;
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
      ByteBuffer buffer = getBuffer();
      List<StarTreeRecord> records = new LinkedList<StarTreeRecord>();
      buffer.rewind();
      while (buffer.position() < buffer.limit())
      {
        records.add(getRecord(buffer));
      }
      return records.iterator();
    }
  }

  @Override
  public void clear()
  {
    synchronized (sync)
    {
      buffer = createBuffer(bufferSize);
      buffer.limit(0);
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
      Map<String, Integer> valueIds = forwardIndex.get(dimensionName);
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
      Map<String, Integer> valueIds = forwardIndex.get(dimensionName);
      if (valueIds != null)
      {
        Set<String> values = new HashSet<String>(valueIds.keySet());
        values.remove(StarTreeConstants.STAR);
        values.remove(StarTreeConstants.OTHER);
        return values;
      }
      return null;
    }
  }

  @Override
  public long[] getMetricSums(StarTreeQuery query)
  {
    synchronized (sync)
    {
      long[] sums = new long[metricNames.size()];

      ByteBuffer buffer = getBuffer();
      buffer.rewind();
      while (buffer.position() < buffer.limit())
      {
        boolean matches = true;

        // Dimensions
        for (String dimensionName : dimensionNames)
        {
          int valueId = buffer.getInt();
          String recordValue = reverseIndex.get(dimensionName).get(valueId);
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

      return sums;
    }
  }

  @Override
  public byte[] encode()
  {
    synchronized (sync)
    {
      buffer.clear();
      byte[] bytes = new byte[buffer.capacity()];
      buffer.get(bytes);
      return bytes;
    }
  }

  private ByteBuffer createBuffer(int size)
  {
    ByteBuffer buffer;
    if (useDirect)
    {
      buffer = ByteBuffer.allocateDirect(size);
    }
    else
    {
      buffer = ByteBuffer.allocate(size);
    }
    return buffer;
  }

  private ByteBuffer getBuffer()
  {
    if (buffer == null)
    {
      buffer = createBuffer(bufferSize);
      buffer.limit(0);
    }

    if (buffer.limit() + entrySize > buffer.capacity())
    {
      int oldLimit = buffer.limit();
      compactBuffer(buffer);
      int newLimit = buffer.limit();
      double loadFactor = (1.0 * newLimit) / oldLimit;

      LOG.info(String.format("Compacted buffer (loadFactor=%.02f, oldLimit=%d, newLimit=%d)", loadFactor, oldLimit, newLimit));

      if (loadFactor > targetLoadFactor)
      {
        ByteBuffer expandedBuffer = createBuffer(buffer.capacity() + bufferSize);
        buffer.rewind();
        expandedBuffer.put(buffer);
        expandedBuffer.limit(expandedBuffer.position());
        buffer = expandedBuffer;

        LOG.info("Expanded buffer: " + expandedBuffer);
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
        Map<String, Integer> valueIds = forwardIndex.get(dimensionName);
        if (valueIds == null)
        {
          valueIds = new HashMap<String, Integer>();
          forwardIndex.put(dimensionName, valueIds);
        }

        Integer valueId = valueIds.get(dimensionValue);
        if (valueId == null)
        {
          valueId = nextValueId.getAndIncrement();
          valueIds.put(dimensionValue, valueId);
          reverseIndex.get(dimensionName).put(valueId, dimensionValue);
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
      String dimensionValue = reverseIndex.get(dimensionName).get(buffer.getInt());
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
      List<StarTreeRecord> group = groupedRecords.get(record.getKey(true));
      if (group == null)
      {
        group = new ArrayList<StarTreeRecord>();
        groupedRecords.put(record.getKey(true), group);
      }
      group.add(record);
    }
    return groupedRecords;
  }

  /**
   * Replaces entries in the buffer which share the same dimension + time combination with an aggregate.
   */
  protected void compactBuffer(ByteBuffer buffer)
  {
    Map<String, List<StarTreeRecord>> groupedRecords = getGroupedRecords(buffer);

    buffer.rewind();

    for (List<StarTreeRecord> group : groupedRecords.values())
    {
      StarTreeRecord mergedRecord = StarTreeUtils.merge(group);
      putRecord(buffer, mergedRecord);
    }

    buffer.limit(buffer.position());

    size.set(groupedRecords.size());
  }
}
