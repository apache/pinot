package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.StarTreeRecordStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class StarTreeRecordStoreLogBufferImpl implements StarTreeRecordStore
{
  private static final Logger LOG = LoggerFactory.getLogger(StarTreeRecordStoreLogBufferImpl.class);
  private static final int STAR_VALUE = 0;

  private final UUID nodeId;
  private final StarTreeConfig config;
  private final List<DimensionSpec> dimensionSpecs;
  private final List<MetricSpec> metricSpecs;
  private final int bufferSize;
  private final boolean useDirect;
  private final double targetLoadFactor;
  private final AtomicInteger nextValueId;
  private final AtomicInteger recordCount;
  private final int metricSize;
  private final Object sync;
  private final MetricSchema metricSchema;

  private final Map<String, Map<String, Integer>> forwardIndex;
  private final Map<String, Map<Integer, String>> reverseIndex;

  private final AtomicLong minTime;
  private final AtomicLong maxTime;

  private ByteBuffer buffer;

  public StarTreeRecordStoreLogBufferImpl(UUID nodeId,
                                          StarTreeConfig config,
                                          int bufferSize,
                                          boolean useDirect,
                                          double targetLoadFactor)
  {
    this.nodeId = nodeId;
    this.config = config;
    this.dimensionSpecs = config.getDimensions();
    this.metricSpecs = config.getMetrics();
    this.bufferSize = bufferSize;
    this.useDirect = useDirect;
    this.targetLoadFactor = targetLoadFactor;
    this.nextValueId = new AtomicInteger(StarTreeConstants.FIRST_VALUE);
    this.sync = new Object();
    this.recordCount = new AtomicInteger(0);
    this.metricSchema = MetricSchema.fromMetricSpecs(config.getMetrics());
    int metricSize = 0;
    for (MetricSpec spec : metricSpecs)
    {
      metricSize += spec.getType().byteSize();
    }
    this.metricSize = metricSize;

    this.forwardIndex = new HashMap<String, Map<String, Integer>>();
    this.reverseIndex = new HashMap<String, Map<Integer, String>>();

    for (DimensionSpec dimensionSpec : dimensionSpecs)
    {
      Map<String, Integer> forward = new HashMap<String, Integer>();
      forward.put(StarTreeConstants.STAR, StarTreeConstants.STAR_VALUE);
      forward.put(StarTreeConstants.OTHER, StarTreeConstants.OTHER_VALUE);
      getForwardIndex().put(dimensionSpec.getName(), forward);

      Map<Integer, String> reverse = new HashMap<Integer, String>();
      reverse.put(StarTreeConstants.STAR_VALUE, StarTreeConstants.STAR);
      reverse.put(StarTreeConstants.OTHER_VALUE, StarTreeConstants.OTHER);
      reverseIndex.put(dimensionSpec.getName(), reverse);
    }

    this.minTime = new AtomicLong(-1);
    this.maxTime = new AtomicLong(-1);
  }

  @Override
  public void update(StarTreeRecord record)
  {
    int entrySize = dimensionSpecs.size() * Integer.SIZE / 8
            + Integer.SIZE / 8
            + record.getMetricTimeSeries().getTimeWindowSet().size() * (Long.SIZE / 8 + metricSize);

    synchronized (sync)
    {
      ByteBuffer buffer = getBuffer(entrySize);
      buffer.position(buffer.limit());
      buffer.limit(buffer.position() + entrySize);
      putRecord(buffer, record);
      recordCount.incrementAndGet();

      for (Long time : record.getMetricTimeSeries().getTimeWindowSet())
      {
        if (minTime.get() == -1 || time < minTime.get())
        {
          minTime.set(time);
        }

        if (maxTime.get() == -1 || time > maxTime.get())
        {
          maxTime.set(time);
        }
      }
    }
  }

  @Override
  public MetricTimeSeries getTimeSeries(StarTreeQuery query)
  {
    // Check query
    if (query.getTimeRange() == null)
    {
      throw new IllegalArgumentException("Query must have time range " + query);
    }

    synchronized (sync)
    {
      MetricTimeSeries timeSeries = new MetricTimeSeries(metricSchema);

      ByteBuffer buffer = getBuffer();

      buffer.rewind();

      while (buffer.position() < buffer.limit())
      {
        boolean matches = true;

        StarTreeRecord record = getRecord(buffer);

        for (int i = 0; i < dimensionSpecs.size(); i++)
        {
          String recordValue = record.getDimensionKey().getDimensionValues()[i];
          String queryValue = query.getDimensionKey().getDimensionValues()[i];

          if (!StarTreeConstants.STAR.equals(queryValue) && !queryValue.equals(recordValue))
          {
            matches = false;
          }
        }

        if (matches)
        {
          for (Long time : record.getMetricTimeSeries().getTimeWindowSet())
          {
            if (query.getTimeRange() == null || query.getTimeRange().contains(time))
            {
              for (MetricSpec metricSpec : metricSpecs)
              {
                timeSeries.increment(time, metricSpec.getName(), record.getMetricTimeSeries().get(time, metricSpec.getName()));
              }
            }
          }
        }
      }

      return timeSeries;
    }
  }

  @Override
  public Iterator<StarTreeRecord> iterator()
  {
    synchronized (sync)
    {
      ByteBuffer buffer = getBuffer();
      compactBuffer(buffer);
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
      recordCount.set(0);
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
    synchronized (sync) {
      compactBuffer(getBuffer());
    }
  }

  @Override
  public int getRecordCount()
  {
    synchronized (sync) {
      compactBuffer(getBuffer());
    }
    return recordCount.get();
  }

  @Override
  public int getRecordCountEstimate() {
    return recordCount.get();
  }

  @Override
  public int getCardinality(String dimensionName)
  {
    synchronized (sync)
    {
      Map<String, Integer> valueIds = getForwardIndex().get(dimensionName);
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

      for (DimensionSpec dimensionSpec : dimensionSpecs)
      {
        int cardinality = getCardinality(dimensionSpec.getName());
        if (cardinality > maxCardinality && (blacklist == null || !blacklist.contains(dimensionSpec.getName())))
        {
          maxCardinality = cardinality;
          maxDimension = dimensionSpec.getName();
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
      Map<String, Integer> valueIds = getForwardIndex().get(dimensionName);
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
  public Number[] getMetricSums(StarTreeQuery query)
  {
    synchronized (sync)
    {
      MetricTimeSeries timeSeries = new MetricTimeSeries(metricSchema);

      ByteBuffer buffer = getBuffer();

      buffer.rewind();

      while (buffer.position() < buffer.limit())
      {
        StarTreeRecord record = getRecord(buffer);

        boolean matches = true;

        for (int i = 0; i < dimensionSpecs.size(); i++)
        {
          String recordValue = record.getDimensionKey().getDimensionValues()[i];
          String queryValue = query.getDimensionKey().getDimensionValues()[i];

          if (!StarTreeConstants.STAR.equals(queryValue) && !queryValue.equals(recordValue))
          {
            matches = false;
          }
        }

        if (matches)
        {
          for (Long time : record.getMetricTimeSeries().getTimeWindowSet())
          {
            if (query.getTimeRange() == null || query.getTimeRange().contains(time))
            {
              for (MetricSpec metricSpec : metricSpecs)
              {
                timeSeries.increment(time, metricSpec.getName(), record.getMetricTimeSeries().get(time, metricSpec.getName()));
              }
            }
          }
        }
      }

      return timeSeries.getMetricSums();
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
    return getBuffer(-1);
  }

  private ByteBuffer getBuffer(int entrySize)
  {
    if (buffer == null)
    {
      buffer = createBuffer(bufferSize);
      buffer.limit(0);
    }

    if (entrySize >= 0 && buffer.limit() + entrySize > buffer.capacity())
    {
      int oldLimit = buffer.limit();
      compactBuffer(buffer);
      int newLimit = buffer.limit();
      double loadFactor = (1.0 * newLimit) / oldLimit;

      if (loadFactor > targetLoadFactor)
      {
        ByteBuffer expandedBuffer = createBuffer(buffer.capacity() + bufferSize);
        buffer.rewind();
        expandedBuffer.put(buffer);
        expandedBuffer.limit(expandedBuffer.position());
        int oldCapacity = buffer.capacity();
        int newCapacity = expandedBuffer.capacity();
        buffer = expandedBuffer;

        if (LOG.isDebugEnabled())
        {
          LOG.debug("Expanded buffer ({}): oldCapacity={},newCapacity={}", nodeId, oldCapacity, newCapacity);
        }
      }
      else
      {
        if (LOG.isDebugEnabled())
        {
          LOG.debug("Compacted buffer ({}): loadFactor={},capacity={}", nodeId, loadFactor, buffer.capacity());
        }
      }
    }

    return buffer;
  }

  private void putRecord(ByteBuffer buffer, StarTreeRecord record)
  {
    for (int i = 0; i < dimensionSpecs.size(); i++)
    {
      DimensionSpec dimensionSpec = dimensionSpecs.get(i);
      String dimensionValue = record.getDimensionKey().getDimensionValues()[i];

      if (StarTreeConstants.STAR.equals(dimensionValue))
      {
        buffer.putInt(STAR_VALUE);
      }
      else
      {
        Map<String, Integer> valueIds = getForwardIndex().get(dimensionSpec.getName());
        if (valueIds == null)
        {
          valueIds = new HashMap<String, Integer>();
          getForwardIndex().put(dimensionSpec.getName(), valueIds);
        }

        Integer valueId = valueIds.get(dimensionValue);
        if (valueId == null)
        {
          valueId = nextValueId.getAndIncrement();
          valueIds.put(dimensionValue, valueId);
          reverseIndex.get(dimensionSpec.getName()).put(valueId, dimensionValue);
        }

        buffer.putInt(valueId);
      }
    }

    Set<Long> times = record.getMetricTimeSeries().getTimeWindowSet();

    buffer.putInt(times.size());

    for (Long time : times)
    {
      buffer.putLong(time);

      for (int i = 0; i < metricSpecs.size(); i++)
      {
        String metricName = metricSpecs.get(i).getName();
        Number value = record.getMetricTimeSeries().get(time, metricName);
        NumberUtils.addToBuffer(buffer, value, metricSpecs.get(i).getType());
      }
    }
  }

  private StarTreeRecord getRecord(ByteBuffer buffer)
  {
    StarTreeRecordImpl.Builder builder = new StarTreeRecordImpl.Builder();

    String[] dimensionValues = new String[dimensionSpecs.size()];
    for (int i = 0; i < dimensionSpecs.size(); i++)
    {
      String dimensionValue = reverseIndex.get(dimensionSpecs.get(i).getName()).get(buffer.getInt());
      dimensionValues[i] = dimensionValue;
    }
    builder.setDimensionKey(new DimensionKey(dimensionValues));

    MetricTimeSeries timeSeries = new MetricTimeSeries(metricSchema);

    int numTimes = buffer.getInt();

    for (int i = 0; i < numTimes; i++)
    {
      long time = buffer.getLong();

      for (int j = 0; j < metricSpecs.size(); j++)
      {
        String metricName = metricSpecs.get(j).getName();
        Number value = NumberUtils.readFromBuffer(buffer, metricSpecs.get(j).getType());
        timeSeries.increment(time, metricName, value);
      }
    }
    builder.setMetricTimeSeries(timeSeries);

    return builder.build(config);
  }

  /**
   * Replaces entries in the buffer which share the same dimension + time combination with an aggregate.
   */
  protected void compactBuffer(ByteBuffer buffer)
  {
    Map<DimensionKey, StarTreeRecord> groupedRecords = new HashMap<DimensionKey, StarTreeRecord>();

    buffer.rewind();
    while (buffer.position() < buffer.limit())
    {
      StarTreeRecord record = getRecord(buffer);

      StarTreeRecord group = groupedRecords.get(record.getDimensionKey());
      if (group == null)
      {
        group = new StarTreeRecordImpl(config, record.getDimensionKey(), new MetricTimeSeries(metricSchema));
        groupedRecords.put(group.getDimensionKey(), group);
      }

      group.getMetricTimeSeries().aggregate(record.getMetricTimeSeries());
    }

    buffer.rewind();

    for (StarTreeRecord group : groupedRecords.values())
    {
      putRecord(buffer, group);
    }

    buffer.limit(buffer.position());

    recordCount.set(groupedRecords.size());
  }

  @Override
  public Long getMinTime()
  {
    return minTime.get();
  }

  @Override
  public Long getMaxTime()
  {
    return maxTime.get();
  }
  @Override
  public Map<String, Map<String, Integer>> getForwardIndex() {
    return forwardIndex;
  }


}
