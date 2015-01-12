package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.StarTreeRecordStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A record store implemented on top of a fixed mmap'ed buffer.
 *
 * <p>
 *   The format of the buffer is the following:
 *   <pre>
 *     [ dimensions | [ t_1 | m_1 ] | [t_2 | m_2 ] ... | [t_n | m_n] ]
 *   </pre>
 * </p>
 *
 * <p>
 *   The buffer must be sorted by dimension names according to the forward index int values.
 * </p>
 *
 * <p>
 *   On a read, if there is not an exact match in the buffer, we scan the entire buffer and aggregate.
 * </p>
 *
 * <p>
 *   On a write, if there is not an exact match in the buffer, we find the record with minimum number of "other"
 *   dimension values, and update that. There will always be a record with all "other" values.
 * </p>
 */
public class StarTreeRecordStoreCircularBufferImpl implements StarTreeRecordStore
{
  private static final Logger LOG = LoggerFactory.getLogger(StarTreeRecordStoreCircularBufferImpl.class);
  private static final Comparator<int[]> COMPARATOR = new DimensionComparator();

  private final UUID nodeId;
  private final File file;
  private final List<DimensionSpec> dimensionSpecs;
  private final List<MetricSpec> metricSpecs;
  private final Map<String, Map<String, Integer>> forwardIndex;
  private final Map<String, Map<Integer, String>> reverseIndex;
  private final Map<String, Set<String>> dimensionValues;
  private final int numTimeBuckets;
  private final AtomicLong minTime;
  private final AtomicLong maxTime;

  private final int dimensionSize;
  private final int timeBucketSize;
  private final int entrySize;

  protected final Object sync;

  protected boolean isOpen;
  protected ByteBuffer buffer;
  private int recordCount;

  public StarTreeRecordStoreCircularBufferImpl(UUID nodeId,
                                               File file,
                                               List<DimensionSpec> dimensionSpecs,
                                               List<MetricSpec> metricSpecs,
                                               Map<String, Map<String, Integer>> forwardIndex,
                                               int numTimeBuckets)
  {
    this.nodeId = nodeId;
    this.file = file;
    this.dimensionSpecs = dimensionSpecs;
    this.metricSpecs = metricSpecs;
    this.forwardIndex = forwardIndex;
    this.numTimeBuckets = numTimeBuckets;

    this.dimensionSize = dimensionSpecs.size() * Integer.SIZE / 8;
    int metricSize =0;
    for (MetricSpec spec : metricSpecs)
    {
      metricSize += spec.getType().byteSize();
    }
    this.timeBucketSize = metricSize + Long.SIZE / 8;
    this.entrySize = dimensionSize + timeBucketSize * numTimeBuckets;

    this.sync = new Object();

    this.reverseIndex = new HashMap<String, Map<Integer, String>>();

    for (Map.Entry<String, Map<String, Integer>> e1 : forwardIndex.entrySet())
    {
      this.reverseIndex.put(e1.getKey(), new HashMap<Integer, String>());
      for (Map.Entry<String, Integer> e2 : e1.getValue().entrySet())
      {
        this.reverseIndex.get(e1.getKey()).put(e2.getValue(), e2.getKey());
      }
    }

    this.dimensionValues = new HashMap<String, Set<String>>();

    this.minTime = new AtomicLong(Long.MAX_VALUE);
    this.maxTime = new AtomicLong(0);
  }

  @Override
  public int getEntrySize()
  {
    return entrySize;
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
  public void update(StarTreeRecord record)
  {
    synchronized (sync)
    {
      // Convert to dimensions
      int[] targetDimensions = translateDimensions(record.getDimensionValues());

      // Get time bucket
      int timeBucket = (int) (record.getTime() % numTimeBuckets);

      // Find specific record
      int idx = binarySearch(targetDimensions);

      // If no match, find record with least "other" matches
      if (idx < 0)
      {
        buffer.clear();

        int[] currentDimensions = new int[dimensionSpecs.size()];

        Integer minOtherIdx = null;
        Integer minOtherDistance = null;

        while (buffer.position() < buffer.limit())
        {
          int currentIdx = buffer.position();

          // Get dimensions and compute distance
          buffer.mark();
          getDimensions(currentDimensions);
          int distance = computeDistance(targetDimensions, currentDimensions);

          // Track min distance
          if (minOtherDistance == null || (distance >= 0 && distance < minOtherDistance))
          {
            minOtherDistance = distance;
            minOtherIdx = currentIdx;
          }

          // Go to next entry
          buffer.reset();
          buffer.position(buffer.position() + entrySize);
        }

        if (minOtherIdx == null)
        {
          throw new IllegalStateException("Could not find index of record with " +
                                                  "minimum others in buffer " + nodeId + " for " + record);
        }

        idx = minOtherIdx;
      }

      // Update metrics
      buffer.clear();
      buffer.position(idx + dimensionSize + timeBucket * timeBucketSize);
      updateMetrics(record);

      // Update time

      if (record.getTime() < minTime.get())
      {
        minTime.set(record.getTime());
      }

      if (record.getTime() > maxTime.get())
      {
        maxTime.set(record.getTime());
      }
    }
  }

  @Override
  public Iterator<StarTreeRecord> iterator()
  {
    synchronized (sync)
    {
      List<StarTreeRecord> list = new LinkedList<StarTreeRecord>();

      buffer.clear();

      int[] currentDimensions = new int[dimensionSpecs.size()];
      Number[] currentMetrics = new Number[metricSpecs.size()];

      while (buffer.position() < buffer.limit())
      {
        // Get dimensions
        getDimensions(currentDimensions);
        Map<String, String> values = translateDimensions(currentDimensions);

        // Add records for all time buckets
        for (int i = 0; i < numTimeBuckets; i++)
        {
          long time = getMetrics(currentMetrics);

          StarTreeRecordImpl.Builder builder = new StarTreeRecordImpl.Builder();
          builder.setDimensionValues(values).setTime(time);

          for (int j = 0; j < metricSpecs.size(); j++)
          {
            builder.setMetricValue(metricSpecs.get(j).getName(), currentMetrics[j]);
            builder.setMetricType(metricSpecs.get(j).getName(), metricSpecs.get(j).getType());
          }

          list.add(builder.build());
        }
      }

      return list.iterator();
    }
  }

  @Override
  public void clear()
  {
    synchronized (sync)
    {
      buffer.clear();

      while (buffer.position() < buffer.limit())
      {
        for (DimensionSpec dimensionSpec : dimensionSpecs)
        {
          buffer.getInt();
        }

        for (int i = 0; i < numTimeBuckets; i++)
        {
          buffer.getLong(); // time

          // Set all metric values in buffer to zero
          for (MetricSpec spec : metricSpecs)
          {
            buffer.putInt(0);
          }
        }
      }
    }
  }

  @Override
  public void open() throws IOException
  {
    synchronized (sync)
    {
      if (!isOpen)
      {
        isOpen = true;

        FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel();
        buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, file.length());
        buffer.order(ByteOrder.BIG_ENDIAN);

        checkBuffer();

        LOG.info("Opened record store {}", nodeId);
      }
    }
  }

  @Override
  public void close() throws IOException
  {
    synchronized (sync)
    {
      if (isOpen)
      {
        isOpen = false;

        ((MappedByteBuffer) buffer).force();

        LOG.info("Closed record store {}", nodeId);
      }
    }
  }

  @Override
  public int getRecordCount()
  {
    return recordCount;
  }
  
  @Override
  public int getRecordCountEstimate()
  {
    return recordCount;
  }

  @Override
  public long getByteCount()
  {
    synchronized (sync)
    {
      return buffer.capacity();
    }
  }

  @Override
  public int getCardinality(String dimensionName)
  {
    Set<String> values = dimensionValues.get(dimensionName);
    if (values == null)
    {
      return 0;
    }
    return values.size();
  }

  @Override
  public String getMaxCardinalityDimension()
  {
    return getMaxCardinalityDimension(null);
  }

  @Override
  public String getMaxCardinalityDimension(Collection<String> blacklist)
  {
    String maxDimensionName = null;
    Integer maxCardinality = null;

    for (DimensionSpec dimensionSpec : dimensionSpecs)
    {
      int cardinality = getCardinality(dimensionSpec.getName());

      if ((blacklist == null || !blacklist.contains(dimensionSpec.getName()))
              && (maxCardinality == null || cardinality > maxCardinality))
      {
        maxCardinality = cardinality;
        maxDimensionName = dimensionSpec.getName();
      }
    }

    return maxDimensionName;
  }

  @Override
  public Set<String> getDimensionValues(String dimensionName)
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

  @Override
  public Number[] getMetricSums(StarTreeQuery query)
  {
    synchronized (sync)
    {
      Number[] sums = new Number[metricSpecs.size()];
      Arrays.fill(sums, 0);
      // Compute time buckets for getAggregate
      Set<Long> timeBuckets = getTimeBuckets(query);

      // Translate dimension combination
      int[] targetDimensions = translateDimensions(query.getDimensionValues());

      // Search for dimension combination in buffer
      int idx = binarySearch(targetDimensions);

      // If exact match, find aggregate across time buckets
      if (idx >= 0)
      {
        buffer.clear();
        buffer.position(idx + dimensionSize);

        // Scan all buckets
        updateSums(sums, timeBuckets);
      }
      // If no exact match, scan buffer and aggregate
      else
      {
        buffer.clear();

        int[] currentDimensions = new int[dimensionSpecs.size()];

        while (buffer.position() < buffer.limit())
        {
          buffer.mark();

          // Read dimension values
          getDimensions(currentDimensions);

          // Update metrics if matches
          if (matches(targetDimensions, currentDimensions))
          {
            updateSums(sums, timeBuckets);
          }

          // Move to next entry
          buffer.reset();
          buffer.position(buffer.position() + entrySize);
        }
      }

      return sums;
    }
  }

  @Override
  public List<StarTreeRecord> getTimeSeries(StarTreeQuery query)
  {
    synchronized (sync)
    {
      Map<Long, Number[]> allSums = new HashMap<Long, Number[]>();

      // Compute time buckets for getAggregate
      Set<Long> timeBuckets = getTimeBuckets(query);
      if (timeBuckets == null)
      {
        throw new IllegalArgumentException("Must specify time range in query " + query);
      }

      // Translate dimension combination
      int[] targetDimensions = translateDimensions(query.getDimensionValues());

      // Search for dimension combination in buffer
      int idx = binarySearch(targetDimensions);

      // If exact match, find aggregate across time buckets
      if (idx >= 0)
      {
        buffer.clear();
        buffer.position(idx + dimensionSize);

        // Scan all buckets
        updateAllSums(allSums, timeBuckets);
      }
      // If no exact match, scan buffer and aggregate
      else
      {
        buffer.clear();

        int[] currentDimensions = new int[dimensionSpecs.size()];

        while (buffer.position() < buffer.limit())
        {
          buffer.mark();

          // Read dimension values
          getDimensions(currentDimensions);

          // Update metrics if matches
          if (matches(targetDimensions, currentDimensions))
          {
            updateAllSums(allSums, timeBuckets);
          }

          // Move to next entry
          buffer.reset();
          buffer.position(buffer.position() + entrySize);
        }
      }

      // Convert to time series
      List<StarTreeRecord> timeSeries = new ArrayList<StarTreeRecord>();
      for (Map.Entry<Long,Number[]> entry : allSums.entrySet())
      {
        StarTreeRecordImpl.Builder record = new StarTreeRecordImpl.Builder()
                .setTime(entry.getKey())
                .setDimensionValues(query.getDimensionValues());
        for (int i = 0; i < metricSpecs.size(); i++)
        {
          record.setMetricValue(metricSpecs.get(i).getName(), entry.getValue()[i]);
          record.setMetricType(metricSpecs.get(i).getName(), metricSpecs.get(i).getType());
        }
        timeSeries.add(record.build());
      }

      // Sort it (by time)
      Collections.sort(timeSeries);

      return timeSeries;
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

  /**
   * Performs binary search on buffer for targetDimensions, and returns index of that combination (or -1 if not found)
   */
  private int binarySearch(int[] targetDimensions)
  {
    buffer.clear();

    int[] currentDimensions = new int[targetDimensions.length];

    while (buffer.position() < buffer.limit())
    {
      int idx = ((buffer.limit() + buffer.position()) / entrySize / 2) * entrySize;

      // Read dimensions
      buffer.mark();
      buffer.position(idx);
      for (int i = 0; i < dimensionSpecs.size(); i++)
      {
        currentDimensions[i] = buffer.getInt();
      }
      buffer.reset();

      // Compare
      int compare = COMPARATOR.compare(targetDimensions, currentDimensions);
      if (compare == 0)
      {
        return idx;
      }
      else if (compare < 0)
      {
        buffer.limit(idx); // go left
      }
      else
      {
        buffer.position(idx + entrySize); // go right
      }
    }

    return -1;
  }

  private void checkBuffer()
  {
    buffer.clear();

    int[] lastDimensions = null;
    int[] currentDimensions = new int[dimensionSpecs.size()];

    while (buffer.position() < buffer.limit())
    {
      // Dimensions
      for (int i = 0; i < dimensionSpecs.size(); i++)
      {
        currentDimensions[i] = buffer.getInt();

        // Check value
        Map<Integer, String> reverse = reverseIndex.get(dimensionSpecs.get(i).getName());
        String dimensionValue = reverse.get(currentDimensions[i]);
        if (dimensionValue == null)
        {
          throw new IllegalArgumentException("No dimension value in index for ID " + currentDimensions[i]);
        }

        // Update values
        Set<String> values = dimensionValues.get(dimensionSpecs.get(i).getName());
        if (values == null)
        {
          values = new HashSet<String>();
          dimensionValues.put(dimensionSpecs.get(i).getName(), values);
        }
        values.add(dimensionValue);
      }

      // Check dimension ordering
      if (lastDimensions != null && COMPARATOR.compare(lastDimensions, currentDimensions) >= 0)
      {
        throw new IllegalStateException(
                "Buffer dimension values are not unique and sorted: "
                        + Arrays.toString(lastDimensions) + "; " + Arrays.toString(currentDimensions));
      }

      // Move past all metric values
      for (int i = 0; i < numTimeBuckets; i++)
      {
        // Time
        long time = buffer.getLong();
        if (time < minTime.get())
        {
          minTime.set(time);
        }
        if (time > maxTime.get())
        {
          maxTime.set(time);
        }

        // Check bucket
        if (time != 0 && time % numTimeBuckets != i)
        {
          throw new IllegalStateException("Time bucket violation: " + time + " % " + numTimeBuckets + " != " + i);
        }

        for (int j=0;j<metricSpecs.size();j++)
        {
          String metricName = metricSpecs.get(j).getName();
          MetricType metricType = metricSpecs.get(j).getType();
          Number val = NumberUtils.readFromBuffer(buffer, metricType); // just pass by metrics
        }
      }

      // Update last dimensions
      if (lastDimensions == null)
      {
        lastDimensions = new int[dimensionSpecs.size()];
      }
      System.arraycopy(currentDimensions, 0, lastDimensions, 0, currentDimensions.length);

      recordCount++;
    }
  }

  /**
   * Translates a record's dimension values into ints using index
   *
   * <p>
   *   Aliases value to "other" if not in index
   * </p>
   */
  private int[] translateDimensions(Map<String, String> dimensionValues)
  {
    int[] dimensions = new int[dimensionSpecs.size()];

    for (int i = 0; i < dimensionSpecs.size(); i++)
    {
      String dimensionName = dimensionSpecs.get(i).getName();
      String dimensionValue = dimensionValues.get(dimensionName);

      Integer valueId = forwardIndex.get(dimensionName).get(dimensionValue);
      if (valueId == null)
      {
        valueId = StarTreeConstants.OTHER_VALUE;
      }

      dimensions[i] = valueId;
    }

    return dimensions;
  }

  /**
   * Translates int-encoded dimension values to their string counterparts
   */
  private Map<String, String> translateDimensions(int[] dimensionValues)
  {
    Map<String, String> dimensions = new HashMap<String, String>();

    for (int i = 0; i < dimensionSpecs.size(); i++)
    {
      String dimensionName = dimensionSpecs.get(i).getName();
      int valueId = dimensionValues[i];

      String dimensionValue = reverseIndex.get(dimensionName).get(valueId);
      if (dimensionValue == null)
      {
        throw new IllegalStateException("No value in index for ID " + valueId);
      }

      dimensions.put(dimensionName, dimensionValue);
    }

    return dimensions;
  }

  /**
   * Populates dimensions parameter with dimensions in buffer and advances position
   */
  private void getDimensions(int[] dimensions)
  {
    for (int i = 0; i < dimensionSpecs.size(); i++)
    {
      dimensions[i] = buffer.getInt();
    }
  }

  /**
   * Populates metrics with values from buffer and returns corresponding time
   */
  private long getMetrics(Number[] metrics)
  {
    long time = buffer.getLong();

    for (int i = 0; i < metricSpecs.size(); i++)
    {
      metrics[i] = NumberUtils.readFromBuffer(buffer, metricSpecs.get(i).getType());
    }

    return time;
  }

  /**
   * Computes all discrete time buckets that need to be accessed
   */
  private Set<Long> getTimeBuckets(StarTreeQuery query)
  {
    Set<Long> timeBuckets;

    if (query.getTimeBuckets() != null)
    {
      timeBuckets = query.getTimeBuckets();
    }
    else if (query.getTimeRange() != null)
    {
      timeBuckets = new HashSet<Long>();
      for (long time = query.getTimeRange().getKey(); time <= query.getTimeRange().getValue(); time++)
      {
        timeBuckets.add(time);
      }
    }
    else
    {
      timeBuckets = null;
    }

    return timeBuckets;
  }

  /**
   * Adds metrics values to sums if timeBuckets == null or if the exact time is in the buckets
   */
  private void updateSums(Number[] sums, Set<Long> timeBuckets)
  {
    if (timeBuckets == null) // All
    {
      for (int i = 0; i < numTimeBuckets; i++)
      {
        long time = buffer.getLong();

        for (int j = 0; j < metricSpecs.size(); j++)
        {
          Number val = NumberUtils.readFromBuffer(buffer, metricSpecs.get(j).getType());
          if(sums[j] == null)
          {
            sums[j] = val;
          }
          else
          {
            sums[j] = NumberUtils.sum(sums[j], val, metricSpecs.get(j).getType());
          }
        }
      }
    }
    else // Selected
    {
      int base = buffer.position();

      for (Long time : timeBuckets)
      {
        int bucket = (int) (time % numTimeBuckets);

        buffer.position(base + bucket * timeBucketSize);

        long t = buffer.getLong();
        if (t == time)
        {
          for (int i = 0; i < metricSpecs.size(); i++)
          {
            Number val = NumberUtils.readFromBuffer(buffer, metricSpecs.get(i).getType());
            if(sums[i] != null)
            {
              sums[i] = NumberUtils.sum(sums[i], val, metricSpecs.get(i).getType());
            }
            else
            {
              sums[i] = val;
            }
          }
        }
      }
    }
  }

  /**
   * Aggregates metric values grouped by time (timeBuckets must be non-null)
   */
  private void updateAllSums(Map<Long, Number[]> allSums, Set<Long> timeBuckets)
  {
    int base = buffer.position();

    for (Long time : timeBuckets)
    {
      Number[] sums = allSums.get(time);
      if (sums == null)
      {
        sums = new Number[metricSpecs.size()];
        Arrays.fill(sums, 0);
        allSums.put(time, sums);
      }

      int bucket = (int) (time % numTimeBuckets);

      buffer.position(base + bucket * timeBucketSize);

      long t = buffer.getLong();
      if (t == time)
      {
        for (int i = 0; i < metricSpecs.size(); i++)
        {
          Number val = NumberUtils.readFromBuffer(buffer, metricSpecs.get(i).getType());
          sums[i] = NumberUtils.sum(sums[i], val, metricSpecs.get(i).getType());
        }
      }
    }
  }

  /**
   * Computes a distance metric from target dimensions to dimensions.
   *
   * <p>
   *   If a dimension value matches, this contributes 0 to the score.
   * </p>
   *
   * <p>
   *   If dimensions has "other" for a value, this contributes 1 to the score.
   * </p>
   *
   * <p>
   *   If the dimension values don't match, we return -1 (i.e. invalid)
   * </p>
   */
  private int computeDistance(int[] targetDimensions, int[] dimensions)
  {
    if (targetDimensions.length != dimensions.length)
    {
      throw new IllegalArgumentException(
              "Dimension arrays must be same size: "
                      + Arrays.toString(targetDimensions) + "; " + Arrays.toString(dimensions));
    }

    int distance = 0;

    for (int i = 0; i < targetDimensions.length; i++)
    {
      if (targetDimensions[i] != dimensions[i])
      {
        if (dimensions[i] == StarTreeConstants.OTHER_VALUE)
        {
          distance += 1;
        }
        else
        {
          return -1; // no match
        }
      }
    }

    return distance;
  }

  /**
   * Updates (if time matches) or over-writes (if time rolled over) the metrics for a time bucket
   */
  private void updateMetrics(StarTreeRecord record)
  {
    // Read current value
    buffer.mark();
    Number[] metrics = new Number[metricSpecs.size()];
    long time = getMetrics(metrics);
    buffer.reset();

    // Update time
    buffer.putLong(record.getTime());

    // Update metrics
    for (int i = 0; i < metricSpecs.size(); i++)
    {
      String name = metricSpecs.get(i).getName();
      MetricType type = metricSpecs.get(i).getType();
      Number metricValue = record.getMetricValues().get(name);
      if (time == record.getTime())
      {
        Number sum = NumberUtils.sum(metrics[i], metricValue, type);
        NumberUtils.addToBuffer(buffer, sum, type);
      }
      else
      {
        NumberUtils.addToBuffer(buffer, metricValue, type);
      }
    }
  }

  /**
   * Returns true if all dimension values for targetDimensions match that of dimensions or are *
   */
  private boolean matches(int[] targetDimensions, int[] dimensions)
  {
    if (targetDimensions.length != dimensions.length)
    {
      throw new IllegalArgumentException(
              "Dimension arrays must be same size: "
                      + Arrays.toString(targetDimensions) + "; " + Arrays.toString(dimensions));
    }

    for (int i = 0; i < targetDimensions.length; i++)
    {
      if (targetDimensions[i] != dimensions[i] && targetDimensions[i] != StarTreeConstants.STAR_VALUE)
      {
        return false;
      }
    }

    return true;
  }

  /**
   * Compares dimension vectors in buffer in index order
   */
  private static class DimensionComparator implements Comparator<int[]>
  {
    @Override
    public int compare(int[] o1, int[] o2)
    {
      if (o1.length != o2.length)
      {
        throw new IllegalArgumentException("Dimension arrays must be same length: "
                                                   + Arrays.toString(o1) + "; " + Arrays.toString(o2));
      }

      for (int i = 0; i < o1.length; i++)
      {
        if (o1[i] != o2[i])
        {
          return o1[i] - o2[i];
        }
      }

      return 0;
    }
  }

  /**
   * Fills byteBuffer with sorted / bucketized records
   */
  public static void fillBuffer(final OutputStream outputStream,
                                final List<DimensionSpec> dimensionSpecs,
                                final List<MetricSpec> metricSpecs,
                                final Map<String, Map<String, Integer>> forwardIndex,
                                final Iterable<StarTreeRecord> records,
                                final int numTimeBuckets,
                                final boolean keepMetricValues) throws IOException
  {
    // n.b. writes in big-endian
    DataOutputStream dataOutputStream = new DataOutputStream(outputStream);

    // Group records by dimensions
    Map<Map<String, String>, List<StarTreeRecord>> groups = new HashMap<Map<String, String>, List<StarTreeRecord>>();
    for (StarTreeRecord record : records)
    {
      List<StarTreeRecord> group = groups.get(record.getDimensionValues());

      if (group == null)
      {
        group = new ArrayList<StarTreeRecord>();
        groups.put(record.getDimensionValues(), group);
      }

      group.add(record);
    }

    // Find sort order w.r.t. ints in forward index
    List<Map<String, String>> orderedCombinations = new ArrayList<Map<String, String>>(groups.keySet());
    Collections.sort(orderedCombinations, new Comparator<Map<String, String>>()
    {
      @Override
      public int compare(Map<String, String> c1, Map<String, String> c2)
      {
        for (DimensionSpec dimensionSpec : dimensionSpecs)
        {
          String v1 = c1.get(dimensionSpec.getName());
          String v2 = c2.get(dimensionSpec.getName());
          int i1 = forwardIndex.get(dimensionSpec.getName()).get(v1);
          int i2 = forwardIndex.get(dimensionSpec.getName()).get(v2);

          if (i1 != i2)
          {
            return i1 - i2;
          }
        }

        return 0;
      }
    });

    // For each group, fill in dimensions -> buckets
    for (Map<String, String> combination : orderedCombinations)
    {
      List<StarTreeRecord> group = groups.get(combination);

      // Group records by time
      Map<Long, List<StarTreeRecord>> timeGroup = new HashMap<Long, List<StarTreeRecord>>();
      for (StarTreeRecord r : group)
      {
        List<StarTreeRecord> rs = timeGroup.get(r.getTime());
        if (rs == null)
        {
          rs = new ArrayList<StarTreeRecord>();
          timeGroup.put(r.getTime(), rs);
        }
        rs.add(r);
      }

      // Merge records with like times
      Long minTime = null;
      List<StarTreeRecord> merged = new ArrayList<StarTreeRecord>();
      Set<Integer> representedBuckets = new HashSet<Integer>();
      for (List<StarTreeRecord> rs : timeGroup.values())
      {
        StarTreeRecord r = StarTreeUtils.merge(rs);
        merged.add(r);

        representedBuckets.add((int) (r.getTime() % numTimeBuckets));

        if (minTime == null || r.getTime() < minTime)
        {
          minTime = r.getTime();
        }
      }

      // Add dummy records w/ times for each non-represented bucket
      if (minTime != null)
      {
        int startBucket = (int) (minTime % numTimeBuckets);

        int currentBucket = startBucket;
        long currentTime = minTime;
        do
        {
          if (!representedBuckets.contains(currentBucket))
          {
            StarTreeRecordImpl.Builder builder = new StarTreeRecordImpl.Builder()
                    .setDimensionValues(combination)
                    .setTime(currentTime);

            for (int i=0; i< metricSpecs.size();i++)
            {
              String metricName = metricSpecs.get(i).getName();
              builder.setMetricValue(metricName, 0);
              builder.setMetricType(metricName, metricSpecs.get(i).getType());
              
            }

            merged.add(builder.build());
          }

          currentTime++;
          currentBucket = (int) (currentTime % numTimeBuckets);
        }
        while (currentBucket != startBucket);
      }

      // Sort group by time bucket
      Collections.sort(merged, new Comparator<StarTreeRecord>()
      {
        @Override
        public int compare(StarTreeRecord r1, StarTreeRecord r2)
        {
          int b1 = (int) (r1.getTime() % numTimeBuckets);
          int b2 = (int) (r2.getTime() % numTimeBuckets);
          return b1 - b2;
        }
      });

      // Fill in dimensions
      for (DimensionSpec dimensionSpec : dimensionSpecs)
      {
        String dimensionValue = combination.get(dimensionSpec.getName());
        Integer valueId = forwardIndex.get(dimensionSpec.getName()).get(dimensionValue);
        if (valueId == null)
        {
          throw new IllegalStateException("No ID for dimension value " + dimensionSpec.getName() + ":" + dimensionValue);
        }
        dataOutputStream.writeInt(valueId);
      }

      // Fill in time buckets w/ time + metrics
      for (StarTreeRecord record : merged)
      {
        dataOutputStream.writeLong(record.getTime());
        for (int i = 0; i < metricSpecs.size(); i++)
        {
          String metricName = metricSpecs.get(i).getName();
          if (keepMetricValues)
          {
            Number val = record.getMetricValues().get(metricName);
            NumberUtils.addToDataOutputStream(dataOutputStream, val, metricSpecs.get(i).getType());
          }
          else
          {
            NumberUtils.addToDataOutputStream(dataOutputStream, 0, metricSpecs.get(i).getType());
          }
        }
      }
    }
  }

  @Override
  public Map<String, Map<String, Integer>> getForwardIndex() {
    return forwardIndex;
  }
}
