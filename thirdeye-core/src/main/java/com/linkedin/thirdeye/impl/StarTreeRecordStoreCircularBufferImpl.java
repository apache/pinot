package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.StarTreeRecordStore;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

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
  private static final Comparator<int[]> COMPARATOR = new DimensionComparator();

  private final UUID nodeId;
  private final File file;
  private final List<String> dimensionNames;
  private final List<String> metricNames;
  private final Map<String, Map<String, Integer>> forwardIndex;
  private final Map<String, Map<Integer, String>> reverseIndex;
  private final Map<String, Set<String>> dimensionValues;
  private final int numTimeBuckets;

  private final int dimensionSize;
  private final int timeBucketSize;
  private final int entrySize;

  private final Object sync;

  private boolean isOpen;
  private MappedByteBuffer buffer;

  public StarTreeRecordStoreCircularBufferImpl(UUID nodeId,
                                               File file,
                                               List<String> dimensionNames,
                                               List<String> metricNames,
                                               Map<String, Map<String, Integer>> forwardIndex,
                                               int numTimeBuckets)
  {
    this.nodeId = nodeId;
    this.file = file;
    this.dimensionNames = dimensionNames;
    this.metricNames = metricNames;
    this.forwardIndex = forwardIndex;
    this.numTimeBuckets = numTimeBuckets;

    this.dimensionSize = dimensionNames.size() * Integer.SIZE / 8;
    this.timeBucketSize = (metricNames.size() + 1) * Long.SIZE / 8;
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

        int[] currentDimensions = new int[dimensionNames.size()];

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
          if (minOtherDistance == null || distance < minOtherDistance)
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
    }
  }

  @Override
  public Iterator<StarTreeRecord> iterator()
  {
    return null;
  }

  @Override
  public void clear()
  {
    throw new UnsupportedOperationException(); // used during splits, should not be used
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
        buffer.load();

        checkBuffer();
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

        buffer.force();
      }
    }
  }

  @Override
  public int size()
  {
    return 0;
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

    for (String dimensionName : dimensionNames)
    {
      int cardinality = getCardinality(dimensionName);

      if ((blacklist == null || !blacklist.contains(dimensionName))
              && (maxCardinality == null || cardinality > maxCardinality))
      {
        maxCardinality = cardinality;
        maxDimensionName = dimensionName;
      }
    }

    return maxDimensionName;
  }

  @Override
  public Set<String> getDimensionValues(String dimensionName)
  {
    return dimensionValues.get(dimensionName);
  }

  @Override
  public long[] getMetricSums(StarTreeQuery query)
  {
    synchronized (sync)
    {
      long[] sums = new long[metricNames.size()];

      // Compute time buckets for query
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

      // Translate dimension combination
      int[] targetDimensions = translateDimensions(query.getDimensionValues());

      // Search for dimension combination in buffer
      int idx = binarySearch(targetDimensions);

      // If exact match, find aggregate across time buckets
      if (idx >= 0)
      {
        buffer.clear();
        buffer.position(idx);

        // Scan all buckets
        for (int i = 0; i < numTimeBuckets; i++)
        {
          updateSums(sums, timeBuckets);
        }
      }
      // If no exact match, scan buffer and aggregate
      else
      {
        buffer.clear();

        int[] currentDimensions = new int[dimensionNames.size()];

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
      for (int i = 0; i < dimensionNames.size(); i++)
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
    int[] currentDimensions = new int[dimensionNames.size()];

    while (buffer.position() < buffer.limit())
    {
      // Dimensions
      for (int i = 0; i < dimensionNames.size(); i++)
      {
        currentDimensions[i] = buffer.getInt();

        // Check value
        Map<Integer, String> reverse = reverseIndex.get(dimensionNames.get(i));
        String dimensionValue = reverse.get(currentDimensions[i]);
        if (dimensionValue == null)
        {
          throw new IllegalArgumentException("No dimension value in index for ID " + currentDimensions[i]);
        }

        // Update values
        Set<String> values = dimensionValues.get(dimensionNames.get(i));
        if (values == null)
        {
          values = new HashSet<String>();
          dimensionValues.put(dimensionNames.get(i), values);
        }
        values.add(dimensionValue);
      }

      // Check ordering
      if (lastDimensions != null && COMPARATOR.compare(lastDimensions, currentDimensions) >= 0)
      {
        throw new IllegalStateException(
                "Buffer dimension values are not unique and sorted: "
                        + Arrays.toString(lastDimensions) + "; " + Arrays.toString(currentDimensions));
      }

      // Time buckets (need to be ordered too)
      Long lastTime = null;
      for (int i = 0; i < numTimeBuckets; i++)
      {
        long time = buffer.getLong();

        for (String metricName : metricNames)
        {
          buffer.getLong(); // just pass by metrics
        }

        if (lastTime != null && time <= lastTime)
        {
          throw new IllegalStateException(
                  "Time buckets for dimension "
                          + Arrays.toString(currentDimensions) + " are not ordered: " + lastTime + " -> " + time);
        }

        lastTime = time;
      }

      // Update last dimensions
      if (lastDimensions == null)
      {
        lastDimensions = new int[dimensionNames.size()];
      }
      System.arraycopy(currentDimensions, 0, lastDimensions, 0, currentDimensions.length);
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
    int[] dimensions = new int[dimensionNames.size()];

    for (int i = 0; i < dimensionNames.size(); i++)
    {
      String dimensionName = dimensionNames.get(i);
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
   * Populates dimensions parameter with dimensions in buffer and advances position
   */
  private void getDimensions(int[] dimensions)
  {
    for (int i = 0; i < dimensionNames.size(); i++)
    {
      dimensions[i] = buffer.getInt();
    }
  }

  /**
   * Populates metrics with values from buffer and returns corresponding time
   */
  private long getMetrics(ByteBuffer buffer, long[] metrics)
  {
    long time = buffer.getLong();

    for (int i = 0; i < metricNames.size(); i++)
    {
      metrics[i] = buffer.getLong();
    }

    return time;
  }

  /**
   * Adds metrics values to sums if timeBuckets == null or if the exact time is in the buckets
   */
  private void updateSums(long[] sums, Set<Long> timeBuckets)
  {
    long time = buffer.getLong();

    for (int i = 0; i < metricNames.size(); i++)
    {
      long metricValue = buffer.getLong();

      if (timeBuckets == null || timeBuckets.contains(time))
      {
        sums[i] += metricValue;
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
    long[] metrics = new long[metricNames.size()];
    long time = getMetrics(buffer, metrics);
    buffer.reset();

    // Update time
    buffer.putLong(record.getTime());

    // Update metrics
    for (int i = 0; i < metricNames.size(); i++)
    {
      long metricValue = record.getMetricValues().get(metricNames.get(i));
      if (time == record.getTime())
      {
        buffer.putLong(metrics[i] + metricValue);
      }
      else
      {
        buffer.putLong(metricValue);
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
}
