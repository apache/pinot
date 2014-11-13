package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.StarTreeRecordStore;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
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

  protected final Object sync;

  protected boolean isOpen;
  protected ByteBuffer buffer;
  private int size;

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
    this.timeBucketSize = metricNames.size() * Integer.SIZE / 8 + Long.SIZE / 8; // plus time
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
  public int getEntrySize()
  {
    return entrySize;
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
    synchronized (sync)
    {
      List<StarTreeRecord> list = new LinkedList<StarTreeRecord>();

      buffer.clear();

      int[] currentDimensions = new int[dimensionNames.size()];
      int[] currentMetrics = new int[metricNames.size()];

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

          for (int j = 0; j < metricNames.size(); j++)
          {
            builder.setMetricValue(metricNames.get(j), currentMetrics[j]);
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
        for (String dimensionName : dimensionNames)
        {
          buffer.getInt();
        }

        for (int i = 0; i < numTimeBuckets; i++)
        {
          buffer.getLong(); // time

          // Set all metric values in buffer to zero
          for (String metricName : metricNames)
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
      }
    }
  }

  @Override
  public int size()
  {
    return size;
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
  public int[] getMetricSums(StarTreeQuery query)
  {
    synchronized (sync)
    {
      int[] sums = new int[metricNames.size()];

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
        buffer.position(idx + dimensionSize);

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
          for (int i = 0; i < numTimeBuckets; i++)
          {
            if (matches(targetDimensions, currentDimensions))
            {
              updateSums(sums, timeBuckets);
            }
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
        long time = buffer.getLong();

        if (time % numTimeBuckets != i)
        {
          throw new IllegalStateException("Time bucket violation: " + time + " % " + numTimeBuckets + " != " + i);
        }

        for (String metricName : metricNames)
        {
          buffer.getInt(); // just pass by metrics
        }
      }

      // Update last dimensions
      if (lastDimensions == null)
      {
        lastDimensions = new int[dimensionNames.size()];
      }
      System.arraycopy(currentDimensions, 0, lastDimensions, 0, currentDimensions.length);

      size++;
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
   * Translates int-encoded dimension values to their string counterparts
   */
  private Map<String, String> translateDimensions(int[] dimensionValues)
  {
    Map<String, String> dimensions = new HashMap<String, String>();

    for (int i = 0; i < dimensionNames.size(); i++)
    {
      String dimensionName = dimensionNames.get(i);
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
    for (int i = 0; i < dimensionNames.size(); i++)
    {
      dimensions[i] = buffer.getInt();
    }
  }

  /**
   * Populates metrics with values from buffer and returns corresponding time
   */
  private long getMetrics(int[] metrics)
  {
    long time = buffer.getLong();

    for (int i = 0; i < metricNames.size(); i++)
    {
      metrics[i] = buffer.getInt();
    }

    return time;
  }

  /**
   * Adds metrics values to sums if timeBuckets == null or if the exact time is in the buckets
   */
  private void updateSums(int[] sums, Set<Long> timeBuckets)
  {
    long time = buffer.getLong();

    for (int i = 0; i < metricNames.size(); i++)
    {
      int metricValue = buffer.getInt();

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
    int[] metrics = new int[metricNames.size()];
    long time = getMetrics(metrics);
    buffer.reset();

    // Update time
    buffer.putLong(record.getTime());

    // Update metrics
    for (int i = 0; i < metricNames.size(); i++)
    {
      int metricValue = record.getMetricValues().get(metricNames.get(i));
      if (time == record.getTime())
      {
        buffer.putInt(metrics[i] + metricValue);
      }
      else
      {
        buffer.putInt(metricValue);
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
                                final List<String> dimensionNames,
                                final List<String> metricNames,
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
        for (String dimensionName : dimensionNames)
        {
          String v1 = c1.get(dimensionName);
          String v2 = c2.get(dimensionName);
          int i1 = forwardIndex.get(dimensionName).get(v1);
          int i2 = forwardIndex.get(dimensionName).get(v2);

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
      List<StarTreeRecord> merged = new ArrayList<StarTreeRecord>();
      Set<Integer> representedBuckets = new HashSet<Integer>();
      for (List<StarTreeRecord> rs : timeGroup.values())
      {
        StarTreeRecord r = StarTreeUtils.merge(rs);
        merged.add(r);
        representedBuckets.add((int) (r.getTime() % numTimeBuckets));
      }

      // Add any times in range we haven't seen w/ dummy record (n.b. bucket instead of specific time will suffice)
      for (int i = 0; i < numTimeBuckets; i++)
      {
        if (!representedBuckets.contains(i))
        {
          StarTreeRecordImpl.Builder builder = new StarTreeRecordImpl.Builder();
          builder.setDimensionValues(combination).setTime((long) i);
          for (String metricName : metricNames)
          {
            builder.setMetricValue(metricName, 0);
          }
          merged.add(builder.build());
        }
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
      for (String dimensionName : dimensionNames)
      {
        String dimensionValue = combination.get(dimensionName);
        Integer valueId = forwardIndex.get(dimensionName).get(dimensionValue);
        if (valueId == null)
        {
          throw new IllegalStateException("No ID for dimension value " + dimensionName + ":" + dimensionValue);
        }
        dataOutputStream.writeInt(valueId);
      }

      // Fill in time buckets w/ time + metrics
      for (StarTreeRecord record : merged)
      {
        dataOutputStream.writeLong(record.getTime());
        for (String metricName : metricNames)
        {
          if (keepMetricValues)
          {
            dataOutputStream.writeInt(record.getMetricValues().get(metricName));
          }
          else
          {
            dataOutputStream.writeInt(0);
          }
        }
      }
    }
  }

  /**
   * Dumps a line-by-line string representation of a buffer
   */
  public static void dumpBuffer(ByteBuffer externalBuffer,
                                OutputStream outputStream,
                                List<String> dimensionNames,
                                List<String> metricNames,
                                int numTimeBuckets) throws IOException
  {
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));

    externalBuffer.mark();

    int[] currentDimensions = new int[dimensionNames.size()];
    int[] currentMetrics = new int[metricNames.size()];

    while (externalBuffer.position() < externalBuffer.limit())
    {
      // Dimensions
      for (int i = 0; i < dimensionNames.size(); i++)
      {
        currentDimensions[i] = externalBuffer.getInt();
      }

      writer.write(Arrays.toString(currentDimensions));
      writer.newLine();

      // Each metric time bucket
      for (int i = 0; i < numTimeBuckets; i++)
      {
        long time = externalBuffer.getLong();

        for (int j = 0; j < metricNames.size(); j++)
        {
          currentMetrics[j] = externalBuffer.getInt();
        }

        writer.write("|--");
        writer.write(Arrays.toString(currentMetrics));
        writer.write("@");
        writer.write(Long.toString(time));
        writer.newLine();
      }

      writer.newLine();
    }

    externalBuffer.reset();

    writer.flush();
  }
}
