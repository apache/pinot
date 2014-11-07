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
import java.util.concurrent.atomic.AtomicBoolean;

public class StarTreeRecordStoreFixedCircularBufferImpl implements StarTreeRecordStore
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

  private final UUID nodeId;
  private final File file;
  private final List<String> dimensionNames;
  private final List<String> metricNames;
  private final Map<String, Map<String, Integer>> forwardIndex;
  private final Map<String, Map<Integer, String>> reverseIndex;
  private final Map<String, Set<String>> dimensionValues;
  private List<Integer> timeBuckets;
  private final int entrySize;
  private final Object sync;
  private final AtomicBoolean isOpen;

  private MappedByteBuffer buffer;
  private int size;

  public StarTreeRecordStoreFixedCircularBufferImpl(UUID nodeId,
                                                    File file,
                                                    List<String> dimensionNames,
                                                    List<String> metricNames,
                                                    Map<String, Map<String, Integer>> forwardIndex)
  {
    this.nodeId = nodeId;
    this.file = file;
    this.dimensionNames = dimensionNames;
    this.metricNames = metricNames;
    this.forwardIndex = forwardIndex;
    this.reverseIndex = new HashMap<String, Map<Integer, String>>();
    this.dimensionValues = new HashMap<String, Set<String>>();
    this.timeBuckets = new LinkedList<Integer>();
    this.sync = new Object();
    this.isOpen = new AtomicBoolean();

    for (Map.Entry<String, Map<String, Integer>> e1 : forwardIndex.entrySet())
    {
      reverseIndex.put(e1.getKey(), new HashMap<Integer, String>());
      for (Map.Entry<String, Integer> e2 : e1.getValue().entrySet())
      {
        reverseIndex.get(e1.getKey()).put(e2.getValue(), e2.getKey());
      }
    }

    this.entrySize = getEntrySize(dimensionNames, metricNames);
  }

  @Override
  public void update(StarTreeRecord record)
  {
    synchronized (sync)
    {
      // Translate record dimension values to int equivalents
      int[] targetDimensions = new int[dimensionNames.size()];
      for (int i = 0; i < dimensionNames.size(); i++)
      {
        String dimensionName = dimensionNames.get(i);
        String dimensionValue = record.getDimensionValues().get(dimensionName);

        Integer valueId = forwardIndex.get(dimensionName).get(dimensionValue);
        if (valueId == null)
        {
          valueId = StarTreeConstants.OTHER_VALUE;
        }

        targetDimensions[i] = valueId;
      }

      // Find position of row in buffer
      int bucket = (int) (record.getTime() % timeBuckets.size());
      int idx = search(buffer, bucket, targetDimensions);
      if (idx < 0)
      {
        idx = seekClosestMatch(buffer, targetDimensions, bucket);
        if (idx < 0) // still!
        {
          throw new IllegalArgumentException("No match could be found for record " + record);
        }
      }
      buffer.clear();
      buffer.position(idx);

      buffer.getInt(); // bucket

      // Read dimension values
      for (String dimensionName : dimensionNames)
      {
        buffer.getInt();
      }
      buffer.mark();

      // Read time
      long currentTime = buffer.getLong();

      // Read metric values
      long[] metricValues = new long[metricNames.size()];
      for (int i = 0; i < metricNames.size(); i++)
      {
        long baseValue = buffer.getLong();
        if (record.getTime().equals(currentTime))
        {
          metricValues[i] = baseValue + record.getMetricValues().get(metricNames.get(i));
        }
        else
        {
          metricValues[i] = record.getMetricValues().get(metricNames.get(i)); // roll over
        }
      }
      buffer.reset();

      // Re-write time (n.b. may be same as currentTime, but either value will do)
      buffer.putLong(record.getTime());

      // Re-write metric values
      for (long metricValue : metricValues)
      {
        buffer.putLong(metricValue);
      }
    }
  }

  @Override
  public Iterator<StarTreeRecord> iterator()
  {
    List<StarTreeRecord> records = new ArrayList<StarTreeRecord>();

    synchronized (sync)
    {
      buffer.rewind();

      while (buffer.position() < buffer.limit())
      {
        StarTreeRecordImpl.Builder builder = new StarTreeRecordImpl.Builder();

        buffer.getInt(); // bucket

        for (String dimensionName : dimensionNames)
        {
          Map<Integer, String> valueIds = reverseIndex.get(dimensionName);
          int valueId = buffer.getInt();
          String value = valueIds.get(valueId);
          builder.setDimensionValue(dimensionName, value);
        }

        builder.setTime(buffer.getLong());

        for (String metricName : metricNames)
        {
          long metricValue = buffer.getLong();
          builder.setMetricValue(metricName, metricValue);
        }

        records.add(builder.build());
      }

      // Sort by time
      Collections.sort(records, new Comparator<StarTreeRecord>()
      {
        @Override
        public int compare(StarTreeRecord o1, StarTreeRecord o2)
        {
          return (int) (o1.getTime() - o2.getTime());
        }
      });

      return records.iterator();
    }
  }

  @Override
  public void clear()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void open() throws IOException
  {
    if (!isOpen.getAndSet(true))
    {
      FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel();
      buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, file.length());
      buffer.rewind();

      // Check buffer sort order
      int[] lastDimensions = new int[dimensionNames.size()];
      int[] currentDimensions = new int[dimensionNames.size()];
      Integer lastBucket = null;
      Long lastTime = null;
      while (buffer.position() < buffer.limit())
      {
        // Get dimensions / time
        int currentBucket = buffer.getInt();
        for (int i = 0; i < dimensionNames.size(); i++)
        {
          int valueId = buffer.getInt();

          // Set int value
          currentDimensions[i] = valueId;

          // Record dimension value
          String dimensionName = dimensionNames.get(i);
          Set<String> values = dimensionValues.get(dimensionName);
          if (values == null)
          {
            values = new HashSet<String>();
            dimensionValues.put(dimensionName, values);
          }
          values.add(reverseIndex.get(dimensionName).get(valueId));
        }

        long currentTime = buffer.getLong(); // time

        // Check ordering
        if (lastBucket != null) // i.e. not first
        {
          int compareResult = 0;
          if (!lastBucket.equals(currentBucket))
          {
            compareResult = lastBucket - currentBucket;
          }
          else if (!Arrays.equals(lastDimensions, currentDimensions))
          {
            compareResult = DIMENSION_COMBINATION_COMPARATOR.compare(lastDimensions, currentDimensions);
          }
          else
          {
            compareResult = (int) (lastTime - currentTime);
          }

          if (compareResult >= 0)
          {
            throw new IllegalStateException(
                    "(" + nodeId + ") " +
                    "Buffer is not sorted by time then dimensions: " +
                            "last=" + lastBucket + ":" + Arrays.toString(lastDimensions) +
                            "; current=" + currentBucket + ":" + Arrays.toString(currentDimensions));
          }
        }

        // Read through the metrics to position at next record
        for (String metricName : metricNames)
        {
          buffer.getLong();
        }

        // Add time bucket to list of all
        if (lastBucket == null || lastBucket < currentBucket)
        {
          timeBuckets.add(currentBucket);
        }

        // Reset last time
        System.arraycopy(currentDimensions, 0, lastDimensions, 0, currentDimensions.length);
        lastBucket = currentBucket;
        lastTime = currentTime;

        // Update size
        size++;
      }
    }
  }

  @Override
  public void close() throws IOException
  {
    if (isOpen.getAndSet(false))
    {
      buffer.force();
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
    return values == null ? 0 : values.size();
  }

  @Override
  public String getMaxCardinalityDimension()
  {
    return getMaxCardinalityDimension(null);
  }

  @Override
  public String getMaxCardinalityDimension(Collection<String> blacklist)
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

      int[] targetDimensions = new int[dimensionNames.size()];
      for (int i = 0; i < dimensionNames.size(); i++)
      {
        String dimensionName = dimensionNames.get(i);
        String dimensionValue = query.getDimensionValues().get(dimensionName);
        Integer valueId = forwardIndex.get(dimensionName).get(dimensionValue);
        if (valueId == null)
        {
          valueId = StarTreeConstants.OTHER_VALUE; // Haven't seen it, so alias to other
        }
        targetDimensions[i] = valueId;
      }

      Set<Integer> missedBuckets = new HashSet<Integer>();

      if (query.getTimeBuckets() != null)
      {
        for (Long time : query.getTimeBuckets())
        {
          int bucket = (int) (time % timeBuckets.size());
          int idx = search(buffer, bucket, targetDimensions);
          if (idx >= 0)
          {
            buffer.position(idx);
            updateSums(buffer, sums, time);
          }
          else
          {
            missedBuckets.add(bucket);
          }
        }
      }
      else if (query.getTimeRange() != null)
      {
        for (long time = query.getTimeRange().getKey(); time <= query.getTimeRange().getValue(); time++)
        {
          int bucket = (int) (time % timeBuckets.size());
          int idx = search(buffer, bucket, targetDimensions);
          if (idx >= 0)
          {
            buffer.position(idx);
            updateSums(buffer, sums, time);
          }
          else
          {
            missedBuckets.add(bucket);
          }
        }
      }
      else // Everything
      {
        for (int bucket : timeBuckets)
        {
          int idx = search(buffer, bucket, targetDimensions);
          if (idx >= 0)
          {
            buffer.position(idx);
            updateSums(buffer, sums, -1);
          }
          else
          {
            missedBuckets.add(bucket);
          }
        }
      }

      // Stragglers
      if (!missedBuckets.isEmpty())
      {
        filterAggregate(buffer, sums, targetDimensions, missedBuckets);
      }

      return sums;
    }
  }

  /**
   * Reads the next record in the buffer, and updates sums if the time matches the expected time
   */
  private void updateSums(ByteBuffer buffer, long[] sums, long expectedTime)
  {
    buffer.getInt(); // bucket

    for (String dimensionName : dimensionNames)
    {
      buffer.getInt();
    }

    long currentTime = buffer.getLong(); // time

    if (expectedTime < 0 || currentTime == expectedTime)
    {
      for (int i = 0; i < metricNames.size(); i++)
      {
        sums[i] += buffer.getLong();
      }
    }
  }

  /**
   * Scans the provided buckets for records that match targetDimensions and updates sums with their metric values.
   */
  private void filterAggregate(ByteBuffer buffer, long[] sums, int[] targetDimensions, Set<Integer> buckets)
  {
    for (Integer bucket : buckets)
    {
      int idx = seekBucket(buffer, bucket);
      buffer.clear();
      buffer.position(idx);

      int currentBucket;
      int[] currentDimensions = new int[targetDimensions.length];

      while (buffer.position() < buffer.limit())
      {
        currentBucket = buffer.getInt();

        if (currentBucket != bucket)
        {
          break;
        }

        boolean dimensionsOk = true;

        for (int i = 0; i < dimensionNames.size(); i++)
        {
          currentDimensions[i] = buffer.getInt();

          if (targetDimensions[i] != StarTreeConstants.STAR_VALUE && targetDimensions[i] != currentDimensions[i])
          {
            dimensionsOk = false;
          }
        }

        buffer.getLong(); // time

        for (int i = 0; i < metricNames.size(); i++)
        {
          long metricValue = buffer.getLong();

          if (dimensionsOk)
          {
            sums[i] += metricValue;
          }
        }
      }
    }
  }

  /**
   * Performs binary search to determine position of target dimensions / bucket in buffer
   *
   * @return
   *  -1 if the combination wasn't found
   */
  private int search(ByteBuffer buffer, long targetBucket, int[] targetDimensions)
  {
    buffer.clear();

    int[] currentDimensions = new int[dimensionNames.size()];

    while (buffer.position() < buffer.limit())
    {
      int idx = ((buffer.limit() + buffer.position()) / entrySize / 2) * entrySize;

      buffer.mark();
      buffer.position(idx);

      int currentBucket = buffer.getInt();
      for (int i = 0; i < currentDimensions.length; i++)
      {
        currentDimensions[i] = buffer.getInt();
      }
      long currentTime = buffer.getLong();

      buffer.reset();

      int compareResult = targetBucket != currentBucket
              ? (int) (targetBucket - currentBucket)
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

  /**
   * Seeks to the first entry in targetBucket (assumes the bucket exists)
   */
  private int seekBucket(ByteBuffer buffer, int targetBucket)
  {
    buffer.clear(); // If bucket is 0, we're done

    if (targetBucket > 0)
    {
      // We want to find AN index in previous bucket, then seek to first index thereafter
      int previousBucket = targetBucket - 1;

      int startIdx = -1;

      while (buffer.position() < buffer.limit())
      {
        // Determine midpoint
        int idx = ((buffer.limit() + buffer.position()) / entrySize / 2) * entrySize;

        // Read bucket value
        buffer.mark();
        buffer.position(idx);
        int currentBucket = buffer.getInt();
        buffer.reset();

        // Compare
        if (previousBucket == currentBucket)
        {
          startIdx = idx;
          break;
        }
        else if (previousBucket < currentBucket)
        {
          buffer.limit(idx);
        }
        else
        {
          buffer.position(idx + entrySize);
        }
      }

      // Check valid
      if (buffer.position() == buffer.limit())
      {
        throw new IllegalStateException("Could not find target bucket " + targetBucket);
      }

      // Seek until next bucket
      buffer.clear();
      buffer.position(startIdx);
      int currentBucket = previousBucket;
      while (buffer.position() < buffer.limit())
      {
        buffer.mark();

        // Read bucket
        currentBucket = buffer.getInt();
        if (currentBucket == targetBucket)
        {
          break;
        }

        // Otherwise, advance to next record
        for (String dimensionName : dimensionNames)
        {
          buffer.getInt();
        }
        buffer.getLong(); // time
        for (String metricName : metricNames)
        {
          buffer.getLong();
        }
      }

      buffer.reset(); // At first record now

    }

    return buffer.position();
  }

  /**
   * Returns the position of the record in the buffer with fewest "other" dimension values.
   *
   * <p>
   *   This assumes there is a record in the buffer in each bucket that is all "other" dimension
   *   values, so something is guaranteed to be returned every time.
   * </p>
   */
  private int seekClosestMatch(ByteBuffer buffer, int[] targetDimensions, int targetBucket)
  {
    int idx = -1;
    int otherScore = -1;

    // Seek to the target bucket
    int bucketStartIdx = seekBucket(buffer, targetBucket);
    buffer.clear();
    buffer.position(bucketStartIdx);

    // Scan the target bucket for position of record with fewest "other" dimension values
    int[] currentDimensions = new int[targetDimensions.length];
    while (buffer.position() < buffer.limit())
    {
      int currentIdx = buffer.position();

      // Check bucket
      int currentBucket = buffer.getInt();
      if (currentBucket != targetBucket)
      {
        break;
      }

      // Get dimensions
      int currentScore = 0;
      boolean matches = true;
      for (int i = 0; i < dimensionNames.size(); i++)
      {
        currentDimensions[i] = buffer.getInt();

        // Compute score
        if (currentDimensions[i] == targetDimensions[i])
        {
          currentScore += 0;
        }
        else if (currentDimensions[i] == StarTreeConstants.OTHER_VALUE)
        {
          currentScore += 1;
        }
        else
        {
          matches = false;
        }
      }

      // Check score
      if (matches && (otherScore < 0 || currentScore < otherScore))
      {
        otherScore = currentScore;
        idx = currentIdx;
      }

      // Move past time / metrics
      buffer.getLong();
      for (String metricName : metricNames)
      {
        buffer.getLong();
      }
    }

    return idx;
  }

  /**
   * Utility function to write a record into a buffer in the format used by this implementation.
   */
  public static void writeRecord(ByteBuffer buffer,
                                 StarTreeRecord record,
                                 List<String> dimensionNames,
                                 List<String> metricNames,
                                 Map<String, Map<String, Integer>> forwardIndex,
                                 int numTimeBuckets)
  {
    buffer.putInt((int) (record.getTime() % numTimeBuckets));

    for (String dimensionName : dimensionNames)
    {
      buffer.putInt(forwardIndex.get(dimensionName).get(record.getDimensionValues().get(dimensionName)));
    }

    buffer.putLong(record.getTime());

    for (String metricName : metricNames)
    {
      buffer.putLong(record.getMetricValues().get(metricName));
    }
  }

  /**
   * Utility function to determine a record size in the buffer for this implementation.
   */
  public static int getEntrySize(List<String> dimensionNames, List<String> metricNames)
  {
    return (dimensionNames.size() + 1) * Integer.SIZE / 8 + (metricNames.size() + 1) * Long.SIZE / 8;
  }
}
