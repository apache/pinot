package com.linkedin.thirdeye.dataframe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang.ArrayUtils;


public abstract class Grouping {
  public static final String GROUP_KEY = "key";
  public static final String GROUP_VALUE = "value";

  final Series keys;

  Grouping(Series keys) {
    this.keys = keys;
  }

  /**
   * Applies {@code function} as aggregation function to all values per group and
   * returns the result as a new DataFrame with the number of elements equal to the size
   * of the key series.
   * If the series' native types do not match the required input type of {@code function},
   * the series are converted transparently. The native type of the aggregated series is
   * determined by {@code function}'s output type.
   *
   * @param s input series to apply grouping to
   * @param function aggregation function to map to each grouped series
   * @return grouped aggregation series
   */
  GroupingDataFrame aggregate(Series s, Series.Function function) {
    Series.Builder builder = s.getBuilder();
    for(int i=0; i<this.size(); i++) {
      builder.addSeries(this.apply(s, i).aggregate(function));
    }
    return new GroupingDataFrame(GROUP_KEY, GROUP_VALUE, this.keys, builder.build());
  }

  /**
   * Counts the number of elements in each group and returns the result as a new DataFrame
   * with the number of elements equal to the size of the key series.
   *
   * @param s input series to apply grouping to
   * @return group sizes
   */
  public GroupingDataFrame count(Series s) {
    long[] values = new long[this.size()];
    for(int i=0; i<this.size(); i++) {
      values[i++] = this.apply(s, i).size();
    }
    return new GroupingDataFrame(GROUP_KEY, GROUP_VALUE, this.keys, LongSeries.buildFrom(values));
  }

  /**
   * Returns the number of groups
   *
   * @return group count
   */
  public int size() {
    return this.keys.size();
  }

  /**
   * Returns the keys of each group in the container as series.
   *
   * @return key series
   */
  public Series keys() {
    return this.keys;
  }

  /**
   * Returns {@code true} if the grouping container does not hold any groups.
   *
   * @return {@code true} is empty, {@code false} otherwise.
   */
  public boolean isEmpty() {
    return this.keys.isEmpty();
  }

  /**
   * Generates a concrete instance of the group indicated by index for a given series.
   * <br/><b>INVARIANT:</b> the caller guarantees that group index will be between {@code 0}
   * and {@code size() - 1}, and that the method will not be called for groupings of size
   * {@code 0}.
   *
   * @param s input series
   * @param groupIndex group index
   * @return instance of group
   */
  abstract Series apply(Series s, int groupIndex);

  /**
   * Grouping container referencing a single series. Holds group keys and the indices of group
   * elements in the source series. Enables aggregation with custom user functions.
   */
  public static class SeriesGrouping extends Grouping {
    final Series source;
    final Grouping grouping;

    SeriesGrouping(Series source, Grouping grouping) {
      super(grouping.keys);
      this.source = source;
      this.grouping = grouping;
    }

    /**
     * Returns the SeriesGrouping's source series.
     *
     * @return source series
     */
    public Series source() {
      return this.source;
    }

    /**
     * @see Grouping#aggregate(Series, Series.Function)
     */
    public GroupingDataFrame aggregate(Series.Function function) {
      return super.aggregate(this.source, function);
    }

    /**
     * @see Grouping#count(Series)
     */
    public GroupingDataFrame count() {
      return super.count(this.source);
    }

    @Override
    Series apply(Series s, int groupIndex) {
      return this.grouping.apply(s, groupIndex);
    }

    Series apply(int groupIndex) {
      return this.grouping.apply(this.source, groupIndex);
    }
  }

  /**
   * Container object for the grouping of multiple rows across different series
   * based on a common key.
   */
  public static class DataFrameGrouping extends Grouping {
    final String keyName;
    final DataFrame source;
    final Grouping grouping;

    DataFrameGrouping(String keyName, DataFrame source, Grouping grouping) {
      super(grouping.keys);
      this.keyName = keyName;
      this.source = source;
      this.grouping = grouping;
    }

    /**
     * Returns the DataFrameGrouping's source DataFrame.
     *
     * @return source DataFrame
     */
    public DataFrame source() {
      return this.source;
    }

    /**
     * @see Grouping#aggregate(Series, Series.Function)
     */
    public GroupingDataFrame aggregate(String seriesName, Series.Function function) {
      return super.aggregate(this.source.get(seriesName), function)
          .withKeyName(this.keyName).withValueName(seriesName);
    }

    /**
     * Counts the number of elements in each group and returns the result as a new DataFrame
     * with the number of elements equal to the size of the key series.
     *
     * @return group sizes
     */
    public GroupingDataFrame count() {
      if(this.source.isEmpty()) {
        return new GroupingDataFrame(this.keyName, GROUP_VALUE, this.keys, LongSeries.zeros(this.size()));
      }
      // TODO data frames without index
      return this.grouping.count(this.source.getIndex());
    }

    @Override
    Series apply(Series s, int groupIndex) {
      return this.grouping.apply(s, groupIndex);
    }

    Series apply(String seriesName, int groupIndex) {
      return this.grouping.apply(this.source.get(seriesName), groupIndex);
    }
  }

  /**
   * GroupingDataFrame holds the result of a series aggregation after grouping. It functions like
   * a regular DataFrame, but provides additional comfort for accessing key and value columns.
   *
   * @see DataFrame
   */
  public static final class GroupingDataFrame extends DataFrame {
    final String keyName;
    final String valueName;

    GroupingDataFrame(String keyName, String valueName, Series keys, Series values) {
      this.keyName = keyName;
      this.valueName = valueName;
      this.addSeries(keyName, keys);
      this.addSeries(valueName, values);
      this.setIndex(keyName);
    }

    public Series getKeys() {
      return this.get(this.keyName);
    }

    public Series getValues() {
      return this.get(this.valueName);
    }

    public String getKeyName() {
      return this.keyName;
    }

    public String getValueName() {
      return this.valueName;
    }

    GroupingDataFrame withKeyName(String keyName) {
      return new GroupingDataFrame(keyName, this.valueName, this.get(this.keyName), this.get(this.valueName));
    }

    GroupingDataFrame withValueName(String valueName) {
      return new GroupingDataFrame(this.keyName, valueName, this.get(this.keyName), this.get(this.valueName));
    }
  }

//  public final SeriesGrouping groupByValue() {
//    if(this.isEmpty())
//      return new SeriesGrouping(this);
//
//    List<ArrayBucket> buckets = new ArrayList<>();
//    int[] sref = this.sortedIndex();
//
//    int bucketOffset = 0;
//    for(int i=1; i<sref.length; i++) {
//      if(this.compare(this, sref[i-1], sref[i]) != 0) {
//        int[] fromIndex = Arrays.copyOfRange(sref, bucketOffset, i);
//        buckets.add(new ArrayBucket(fromIndex));
//        bucketOffset = i;
//      }
//    }
//
//    int[] fromIndex = Arrays.copyOfRange(sref, bucketOffset, sref.length);
//    buckets.add(new ArrayBucket(fromIndex));
//
//    // keys from buckets
//    int[] keyIndex = new int[buckets.size()];
//    int i = 0;
//    for(ArrayBucket b : buckets) {
//      keyIndex[i++] = b.indices[0];
//    }
//
//    return new SeriesGrouping(this.project(keyIndex), this, buckets);
//  }

  /**
   * Represents a Grouping based on value. Elements are grouped into separate buckets for each
   * distinct value in the series.
   * <br/><b>NOTE:</b> the resulting keys are equivalent to calling {@code unique()} on the series.
   */
  public static final class GroupingByValue extends Grouping {
    private final List<int[]> buckets;

    private GroupingByValue(Series keys, List<int[]> buckets) {
      super(keys);
      this.buckets = buckets;
    }

    @Override
    Series apply(Series s, int groupIndex) {
      return s.project(this.buckets.get(groupIndex));
    }

    public static GroupingByValue from(Series series) {
      if(series.isEmpty())
        return new GroupingByValue(series.getBuilder().build(), new ArrayList<int[]>());

      List<int[]> buckets = new ArrayList<>();
      int[] sref = series.sortedIndex();

      int bucketOffset = 0;
      for(int i=1; i<sref.length; i++) {
        if(series.compare(series, sref[i-1], sref[i]) != 0) {
          int[] fromIndex = Arrays.copyOfRange(sref, bucketOffset, i);
          buckets.add(fromIndex);
          bucketOffset = i;
        }
      }

      int[] fromIndex = Arrays.copyOfRange(sref, bucketOffset, sref.length);
      buckets.add(fromIndex);

      // keys from buckets
      int[] keyIndex = new int[buckets.size()];
      int i = 0;
      for(int[] b : buckets) {
        keyIndex[i++] = b[0];
      }

      return new GroupingByValue(series.project(keyIndex), buckets);
    }
  }

  /**
   * Represents a Grouping based on value intervals. Elements are grouped into separate buckets
   * for each distinct interval between the min and max elements of the series.
   * <br/><b>NOTE:</b> requires a numeric series. Produces a LongSeries as keys.
   */
  public static final class GroupingByInterval extends Grouping {
    private final List<int[]> buckets;

    private GroupingByInterval(Series keys, List<int[]> buckets) {
      super(keys);
      this.buckets = buckets;
    }

    @Override
    Series apply(Series s, int groupIndex) {
      return s.project(this.buckets.get(groupIndex));
    }

    public static GroupingByInterval from(Series series, long interval) {
      if(interval <= 0)
        throw new IllegalArgumentException("interval must be > 0");
      if(series.isEmpty())
        return new GroupingByInterval(series.getBuilder().build(), new ArrayList<int[]>());

      LongSeries s = series.getLongs();
      int start = (int)(s.min().value() / interval);
      int stop = (int)(s.max().value() / interval + 1);
      int count = stop - start;

      long[] keys = new long[count];
      List<List<Integer>> buckets = new ArrayList<>();
      for(int i=0; i<count; i++) {
        keys[i] = (i + start) * interval;
        buckets.add(new ArrayList<Integer>());
      }

      long[] values = s.values();
      for(int i=0; i<s.size(); i++) {
        int bucketIndex = (int)((values[i] - (start * interval)) / interval);
        buckets.get(bucketIndex).add(i);
      }

      List<int[]> arrayBuckets = new ArrayList<>();
      for(List<Integer> b : buckets) {
        arrayBuckets.add(ArrayUtils.toPrimitive(b.toArray(new Integer[b.size()])));
      }

      return new GroupingByInterval(LongSeries.buildFrom(keys), arrayBuckets);
    }
  }

  /**
   * Represents a SeriesGrouping based on element count per buckets. Elements are grouped into buckets
   * based on a greedy algorithm with fixed bucket size. The size of all buckets (except for the
   * last) is guaranteed to be equal to {@code bucketSize}.
   */
  public static final class GroupingByCount extends Grouping {
    final int partitionSize;
    final int size;

    private GroupingByCount(Series keys, int partitionSize, int size) {
      super(keys);
      this.partitionSize = partitionSize;
      this.size = size;
    }

    @Override
    Series apply(Series s, int groupIndex) {
      int from = groupIndex * this.partitionSize;
      int to = Math.min((groupIndex + 1) * this.partitionSize, this.size);
      return s.slice(from, to);
    }

    private static LongSeries makeKeys(int size, int partitionSize) {
      int numPartitions = (size - 1) / partitionSize + 1;
      return LongSeries.sequence(0, numPartitions);
    }

    public static GroupingByCount from(int partitionSize, int size) {
      if(partitionSize <= 0)
        throw new IllegalArgumentException("partitionSize must be > 0");
      return new GroupingByCount(makeKeys(size, partitionSize), partitionSize, size);
    }
  }

//  public final SeriesGrouping groupByCount(int bucketSize) {
//    return groupByCount(this.size(), bucketSize).applyTo(this);
//  }
//
//  /**
//   * @see Series#groupByCount(int)
//   */
//  static final SeriesGrouping groupByCount(int size, int bucketSize) {
//    if(bucketSize <= 0)
//      throw new IllegalArgumentException("bucketSize must be greater than 0");
//    if(size <= 0)
//      return new SeriesGrouping(null);
//
//    bucketSize = Math.min(bucketSize, size);
//
//    int numBuckets = (size - 1) / bucketSize + 1;
//    long[] keys = new long[numBuckets];
//    List<Bucket> buckets = new ArrayList<>();
//    for(int i=0; i<numBuckets; i++) {
//      int from = i * bucketSize;
//      int to = Math.min((i + 1) * bucketSize, size);
//      buckets.add(new SequenceBucket(from, to));
//      keys[i] = i;
//    }
//    return new SeriesGrouping(DataFrame.toSeries(keys), null, buckets);
//  }

//  public final SeriesGrouping groupByPartitions(int partitionCount) {
//    return groupByPartitions(this.size(), partitionCount).applyTo(this);
//  }
//
//  /**
//   * @see Series#groupByPartitions(int)
//   */
//  static final SeriesGrouping groupByPartitions(int size, int partitionCount) {
//    if(partitionCount <= 0)
//      throw new IllegalArgumentException("partitionCount must be greater than 0");
//    if(size <= 0)
//      return new SeriesGrouping(null);
//
//    double perPartition = size /  (double)partitionCount;
//
//    long[] keys = new long[partitionCount];
//    List<Bucket> buckets = new ArrayList<>();
//    for(int i=0; i<partitionCount; i++) {
//      int from = (int)Math.round(i * perPartition);
//      int to = (int)Math.round((i+1) * perPartition);
//      buckets.add(new SequenceBucket(from, to));
//      keys[i] = i;
//    }
//    return new SeriesGrouping(DataFrame.toSeries(keys), null, buckets);
//  }

  /**
   * Represents a Grouping based on a fixed number of buckets. Elements are grouped into buckets
   * based on a greedy algorithm to approximately evenly fill buckets. The number of buckets
   * is guaranteed to be equal to {@code partitionCount} even if some remain empty.
   */
  public static final class GroupingByPartitions extends Grouping {
    final int partitionCount;
    final int size;

    private GroupingByPartitions(Series keys, int partitionCount, int size) {
      super(keys);
      this.partitionCount = partitionCount;
      this.size = size;
    }

    @Override
    Series apply(Series s, int groupIndex) {
      double perPartition = this.size / (double)this.partitionCount;
      int from = (int)Math.round(groupIndex * perPartition);
      int to = (int)Math.round((groupIndex + 1) * perPartition);
      return s.slice(from, to);
    }

    public static GroupingByPartitions from(int partitionCount, int size) {
      if(partitionCount <= 0)
        throw new IllegalArgumentException("partitionCount must be > 0");
      return new GroupingByPartitions(LongSeries.sequence(0, partitionCount), partitionCount, size);
    }
  }

//  public final SeriesGrouping groupByMovingWindow(int windowSize) {
//    return groupByMovingWindow(this.size(), windowSize).applyTo(this);
//  }
//
//  /**
//   * @see Series#groupByMovingWindow(int)
//   */
//  static final SeriesGrouping groupByMovingWindow(int size, int windowSize) {
//    if(windowSize <= 0)
//      throw new IllegalArgumentException("windowSize must be greater than 0");
//    if(size < windowSize)
//      return new SeriesGrouping(null);
//
//    int windowCount = size - windowSize + 1;
//    long[] keys = new long[windowCount];
//    List<Bucket> buckets = new ArrayList<>();
//    for(int i=0; i<windowCount; i++) {
//      buckets.add(new SequenceBucket(i, i + windowSize));
//      keys[i] = i;
//    }
//    return new SeriesGrouping(DataFrame.toSeries(keys), null, buckets);
//  }

  /**
   * Represents an (overlapping) Grouping based on a moving window size. Elements are grouped
   * into overlapping buckets in sequences of {@code windowSize} consecutive items. The number
   * of buckets is guaranteed to be equal to {@code series_size - moving_window_size + 1}, or
   * 0 if the window size is greater than the series size.
   */
  public static final class GroupingByMovingWindow extends Grouping {
    final int windowSize;
    final int size;

    private GroupingByMovingWindow(Series keys, int windowSize, int size) {
      super(keys);
      this.windowSize = windowSize;
      this.size = size;
    }

    @Override
    Series apply(Series s, int groupIndex) {
      return s.slice(groupIndex, groupIndex + this.windowSize);
    }

    public static GroupingByMovingWindow from(int windowSize, int size) {
      if(windowSize <= 0)
        throw new IllegalArgumentException("windowSize must be > 0");
      return new GroupingByMovingWindow(LongSeries.sequence(0, size - windowSize + 1), windowSize, size);
    }
  }

//  public final SeriesGrouping groupByExpandingWindow() {
//    return groupByExpandingWindow(this.size()).applyTo(this);
//  }
//
//  /**
//   * @see Series#groupByExpandingWindow()
//   */
//  static final SeriesGrouping groupByExpandingWindow(int size) {
//    if(size <= 0)
//      return new SeriesGrouping(null);
//
//    long[] keys = new long[size];
//    List<Bucket> buckets = new ArrayList<>();
//    for(int i=0; i<size; i++) {
//      buckets.add(new SequenceBucket(0, i + 1));
//      keys[i] = i;
//    }
//    return new SeriesGrouping(DataFrame.toSeries(keys), null, buckets);
//  }
  /**
   * Represents an (overlapping) Grouping based on an expanding window. Elements are grouped
   * into overlapping buckets in expanding sequences of consecutive items (always starting with
   * index {@code 0}). The number of buckets is guaranteed to be equal to {@code series_size}.
   */
  public static final class GroupingByExpandingWindow extends Grouping {
    final int size;

    private GroupingByExpandingWindow(Series keys, int size) {
      super(keys);
      this.size = size;
    }

    @Override
    Series apply(Series s, int groupIndex) {
      return s.slice(0, groupIndex + 1);
    }

    public static GroupingByExpandingWindow from(int size) {
      return new GroupingByExpandingWindow(LongSeries.sequence(0, size), size);
    }
  }

  /**
   * Represents a static, user-defined Grouping. The grouping is defined via buckets of indices.
   */
  public static final class GroupingStatic extends Grouping {
    final List<int[]> buckets;

    private GroupingStatic(Series keys, List<int[]> buckets) {
      super(keys);
      this.buckets = buckets;
    }

    @Override
    Series apply(Series s, int groupIndex) {
      return s.project(this.buckets.get(groupIndex));
    }

    public static GroupingStatic from(Series keys, List<int[]> buckets) {
      return new GroupingStatic(keys, buckets);
    }

    public static GroupingStatic from(Series keys, int[]... buckets) {
      return from(keys, Arrays.asList(buckets));
    }
  }
}
