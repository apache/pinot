/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.dataframe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.commons.lang3.ArrayUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;
import org.joda.time.DateTimeZone;
import org.joda.time.DurationFieldType;
import org.joda.time.LocalDateTime;
import org.joda.time.Period;
import org.joda.time.PeriodType;

import static org.apache.pinot.thirdeye.dataframe.DoubleSeries.*;
import static org.apache.pinot.thirdeye.dataframe.util.DataFrameUtils.*;


public abstract class Grouping {
  public static final String GROUP_KEY = "key";
  public static final String GROUP_VALUE = "value";

  public static final String OP_SUM = "SUM";
  public static final String OP_PRODUCT = "PRODUCT";
  public static final String OP_MIN = "MIN";
  public static final String OP_MAX = "MAX";
  public static final String OP_FIRST = "FIRST";
  public static final String OP_LAST = "LAST";
  public static final String OP_MEAN = "MEAN";
  public static final String OP_MEDIAN = "MEDIAN";
  public static final String OP_STD = "STD";

  // TODO generate keys on-demand only
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
    return makeResult(builder.build());
  }

  /**
   * Counts the number of elements in each group and returns the result as a new DataFrame
   * with the number of elements equal to the size of the key series.
   *
   * @param s input series to apply grouping to
   * @return group sizes
   */
  GroupingDataFrame count(Series s) {
    long[] values = new long[this.size()];
    for(int i=0; i<this.size(); i++) {
      values[i] = this.apply(s, i).size();
    }
    return makeResult(LongSeries.buildFrom(values));
  }

  GroupingDataFrame sum(Series s) {
    Series.Builder builder = s.getBuilder();
    for(int i=0; i<this.size(); i++) {
      builder.addSeries(this.apply(s, i).sum());
    }
    return makeResult(builder.build());
  }

  GroupingDataFrame sum(DataFrame source, String groupBySeriesName, String sumSeriesName) {
    Series.Builder builder = source.get(sumSeriesName).getBuilder();
    for (int i = 0; i < this.size(); i++) {
      Series group = this.apply(source.get(groupBySeriesName), i);
      builder.addSeries(
          source.filter((LongConditional) values -> group.getLongs().contains(values[0]), groupBySeriesName)
              .dropNull(groupBySeriesName)
              .get(sumSeriesName)
              .sum());
    }
    return makeResult(builder.build(), groupBySeriesName);
  }

  GroupingDataFrame mean(DataFrame source, String groupBySeriesName, String meanSeriesName) {
    Series.Builder builder = source.get(meanSeriesName).getBuilder();
    for (int i = 0; i < this.size(); i++) {
      Series group = this.apply(source.get(groupBySeriesName), i);
      builder.addSeries(
          source.filter((LongConditional) values -> group.getLongs().contains(values[0]), groupBySeriesName)
              .dropNull(groupBySeriesName)
              .get(meanSeriesName)
              .mean());
    }
    return makeResult(builder.build(), groupBySeriesName);
  }

  GroupingDataFrame product(Series s) {
    Series.Builder builder = s.getBuilder();
    for(int i=0; i<this.size(); i++) {
      builder.addSeries(this.apply(s, i).product());
    }
    return makeResult(builder.build());
  }

  GroupingDataFrame min(Series s) {
    Series.Builder builder = s.getBuilder();
    for(int i=0; i<this.size(); i++) {
      builder.addSeries(this.apply(s, i).min());
    }
    return makeResult(builder.build());
  }

  GroupingDataFrame max(Series s) {
    Series.Builder builder = s.getBuilder();
    for(int i=0; i<this.size(); i++) {
      builder.addSeries(this.apply(s, i).max());
    }
    return makeResult(builder.build());
  }

  GroupingDataFrame first(Series s) {
    Series.Builder builder = s.getBuilder();
    for(int i=0; i<this.size(); i++) {
      builder.addSeries(this.apply(s, i).first());
    }
    return makeResult(builder.build());
  }

  GroupingDataFrame last(Series s) {
    Series.Builder builder = s.getBuilder();
    for(int i=0; i<this.size(); i++) {
      builder.addSeries(this.apply(s, i).last());
    }
    return makeResult(builder.build());
  }

  GroupingDataFrame mean(Series s) {
    Series.Builder builder = DoubleSeries.builder();
    for(int i=0; i<this.size(); i++) {
      builder.addSeries(this.apply(s, i).mean());
    }
    return makeResult(builder.build());
  }

  GroupingDataFrame median(Series s) {
    Series.Builder builder = DoubleSeries.builder();
    for(int i=0; i<this.size(); i++) {
      builder.addSeries(this.apply(s, i).median());
    }
    return makeResult(builder.build());
  }

  GroupingDataFrame std(Series s) {
    Series.Builder builder = DoubleSeries.builder();
    for(int i=0; i<this.size(); i++) {
      builder.addSeries(this.apply(s, i).std());
    }
    return makeResult(builder.build());
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

  private GroupingDataFrame makeResult(Series s) {
    return new GroupingDataFrame(GROUP_KEY, GROUP_VALUE, this.keys, s);
  }

  private GroupingDataFrame makeResult(Series s, String keyName) {
    return new GroupingDataFrame(keyName, GROUP_VALUE, this.keys, s);
  }

  /**
   * Grouping container referencing a single series. Holds group keys and the indices of group
   * elements in the source series. Enables aggregation with custom user functions.
   */
  public static class SeriesGrouping {
    final Series source;
    final Grouping grouping;

    SeriesGrouping(Series source, Grouping grouping) {
      this.source = source;
      this.grouping = grouping;
    }

    public int size() {
      return this.grouping.size();
    }

    public boolean isEmpty() {
      return this.grouping.isEmpty();
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
      return this.grouping.aggregate(this.source, function);
    }

    /**
     * @see Grouping#count(Series)
     */
    public GroupingDataFrame count() {
      return this.grouping.count(this.source);
    }

    public GroupingDataFrame sum() {
      return this.grouping.sum(this.source);
    }

    public GroupingDataFrame product() {
      return this.grouping.product(this.source);
    }

    public GroupingDataFrame min() {
      return this.grouping.min(this.source);
    }

    public GroupingDataFrame max() {
      return this.grouping.max(this.source);
    }

    public GroupingDataFrame first() {
      return this.grouping.first(this.source);
    }

    public GroupingDataFrame last() {
      return this.grouping.last(this.source);
    }

    public GroupingDataFrame mean() {
      return this.grouping.mean(this.source);
    }

    public GroupingDataFrame median() {
      return this.grouping.median(this.source);
    }

    public GroupingDataFrame std() {
      return this.grouping.std(this.source);
    }

    Series apply(int groupIndex) {
      return this.grouping.apply(this.source, groupIndex);
    }
  }

  /**
   * Container object for the grouping of multiple rows across different series
   * based on a common key.
   */
  public static class DataFrameGrouping {
    final String keyName;
    final DataFrame source;
    final Grouping grouping;

    DataFrameGrouping(String keyName, DataFrame source, Grouping grouping) {
      this.keyName = keyName;
      this.source = source;
      this.grouping = grouping;
    }

    public int size() {
      return this.grouping.size();
    }

    public boolean isEmpty() {
      return this.grouping.isEmpty();
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
      return this.grouping.aggregate(this.source.get(seriesName), function)
          .withKeyName(this.keyName).withValueName(seriesName);
    }

    /**
     * Applies the aggregation {@code functions} to the series referenced by {@code seriesNames},
     * and returns the result as a DataFrame with the index corresponding to the grouping key
     * column.
     *
     * <br/><b>NOTE:</b> This method is generally slower than aggregating with implicit function
     * references. It allows for the use of custom function implementations however.
     *
     * @see DataFrameGrouping#aggregate(String...)
     *
     * @param seriesNames
     * @param functions
     * @return
     */
    public DataFrame aggregate(String[] seriesNames, Series.Function[] functions) {
      if(seriesNames.length != functions.length)
        throw new IllegalArgumentException("Must provide equal number of series names and functions");

      // check for duplicate use of series name
      Set<String> outNames = new HashSet<>();
      outNames.add(this.keyName);

      for(String seriesName : seriesNames) {
        if(outNames.contains(seriesName))
          throw new IllegalArgumentException(String.format("Duplicate use of seriesName '%s'", seriesName));
        outNames.add(seriesName);
      }

      DataFrame df = new DataFrame();
      df.addSeries(this.keyName, this.grouping.keys);
      df.setIndex(this.keyName);

      for(int i=0; i<seriesNames.length; i++) {
        Series s = this.source.get(seriesNames[i]);
        df.addSeries(seriesNames[i], this.grouping.aggregate(s, functions[i]).getValues());
      }

      return df;
    }

    /**
     * Evaluates the {@code aggregationExpressions} and returns the result as a DataFrame with
     * the index corresponding to the grouping key column. Each expression takes the format
     * {@code seriesName[:operation[:outputName]]}, where {@code seriesName} is a valid column name in the
     * underlying DataFrame and {@code operation} is one of {@code SUM, PRODUCT, MIN, MAX,
     * FIRST, LAST, MEAN, MEDIAN, STD} and {@code outputName} is the name of the result series.
     *
     * <br/><b>NOTE:</b> This method is generally faster than aggregating with explicit function
     * references, as it may take advantage of specialized code depending on the grouping.
     *
     * @see DataFrameGrouping#aggregate(String[], Series.Function[])
     *
     * @param aggregationExpressions {@code seriesName:operation} tuples
     * @return DataFrame with grouping results
     */
    public DataFrame aggregate(String... aggregationExpressions) {
      return this.aggregate(Arrays.asList(aggregationExpressions));
    }

    /**
     * Evaluates the {@code aggregationExpressions} and returns the result as a DataFrame with
     * the index corresponding to the grouping key column. Each expression takes the format
     * {@code seriesName[:operation[:outputName]]}, where {@code seriesName} is a valid column name in the
     * underlying DataFrame and {@code operation} is one of {@code SUM, PRODUCT, MIN, MAX,
     * FIRST, LAST, MEAN, MEDIAN, STD} and {@code outputName} is the name of the result series.
     *
     * <br/><b>NOTE:</b> This method is generally faster than aggregating with explicit function
     * references, as it may take advantage of specialized code depending on the grouping.
     *
     * @see DataFrameGrouping#aggregate(String[], Series.Function[])
     *
     * @param aggregationExpressions {@code seriesName:operation} tuples
     * @return DataFrame with grouping results
     */
    public DataFrame aggregate(List<String> aggregationExpressions) {
      DataFrame df = new DataFrame();
      df.addSeries(this.keyName, this.grouping.keys);
      df.setIndex(this.keyName);

      Set<String> outNames = new HashSet<>();
      outNames.add(this.keyName);

      for(String expression : aggregationExpressions) {
        // parse expression
        String[] parts = expression.split(":", 3);
        if(parts.length > 3)
          throw new IllegalArgumentException(String.format("Could not parse expression '%s'", expression));

        String name = parts[0];
        String operation = OP_FIRST;
        if (parts.length >= 2)
          operation = parts[1];
        String outName = name;
        if (parts.length >= 3)
          outName = parts[2];

        // check for duplicate use of output name
        if(outNames.contains(outName))
          throw new IllegalArgumentException(String.format("Duplicate use of outputName '%s'", outName));
        outNames.add(outName);

        // compute expression
        df.addSeries(outName, this.aggregateExpression(name, operation).getValues());
      }

      return df;
    }

    GroupingDataFrame aggregateExpression(String seriesName, String operation) {
      switch(operation.toUpperCase()) {
        case OP_SUM:
          return this.sum(seriesName);
        case OP_PRODUCT:
          return this.product(seriesName);
        case OP_MIN:
          return this.min(seriesName);
        case OP_MAX:
          return this.max(seriesName);
        case OP_FIRST:
          return this.first(seriesName);
        case OP_LAST:
          return this.last(seriesName);
        case OP_MEAN:
          return this.mean(seriesName);
        case OP_MEDIAN:
          return this.median(seriesName);
        case OP_STD:
          return this.std(seriesName);
      }
      throw new IllegalArgumentException(String.format("Unknown operation '%s'", operation));
    }

    /**
     * Counts the number of elements in each group and returns the result as a new DataFrame
     * with the number of elements equal to the size of the key series.
     *
     * @return group sizes
     */
    public GroupingDataFrame count() {
      Series anySeries = this.source.series.values().iterator().next();
      return this.grouping.count(anySeries);
    }

    public GroupingDataFrame sum(String seriesName) {
      return this.grouping.sum(this.source.get(seriesName));
    }

    /**
     * Sums the value in a given series for each group and returns the result as a new DataFrame
     * @param groupBySeriesName the group-by series name
     * @param sumSeriesName the series name for sum
     * @return a new data frame with the sums
     */
    public GroupingDataFrame sum(String groupBySeriesName, String sumSeriesName) {
      return this.grouping.sum(source, groupBySeriesName, sumSeriesName);
    }

    /**
     * Averages the value in a given series for each group and returns the result as a new DataFrame
     * @param groupBySeriesName the group-by series name
     * @param meanSeriesName the series name for sum
     * @return a new data frame with the sums
     */
    public GroupingDataFrame mean(String groupBySeriesName, String meanSeriesName) {
      return this.grouping.mean(source, groupBySeriesName, meanSeriesName);
    }

    public GroupingDataFrame product(String seriesName) {
      return this.grouping.product(this.source.get(seriesName));
    }

    public GroupingDataFrame min(String seriesName) {
      return this.grouping.min(this.source.get(seriesName));
    }

    public GroupingDataFrame max(String seriesName) {
      return this.grouping.max(this.source.get(seriesName));
    }

    public GroupingDataFrame first(String seriesName) {
      return this.grouping.first(this.source.get(seriesName));
    }

    public GroupingDataFrame last(String seriesName) {
      return this.grouping.last(this.source.get(seriesName));
    }

    public GroupingDataFrame mean(String seriesName) {
      return this.grouping.mean(this.source.get(seriesName));
    }

    public GroupingDataFrame median(String seriesName) {
      return this.grouping.median(this.source.get(seriesName));
    }

    public GroupingDataFrame std(String seriesName) {
      return this.grouping.std(this.source.get(seriesName));
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
      if(Series.SeriesType.OBJECT.equals(series.type()))
        return from(series.getObjects());

      List<int[]> buckets = new ArrayList<>();
      int[] sref = series.sortedIndex();

      int bucketOffset = 0;
      for(int i=1; i<sref.length; i++) {
        if(!series.equals(series, sref[i-1], sref[i])) {
          int[] fromIndex = Arrays.copyOfRange(sref, bucketOffset, i);
          buckets.add(fromIndex);
          bucketOffset = i;
        }
      }

      int[] fromIndex = Arrays.copyOfRange(sref, bucketOffset, sref.length);
      buckets.add(fromIndex);

      return new GroupingByValue(series.project(keysFromBuckets(buckets)), buckets);
    }

    public static GroupingByValue from(ObjectSeries series) {
      Map<Object, List<Integer>> dynBuckets = new LinkedHashMap<>();

      for(int i=0; i<series.size(); i++) {
        Object key = series.getObject(i);
        if(!dynBuckets.containsKey(key))
          dynBuckets.put(key, new ArrayList<Integer>());
        dynBuckets.get(key).add(i);
      }

      List<int[]> buckets = new ArrayList<>();
      for(Map.Entry<Object, List<Integer>> entry : dynBuckets.entrySet()) {
        buckets.add(ArrayUtils.toPrimitive(
            entry.getValue().toArray(new Integer[entry.getValue().size()])));
      }

      return new GroupingByValue(series.project(keysFromBuckets(buckets)), buckets);
    }

    public static GroupingByValue from(Series[] series) {
      Series.assertSameLength(series);

      List<int[]> buckets = new ArrayList<>();
      PrimitiveMultimap m = new PrimitiveMultimap(series);
      BitSet b = new BitSet(series[0].size());

      for(int i=0; i<series[0].size(); i++) {
        if(!b.get(i)) {
          int[] keys = m.get(series, i, series);
          for(int k : keys)
            b.set(k);
          buckets.add(keys);
        }
      }

      int[] keys = keysFromBuckets(buckets);
      DataFrame.Tuple[] tuples = new DataFrame.Tuple[keys.length];
      for(int i=0; i<keys.length; i++) {
        tuples[i] = DataFrame.Tuple.buildFrom(series, keys[i]);
      }

      return new GroupingByValue(ObjectSeries.buildFrom((Object[])tuples), buckets);
    }

    private static int[] keysFromBuckets(List<int[]> buckets) {
      int[] keyIndex = new int[buckets.size()];
      int i = 0;
      for(int[] b : buckets) {
        keyIndex[i++] = b[0];
      }
      return keyIndex;
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
      int start = groupIndex + 1 - this.windowSize;
      if(start < 0)
        return s.getBuilder().build();
      return s.slice(start, groupIndex + 1);
    }

    @Override
    GroupingDataFrame sum(Series s) {
      switch(s.type()) {
        case BOOLEAN:
        case LONG:
          return this.sum(s.getLongs());
        case DOUBLE:
          return this.sum(s.getDoubles());
        case STRING:
          return this.sum(s.getStrings());
      }
      return super.sum(s);
    }

    private GroupingDataFrame sum(LongSeries s) {
      long[] values = new long[super.size()];
      long rollingSum = 0;
      int valueCount = 0;

      for(int i=0; i<super.size(); i++) {
        if(!s.isNull(i)) {
          rollingSum += s.getLong(i);
          valueCount += 1;
        } else {
          valueCount -= 1;
          valueCount = Math.max(valueCount, 0);
        }
        if(i >= this.windowSize && !s.isNull(i - this.windowSize))
          rollingSum -= s.getLong(i - this.windowSize);
        if(i >= this.windowSize - 1 && valueCount > 0)
          values[i] = rollingSum;
        else
          values[i] = LongSeries.NULL;
      }
      return super.makeResult(LongSeries.buildFrom(values));
    }

    private GroupingDataFrame sum(DoubleSeries s) {
      double[] values = new double[super.size()];
      double rollingSum = 0;
      int valueCount = 0;

      for(int i=0; i<super.size(); i++) {
        if(!s.isNull(i)) {
          rollingSum += s.getDouble(i);
          valueCount += 1;
        } else {
          valueCount -= 1;
          valueCount = Math.max(valueCount, 0);
        }
        if(i >= this.windowSize && !s.isNull(i - this.windowSize))
          rollingSum -= s.getDouble(i - this.windowSize);
        if(i >= this.windowSize - 1 && valueCount > 0)
          values[i] = rollingSum;
        else
          values[i] = DoubleSeries.NULL;
      }
      return super.makeResult(DoubleSeries.buildFrom(values));
    }

    private GroupingDataFrame sum(StringSeries s) {
      String[] values = new String[super.size()];
      StringBuilder sb = new StringBuilder();
      int valueCount = 0;

      for(int i=0; i<super.size(); i++) {
        if(!s.isNull(i)) {
          sb.append(s.getString(i));
          valueCount += 1;
        } else {
          valueCount -= 1;
          valueCount = Math.max(valueCount, 0);
        }
        if(i >= this.windowSize && !s.isNull(i - this.windowSize))
          sb.deleteCharAt(0);
        if(i >= this.windowSize - 1 && valueCount > 0)
          values[i] = sb.toString();
        else
          values[i] = StringSeries.NULL;
      }
      return super.makeResult(StringSeries.buildFrom(values));
    }

    public static GroupingByMovingWindow from(int windowSize, int size) {
      if(windowSize <= 0)
        throw new IllegalArgumentException("windowSize must be > 0");
      return new GroupingByMovingWindow(LongSeries.sequence(0, size), windowSize, size);
    }
  }

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

    @Override
    GroupingDataFrame sum(Series s) {
      switch(s.type()) {
        case BOOLEAN:
        case LONG:
          return this.sumLong(s);
        case DOUBLE:
          return this.sumDouble(s);
        case STRING:
          return this.sumString(s);
      }
      return super.sum(s);
    }

    private GroupingDataFrame sumLong(Series s) {
      long[] values = new long[super.size()];
      long rollingSum = 0;
      int first = 0;
      for(; first<super.size(); first++) {
        if(!s.isNull(first))
          break;
        values[first] = LongSeries.NULL;
      }
      for(int i=first; i<super.size(); i++) {
        if(!s.isNull(i))
          rollingSum += s.getLong(i);
        values[i] = rollingSum;
      }
      return super.makeResult(LongSeries.buildFrom(values));
    }

    private GroupingDataFrame sumDouble(Series s) {
      double[] values = new double[super.size()];
      double rollingSum = 0;
      int first = 0;
      for(; first<super.size(); first++) {
        if(!s.isNull(first))
          break;
        values[first] = DoubleSeries.NULL;
      }
      for(int i=first; i<super.size(); i++) {
        if(!s.isNull(i))
          rollingSum += s.getDouble(i);
        values[i] = rollingSum;
      }
      return super.makeResult(DoubleSeries.buildFrom(values));
    }

    private GroupingDataFrame sumString(Series s) {
      String[] values = new String[super.size()];
      StringBuilder sb = new StringBuilder();
      int first = 0;
      for(; first<super.size(); first++) {
        if(!s.isNull(first))
          break;
        values[first] = StringSeries.NULL;
      }
      for(int i=first; i<super.size(); i++) {
        if(!s.isNull(i))
          sb.append(s.getString(i));
        values[i] = sb.toString();
      }
      return super.makeResult(StringSeries.buildFrom(values));
    }

    @Override
    GroupingDataFrame min(Series s) {
      if(super.isEmpty())
        return super.makeResult(s.getBuilder().build());

      switch(s.type()) {
        case BOOLEAN:
          return longToBoolean(this.minLong(s));
        case LONG:
          return this.minLong(s);
        case DOUBLE:
          return this.minDouble(s);
      }
      return this.minGeneric(s);
    }

    GroupingDataFrame minGeneric(Series s) {
      Series.Builder builder = s.getBuilder();

      Series vmin = s.slice(0, 1);
      builder.addSeries(vmin);
      for(int i=1; i<super.size(); i++) {
        if(!s.isNull(i) && (vmin.isNull(0) || vmin.compare(s, 0, i) > 0))
          vmin = s.slice(i, i + 1);
        builder.addSeries(vmin);
      }

      return super.makeResult(builder.build());
    }

    GroupingDataFrame minLong(Series s) {
      long[] values = new long[super.size()];
      long min = s.getLong(0);
      for(int i=0; i<super.size(); i++) {
        long val = s.getLong(i);
        if(!s.isNull(i) && (LongSeries.isNull(min) || min > val))
          min = val;
        values[i] = min;
      }
      return super.makeResult(LongSeries.buildFrom(values));
    }

    GroupingDataFrame minDouble(Series s) {
      double[] values = new double[super.size()];
      double min = s.getDouble(0);
      for(int i=0; i<super.size(); i++) {
        double val = s.getDouble(i);
        if(!s.isNull(i) && (DoubleSeries.isNull(min) || min > val))
          min = val;
        values[i] = min;
      }
      return super.makeResult(DoubleSeries.buildFrom(values));
    }

    @Override
    GroupingDataFrame max(Series s) {
      if(super.isEmpty())
        return super.makeResult(s.getBuilder().build());

      switch(s.type()) {
        case BOOLEAN:
          return longToBoolean(this.maxLong(s));
        case LONG:
          return this.maxLong(s);
        case DOUBLE:
          return this.maxDouble(s);
      }
      return this.maxGeneric(s);
    }

    GroupingDataFrame maxGeneric(Series s) {
      Series.Builder builder = s.getBuilder();

      Series vmax = s.slice(0, 1);
      builder.addSeries(vmax);
      for(int i=1; i<super.size(); i++) {
        if(!s.isNull(i) && (vmax.isNull(0) || vmax.compare(s, 0, i) < 0))
          vmax = s.slice(i, i + 1);
        builder.addSeries(vmax);
      }

      return super.makeResult(builder.build());
    }

    GroupingDataFrame maxLong(Series s) {
      long[] values = new long[super.size()];
      long max = s.getLong(0);
      for(int i=0; i<super.size(); i++) {
        long val = s.getLong(i);
        if(!s.isNull(i) && (LongSeries.isNull(max) || max < val))
          max = val;
        values[i] = max;
      }
      return super.makeResult(LongSeries.buildFrom(values));
    }

    GroupingDataFrame maxDouble(Series s) {
      double[] values = new double[super.size()];
      double max = s.getDouble(0);
      for(int i=0; i<super.size(); i++) {
        double val = s.getDouble(i);
        if(!s.isNull(i) && (DoubleSeries.isNull(max) || max < val))
          max = val;
        values[i] = max;
      }
      return super.makeResult(DoubleSeries.buildFrom(values));
    }

    private static GroupingDataFrame longToBoolean(GroupingDataFrame gdf) {
      return new GroupingDataFrame(gdf.keyName, gdf.valueName, gdf.getKeys(), gdf.getValues().getBooleans());
    }

    public static GroupingByExpandingWindow from(int size) {
      return new GroupingByExpandingWindow(LongSeries.sequence(0, size), size);
    }
  }

  /**
   * Represents a SeriesGrouping based on the time period from an origin.
   * The Origin can either be provided or be estimated based on the input data.
   * Handles timezone-specific properties, such as DST and time leaps.
   */
  public static final class GroupingByPeriod extends Grouping {
    final long[] cutoffs;

    private GroupingByPeriod(long[] cutoffs) {
      super(LongSeries.buildFrom(cutoffs));
      this.cutoffs = cutoffs;
    }

    @Override
    Series apply(Series s, int groupIndex) {
      final long lower = this.cutoffs[groupIndex];
      final long upper = groupIndex + 1 < this.cutoffs.length ?
          this.cutoffs[groupIndex + 1] : Long.MAX_VALUE;

      return s.filter(new Series.LongConditional() {
        @Override
        public boolean apply(long... values) {
          return values[0] >= lower && values[0] < upper;
        }
      }).dropNull();
    }

    private static long[] makeCutoffs(DateTime origin, DateTime max, Period bucketSize) {
      List<Long> offsets = new ArrayList<>();
      DateTime offset = origin;
      while (offset.isBefore(max) || offset.isEqual(max)) {
        offsets.add(offset.getMillis());
        offset = offset.plus(bucketSize);
      }
      return ArrayUtils.toPrimitive(offsets.toArray(new Long[offsets.size()]));
    }

    public static GroupingByPeriod from(LongSeries timestamps, DateTimeZone timezone, Period bucketSize) {
      DateTime origin = makeOrigin(new DateTime(timestamps.min().longValue(), timezone), bucketSize);
      return from(timestamps, origin, bucketSize);
    }

    public static GroupingByPeriod from(LongSeries timestamps, DateTime origin, Period bucketSize) {
      DateTime max = new DateTime(timestamps.max().longValue(), origin.getZone());
      return new GroupingByPeriod(makeCutoffs(origin, max, bucketSize));
    }

    private static DateTime makeOrigin(DateTime first, Period period) {
      if (period.getYears() > 0) {
        return first.year().roundFloorCopy().toDateTime();

      } else if (period.getMonths() > 0) {
        return first.monthOfYear().roundFloorCopy().toDateTime();

      } else if (period.getWeeks() > 0) {
        return first.weekOfWeekyear().roundFloorCopy().toDateTime();

      } else if (period.getDays() > 0) {
        return first.dayOfYear().roundFloorCopy().toDateTime();

      } else if (period.getHours() > 0) {
        return first.hourOfDay().roundFloorCopy().toDateTime();

      } else if (period.getMinutes() > 0) {
        return first.minuteOfHour().roundFloorCopy().toDateTime();

      } else if (period.getSeconds() > 0) {
        return first.secondOfMinute().roundFloorCopy().toDateTime();

      } else if (period.getMillis() > 0) {
        return first.millisOfSecond().roundFloorCopy().toDateTime();

      }

      throw new IllegalArgumentException(String.format("Unsupported Period '%s'", period));
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
