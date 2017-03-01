package com.linkedin.thirdeye.detector.functionex.dataframe;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.lang.math.NumberUtils;


public class DataFrame {

  public interface ResamplingStrategy {
    Series apply(Series s, List<Series.Bucket> buckets);
  }

  public static class ResampleLast implements ResamplingStrategy {
    @Override
    public Series apply(Series s, List<Series.Bucket> buckets) {
      switch(s.type()) {
        case DOUBLE:
          return ((DoubleSeries)s).groupBy(buckets, new DoubleSeries.DoubleBatchLast());
        case LONG:
          return ((LongSeries)s).groupBy(buckets, new LongSeries.LongBatchLast());
        case STRING:
          return ((StringSeries)s).groupBy(buckets, new StringSeries.StringBatchLast());
        case BOOLEAN:
          return ((BooleanSeries)s).groupBy(buckets, new BooleanSeries.BooleanBatchLast());
        default:
          throw new IllegalArgumentException(String.format("Cannot resample series type '%s'", s.type()));
      }
    }
  }

  LongSeries index;
  Map<String, Series> series = new HashMap<>();

  public static DoubleSeries toSeries(double... values) {
    return new DoubleSeries(values);
  }

  public static LongSeries toSeries(long... values) {
    return new LongSeries(values);
  }

  public static BooleanSeries toSeries(boolean... values) {
    return new BooleanSeries(values);
  }

  public static StringSeries toSeries(String... values) {
    return new StringSeries(values);
  }

  public DataFrame(int defaultIndexSize) {
    long[] indexValues = new long[defaultIndexSize];
    for(int i=0; i<defaultIndexSize; i++) {
      indexValues[i] = i;
    }
    this.index = new LongSeries(indexValues);
  }

  public DataFrame(long[] indexValues) {
    this.index = new LongSeries(indexValues);
  }

  public DataFrame(LongSeries index) {
    this.index = index;
  }

  public DataFrame copy() {
    DataFrame newDataFrame = new DataFrame(this.index.copy());
    for(Map.Entry<String, Series> e : this.series.entrySet()) {
      newDataFrame.addSeries(e.getKey(), e.getValue().copy());
    }
    return newDataFrame;
  }

  public void addSeries(String seriesName, Series s) {
    if(s.size() != this.index.size())
      throw new IllegalArgumentException("DataFrame index and series must be of same length");
    series.put(seriesName, s);
  }

  public void addSeries(String seriesName, double... values) {
    addSeries(seriesName, DataFrame.toSeries(values));
  }

  public void addSeries(String seriesName, long... values) {
    addSeries(seriesName, DataFrame.toSeries(values));
  }

  public void addSeries(String seriesName, String... values) {
    addSeries(seriesName, DataFrame.toSeries(values));
  }

  public void addSeries(String seriesName, boolean... values) {
    addSeries(seriesName, DataFrame.toSeries(values));
  }

  public void dropSeries(String seriesName) {
    this.series.remove(seriesName);
  }

  public void renameSeries(String oldName, String newName) {
    Series s = assertSeriesExists(oldName);
    this.dropSeries(oldName);
    this.addSeries(newName, s);
  }

  public LongSeries getIndex() {
    return index;
  }

  public Set<String> getSeriesNames() {
    return Collections.unmodifiableSet(this.series.keySet());
  }

  public Map<String, Series> getSeries() {
    return Collections.unmodifiableMap(this.series);
  }

  public Series get(String seriesName) {
    return assertSeriesExists(seriesName);
  }

  public boolean contains(String seriesName) {
    return this.series.containsKey(seriesName);
  }

  public DoubleSeries toDoubles(String seriesName) {
    return assertSeriesExists(seriesName).toDoubles();
  }

  public LongSeries toLongs(String seriesName) {
    return assertSeriesExists(seriesName).toLongs();
  }

  public StringSeries toStrings(String seriesName) {
    return assertSeriesExists(seriesName).toStrings();
  }

  public BooleanSeries toBooleans(String seriesName) {
   return assertSeriesExists(seriesName).toBooleans();
  }

  public DoubleSeries map(DoubleSeries.DoubleBatchFunction function, String... seriesNames) {
    DoubleSeries[] inputSeries = new DoubleSeries[seriesNames.length];
    for(int i=0; i<seriesNames.length; i++) {
      inputSeries[i] = assertSeriesExists(seriesNames[i]).toDoubles();
    }

    double[] output = new double[this.index.size()];
    for(int i=0; i<this.index.size(); i++) {
      double[] input = new double[seriesNames.length];
      for(int j=0; j<inputSeries.length; j++) {
        input[j] = inputSeries[j].values[i];
      }
      output[i] = function.apply(input);
    }

    return new DoubleSeries(output);
  }

  public LongSeries map(LongSeries.LongBatchFunction function, String... seriesNames) {
    LongSeries[] inputSeries = new LongSeries[seriesNames.length];
    for(int i=0; i<seriesNames.length; i++) {
      inputSeries[i] = assertSeriesExists(seriesNames[i]).toLongs();
    }

    long[] output = new long[this.index.size()];
    for(int i=0; i<this.index.size(); i++) {
      long[] input = new long[seriesNames.length];
      for(int j=0; j<inputSeries.length; j++) {
        input[j] = inputSeries[j].values[i];
      }
      output[i] = function.apply(input);
    }

    return new LongSeries(output);
  }

  public StringSeries map(StringSeries.StringBatchFunction function, String... seriesNames) {
    StringSeries[] inputSeries = new StringSeries[seriesNames.length];
    for(int i=0; i<seriesNames.length; i++) {
      inputSeries[i] = assertSeriesExists(seriesNames[i]).toStrings();
    }

    String[] output = new String[this.index.size()];
    for(int i=0; i<this.index.size(); i++) {
      String[] input = new String[seriesNames.length];
      for(int j=0; j<inputSeries.length; j++) {
        input[j] = inputSeries[j].values[i];
      }
      output[i] = function.apply(input);
    }

    return new StringSeries(output);
  }

  public BooleanSeries map(BooleanSeries.BooleanBatchFunction function, String... seriesNames) {
    BooleanSeries[] inputSeries = new BooleanSeries[seriesNames.length];
    for(int i=0; i<seriesNames.length; i++) {
      inputSeries[i] = assertSeriesExists(seriesNames[i]).toBooleans();
    }

    boolean[] output = new boolean[this.index.size()];
    for(int i=0; i<this.index.size(); i++) {
      boolean[] input = new boolean[seriesNames.length];
      for(int j=0; j<inputSeries.length; j++) {
        input[j] = inputSeries[j].values[i];
      }
      output[i] = function.apply(input);
    }

    return new BooleanSeries(output);
  }

 public DataFrame reorder(int[] toIndex) {
    if(toIndex.length != this.index.size())
      throw new IllegalArgumentException("toIndex size does not equal series size");

    LongSeries newIndex = this.index.reorder(toIndex);
    DataFrame newDataFrame = new DataFrame(newIndex);

    for(Map.Entry<String, Series> e : this.series.entrySet()) {
      newDataFrame.addSeries(e.getKey(), e.getValue().reorder(toIndex));
    }
    return newDataFrame;
  }

  public DataFrame sortByIndex() {
    return this.reorder(this.index.sortedIndex());
  }

  /**
   * Sort data frame by series values.  The resulting sort order is the equivalent of applying
   * a stable sort to nth series first, and then sorting iteratively until the 1st series.
   *
   * @param seriesNames 1st series, 2nd series, ..., nth series
   * @return sorted data frame
   */
  public DataFrame sortBySeries(String... seriesNames) {
    DataFrame df = this;
    for(int i=seriesNames.length-1; i>=0; i--) {
      df = df.reorder(assertSeriesExists(seriesNames[i]).sortedIndex());
    }
    return df;
  }

  public DataFrame reverse() {
    DataFrame newDataFrame = new DataFrame(this.index.reverse());
    for(Map.Entry<String, Series> e : this.series.entrySet()) {
      newDataFrame.addSeries(e.getKey(), e.getValue().reverse());
    }
    return newDataFrame;
  }

  public DataFrame resample(long interval, ResamplingStrategy strategy) {
    DataFrame baseDataFrame = this.sortByIndex();

    List<Series.Bucket> buckets = baseDataFrame.getIndex().bucketsByInterval(interval);

    // new index from intervals
    int startIndex = (int)(baseDataFrame.getIndex().min() / interval);

    long[] ivalues = new long[buckets.size()];
    for(int i=0; i<buckets.size(); i++) {
      ivalues[i] = (i + startIndex) * interval;
    }

    // resample series
    DataFrame newDataFrame = new DataFrame(ivalues);

    for(Map.Entry<String, Series> e : baseDataFrame.getSeries().entrySet()) {
      newDataFrame.addSeries(e.getKey(), strategy.apply(e.getValue(), buckets));
    }
    return newDataFrame;
  }

  DataFrame filter(int[] fromIndex) {
    LongSeries index = this.index.filter(fromIndex);
    DataFrame df = new DataFrame(index);
    for(Map.Entry<String, Series> e : this.getSeries().entrySet()) {
      df.addSeries(e.getKey(), e.getValue().filter(fromIndex));
    }

    return df;
  }

  public DataFrame filter(BooleanSeries series) {
    if(series.size() != this.index.size())
      throw new IllegalArgumentException("Series size must be equal to index size");

    int[] fromIndex = new int[series.size()];
    int fromIndexCount = 0;
    for(int i=0; i<series.size(); i++) {
      if(series.values[i]) {
        fromIndex[fromIndexCount] = i;
        fromIndexCount++;
      }
    }

    int[] fromIndexCompressed = Arrays.copyOf(fromIndex, fromIndexCount);

    return this.filter(fromIndexCompressed);
  }

  public DataFrame filter(String seriesName) {
    return this.filter(this.toBooleans(seriesName));
  }

  public DataFrame filter(String seriesName, DoubleSeries.DoubleConditional conditional) {
    return this.filter(assertSeriesExists(seriesName).toDoubles().map(conditional));
  }

  public DataFrame filter(String seriesName, LongSeries.LongConditional conditional) {
    return this.filter(assertSeriesExists(seriesName).toLongs().map(conditional));
  }

  public DataFrame filter(String seriesName, StringSeries.StringConditional conditional) {
    return this.filter(assertSeriesExists(seriesName).toStrings().map(conditional));
  }

  public DataFrame filterEquals(String seriesName, double value) {
    return this.filter(seriesName, new DoubleSeries.DoubleConditional() {
      @Override
      public boolean apply(double v) {
        return value == v;
      }
    });
  }

  public DataFrame filterEquals(String seriesName, long value) {
    return this.filter(seriesName, new LongSeries.LongConditional() {
      @Override
      public boolean apply(long v) {
        return value == v;
      }
    });
  }

  public DataFrame filterEquals(String seriesName, String value) {
    return this.filter(seriesName, new StringSeries.StringConditional() {
      @Override
      public boolean apply(String v) {
        return value.equals(v);
      }
    });
  }

  public double getDouble(String seriesName) {
    return assertSingleValue(seriesName).toDoubles().first();
  }

  public long getLong(String seriesName) {
    return assertSingleValue(seriesName).toLongs().first();
  }

  public String getString(String seriesName) {
    return assertSingleValue(seriesName).toStrings().first();
  }

  public boolean getBoolean(String seriesName) {
    return assertSingleValue(seriesName).toBooleans().first();
  }

  public static DoubleSeries toDoubles(DoubleSeries s) {
    return s;
  }

  public static DoubleSeries toDoubles(LongSeries s) {
    double[] values = new double[s.size()];
    for(int i=0; i<values.length; i++) {
      if(LongSeries.isNull(s.values[i])) {
        values[i] = DoubleSeries.NULL_VALUE;
      } else {
        values[i] = (double) s.values[i];
      }
    }
    return new DoubleSeries(values);
  }

  public static DoubleSeries toDoubles(StringSeries s) {
    double[] values = new double[s.size()];
    for(int i=0; i<values.length; i++) {
      if(StringSeries.isNull(s.values[i])) {
        values[i] = DoubleSeries.NULL_VALUE;
      } else {
        values[i] = Double.parseDouble(s.values[i]);
      }
    }
    return new DoubleSeries(values);
  }

  public static DoubleSeries toDoubles(BooleanSeries s) {
    double[] values = new double[s.size()];
    for(int i=0; i<values.length; i++) {
      values[i] = s.values[i] ? 1.0d : 0.0d;
    }
    return new DoubleSeries(values);
  }

  public static LongSeries toLongs(DoubleSeries s) {
    long[] values = new long[s.size()];
    for(int i=0; i<values.length; i++) {
      if(DoubleSeries.isNull(s.values[i])) {
        values[i] = LongSeries.NULL_VALUE;
      } else {
        values[i] = (long) s.values[i];
      }
    }
    return new LongSeries(values);
  }

  public static LongSeries toLongs(LongSeries s) {
    return s;
  }

  public static LongSeries toLongs(StringSeries s) {
    long[] values = new long[s.size()];
    for(int i=0; i<values.length; i++) {
      if(StringSeries.isNull(s.values[i])) {
        values[i] = LongSeries.NULL_VALUE;
      } else {
        try {
          values[i] = Long.parseLong(s.values[i]);
        } catch (NumberFormatException e) {
          values[i] = (long) Double.parseDouble(s.values[i]);
        }
      }
    }
    return new LongSeries(values);
  }

  public static LongSeries toLongs(BooleanSeries s) {
    long[] values = new long[s.size()];
    for(int i=0; i<values.length; i++) {
      values[i] = s.values[i] ? 1L : 0L;
    }
    return new LongSeries(values);
  }

  public static BooleanSeries toBooleans(DoubleSeries s) {
    boolean[] values = new boolean[s.size()];
    for(int i=0; i<values.length; i++) {
      if(DoubleSeries.isNull(s.values[i])) {
        values[i] = BooleanSeries.NULL_VALUE;
      } else {
        values[i] = s.values[i] != 0.0d;
      }
    }
    return new BooleanSeries(values);
  }

  public static BooleanSeries toBooleans(LongSeries s) {
    boolean[] values = new boolean[s.size()];
    for(int i=0; i<values.length; i++) {
      if(LongSeries.isNull(s.values[i])) {
        values[i] = BooleanSeries.NULL_VALUE;
      } else {
        values[i] = s.values[i] != 0L;
      }
    }
    return new BooleanSeries(values);
  }

  public static BooleanSeries toBooleans(BooleanSeries s) {
    return s;
  }

  public static BooleanSeries toBooleans(StringSeries s) {
    boolean[] values = new boolean[s.size()];
    for(int i=0; i<values.length; i++) {
      if(StringSeries.isNull(s.values[i])) {
        values[i] = BooleanSeries.NULL_VALUE;
      } else {
        if(NumberUtils.isNumber(s.values[i])) {
          values[i] = Double.parseDouble(s.values[i]) != 0.0d;
        } else {
          values[i] = Boolean.parseBoolean(s.values[i]);
        }
      }
    }
    return new BooleanSeries(values);
  }

  public static StringSeries toStrings(DoubleSeries s) {
    String[] values = new String[s.size()];
    for(int i=0; i<values.length; i++) {
      if(DoubleSeries.isNull(s.values[i])) {
        values[i] = StringSeries.NULL_VALUE;
      } else {
        values[i] = String.valueOf(s.values[i]);
      }
    }
    return new StringSeries(values);
  }

  public static StringSeries toStrings(LongSeries s) {
    String[] values = new String[s.size()];
    for(int i=0; i<values.length; i++) {
      if(LongSeries.isNull(s.values[i])) {
        values[i] = StringSeries.NULL_VALUE;
      } else {
        values[i] = String.valueOf(s.values[i]);
      }
    }
    return new StringSeries(values);
  }

  public static StringSeries toStrings(BooleanSeries s) {
    String[] values = new String[s.size()];
    for(int i=0; i<values.length; i++) {
      values[i] = String.valueOf(s.values[i]);
    }
    return new StringSeries(values);
  }

  public static StringSeries toStrings(StringSeries s) {
    return s;
  }

  public DataFrame dropNullRows() {
    int[] fromIndex = new int[this.index.size()];
    for(int i=0; i<fromIndex.length; i++) {
      fromIndex[i] = i;
    }

    for(Series s : this.series.values()) {
      int[] nulls = s.nullIndex();
      for(int n : nulls) {
        fromIndex[n] = -1;
      }
    }

    int countNotNull = 0;
    for(int i=0; i<fromIndex.length; i++) {
      if(fromIndex[i] >= 0) {
        fromIndex[countNotNull] = fromIndex[i];
        countNotNull++;
      }
    }

    int[] fromIndexCompressed = Arrays.copyOf(fromIndex, countNotNull);

    return this.filter(fromIndexCompressed);
  }

  public DataFrame dropNullColumns() {
    DataFrame df = new DataFrame(index);
    for(Map.Entry<String, Series> e : this.getSeries().entrySet()) {
      if(!e.getValue().hasNull())
        df.addSeries(e.getKey(), e.getValue());
    }

    return df;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("DataFrame{\n");
    for(Map.Entry<String, Series> e : this.series.entrySet()) {
      builder.append(e.getKey());
      builder.append(": ");
      builder.append(e.getValue());
      builder.append("\n");
    }
    builder.append("}");
    return builder.toString();
  }

  private Series assertSeriesExists(String name) {
    if(!series.containsKey(name))
      throw new IllegalArgumentException(String.format("Unknown series '%s'", name));
    return series.get(name);
  }

  private Series assertSingleValue(String name) {
    if(assertSeriesExists(name).size() != 1)
      throw new IllegalArgumentException("Series must have exactly one element");
    return series.get(name);
  }

}
