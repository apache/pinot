package com.linkedin.thirdeye.dataframe;

import com.linkedin.pinot.client.ResultSet;
import com.linkedin.pinot.client.ResultSetGroup;
import com.udojava.evalex.Expression;
import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;


/**
 * Container class for a data frame with multiple typed series with equivalent row count.
 */
public class DataFrame {
  public static Pattern SERIES_NAME_PATTERN = Pattern.compile("([A-Za-z_]\\w*)");

  public static final String COLUMN_INDEX = "index";
  public static final String COLUMN_JOIN_POSTFIX = "_right";

  /**
   * Strategy interface for resampling series with different native types with a common
   * strategy.
   */
  public interface ResamplingStrategy {
    DataFrame apply(Series.SeriesGrouping grouping, Series s);
  }

  /**
   * Resampling by last value in each grouped interval
   */
  public static final class ResampleLast implements ResamplingStrategy {
    @Override
    public DataFrame apply(Series.SeriesGrouping grouping, Series s) {
      switch(s.type()) {
        case DOUBLE:
          return grouping.applyTo(s).aggregate(new DoubleSeries.DoubleBatchLast());
        case LONG:
          return grouping.applyTo(s).aggregate(new LongSeries.LongBatchLast());
        case STRING:
          return grouping.applyTo(s).aggregate(new StringSeries.StringBatchLast());
        case BOOLEAN:
          return grouping.applyTo(s).aggregate(new BooleanSeries.BooleanBatchLast());
        default:
          throw new IllegalArgumentException(String.format("Cannot resample series type '%s'", s.type()));
      }
    }
  }

  /**
   * Container object for the grouping of multiple rows across different series
   * based on a common key.
   */
  public static final class DataFrameGrouping {
    final Series keys;
    final List<Series.Bucket> buckets;
    final DataFrame source;

    DataFrameGrouping(Series keys, DataFrame source, List<Series.Bucket> buckets) {
      this.keys = keys;
      this.buckets = buckets;
      this.source = source;
    }

    public int size() {
      return this.keys.size();
    }

    public DataFrame source() {
      return this.source;
    }

    public boolean isEmpty() {
      return this.keys.isEmpty();
    }

    public Series.SeriesGrouping get(String seriesName) {
      return new Series.SeriesGrouping(keys, this.source.get(seriesName), this.buckets);
    }

    public DataFrame aggregate(String seriesName, Series.DoubleFunction function) {
      return this.get(seriesName).aggregate(function);
    }

    public DataFrame aggregate(String seriesName, Series.LongFunction function) {
      return this.get(seriesName).aggregate(function);
    }

    public DataFrame aggregate(String seriesName, Series.StringFunction function) {
      return this.get(seriesName).aggregate(function);
    }

    public DataFrame aggregate(String seriesName, Series.BooleanFunction function) {
      return this.get(seriesName).aggregate(function);
    }
  }



  Map<String, Series> series = new HashMap<>();

  /**
   * Returns a DoubleSeries wrapping the values array
   *
   * @param values base array
   * @return LongSeries wrapping the array
   */
  public static DoubleSeries toSeries(double... values) {
    return DoubleSeries.buildFrom(values);
  }

  /**
   * Returns a LongSeries wrapping the values array
   *
   * @param values base array
   * @return LongSeries wrapping the array
   */
  public static LongSeries toSeries(long... values) {
    return LongSeries.buildFrom(values);
  }

  /**
   * Returns a StringSeries wrapping the values array
   *
   * @param values base array
   * @return StringSeries wrapping the array
   */
  public static StringSeries toSeries(String... values) {
    return StringSeries.buildFrom(values);
  }

  /**
   * Returns a BooleanSeries wrapping the values array
   *
   * @param values base array
   * @return BooleanSeries wrapping the array
   */
  public static BooleanSeries toSeries(byte... values) {
    return BooleanSeries.buildFrom(values);
  }

  /**
   * Returns a BooleanSeries wrapping the values array (as converted to byte)
   *
   * @param values base array
   * @return BooleanSeries wrapping the array
   */
  public static BooleanSeries toSeries(boolean... values) {
    return BooleanSeries.buildFrom(values);
  }

  /**
   * Returns a builder instance for DoubleSeries
   *
   * @return DoubleSeries builder
   */
  public static DoubleSeries.Builder buildDoubles() {
    return DoubleSeries.builder();
  }

  /**
   * Returns a builder instance for LongSeries
   *
   * @return LongSeries builder
   */
  public static LongSeries.Builder buildLongs() {
    return LongSeries.builder();
  }

  /**
   * Returns a builder instance for StringSeries
   *
   * @return StringSeries builder
   */
  public static StringSeries.Builder buildStrings() {
    return StringSeries.builder();
  }

  /**
   * Returns a builder instance for BooleanSeries
   *
   * @return BooleanSeries builder
   */
  public static BooleanSeries.Builder buildBooleans() {
    return BooleanSeries.builder();
  }

  /**
   * Creates a new DataFrame with a column "INDEX" (as determined by {@code COLUMN_INDEX}) with
   * length {@code defaultIndexSize}, ranging from 0 to {@code defaultIndexSize - 1}.
   *
   * @param defaultIndexSize index column size
   */
  public DataFrame(int defaultIndexSize) {
    long[] indexValues = new long[defaultIndexSize];
    for(int i=0; i<defaultIndexSize; i++) {
      indexValues[i] = i;
    }
    this.addSeries(COLUMN_INDEX, LongSeries.buildFrom(indexValues));
  }

  /**
   * Creates a new DataFrame with a column "INDEX" (as determined by {@code COLUMN_INDEX}) that
   * wraps the array {@code indexValues}.
   *
   * @param indexValues index values
   */
  public DataFrame(long... indexValues) {
    this.addSeries(COLUMN_INDEX, LongSeries.buildFrom(indexValues));
  }

  /**
   * Creates a new DataFrame with a column "INDEX" (as determined by {@code COLUMN_INDEX}) referencing
   * the Series {@code index}.
   *
   * @param index index series
   */
  public DataFrame(LongSeries index) {
    this.addSeries(COLUMN_INDEX, index);
  }

  /**
   * Creates a new DataFrame without any columns. The row count of the DataFrame is determined
   * by the first series added.
   */
  public DataFrame() {
    // left blank
  }

  /**
   * Returns the row count of the DataFrame
   *
   * @return row count
   */
  public int size() {
    if(this.series.isEmpty())
      return 0;
    return this.series.values().iterator().next().size();
  }

  /**
   * Returns a copy of the DataFrame sliced from index {@code from} (inclusive) to index {@code to}
   * (exclusive).
   *
   * @param from start index (inclusive), must be >= 0
   * @param to end index (exclusive), must be <= size
   * @return sliced DataFrame copy
   */
  public DataFrame slice(int from, int to) {
    DataFrame df = new DataFrame();
    for(Map.Entry<String, Series> e : this.series.entrySet()) {
      df.addSeries(e.getKey(), e.getValue().slice(from, to));
    }
    return df;
  }

  /**
   * Returns a copy of the DataFrame omitting any elements before index {@code n}.
   * If {@code n} is {@code 0}, the entire DataFrame is returned. If {@code n} is greater than
   * the DataFrame size, an empty DataFrame is returned.
   *
   * @param from start index of copy (inclusive)
   * @return DataFrame copy with elements from index {@code from}.
   */
  public DataFrame sliceFrom(int from) {
    return this.slice(from, this.size());
  }

  /**
   * Returns a copy of the DataFrame omitting any elements equal to or after index {@code n}.
   * If {@code n} is equal or greater than the DataFrame size, the entire series is returned.
   * If {@code n} is {@code 0}, an empty DataFrame is returned.
   *
   * @param to end index of copy (exclusive)
   * @return DataFrame copy with elements before from index {@code from}.
   */
  public DataFrame sliceTo(int to) {
    return this.slice(0, to);
  }

  /**
   * Returns {@code true} is the DataFrame does not hold any rows. Otherwise, returns {@code false}.
   *
   * @return {@code true} is empty, {@code false} otherwise.
   */
  public boolean isEmpty() {
    return this.size() <= 0;
  }

  /**
   * Returns a deep copy of the DataFrame. Duplicates each series as well as the DataFrame itself.
   * <b>NOTE:</b> use caution when applying this to large DataFrames.
   *
   * @return deep copy of DataFrame
   */
  public DataFrame copy() {
    DataFrame df = new DataFrame();
    for(Map.Entry<String, Series> e : this.series.entrySet()) {
      df.addSeries(e.getKey(), e.getValue().copy());
    }
    return df;
  }

  /**
   * Adds a new series to the DataFrame in-place. The new series must have the same row count
   * as the DataFrame. If this is the first series added to an empty DataFrame, it determines
   * the DataFrame size. Further, {@code seriesName} must match the pattern {@code SERIES_NAME_PATTERN}.
   * If a series with {@code seriesName} already exists in the DataFrame it is replaced by
   * {@code series}.
   *
   * @param seriesName series name
   * @param series series
   * @throws IllegalArgumentException if the series does not have the same size or the series name does not match the pattern
   * @return reference to the modified DataFrame (this)
   */
  public DataFrame addSeries(String seriesName, Series series) {
    if(seriesName == null || !SERIES_NAME_PATTERN.matcher(seriesName).matches())
      throw new IllegalArgumentException(String.format("Series name must match pattern '%s'", SERIES_NAME_PATTERN));
    if(!this.series.isEmpty() && series.size() != this.size())
      throw new IllegalArgumentException("DataFrame index and series must be of same length");
    this.series.put(seriesName, series);
    return this;
  }

  /**
   * Adds a new series to the DataFrame in-place. Wraps {@code values} with a series before adding
   * it to the DataFrame with semantics similar to {@code addSeries(String seriesName, Series series)}
   *
   * @param seriesName series name
   * @param values series
   * @return reference to the modified DataFrame (this)
   */
  public DataFrame addSeries(String seriesName, double... values) {
    return addSeries(seriesName, DataFrame.toSeries(values));
  }

  /**
   * Adds a new series to the DataFrame in-place. Wraps {@code values} with a series before adding
   * it to the DataFrame with semantics similar to {@code addSeries(String seriesName, Series series)}
   *
   * @param seriesName series name
   * @param values series
   * @return reference to the modified DataFrame (this)
   */
  public DataFrame addSeries(String seriesName, long... values) {
    return addSeries(seriesName, DataFrame.toSeries(values));
  }

  /**
   * Adds a new series to the DataFrame in-place. Wraps {@code values} with a series before adding
   * it to the DataFrame with semantics similar to {@code addSeries(String seriesName, Series series)}
   *
   * @param seriesName series name
   * @param values series
   * @return reference to the modified DataFrame (this)
   */
  public DataFrame addSeries(String seriesName, String... values) {
    return addSeries(seriesName, DataFrame.toSeries(values));
  }

  /**
   * Adds a new series to the DataFrame in-place. Wraps {@code values} with a series before adding
   * it to the DataFrame with semantics similar to {@code addSeries(String seriesName, Series series)}
   *
   * @param seriesName series name
   * @param values series
   * @return reference to the modified DataFrame (this)
   */
  public DataFrame addSeries(String seriesName, byte... values) {
    return addSeries(seriesName, DataFrame.toSeries(values));
  }

  /**
   * Adds a new series to the DataFrame in-place. Wraps {@code values} with a series before adding
   * it to the DataFrame with semantics similar to {@code addSeries(String seriesName, Series series)}
   *
   * @param seriesName series name
   * @param values series
   * @return reference to the modified DataFrame (this)
   */
  public DataFrame addSeries(String seriesName, boolean... values) {
    return addSeries(seriesName, DataFrame.toSeries(values));
  }

  /**
   * Removes a series from the DataFrame in-place.
   *
   * @param seriesName
   * @throws IllegalArgumentException if the series does not exist
   * @return reference to the modified DataFrame (this)
   */
  public DataFrame dropSeries(String seriesName) {
    assertSeriesExists(seriesName);
    this.series.remove(seriesName);
    return this;
  }

  /**
   * Renames a series in the DataFrame in-place. If a series with name {@code newName} already
   * exists it is replaced by the series referenced by {@code oldName}.
   *
   * @param oldName name of existing series
   * @param newName new name of series
   * @throws IllegalArgumentException if the series referenced by {@code oldName} does not exist
   * @return reference to the modified DataFrame (this)
   */
  public DataFrame renameSeries(String oldName, String newName) {
    Series s = assertSeriesExists(oldName);
    return this.dropSeries(oldName).addSeries(newName, s);
  }

  /**
   * Converts a series in the DataFrame to a new type. The DataFrame is modified in-place, but
   * the series is allocated new memory.
   *
   * @param seriesName name of existing series
   * @param type new native type of series
   * @throws IllegalArgumentException if the series does not exist
   * @return reference to the modified DataFrame (this)
   */
  public DataFrame convertSeries(String seriesName, Series.SeriesType type) {
    this.series.put(seriesName, assertSeriesExists(seriesName).toType(type));
    return this;
  }

  /**
   * Returns the set of names of series contained in the DataFrame.
   *
   * @return series names
   */
  public Set<String> getSeriesNames() {
    return Collections.unmodifiableSet(this.series.keySet());
  }

  /**
   * Returns a copy of the mapping of series names to series encapsulated by this DataFrame
   *
   * @return series mapping
   */
  public Map<String, Series> getSeries() {
    return Collections.unmodifiableMap(this.series);
  }

  /**
   * Returns the series referenced by {@code seriesName}.
   *
   * @param seriesName series name
   * @throws IllegalArgumentException if the series does not exist
   * @return series
   */
  public Series get(String seriesName) {
    return assertSeriesExists(seriesName);
  }

  /**
   * Returns {@code true} if the DataFrame contains a series {@code seriesName}. Otherwise,
   * return {@code false}.
   *
   * @param seriesName series name
   * @return {@code true} if series exists, {@code false} otherwise.
   */
  public boolean contains(String seriesName) {
    return this.series.containsKey(seriesName);
  }

  /**
   * Returns the series referenced by {@code seriesName}. If the series' native type is not
   * {@code DoubleSeries} it is converted transparently.
   *
   * @param seriesName series name
   * @throws IllegalArgumentException if the series does not exist
   * @return DoubleSeries
   */
  public DoubleSeries getDoubles(String seriesName) {
    return assertSeriesExists(seriesName).getDoubles();
  }

  /**
   * Returns the series referenced by {@code seriesName}. If the series' native type is not
   * {@code LongSeries} it is converted transparently.
   *
   * @param seriesName series name
   * @throws IllegalArgumentException if the series does not exist
   * @return LongSeries
   */
  public LongSeries getLongs(String seriesName) {
    return assertSeriesExists(seriesName).getLongs();
  }

  /**
   * Returns the series referenced by {@code seriesName}. If the series' native type is not
   * {@code StringSeries} it is converted transparently.
   *
   * @param seriesName series name
   * @throws IllegalArgumentException if the series does not exist
   * @return StringSeries
   */
  public StringSeries getStrings(String seriesName) {
    return assertSeriesExists(seriesName).getStrings();
  }

  /**
   * Returns the series referenced by {@code seriesName}. If the series' native type is not
   * {@code BooleanSeries} it is converted transparently.
   *
   * @param seriesName series name
   * @throws IllegalArgumentException if the series does not exist
   * @return BooleanSeries
   */
  public BooleanSeries getBooleans(String seriesName) {
   return assertSeriesExists(seriesName).getBooleans();
  }

  /**
   * Applies {@code function} to the series referenced by {@code seriesNames} row by row
   * and returns the results as a new series. The series' values are mapped to arguments
   * of {@code function} in the same order as they appear in {@code seriesNames}.
   * If the series' native types do not match the required input types of {@code function},
   * the series are converted transparently. The native type of the returned series is
   * determined by {@code function}'s output type.
   *
   * @param function function to apply to each element in the series
   * @throws IllegalArgumentException if the series does not exist
   * @return series with evaluation results
   */
  public DoubleSeries map(Series.DoubleFunction function, String... seriesNames) {
    return map(function, names2series(seriesNames));
  }

  /**
   * Applies {@code function} to {@code series} row by row
   * and returns the results as a new series. The series' values are mapped to arguments
   * of {@code function} in the same order as they appear in {@code series}.
   * If the series' native types do not match the required input types of {@code function},
   * the series are converted transparently. The native type of the returned series is
   * determined by {@code function}'s output type.
   *
   * @param function function to apply to each element in the series
   * @throws IllegalArgumentException if the series does not exist
   * @return series with evaluation results
   */
  public static DoubleSeries map(Series.DoubleFunction function, Series... series) {
    if(series.length <= 0)
      return DoubleSeries.empty();

    assertSameLength(series);

    DoubleSeries[] doubleSeries = new DoubleSeries[series.length];
    for(int i=0; i<series.length; i++) {
      doubleSeries[i] = series[i].getDoubles();
    }

    double[] output = new double[series[0].size()];
    for(int i=0; i<series[0].size(); i++) {
      double[] input = new double[series.length];

      boolean isNull = false;
      for(int j=0; j<series.length; j++) {
        double value = doubleSeries[j].values[i];
        if(DoubleSeries.isNull(value)) {
          isNull = true;
          break;
        } else {
          input[j] = value;
        }
      }

      if(isNull) {
        output[i] = DoubleSeries.NULL_VALUE;
      } else {
        output[i] = function.apply(input);
      }
    }

    return DoubleSeries.buildFrom(output);
  }

  /**
   * Applies {@code function} to the series referenced by {@code seriesNames} row by row
   * and returns the results as a new series. The series' values are mapped to arguments
   * of {@code function} in the same order as they appear in {@code seriesNames}.
   * If the series' native types do not match the required input types of {@code function},
   * the series are converted transparently. The native type of the returned series is
   * determined by {@code function}'s output type.
   *
   * @param function function to apply to each element in the series
   * @throws IllegalArgumentException if the series does not exist
   * @return series with evaluation results
   */
  public LongSeries map(Series.LongFunction function, String... seriesNames) {
    return map(function, names2series(seriesNames));
  }

  /**
   * Applies {@code function} to {@code series} row by row
   * and returns the results as a new series. The series' values are mapped to arguments
   * of {@code function} in the same order as they appear in {@code series}.
   * If the series' native types do not match the required input types of {@code function},
   * the series are converted transparently. The native type of the returned series is
   * determined by {@code function}'s output type.
   *
   * @param function function to apply to each element in the series
   * @throws IllegalArgumentException if the series does not exist
   * @return series with evaluation results
   */
  public static LongSeries map(Series.LongFunction function, Series... series) {
    if(series.length <= 0)
      return LongSeries.empty();

    assertSameLength(series);

    LongSeries[] longSeries = new LongSeries[series.length];
    for(int i=0; i<series.length; i++) {
      longSeries[i] = series[i].getLongs();
    }

    long[] output = new long[series[0].size()];
    for(int i=0; i<series[0].size(); i++) {
      long[] input = new long[series.length];
      boolean isNull = false;
      for(int j=0; j<series.length; j++) {
        long value = longSeries[j].values[i];
        if(LongSeries.isNull(value)) {
          isNull = true;
          break;
        } else {
          input[j] = value;
        }
      }

      if(isNull) {
        output[i] = LongSeries.NULL_VALUE;
      } else {
        output[i] = function.apply(input);
      }
    }

    return LongSeries.buildFrom(output);
  }

  /**
   * Applies {@code function} to the series referenced by {@code seriesNames} row by row
   * and returns the results as a new series. The series' values are mapped to arguments
   * of {@code function} in the same order as they appear in {@code seriesNames}.
   * If the series' native types do not match the required input types of {@code function},
   * the series are converted transparently. The native type of the returned series is
   * determined by {@code function}'s output type.
   *
   * @param function function to apply to each element in the series
   * @throws IllegalArgumentException if the series does not exist
   * @return series with evaluation results
   */
  public StringSeries map(Series.StringFunction function, String... seriesNames) {
    return map(function, names2series(seriesNames));
  }

  /**
   * Applies {@code function} to {@code series} row by row
   * and returns the results as a new series. The series' values are mapped to arguments
   * of {@code function} in the same order as they appear in {@code series}.
   * If the series' native types do not match the required input types of {@code function},
   * the series are converted transparently. The native type of the returned series is
   * determined by {@code function}'s output type.
   *
   * @param function function to apply to each element in the series
   * @throws IllegalArgumentException if the series does not exist
   * @return series with evaluation results
   */
  public static StringSeries map(Series.StringFunction function, Series... series) {
    if(series.length <= 0)
      return StringSeries.empty();

    assertSameLength(series);

    StringSeries[] stringSeries = new StringSeries[series.length];
    for(int i=0; i<series.length; i++) {
      stringSeries[i] = series[i].getStrings();
    }

    String[] output = new String[series[0].size()];
    for(int i=0; i<series[0].size(); i++) {
      String[] input = new String[series.length];
      boolean isNull = false;
      for(int j=0; j<series.length; j++) {
        String value = stringSeries[j].values[i];
        if(StringSeries.isNull(value)) {
          isNull = true;
          break;
        } else {
          input[j] = value;
        }
      }

      if(isNull) {
        output[i] = StringSeries.NULL_VALUE;
      } else {
        output[i] = function.apply(input);
      }
    }

    return StringSeries.buildFrom(output);
  }

  /**
   * Applies {@code function} to the series referenced by {@code seriesNames} row by row
   * and returns the results as a new series. The series' values are mapped to arguments
   * of {@code function} in the same order as they appear in {@code seriesNames}.
   * If the series' native types do not match the required input types of {@code function},
   * the series are converted transparently. The native type of the returned series is
   * determined by {@code function}'s output type.
   *
   * @param function function to apply to each element in the series
   * @throws IllegalArgumentException if the series does not exist
   * @return series with evaluation results
   */
  public BooleanSeries map(Series.BooleanFunction function, String... seriesNames) {
    return map(function, names2series(seriesNames));
  }

  /**
   * Applies {@code function} to {@code series} row by row
   * and returns the results as a new series. The series' values are mapped to arguments
   * of {@code function} in the same order as they appear in {@code series}.
   * If the series' native types do not match the required input types of {@code function},
   * the series are converted transparently. The native type of the returned series is
   * determined by {@code function}'s output type.
   *
   * @param function function to apply to each element in the series
   * @throws IllegalArgumentException if the series does not exist
   * @return series with evaluation results
   */
  public static BooleanSeries map(Series.BooleanFunction function, Series... series) {
    if(series.length <= 0)
      return BooleanSeries.empty();

    assertSameLength(series);

    BooleanSeries[] booleanSeries = new BooleanSeries[series.length];
    for(int i=0; i<series.length; i++) {
      booleanSeries[i] = series[i].getBooleans();
    }

    byte[] output = new byte[series[0].size()];
    for(int i=0; i<series[0].size(); i++) {
      byte[] input = new byte[series.length];
      boolean isNull = false;
      for(int j=0; j<series.length; j++) {
        byte value = booleanSeries[j].values[i];
        if(BooleanSeries.isNull(value)) {
          isNull = true;
          break;
        } else {
          input[j] = value;
        }
      }

      if(isNull) {
        output[i] = BooleanSeries.NULL_VALUE;
      } else {
        output[i] = function.apply(input);
      }
    }

    return BooleanSeries.buildFrom(output);
  }

  /**
   * Applies {@code doubleExpression} compiled to an expression to the series referenced by
   * {@code seriesNames} row by row and returns the results as a new series. The series' values
   * are mapped to variables in {@code doubleExpression} by series names. Only series referenced
   * by {@code seriesNames} can be referenced by the expression.
   * The series are converted to {@code DoubleSeries} transparently and the results
   * are returned as DoubleSeries as well.
   *
   * <b>NOTE:</b> doubleExpression is compiled to an {@code EvalEx} expression.
   *
   * @param doubleExpression expression to be compiled and applied using EvalEx
   * @throws IllegalArgumentException if the series does not exist
   * @return series with evaluation results
   */
  public DoubleSeries map(String doubleExpression, final String... seriesNames) {
    final Expression e = new Expression(doubleExpression);

    return this.map(new Series.DoubleFunction() {
      @Override
      public double apply(double[] values) {
        for(int i=0; i<values.length; i++) {
          e.with(seriesNames[i], new BigDecimal(values[i]));
        }
        return e.eval().doubleValue();
      }
    }, seriesNames);
  }

  /**
   * Applies {@code doubleExpression} compiled to an expression to the series referenced by
   * {@code seriesNames} row by row and returns the results as a new series. The series' values
   * are mapped to variables in {@code doubleExpression} by series names. All series contained
   * in the DataFrame can be referenced by the expression.
   * The series are converted to {@code DoubleSeries} transparently and the results
   * are returned as DoubleSeries as well.
   *
   * <b>NOTE:</b> doubleExpression is compiled to an {@code EvalEx} expression.
   *
   * @param doubleExpression expression to be compiled and applied using EvalEx
   * @throws IllegalArgumentException if the series does not exist
   * @return series with evaluation results
   */
  public DoubleSeries map(String doubleExpression) {
    Set<String> variables = extractSeriesNames(doubleExpression);
    return this.map(doubleExpression, variables.toArray(new String[variables.size()]));
  }

  /**
   * Returns a projection of the DataFrame.
   *
   * <b>NOTE:</b> fromIndex <= -1 is filled with {@code null}.
   * <b>NOTE:</b> array with length 0 produces empty series.
   *
   * @param fromIndex array with indices to project from (must be <= series size)
   * @return DataFrame projection
   */
  public DataFrame project(int[] fromIndex) {
    DataFrame newDataFrame = new DataFrame();
    for(Map.Entry<String, Series> e : this.series.entrySet()) {
      newDataFrame.addSeries(e.getKey(), e.getValue().project(fromIndex));
    }
    return newDataFrame;
  }

  /**
   * Returns a copy of the DataFrame sorted by series values referenced by {@code seriesNames}.
   * The resulting sorted order is the equivalent of applying a stable sort to the nth series
   * first, and then sorting iteratively by series until the 1st series.
   *
   * @param seriesNames 1st series, 2nd series, ..., nth series
   * @throws IllegalArgumentException if the series does not exist
   * @return sorted DataFrame copy
   */
  public DataFrame sortedBy(String... seriesNames) {
    DataFrame df = this;
    for(int i=seriesNames.length-1; i>=0; i--) {
      df = df.project(assertSeriesExists(seriesNames[i]).sortedIndex());
    }
    return df;
  }

  /**
   * Returns a copy of the DataFrame with the order of values in the series reversed.
   *
   * @return reversed DataFrame copy
   */
  public DataFrame reverse() {
    DataFrame newDataFrame = new DataFrame();
    for(Map.Entry<String, Series> e : this.series.entrySet()) {
      newDataFrame.addSeries(e.getKey(), e.getValue().reverse());
    }
    return newDataFrame;
  }

  /**
   * Returns a copy of the DataFrame with values resampled by {@code interval} using {@code strategy}
   * on the series referenced by {@code seriesName}. The method first applies an interval-based
   * grouping to the series and then aggregates the DataFrame using the specified strategy. If
   * the series referenced by {@code seriesName} is not of native type {@code LongSeries} it is
   * converted transparently.
   *
   * @param seriesName target series for resampling
   * @param interval resampling interval
   * @param strategy resampling strategy
   * @throws IllegalArgumentException if the series does not exist
   * @return resampled DataFrame copy
   */
  public DataFrame resampledBy(String seriesName, long interval, ResamplingStrategy strategy) {
    DataFrame baseDataFrame = this.sortedBy(seriesName);

    Series.SeriesGrouping grouping = baseDataFrame.getLongs(seriesName).groupByInterval(interval);

    // resample series
    DataFrame newDataFrame = new DataFrame();

    for(Map.Entry<String, Series> e : baseDataFrame.getSeries().entrySet()) {
      if(e.getKey().equals(seriesName))
        continue;
      newDataFrame.addSeries(e.getKey(), strategy.apply(grouping, e.getValue()).get(Series.COLUMN_VALUE));
    }

    // new series
    newDataFrame.addSeries(seriesName, grouping.keys());
    return newDataFrame;
  }

  /**
   * Returns a copy of the DataFrame with rows filtered by {@code series}. If the value of {@code series}
   * Associated with a row is {@code true} the row is included, otherwise it is omitted.
   *
   * @param series filter series
   * @return filtered DataFrame copy
   */
  public DataFrame filter(BooleanSeries series) {
    if(series.size() != this.size())
      throw new IllegalArgumentException("Series size must be equal to index size");

    int[] fromIndex = new int[series.size()];
    int fromIndexCount = 0;
    for(int i=0; i<series.size(); i++) {
      if(BooleanSeries.isTrue(series.values[i])) {
        fromIndex[fromIndexCount] = i;
        fromIndexCount++;
      }
    }

    int[] fromIndexCompressed = Arrays.copyOf(fromIndex, fromIndexCount);

    return this.project(fromIndexCompressed);
  }

  public DataFrame filter(String seriesName) {
    return this.filter(this.getBooleans(seriesName));
  }

  public DataFrame filter(String seriesName, DoubleSeries.DoubleConditional conditional) {
    return this.filter(assertSeriesExists(seriesName).getDoubles().map(conditional));
  }

  public DataFrame filter(String seriesName, LongSeries.LongConditional conditional) {
    return this.filter(assertSeriesExists(seriesName).getLongs().map(conditional));
  }

  public DataFrame filter(String seriesName, StringSeries.StringConditional conditional) {
    return this.filter(assertSeriesExists(seriesName).getStrings().map(conditional));
  }

  public DataFrame filterEquals(String seriesName, final double value) {
    return this.filter(seriesName, new DoubleSeries.DoubleConditional() {
      @Override
      public boolean apply(double v) {
        return value == v;
      }
    });
  }

  public DataFrame filterEquals(String seriesName, final long value) {
    return this.filter(seriesName, new LongSeries.LongConditional() {
      @Override
      public boolean apply(long v) {
        return value == v;
      }
    });
  }

  public DataFrame filterEquals(String seriesName, final String value) {
    return this.filter(seriesName, new StringSeries.StringConditional() {
      @Override
      public boolean apply(String v) {
        return value.equals(v);
      }
    });
  }

  /**
   * Returns series {@code s} converted to type {@code type} unless native type matches already.
   *
   * @param s input series
   * @param type target type
   * @return converted series
   */
  public static Series asType(Series s, Series.SeriesType type) {
    switch(type) {
      case DOUBLE:
        return s.getDoubles();
      case LONG:
        return s.getLongs();
      case BOOLEAN:
        return s.getBooleans();
      case STRING:
        return s.getStrings();
      default:
        throw new IllegalArgumentException(String.format("Unknown series type '%s'", type));
    }
  }

  /**
   * Returns a DataFrameGrouping based on the labels provided by {@code labels} row by row.
   * The size of {@code labels} must match the size of the DataFrame.
   *
   * @param labels grouping labels
   * @return DataFrameGrouping
   */
  public DataFrameGrouping groupBy(Series labels) {
    Series.SeriesGrouping grouping = labels.groupByValue();
    return new DataFrameGrouping(grouping.keys(), this, grouping.buckets);
  }

  /**
   * Returns a DataFrameGrouping based on the labels provided by the series referenced by
   * {@code seriesName} row by row.
   *
   * @param seriesName series containing grouping labels
   * @return DataFrameGrouping
   */
  public DataFrameGrouping groupBy(String seriesName) {
    return this.groupBy(this.get(seriesName));
  }

  /**
   * Returns a copy of the DataFrame omitting rows that contain a {@code null} value in any series.
   *
   * @return DataFrame copy without null rows
   */
  public DataFrame dropNull() {
    int[] fromIndex = new int[this.size()];
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

    return this.project(fromIndexCompressed);
  }

  /**
   * Returns a copy of the DataFrame omitting series that contain a {@code null} value.
   *
   * @return DataFrame copy without null series
   */
  public DataFrame dropNullColumns() {
    DataFrame df = new DataFrame();
    for(Map.Entry<String, Series> e : this.getSeries().entrySet()) {
      if(!e.getValue().hasNull())
        df.addSeries(e.getKey(), e.getValue());
    }
    return df;
  }

  public DataFrame joinInner(DataFrame other, String onSeriesLeft, String onSeriesRight) {
    List<Series.JoinPair> pairs = this.get(onSeriesLeft).join(other.get(onSeriesRight), Series.JoinType.INNER);
    return DataFrame.join(this, other, pairs);
  }

  public DataFrame joinLeft(DataFrame other, String onSeriesLeft, String onSeriesRight) {
    List<Series.JoinPair> pairs = this.get(onSeriesLeft).join(other.get(onSeriesRight), Series.JoinType.LEFT);
    return DataFrame.join(this, other, pairs);
  }

  public DataFrame joinRight(DataFrame other, String onSeriesLeft, String onSeriesRight) {
    List<Series.JoinPair> pairs = this.get(onSeriesLeft).join(other.get(onSeriesRight), Series.JoinType.RIGHT);
    return DataFrame.join(this, other, pairs);
  }

  public DataFrame joinOuter(DataFrame other, String onSeriesLeft, String onSeriesRight) {
    List<Series.JoinPair> pairs = this.get(onSeriesLeft).join(other.get(onSeriesRight), Series.JoinType.OUTER);
    return DataFrame.join(this, other, pairs);
  }

  private static DataFrame join(DataFrame left, DataFrame right, List<Series.JoinPair> pairs) {
    int[] fromIndexLeft = new int[pairs.size()];
    int i=0;
    for(Series.JoinPair p : pairs) {
      fromIndexLeft[i++] = p.left;
    }

    int[] fromIndexRight = new int[pairs.size()];
    int j=0;
    for(Series.JoinPair p : pairs) {
      fromIndexRight[j++] = p.right;
    }

    DataFrame leftData = left.project(fromIndexLeft);
    DataFrame rightData = right.project(fromIndexRight);

    Set<String> seriesLeft = left.getSeriesNames();
    for(Map.Entry<String, Series> e : rightData.getSeries().entrySet()) {
      String seriesName = e.getKey();
      // TODO: better approach to conditional rename
      if(seriesLeft.contains(seriesName) && !leftData.get(seriesName).equals(rightData.get(seriesName))) {
        seriesName = e.getKey() + COLUMN_JOIN_POSTFIX;
      }

      leftData.addSeries(seriesName, e.getValue());
    }

    return leftData;
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DataFrame dataFrame = (DataFrame) o;

    return series != null ? series.equals(dataFrame.series) : dataFrame.series == null;
  }

  @Override
  public int hashCode() {
    return series != null ? series.hashCode() : 0;
  }

  private Series[] names2series(String... names) {
    Series[] inputSeries = new Series[names.length];
    for(int i=0; i<names.length; i++) {
      inputSeries[i] = assertSeriesExists(names[i]);
    }
    return inputSeries;
  }

  private Series assertSeriesExists(String name) {
    if(!series.containsKey(name))
      throw new IllegalArgumentException(String.format("Unknown series '%s'", name));
    return series.get(name);
  }

  private void assertSameLength(Series s) {
    if(this.size() != s.size())
      throw new IllegalArgumentException("Series size must be equals to DataFrame size");
  }

  private static void assertSameLength(Series... series) {
    for(int i=0; i<series.length-1; i++) {
      if (series[i].size() != series[i+1].size())
        throw new IllegalArgumentException("Series size must be equals to DataFrame size");
    }
  }

  private Set<String> extractSeriesNames(String doubleExpression) {
    Matcher m = SERIES_NAME_PATTERN.matcher(doubleExpression);

    Set<String> variables = new HashSet<>();
    while(m.find()) {
      if(this.series.keySet().contains(m.group()))
        variables.add(m.group());
    }

    return variables;
  }

  /* **************************************************************************
   * DataFrame parsers
   ***************************************************************************/

  /**
   * Reads in a CSV structured stream and returns it as a DataFrame.
   *
   * @param in input reader
   * @return CSV as DataFrame
   * @throws IOException if a read error is encountered
   */
  public static DataFrame fromCsv(Reader in) throws IOException {
    Iterator<CSVRecord> it = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(in).iterator();
    if(!it.hasNext())
      return new DataFrame();

    CSVRecord first = it.next();
    Set<String> headers = first.toMap().keySet();

    Map<String, StringSeries.Builder> builders = new HashMap<>();
    for(String h : headers) {
      StringSeries.Builder builder = StringSeries.builder();
      builder.add(first.get(h));
      builders.put(h, builder);
    }

    while(it.hasNext()) {
      CSVRecord record = it.next();
      for(String key : headers) {
        String value = record.get(key);
        builders.get(key).add(value);
      }
    }

    DataFrame df = new DataFrame();
    for(Map.Entry<String, StringSeries.Builder> e : builders.entrySet()) {
      StringSeries s = e.getValue().build();
      Series conv = s.toType(s.inferType());
      df.addSeries(e.getKey(), conv);
    }

    return df;
  }

  /**
   * Reads in a Pinot ResultSetGroup and returns it as a DataFrame.
   *
   * <b>NOTE:</b> cannot parse a query result with multiple group aggregations
   *
   * @param resultSetGroup pinot query result
   * @return Pinot query result as DataFrame
   * @throws IllegalArgumentException if the result cannot be parsed
   */
  public static DataFrame fromPinotResult(ResultSetGroup resultSetGroup) {
    if (resultSetGroup.getResultSetCount() <= 0)
      throw new IllegalArgumentException("Query did not return any results");

    if (resultSetGroup.getResultSetCount() > 1)
      throw new IllegalArgumentException("Query returned multiple results");

    ResultSet resultSet = resultSetGroup.getResultSet(0);

    DataFrame df = new DataFrame();

    // TODO conditions not necessarily safe
    if(resultSet.getColumnCount() == 1 && resultSet.getRowCount() == 0) {
      // empty result

    } else if(resultSet.getColumnCount() == 1 && resultSet.getRowCount() == 1 && resultSet.getGroupKeyLength() == 0) {
      // aggregation result

      String function = resultSet.getColumnName(0);
      String value = resultSet.getString(0, 0);
      df.addSeries(function, DataFrame.toSeries(new String[] { value }));

    } else if(resultSet.getColumnCount() == 1 && resultSet.getGroupKeyLength() > 0) {
      // groupby result

      String function = resultSet.getColumnName(0);
      df.addSeries(function, makeGroupByValueSeries(resultSet));
      for(int i=0; i<resultSet.getGroupKeyLength(); i++) {
        String groupKey = resultSet.getGroupKeyColumnName(i);
        df.addSeries(groupKey, makeGroupByGroupSeries(resultSet, i));
      }

    } else if(resultSet.getColumnCount() >= 1 && resultSet.getGroupKeyLength() == 0) {
      // selection result

      for (int i = 0; i < resultSet.getColumnCount(); i++) {
        df.addSeries(resultSet.getColumnName(i), makeSelectionSeries(resultSet, i));
      }

    } else {
      // defensive
      throw new IllegalStateException("Could not determine DataFrame shape from output");
    }

    return df;
  }

  private static Series makeSelectionSeries(ResultSet resultSet, int colIndex) {
    int rowCount = resultSet.getRowCount();
    if(rowCount <= 0)
      return StringSeries.empty();

    //DataFrame.SeriesType type = inferType(resultSet.getString(0, colIndex));

    String[] values = new String[rowCount];
    for(int i=0; i<rowCount; i++) {
      values[i] = resultSet.getString(i, colIndex);
    }

    return DataFrame.toSeries(values);
  }

  private static Series makeGroupByValueSeries(ResultSet resultSet) {
    int rowCount = resultSet.getRowCount();
    if(rowCount <= 0)
      return StringSeries.empty();

    String[] values = new String[rowCount];
    for(int i=0; i<rowCount; i++) {
      values[i] = resultSet.getString(i, 0);
    }

    return DataFrame.toSeries(values);
  }

  private static Series makeGroupByGroupSeries(ResultSet resultSet, int keyIndex) {
    int rowCount = resultSet.getRowCount();
    if(rowCount <= 0)
      return StringSeries.empty();

    String[] values = new String[rowCount];
    for(int i=0; i<rowCount; i++) {
      values[i] = resultSet.getGroupKeyString(i, keyIndex);
    }

    return DataFrame.toSeries(values);
  }
}
