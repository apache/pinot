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
 *
 */

package org.apache.pinot.thirdeye.dataframe;

import com.udojava.evalex.Expression;
import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.client.ResultSet;
import org.apache.pinot.client.ResultSetGroup;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;


/**
 * Container class for a data frame with multiple typed series with equivalent row count.
 */
public class DataFrame {
  public static final Pattern PATTERN_FORMULA_VARIABLE = Pattern.compile("\\$\\{([^}]*)}");

  public static final String COLUMN_JOIN_KEY = "join_key";

  public static final String COLUMN_INDEX_DEFAULT = "index";
  public static final String COLUMN_JOIN_LEFT = "_left";
  public static final String COLUMN_JOIN_RIGHT = "_right";
  public static final int DEFAULT_MAX_COLUMN_WIDTH = 30;

  /**
   * Builder for DataFrame in row-by-row sequence. Constructs each column as a StringSeries
   * and attempts to infer a tighter native type on completion.
   */
  public static final class Builder {
    final List<String> seriesNames;
    final List<Object[]> rows = new ArrayList<>();

    Builder(List<String> seriesNames) {
      this.seriesNames = seriesNames;
    }

    public Builder append(Collection<Object[]> rows) {
      for(Object[] row : rows) {
        if (row.length != this.seriesNames.size())
          throw new IllegalArgumentException(String.format("Expected %d values, but got %d", seriesNames.size(), row.length));
        this.rows.add(row);
      }
      return this;
    }

    public Builder append(Object[]... rows) {
      return this.append(Arrays.asList(rows));
    }

    public Builder append(Object... row) {
      return this.append(Collections.singleton(row));
    }

    public DataFrame build() {
      DataFrame df = new DataFrame();

      // infer column types
      for(int i=0; i<seriesNames.size(); i++) {
        String rawName = seriesNames.get(i);

        String[] parts = rawName.split(":");
        String typeString = parts[parts.length - 1];

        if(parts.length > 1 && getValidTypes().contains(typeString)) {
          // user specified type
          String name = StringUtils.join(Arrays.copyOf(parts, parts.length - 1), ":");
          Series.SeriesType type = Series.SeriesType.valueOf(typeString);
          Series series = buildSeries(type, i);
          df.addSeries(name, series);

        } else {
          // dynamic type
          ObjectSeries series = buildObjectSeries(i);
          Series.SeriesType type = series.inferType();
          df.addSeries(rawName, series.get(type));
        }
      }

      return df;
    }

    private Series buildSeries(Series.SeriesType type, int columnIndex) {
      switch (type) {
        case DOUBLE:
          return buildDoubleSeries(columnIndex);
        case LONG:
          return buildLongSeries(columnIndex);
        case STRING:
          return buildStringSeries(columnIndex);
        case BOOLEAN:
          return buildBooleanSeries(columnIndex);
        case OBJECT:
          return buildObjectSeries(columnIndex);
        default:
          throw new IllegalArgumentException(String.format("Unknown series type '%s'", type));
      }
    }

    // TODO implement ObjectSeries
    private DoubleSeries buildDoubleSeries(int columnIndex) {
      double[] values = new double[this.rows.size()];
      int i = 0;
      for(Object[] r : this.rows) {
        values[i++] = toDouble(r[columnIndex]);
      }
      return DoubleSeries.buildFrom(values);
    }

    private static double toDouble(Object o) {
      if(o == null)
        return DoubleSeries.NULL;
      if(o instanceof Number)
        return ((Number)o).doubleValue();
      return StringSeries.getDouble(o.toString());
    }

    private LongSeries buildLongSeries(int columnIndex) {
      long[] values = new long[this.rows.size()];
      int i = 0;
      for(Object[] r : this.rows) {
        values[i++] = toLong(r[columnIndex]);
      }
      return LongSeries.buildFrom(values);
    }

    private static long toLong(Object o) {
      if(o == null)
        return LongSeries.NULL;
      if(o instanceof Number)
        return ((Number)o).longValue();
      return StringSeries.getLong(o.toString());
    }

    private StringSeries buildStringSeries(int columnIndex) {
      String[] values = new String[this.rows.size()];
      int i = 0;
      for(Object[] r : this.rows) {
        values[i++] = toString(r[columnIndex]);
      }
      return StringSeries.buildFrom(values);
    }

    private static String toString(Object o) {
      if(o == null)
        return StringSeries.NULL;
      return StringSeries.getString(o.toString());
    }

    private BooleanSeries buildBooleanSeries(int columnIndex) {
      byte[] values = new byte[this.rows.size()];
      int i = 0;
      for(Object[] r : this.rows) {
        values[i++] = toBoolean(r[columnIndex]);
      }
      return BooleanSeries.buildFrom(values);
    }

    private static byte toBoolean(Object o) {
      if(o == null)
        return BooleanSeries.NULL;
      if(o instanceof Number)
        return BooleanSeries.valueOf(((Number)o).doubleValue() != 0.0d);
      return StringSeries.getBoolean(o.toString());
    }

    private ObjectSeries buildObjectSeries(int columnIndex) {
      Object[] values = new Object[this.rows.size()];
      int i = 0;
      for(Object[] r : this.rows) {
        values[i++] = r[columnIndex];
      }
      return ObjectSeries.buildFrom(values);
    }
  }

  final List<String> indexNames = new ArrayList<>();
  final Map<String, Series> series = new LinkedHashMap<>();

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
    return BooleanSeries.builder().addBooleanValues(values).build();
  }

  /**
   * Returns a ObjectSeries wrapping the values array
   *
   * @param values base array
   * @return ObjectSeries wrapping the array
   */
  public static ObjectSeries toSeriesObjects(Object... values) {
    return ObjectSeries.builder().addValues(values).build();
  }

  /**
   * Returns a builder instance for DataFrame
   *
   * @param seriesNames series names of the DataFrame
   * @return FDataFrame builder
   */
  public static Builder builder(String... seriesNames) {
    return new Builder(Arrays.asList(seriesNames));
  }

  /**
   * Returns a builder instance for DataFrame
   *
   * @param seriesNames series names of the DataFrame
   * @return FDataFrame builder
   */
  public static Builder builder(List<String> seriesNames) {
    return new Builder(seriesNames);
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
   * Returns a builder instance for ObjectSeries
   *
   * @return ObjectSeries builder
   */
  public static ObjectSeries.Builder buildObjects() {
    return ObjectSeries.builder();
  }

  /**
   * Creates a new DataFrame with a column "index" (as determined by {@code COLUMN_INDEX_DEFAULT}) with
   * length {@code defaultIndexSize}, ranging from 0 to {@code defaultIndexSize - 1}.
   *
   * @param defaultIndexSize index column size
   */
  public DataFrame(int defaultIndexSize) {
    long[] indexValues = new long[defaultIndexSize];
    for(int i=0; i<defaultIndexSize; i++) {
      indexValues[i] = i;
    }
    this.addSeries(COLUMN_INDEX_DEFAULT, LongSeries.buildFrom(indexValues));
    this.indexNames.add(COLUMN_INDEX_DEFAULT);
  }

  /**
   * Creates a new DataFrame with a column "index" (as determined by {@code COLUMN_INDEX_DEFAULT}) that
   * wraps the array {@code indexValues}.
   *
   * @param indexValues index values
   */
  public DataFrame(long... indexValues) {
    this.addSeries(COLUMN_INDEX_DEFAULT, LongSeries.buildFrom(indexValues));
    this.indexNames.add(COLUMN_INDEX_DEFAULT);
  }

  /**
   * Creates a new DataFrame with a column "index" (as determined by {@code COLUMN_INDEX_DEFAULT}) referencing
   * the Series {@code index}.
   *
   * @param index index series
   */
  public DataFrame(Series index) {
    this.addSeries(COLUMN_INDEX_DEFAULT, index);
    this.indexNames.add(COLUMN_INDEX_DEFAULT);
  }

  /**
   * Creates a new DataFrame with a column {@code indexName} referencing the Series {@code index}.
   *
   * @param indexName index series column name
   * @param index index series
   */
  public DataFrame(String indexName, Series index) {
    this.addSeries(indexName, index);
    this.indexNames.add(indexName);
  }

  /**
   * Creates a new DataFrame that copies the properties of {@code df}.
   *
   * <br/><b>NOTE:</b> the copy is shallow, i.e. the contained series are not copied but referenced.
   *
   * @param df DataFrame to copy properties from
   */
  public DataFrame(DataFrame df) {
    this.indexNames.addAll(df.indexNames);
    this.series.putAll(df.series);
  }

  /**
   * Creates a new DataFrame without any columns. The row count of the DataFrame is determined
   * by the first series added.
   */
  public DataFrame() {
    // left blank
  }

  /**
   * Sets the index name to the specified series name in-place.
   *
   * @param seriesNames index series names
   * @throws IllegalArgumentException if the series does not exist
   * @return reference to the modified DataFrame (this)
   */
  public DataFrame setIndex(String... seriesNames) {
    return this.setIndex(Arrays.asList(seriesNames));
  }

  /**
   * Sets the index name to the specified series name in-place.
   *
   * @param seriesNames index series names
   * @throws IllegalArgumentException if the series does not exist
   * @return reference to the modified DataFrame (this)
   */
  public DataFrame setIndex(List<String> seriesNames) {
    assertSeriesExist(seriesNames);
    this.indexNames.clear();
    this.indexNames.addAll(seriesNames);
    return this;
  }

  /**
   * Resets the index name to {@code null} in-place.
   *
   * @return reference to the modified DataFrame (this)
   */
  public DataFrame resetIndex() {
    this.indexNames.clear();
    return this;
  }

  /**
   * Returns the series referenced by indexName, if there is exactly one index column.
   *
   * @throws IllegalArgumentException if the series does not exist
   * @return index series
   */
  public Series getIndexSingleton() {
    if (this.indexNames.size() != 1)
      throw new IllegalArgumentException("Must have exactly one index column");
    return assertSeriesExists(this.indexNames.get(0));
  }

  /**
   * Returns the series referenced by indexNames.
   *
   * @throws IllegalArgumentException if the series does not exist
   * @return index series
   */
  public List<Series> getIndexSeries() {
    List<Series> series = new ArrayList<>();
    for (String name : this.indexNames)
      series.add(this.get(name));
    return series;
  }

  /**
   * Returns {@code true} if a valid index name is set. Otherwise, returns {@code false}.
   *
   * @return {@code true} if a valid index name is set, {@code false} otherwise
   */
  public boolean hasIndex() {
    return !this.indexNames.isEmpty();
  }

  /**
   * Returns the series name of the index, or {@code null} if no index name is set.
   *
   * @return index series name
   */
  public List<String> getIndexNames() {
    return new ArrayList<>(this.indexNames);
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
    DataFrame df = new DataFrame(this);
    df.series.clear();
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
   * Returns a copy of the DataFrame including only the series in {@code seriesNames}.
   *
   * @param seriesNames series names
   * @return sliced DataFrame copy
   */
  public DataFrame slice(String... seriesNames) {
    return this.slice(Arrays.asList(seriesNames));
  }

  /**
   * Returns a copy of the DataFrame including only the series in {@code seriesNames}.
   *
   * @param seriesNames series names
   * @return sliced DataFrame copy
   */
  public DataFrame slice(Iterable<String> seriesNames) {
    DataFrame df = new DataFrame();
    List<String> index = new ArrayList<>();
    df.series.clear();
    for(String name : seriesNames) {
      df.addSeries(name, this.get(name).copy());
      if(this.indexNames.contains(name))
        index.add(name);
    }
    df.setIndex(index);
    return df;
  }

  /**
   * Returns a copy of the DataFrame containing (up to) {@code n} first rows.
   *
   * @param n number of rows to include
   * @return DataFrame copy with first {@code n} rows
   */
  public DataFrame head(int n) {
    return this.slice(0, n);
  }

  /**
   * Returns a copy of the DataFrame containing (up to) {@code n} last rows.
   *
   * @param n number of rows to include
   * @return DataFrame copy with last {@code n} rows
   */
  public DataFrame tail(int n) {
    return this.slice(this.size() - n, this.size());
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
   * <br/><b>NOTE:</b> use caution when applying this to large DataFrames.
   *
   * @return deep copy of DataFrame
   */
  public DataFrame copy() {
    DataFrame df = new DataFrame(this);
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
    if(seriesName == null)
      throw new IllegalArgumentException("series name must not be null");
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
   * Adds a new series to the DataFrame in-place. Wraps {@code values} with a series before adding
   * it to the DataFrame with semantics similar to {@code addSeries(String seriesName, Series series)}
   *
   * @param seriesName series name
   * @param values series
   * @return reference to the modified DataFrame (this)
   */
  public DataFrame addSeriesObjects(String seriesName, Object... values) {
    return addSeries(seriesName, DataFrame.toSeriesObjects(values));
  }

  /**
   * Adds new series to the DataFrame in-place. The series are copied from a source DataFrame
   * and aligned based on the indexes. If the indexes match, the series are copied directly.
   * Otherwise, the method performs a left join to align series from the source with the
   * destination's (this) index.
   *
   * @param source source DataFrame
   * @param seriesNames series names
   * @return reference to modified DataFrame (this)
   * @throws IllegalArgumentException if one of the series names does not exist, any DataFrame's
   * index is missing, or the left-join generates a non-unique mapping between source and
   * destination indexes
   */
  public DataFrame addSeries(DataFrame source, String... seriesNames) {
    return this.addSeries(source, Arrays.asList(seriesNames));
  }

  /**
   * Adds new series to the DataFrame in-place. The series are copied from a source DataFrame
   * and aligned based on the indexes. If the indexes match, the series are copied directly.
   * Otherwise, the method performs a left join to align series from the source with the
   * destination's (this) index.
   *
   * @param source source DataFrame
   * @param seriesNames series names
   * @return reference to modified DataFrame (this)
   * @throws IllegalArgumentException if one of the series names does not exist, any DataFrame's
   * index is missing, or the left-join generates a non-unique mapping between source and
   * destination indexes
   */
  public DataFrame addSeries(DataFrame source, Iterable<String> seriesNames) {
    for(String name : seriesNames)
      if(!source.contains(name))
        throw new IllegalArgumentException(String.format("Source does not contain series '%s'", name));
    if(!this.hasIndex())
      throw new IllegalArgumentException("Destination DataFrame does not have an index");
    if(!source.hasIndex())
      throw new IllegalArgumentException("Source DataFrame does not have an index");
    if(!seriesNames.iterator().hasNext())
      return this;

    // fast - if indexes match
    if(this.getIndexSeries().equals(source.getIndexSeries())) {
      for(String name : seriesNames)
        addSeries(name, source.get(name));
      return this;
    }

    // left join - on minimal structure
    DataFrame dfLeft = new DataFrame();
    for (String name : this.indexNames)
      dfLeft.addSeries(name, this.get(name));
    dfLeft.setIndex(this.indexNames);

    DataFrame dfRight = new DataFrame();
    for (String name : source.indexNames)
      dfRight.addSeries(name, source.get(name));
    dfRight.setIndex(source.indexNames);

    for(String name : seriesNames)
      dfRight.addSeries(name, source.get(name));

    DataFrame joined = dfLeft.joinLeft(dfRight);
    if(joined.size() != this.size())
      throw new IllegalArgumentException("Non-unique mapping between source and destination indexes");

    for(String name : seriesNames)
      addSeries(name, joined.get(name));

    return this;
  }

  /**
   * Adds new series to the DataFrame in-place. All series from the source DataFrame (excluding
   * the index) are added to the destination (this) with {@code addSeries(DataFrame source, String... seriesNames)}
   * semantics.
   *
   * @see DataFrame#addSeries(DataFrame source, String... seriesNames)
   *
   * @param source source DataFrame
   * @return reference to modified DataFrame (this)
   */
  public DataFrame addSeries(DataFrame source) {
    Collection<String> seriesNames = new HashSet<>(source.getSeriesNames());
    for (String name : source.indexNames)
      seriesNames.remove(name);
    return this.addSeries(source, seriesNames);
  }

  /**
   * Removes a series from the DataFrame in-place.
   *
   * @param seriesNames
   * @throws IllegalArgumentException if the series does not exist
   * @return reference to the modified DataFrame (this)
   */
  public DataFrame dropSeries(String... seriesNames) {
    return this.dropSeries(Arrays.asList(seriesNames));
  }

  /**
   * Removes a series from the DataFrame in-place.
   *
   * @param seriesNames
   * @throws IllegalArgumentException if the series does not exist
   * @return reference to the modified DataFrame (this)
   */
  public DataFrame dropSeries(List<String> seriesNames) {
    assertSeriesExist(seriesNames);
    for (String name : seriesNames) {
      this.series.remove(name);
      this.indexNames.remove(name);
    }
    return this;
  }

  /**
   * Retains a series from the DataFrame in-place.
   *
   * @param seriesNames
   * @throws IllegalArgumentException if the series does not exist
   * @return reference to the modified DataFrame (this)
   */
  public DataFrame retainSeries(String... seriesNames) {
    return this.retainSeries(Arrays.asList(seriesNames));
  }

  /**
   * Retains a series from the DataFrame in-place.
   *
   * @param seriesNames
   * @throws IllegalArgumentException if the series does not exist
   * @return reference to the modified DataFrame (this)
   */
  public DataFrame retainSeries(List<String> seriesNames) {
    assertSeriesExist(seriesNames);

    Set<String> deleted = new HashSet<>(this.series.keySet());
    deleted.removeAll(seriesNames);

    for (String name : deleted) {
      this.series.remove(name);
      this.indexNames.remove(name);
    }
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
    List<String> indexNames = new ArrayList<>(this.indexNames);

    this.dropSeries(oldName).addSeries(newName, s);

    for (int i=0; i<indexNames.size(); i++)
      if (indexNames.get(i).equals(oldName))
        indexNames.set(i, newName);

    this.indexNames.clear();
    this.indexNames.addAll(indexNames);

    return this;
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
    this.series.put(seriesName, assertSeriesExists(seriesName).get(type));
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
   * Returns the series referenced by {@code seriesName}. If the series' native type is not
   * {@code ObjectSeries} it is converted transparently.
   *
   * @param seriesName series name
   * @throws IllegalArgumentException if the series does not exist
   * @return ObjectSeries
   */
  public ObjectSeries getObjects(String seriesName) {
    return assertSeriesExists(seriesName).getObjects();
  }

  public double getDouble(String seriesName, int index) {
    return assertSeriesExists(seriesName).getDouble(index);
  }

  public long getLong(String seriesName, int index) {
    return assertSeriesExists(seriesName).getLong(index);
  }

  public String getString(String seriesName, int index) {
    return assertSeriesExists(seriesName).getString(index);
  }

  public byte getBoolean(String seriesName, int index) {
    return assertSeriesExists(seriesName).getBoolean(index);
  }

  public Object getObject(String seriesName, int index) {
    return assertSeriesExists(seriesName).getObject(index);
  }

  /**
   * Applies {@code function} to the series referenced by {@code seriesNames} row by row
   * and returns the results as a new series. The series' values are mapped to arguments
   * of {@code function} in the same order as they appear in {@code seriesNames}.
   * If the series' native types do not match the required input types of {@code function},
   * the series are converted transparently. The native type of the returned series is
   * determined by {@code function}'s output type.
   *
   * @param function function to apply to each row
   * @param seriesNames names of input series
   * @throws IllegalArgumentException if the series does not exist
   * @return series with evaluation results
   */
  public Series map(Series.Function function, String... seriesNames) {
    return map(function, names2series(seriesNames));
  }

  /**
   * @see DataFrame#map(Series.Function, Series...)
   */
  public DoubleSeries map(Series.DoubleFunction function, String... seriesNames) {
    return map(function, names2series(seriesNames));
  }

  /**
   * @see DataFrame#map(Series.Function, Series...)
   */
  public LongSeries map(Series.LongFunction function, String... seriesNames) {
    return map(function, names2series(seriesNames));
  }

  /**
   * @see DataFrame#map(Series.Function, Series...)
   */
  public StringSeries map(Series.StringFunction function, String... seriesNames) {
    return map(function, names2series(seriesNames));
  }

  /**
   * @see DataFrame#map(Series.Function, Series...)
   */
  public BooleanSeries map(Series.BooleanFunction function, String... seriesNames) {
    return map(function, names2series(seriesNames));
  }

  /**
   * @see DataFrame#map(Series.Function, Series...)
   */
  public BooleanSeries map(Series.BooleanFunctionEx function, String... seriesNames) {
    return map(function, names2series(seriesNames));
  }

  /**
   * @see DataFrame#map(Series.Function, Series...)
   */
  public ObjectSeries map(Series.ObjectFunction function, String... seriesNames) {
    return map(function, names2series(seriesNames));
  }

  /**
   * @see DataFrame#map(Series.Function, Series...)
   */
  public BooleanSeries map(Series.Conditional function, String... seriesNames) {
    return map(function, names2series(seriesNames));
  }

  /**
   * @see DataFrame#map(Series.Function, Series...)
   */
  public BooleanSeries map(Series.DoubleConditional function, String... seriesNames) {
    return map(function, names2series(seriesNames));
  }

  /**
   * @see DataFrame#map(Series.Function, Series...)
   */
  public BooleanSeries map(Series.LongConditional function, String... seriesNames) {
    return map(function, names2series(seriesNames));
  }

  /**
   * @see DataFrame#map(Series.Function, Series...)
   */
  public BooleanSeries map(Series.StringConditional function, String... seriesNames) {
    return map(function, names2series(seriesNames));
  }

  /**
   * @see DataFrame#map(Series.Function, Series...)
   */
  public BooleanSeries map(Series.BooleanConditional function, String... seriesNames) {
    return map(function, names2series(seriesNames));
  }

  /**
   * @see DataFrame#map(Series.Function, Series...)
   */
  public BooleanSeries map(Series.ObjectConditional function, String... seriesNames) {
    return map(function, names2series(seriesNames));
  }

  /**
   * Applies {@code function} to the series referenced by {@code seriesNames} row by row
   * and adds the result to the DataFrame as a new series with name {@code outputName}.
   * The series' values are mapped to arguments of {@code function} in the same order
   * as they appear in {@code seriesNames}.
   * If the series' native types do not match the required input types of {@code function},
   * the series are converted transparently. The native type of the returned series is
   * determined by {@code function}'s output type.
   *
   * @param function function to apply to each row
   * @param outputName name of output series
   * @param inputNames names of input series, or none to use output series name as only input
   * @throws IllegalArgumentException if the series does not exist
   * @return series with evaluation results
   */
  public DataFrame mapInPlace(Series.Function function, String outputName, String... inputNames) {
    return this.addSeries(outputName, map(function, names2series(inputNames)));
  }

  /**
   * Applies {@code function} to the series referenced by {@code seriesName} row by row
   * and adds the result to the DataFrame as a new series with the same name.
   * If the series' native types do not match the required input types of {@code function},
   * the series are converted transparently. The native type of the returned series is
   * determined by {@code function}'s output type.
   *
   * @param function function to apply to each row
   * @param seriesName name of series
   * @throws IllegalArgumentException if the series does not exist
   * @return series with evaluation results
   */
  public DataFrame mapInPlace(Series.Function function, String seriesName) {
    return this.addSeries(seriesName, map(function, this.get(seriesName)));
  }

  /**
   * Applies {@code function} to {@code series} row by row
   * and returns the results as a new series. The series' values are mapped to arguments
   * of {@code function} in the same order as they appear in {@code series}.
   * If the series' native types do not match the required input types of {@code function},
   * the series are converted transparently. The native type of the returned series is
   * determined by {@code function}'s output type.
   *
   * @param function function to apply to each row
   * @param series input series for function
   * @throws IllegalArgumentException if the series does not exist
   * @return series with evaluation results
   */
  public static Series map(Series.Function function, Series... series) {
    return Series.map(function, series);
  }

  /**
   * @see DataFrame#map(Series.Function, Series...)
   */
  public static DoubleSeries map(Series.DoubleFunction function, Series... series) {
    return (DoubleSeries)map((Series.Function)function, series);
  }

  /**
   * @see DataFrame#map(Series.Function, Series...)
   */
  public static LongSeries map(Series.LongFunction function, Series... series) {
    return (LongSeries)map((Series.Function)function, series);
  }

  /**
   * @see DataFrame#map(Series.Function, Series...)
   */
  public static StringSeries map(Series.StringFunction function, Series... series) {
    return (StringSeries)map((Series.Function)function, series);
  }

  /**
   * @see DataFrame#map(Series.Function, Series...)
   */
  public static BooleanSeries map(Series.BooleanFunction function, Series... series) {
    return (BooleanSeries)map((Series.Function)function, series);
  }

  /**
   * @see DataFrame#map(Series.Function, Series...)
   */
  public static BooleanSeries map(Series.BooleanFunctionEx function, Series... series) {
    return (BooleanSeries)map((Series.Function)function, series);
  }

  /**
   * @see DataFrame#map(Series.Function, Series...)
   */
  public static ObjectSeries map(Series.ObjectFunction function, Series... series) {
    return (ObjectSeries)map((Series.Function)function, series);
  }

  /**
   * @see DataFrame#map(Series.Function, Series...)
   */
  public static BooleanSeries map(Series.Conditional function, Series... series) {
    return (BooleanSeries)map((Series.Function)function, series);
  }

  /**
   * @see DataFrame#map(Series.Function, Series...)
   */
  public static BooleanSeries map(Series.DoubleConditional function, Series... series) {
    return (BooleanSeries)map((Series.Function)function, series);
  }

  /**
   * @see DataFrame#map(Series.Function, Series...)
   */
  public static BooleanSeries map(Series.LongConditional function, Series... series) {
    return (BooleanSeries)map((Series.Function)function, series);
  }

  /**
   * @see DataFrame#map(Series.Function, Series...)
   */
  public static BooleanSeries map(Series.StringConditional function, Series... series) {
    return (BooleanSeries)map((Series.Function)function, series);
  }

  /**
   * @see DataFrame#map(Series.Function, Series...)
   */
  public static BooleanSeries map(Series.BooleanConditional function, Series... series) {
    return (BooleanSeries)map((Series.Function)function, series);
  }

  /**
   * @see DataFrame#map(Series.Function, Series...)
   */
  public static BooleanSeries map(Series.ObjectConditional function, Series... series) {
    return (BooleanSeries)map((Series.Function)function, series);
  }

  /**
   * Applies {@code doubleExpression} compiled to an expression to the series referenced by
   * {@code seriesNames} row by row and returns the results as a new series. The series' values
   * are mapped to variables in {@code doubleExpression} by series names. Only series referenced
   * by {@code seriesNames} can be referenced by the expression.
   * The series are converted to {@code DoubleSeries} transparently and the results
   * are returned as DoubleSeries as well.
   *
   * <br/><b>NOTE:</b> doubleExpression is compiled to an {@code EvalEx} expression.
   *
   * @param doubleExpression expression to be compiled and applied using EvalEx
   * @throws IllegalArgumentException if the series does not exist
   * @return series with evaluation results
   */
  public DoubleSeries map(String doubleExpression, final String... seriesNames) {
    // TODO support escaping of "}"
    final String[] vars = new String[seriesNames.length];
    for(int i=0; i<seriesNames.length; i++) {
      String pattern = String.format("${%s}", seriesNames[i]);
      String replace = String.format("__%d", i);
      vars[i] = replace;
      doubleExpression = doubleExpression.replace(pattern, replace);
    }

    final Expression e = new Expression(doubleExpression);

    return this.map(new Series.DoubleFunction() {
      @Override
      public double apply(double[] values) {
        for(int i=0; i<values.length; i++) {
          e.with(vars[i], new BigDecimal(values[i]));
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
   * <br/><b>NOTE:</b> doubleExpression is compiled to an {@code EvalEx} expression.
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
   * <br/><b>NOTE:</b> fromIndex <= -1 is filled with {@code null}.
   * <br/><b>NOTE:</b> array with length 0 produces empty series.
   *
   * @param fromIndex array with indices to project from (must be <= series size)
   * @return DataFrame projection
   */
  public DataFrame project(int[] fromIndex) {
    DataFrame newDataFrame = new DataFrame(this);
    newDataFrame.series.clear();
    for(Map.Entry<String, Series> e : this.series.entrySet()) {
      newDataFrame.addSeries(e.getKey(), e.getValue().project(fromIndex));
    }
    return newDataFrame;
  }

  /**
   * Returns a copy of the DataFrame sorted by series values referenced by its index.
   *
   * @throws IllegalArgumentException if the series does not exist
   * @return sorted DataFrame copy
   */
  public DataFrame sortedByIndex() {
    return this.sortedBy(this.indexNames);
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
    return this.sortedBy(Arrays.asList(seriesNames));
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
  public DataFrame sortedBy(List<String> seriesNames) {
    DataFrame df = this;
    for(int i=seriesNames.size()-1; i>=0; i--) {
      // TODO support "-series" order inversion
      df = df.project(df.get(seriesNames.get(i)).sortedIndex());
    }
    return df;
  }

  /**
   * Returns a copy of the DataFrame with the order of values in the series reversed.
   *
   * @return reversed DataFrame copy
   */
  public DataFrame reverse() {
    DataFrame newDataFrame = new DataFrame(this);
    for(Map.Entry<String, Series> e : this.series.entrySet()) {
      newDataFrame.addSeries(e.getKey(), e.getValue().reverse());
    }
    return newDataFrame;
  }

  /**
   * Returns a copy of the DataFrame with rows filtered by {@code series}. If the value of {@code series}
   * associated with a row is {@code true} the row is copied, otherwise it is set to {@code null}.
   *
   * @param series filter series
   * @return filtered DataFrame copy
   */
  public DataFrame filter(BooleanSeries series) {
    if(series.size() != this.size())
      throw new IllegalArgumentException("Series size must be equal to index size");

    int[] fromIndex = new int[series.size()];
    for(int i=0; i<series.size(); i++) {
      if(BooleanSeries.isTrue(series.getBoolean(i))) {
        fromIndex[i] = i;
      } else {
        fromIndex[i] = -1;
      }
    }

    return this.project(fromIndex);
  }

  /**
   * Returns a copy of the DataFrame with rows filtered by series values referenced by {@code seriesName}.
   * If the value referenced by {@code seriesName} associated with a row is {@code true} the row is copied,
   * otherwise it is set to {@code null}.
   *
   * @param seriesName filter series name
   * @return filtered DataFrame copy
   */
  public DataFrame filter(String seriesName) {
    return this.filter(this.getBooleans(seriesName));
  }

  public DataFrame filter(Series.Conditional conditional, String... seriesNames) {
    return filter(conditional, names2series(seriesNames));
  }

  public DataFrame filter(Series.Conditional conditional, Series... series) {
    return filter((BooleanSeries)Series.map(conditional, series));
  }

  public DataFrame filterEquals(String seriesName, final double value) {
    return this.filter(this.get(seriesName).getDoubles().eq(value));
  }

  public DataFrame filterEquals(String seriesName, final long value) {
    return this.filter(this.get(seriesName).getLongs().eq(value));
  }

  public DataFrame filterEquals(String seriesName, final String value) {
    return this.filter(this.get(seriesName).getStrings().eq(value));
  }

  public DataFrame filterEquals(String seriesName, final boolean value) {
    return this.filter(this.get(seriesName).getBooleans().eq(value));
  }

  public DataFrame filterEquals(String seriesName, final Object value) {
    return this.filter(this.get(seriesName).getObjects().eq(value));
  }

  /**
   * Sets the values of the series references by {@code seriesName} and masked by {@code mask}
   * to the corresponding values in {@code values}. Uses a copy of the affected series.
   *
   * @see Series#set(BooleanSeries, Series)
   *
   * @param seriesName series name to set values of
   * @param mask row mask (if {@code true} row is replaced by value)
   * @param values values to replace row with
   * @return DataFrame with values
   */
  public DataFrame set(String seriesName, BooleanSeries mask, Series values) {
    return this.addSeries(seriesName, this.get(seriesName).set(mask, values));
  }

  /**
   * Returns a DataFrameGrouping based on the labels provided by {@code labels} row by row.
   * The size of {@code labels} must match the size of the DataFrame.
   *
   * @see Grouping.GroupingByValue
   *
   * @param labels grouping labels
   * @return DataFrameGrouping
   */
  public Grouping.DataFrameGrouping groupByValue(Series labels) {
    return new Grouping.DataFrameGrouping(Grouping.GROUP_KEY, this, Grouping.GroupingByValue.from(labels));
  }

  /**
   * Returns a DataFrameGrouping based on the labels provided by the series referenced by
   * {@code seriesName} row by row.
   *
   * @see Grouping.GroupingByValue
   *
   * @param seriesName series containing grouping labels
   * @return DataFrameGrouping
   */
  public Grouping.DataFrameGrouping groupByValue(String seriesName) {
    return new Grouping.DataFrameGrouping(seriesName, this, Grouping.GroupingByValue.from(this.get(seriesName)));
  }

  /**
   * Returns a DataFrameGrouping based on the labels provided by the series referenced by
   * {@code seriesNames} row by row.  The method can group across multiple columns.  It returns
   * the key column as object series of {@code Tuples} constructed from the input columns.
   *
   * @see Grouping.GroupingByValue
   *
   * @param seriesNames series containing grouping labels
   * @return DataFrameGrouping
   */
  public Grouping.DataFrameGrouping groupByValue(String... seriesNames) {
    return this.groupByValue(Arrays.asList(seriesNames));
  }

  /**
   * Returns a DataFrameGrouping based on the labels provided by the series referenced by
   * {@code seriesNames} row by row.  The method can group across multiple columns.  It returns
   * the key column as object series of {@code Tuples} constructed from the input columns.
   *
   * @see Grouping.GroupingByValue
   *
   * @param seriesNames series containing grouping labels
   * @return DataFrameGrouping
   */
  public Grouping.DataFrameGrouping groupByValue(List<String> seriesNames) {
    return new Grouping.DataFrameGrouping(Grouping.GROUP_KEY, this, Grouping.GroupingByValue.from(names2series(seriesNames)));
  }

  /**
   * Returns a DataFrameGrouping based on the labels provided by {@code labels} row by row.
   * The size of {@code labels} must match the size of the DataFrame.
   *
   * @see Grouping.GroupingByInterval
   *
   * @param labels grouping labels
   * @return DataFrameGrouping
   */
  public Grouping.DataFrameGrouping groupByInterval(Series labels, long interval) {
    return new Grouping.DataFrameGrouping(Grouping.GROUP_KEY, this, Grouping.GroupingByInterval.from(labels, interval));
  }

  /**
   * Returns a DataFrameGrouping based on the labels provided by the series referenced by
   * {@code seriesName} row by row.
   *
   * @see Grouping.GroupingByInterval
   *
   * @param seriesName series containing grouping labels
   * @return DataFrameGrouping
   */
  public Grouping.DataFrameGrouping groupByInterval(String seriesName, long interval) {
    return new Grouping.DataFrameGrouping(seriesName, this, Grouping.GroupingByInterval.from(this.get(seriesName), interval));
  }

  /**
   * Returns a DataFrameGrouping based on items counts.
   *
   * @see Grouping.GroupingByCount
   *
   * @param count item count
   * @return DataFrameGrouping
   */
  public Grouping.DataFrameGrouping groupByCount(int count) {
    return new Grouping.DataFrameGrouping(Grouping.GROUP_KEY, this, Grouping.GroupingByCount.from(count, this.size()));
  }

  /**
   * Returns a DataFrameGrouping based on partition counts.
   *
   * @see Grouping.GroupingByPartitions
   *
   * @param partitionCount item count
   * @return DataFrameGrouping
   */
  public Grouping.DataFrameGrouping groupByPartitions(int partitionCount) {
    return new Grouping.DataFrameGrouping(Grouping.GROUP_KEY, this, Grouping.GroupingByPartitions.from(partitionCount, this.size()));
  }

  /**
   * Returns a DataFrameGrouping based on a moving window.
   *
   * @see Grouping.GroupingByMovingWindow
   *
   * @param windowSize moving window size
   * @return DataFrameGrouping
   */
  public Grouping.DataFrameGrouping groupByMovingWindow(int windowSize) {
    return new Grouping.DataFrameGrouping(Grouping.GROUP_KEY, this, Grouping.GroupingByMovingWindow.from(windowSize, this.size()));
  }

  /**
   * Returns a DataFrameGrouping based on an expanding window.
   *
   * @see Grouping.GroupingByExpandingWindow
   *
   * @return DataFrameGrouping
   */
  public Grouping.DataFrameGrouping groupByExpandingWindow() {
    return new Grouping.DataFrameGrouping(Grouping.GROUP_KEY, this, Grouping.GroupingByExpandingWindow.from(this.size()));
  }

  /**
   * Returns a DataFrameGrouping based on an time period
   *
   * @see Grouping.GroupingByPeriod
   *
   * @return DataFrameGrouping
   */
  public Grouping.DataFrameGrouping groupByPeriod(Series timestamps, DateTime origin, Period period) {
    return new Grouping.DataFrameGrouping(Grouping.GROUP_KEY, this, Grouping.GroupingByPeriod.from(timestamps.getLongs(), origin, period));
  }

  /**
   * Returns a DataFrameGrouping based on an time period
   *
   * @see Grouping.GroupingByPeriod
   *
   * @return DataFrameGrouping
   */
  public Grouping.DataFrameGrouping groupByPeriod(Series timestamps, DateTimeZone timezone, Period period) {
    return new Grouping.DataFrameGrouping(Grouping.GROUP_KEY, this, Grouping.GroupingByPeriod.from(timestamps.getLongs(), timezone, period));
  }

  /**
   * Returns a DataFrameGrouping based on an time period
   *
   * @see Grouping.GroupingByPeriod
   *
   * @return DataFrameGrouping
   */
  public Grouping.DataFrameGrouping groupByPeriod(String seriesName, DateTimeZone timezone, Period period) {
    return new Grouping.DataFrameGrouping(Grouping.GROUP_KEY, this, Grouping.GroupingByPeriod.from(assertSeriesExists(seriesName).getLongs(), timezone, period));
  }

  /**
   * Returns a copy of the DataFrame omitting rows that contain a {@code null} value in any series.
   *
   * @return DataFrame copy without null rows
   */
  public DataFrame dropNull() {
    return this.dropNull(new ArrayList<>(this.getSeriesNames()));
  }

  /**
   * Returns a copy of the DataFrame omitting rows that contain a {@code null} value in any given series.
   *
   * @param seriesNames series names to drop null values for
   * @return DataFrame copy without null rows
   */
  public DataFrame dropNull(String... seriesNames) {
    return this.dropNull(Arrays.asList(seriesNames));
  }

  /**
   * Returns a copy of the DataFrame omitting rows that contain a {@code null} value in any given series.
   *
   * @param seriesNames series names to drop null values for
   * @return DataFrame copy without null rows
   */
  public DataFrame dropNull(List<String> seriesNames) {
    BooleanSeries isNull = BooleanSeries.fillValues(this.size(), false);
    for(Series s : assertSeriesExist(seriesNames)) {
      isNull = isNull.or(s.isNull());
    }

    int[] fromIndex = new int[isNull.count(false)];
    int countNotNull = 0;
    for(int i=0; i<this.size(); i++) {
      if(BooleanSeries.isFalse(isNull.getBoolean(i))) {
        fromIndex[countNotNull++] = i;
      }
    }

    return this.project(Arrays.copyOf(fromIndex, countNotNull));
  }

  /**
   * Returns {@code true} if series {@code seriesName} value at {@code index} is null.
   *
   * @param seriesName series name
   * @param index row index
   * @return {@code true} is null, otherwise {@code false}
   */
  public boolean isNull(String seriesName, int index) {
    return assertSeriesExists(seriesName).isNull(index);
  }

  /**
   * Returns a copy of the DataFrame omitting series that contain a {@code null} value.
   *
   * @return DataFrame copy without null series
   */
  public DataFrame dropNullColumns() {
    DataFrame df = new DataFrame(this);
    df.series.clear();
    for(Map.Entry<String, Series> e : this.getSeries().entrySet()) {
      if(!e.getValue().hasNull())
        df.addSeries(e.getKey(), e.getValue());
    }
    return df;
  }

  /**
   * Returns a copy of the DataFrame omitting series that contain only {@code null} values.
   *
   * @return DataFrame copy without all-null series
   */
  public DataFrame dropAllNullColumns() {
    DataFrame df = new DataFrame(this);
    df.series.clear();
    for(Map.Entry<String, Series> e : this.getSeries().entrySet()) {
      if(!e.getValue().allNull())
        df.addSeries(e.getKey(), e.getValue());
    }
    return df;
  }

  /**
   * Returns a copy of the DataFrame with series {@code seriesNames} replacing {@code null}
   * values with its native default value.
   *
   * @param seriesNames series names
   * @return DataFrame copy with filled nulls
   */
  public DataFrame fillNull(String... seriesNames) {
    return this.fillNull(Arrays.asList(seriesNames));
  }

  /**
   * Returns a copy of the DataFrame with series {@code seriesNames} replacing {@code null}
   * values with its native default value.
   *
   * @param seriesNames series names
   * @return DataFrame copy with filled nulls
   */
  public DataFrame fillNull(Iterable<String> seriesNames) {
    DataFrame df = new DataFrame(this);
    for(String name : seriesNames)
      df.addSeries(name, assertSeriesExists(name).fillNull());
    return df;
  }

  /**
   * Returns a copy of the DataFrame with series {@code seriesNames} replacing {@code null}
   * values via forward fill.
   *
   * @param seriesNames series names
   * @return DataFrame copy with filled nulls
   */
  public DataFrame fillNullForward(String... seriesNames) {
    return this.fillNullForward(Arrays.asList(seriesNames));
  }

  /**
   * Returns a copy of the DataFrame with series {@code seriesNames} replacing {@code null}
   * values via forward fill.
   *
   * @param seriesNames series names
   * @return DataFrame copy with filled nulls
   */
  public DataFrame fillNullForward(Iterable<String> seriesNames) {
    DataFrame df = new DataFrame(this);
    for(String name : seriesNames)
      df.addSeries(name, assertSeriesExists(name).fillNullForward());
    return df;
  }

  /**
   * Returns a copy of the DataFrame with series {@code seriesNames} replacing {@code null}
   * values via back fill.
   *
   * @param seriesNames series names
   * @return DataFrame copy with filled nulls
   */
  public DataFrame fillNullBackward(String... seriesNames) {
    return this.fillNullBackward(Arrays.asList(seriesNames));
  }

  /**
   * Returns a copy of the DataFrame with series {@code seriesNames} replacing {@code null}
   * values via back fill.
   *
   * @param seriesNames series names
   * @return DataFrame copy with filled nulls
   */
  public DataFrame fillNullBackward(Iterable<String> seriesNames) {
    DataFrame df = new DataFrame(this);
    for(String name : seriesNames)
      df.addSeries(name, assertSeriesExists(name).fillNullBackward());
    return df;
  }

  /* **************************************************************************
   * Joins across data frames
   ***************************************************************************/

  /**
   * Performs an inner join on the index column of two DataFrames.
   * @see DataFrame#join(DataFrame, DataFrame, String[], String[], Series.JoinType)
   *
   * @param other right DataFrame
   * @return joined DataFrame
   */
  public DataFrame joinInner(DataFrame other) {
    assertIndex(this, other);
    return this.joinInner(other, this.indexNames, other.indexNames);
  }

  /**
   * Performs an inner join on the {@code onSeries} columns of two DataFrames.
   * @see DataFrame#join(DataFrame, DataFrame, String[], String[], Series.JoinType)
   *
   * @param other right DataFrame
   * @return joined DataFrame
   */
  public DataFrame joinInner(DataFrame other, String... onSeries) {
    return this.joinInner(other, onSeries, onSeries);
  }

  /**
   * Performs an inner join on the {@code onSeries} columns of two DataFrames.
   * @see DataFrame#join(DataFrame, DataFrame, List, List, Series.JoinType)
   *
   * @param other right DataFrame
   * @return joined DataFrame
   */
  public DataFrame joinInner(DataFrame other, List<String> onSeries) {
    return this.joinInner(other, onSeries, onSeries);
  }

  /**
   * Performs an inner join on the {@code onSeriesLeft} columns of this DataFrame and
   * the {@code onSeriesLeft} columns of the {@code other} DataFrame.
   * @see DataFrame#join(DataFrame, DataFrame, String[], String[], Series.JoinType)
   *
   * @param other right DataFrame
   * @return joined DataFrame
   */
  public DataFrame joinInner(DataFrame other, String[] onSeriesLeft, String[] onSeriesRight) {
    return DataFrame.join(this, other, onSeriesLeft, onSeriesRight, Series.JoinType.INNER);
  }

  /**
   * Performs an inner join on the {@code onSeriesLeft} columns of this DataFrame and
   * the {@code onSeriesLeft} columns of the {@code other} DataFrame.
   * @see DataFrame#join(DataFrame, DataFrame, List, List, Series.JoinType)
   *
   * @param other right DataFrame
   * @return joined DataFrame
   */
  public DataFrame joinInner(DataFrame other, List<String> onSeriesLeft, List<String> onSeriesRight) {
    return DataFrame.join(this, other, onSeriesLeft, onSeriesRight, Series.JoinType.INNER);
  }

  /**
   * Performs a left outer join on the index column of two DataFrames.
   * @see DataFrame#join(DataFrame, DataFrame, String[], String[], Series.JoinType)
   *
   * @param other right DataFrame
   * @return joined DataFrame
   */
  public DataFrame joinLeft(DataFrame other) {
    assertIndex(this, other);
    return this.joinLeft(other, this.indexNames, other.indexNames);
  }

  /**
   * Performs a left outer join on the {@code onSeries} columns of two DataFrames.
   * @see DataFrame#join(DataFrame, DataFrame, String[], String[], Series.JoinType)
   *
   * @param other right DataFrame
   * @return joined DataFrame
   */
  public DataFrame joinLeft(DataFrame other, String... onSeries) {
    return this.joinLeft(other, onSeries, onSeries);
  }

  /**
   * Performs a left outer join on the {@code onSeries} columns of two DataFrames.
   * @see DataFrame#join(DataFrame, DataFrame, List, List, Series.JoinType)
   *
   * @param other right DataFrame
   * @return joined DataFrame
   */
  public DataFrame joinLeft(DataFrame other, List<String> onSeries) {
    return this.joinLeft(other, onSeries, onSeries);
  }

  /**
   * Performs a left outer join on the {@code onSeriesLeft} columns of this DataFrame and
   * the {@code onSeriesLeft} columns of the {@code other} DataFrame.
   * @see DataFrame#join(DataFrame, DataFrame, String[], String[], Series.JoinType)
   *
   * @param other right DataFrame
   * @return joined DataFrame
   */
  public DataFrame joinLeft(DataFrame other, String[] onSeriesLeft, String[] onSeriesRight) {
    return DataFrame.join(this, other, onSeriesLeft, onSeriesRight, Series.JoinType.LEFT);
  }

  /**
   * Performs a left outer join on the {@code onSeriesLeft} columns of this DataFrame and
   * the {@code onSeriesLeft} columns of the {@code other} DataFrame.
   * @see DataFrame#join(DataFrame, DataFrame, List, List, Series.JoinType)
   *
   * @param other right DataFrame
   * @return joined DataFrame
   */
  public DataFrame joinLeft(DataFrame other, List<String> onSeriesLeft, List<String> onSeriesRight) {
    return DataFrame.join(this, other, onSeriesLeft, onSeriesRight, Series.JoinType.LEFT);
  }

  /**
   * Performs a right outer join on the index column of two DataFrames.
   * @see DataFrame#join(DataFrame, DataFrame, String[], String[], Series.JoinType)
   *
   * @param other right DataFrame
   * @return joined DataFrame
   */
  public DataFrame joinRight(DataFrame other) {
    assertIndex(this, other);
    return this.joinRight(other, this.indexNames, other.indexNames);
  }

  /**
   * Performs a right outer join on the {@code onSeries} columns of two DataFrames.
   * @see DataFrame#join(DataFrame, DataFrame, List, List, Series.JoinType)
   *
   * @param other right DataFrame
   * @return joined DataFrame
   */
  public DataFrame joinRight(DataFrame other, List<String> onSeries) {
    return this.joinRight(other, onSeries, onSeries);
  }

  /**
   * Performs a right outer join on the {@code onSeries} columns of two DataFrames.
   * @see DataFrame#join(DataFrame, DataFrame, String[], String[], Series.JoinType)
   *
   * @param other right DataFrame
   * @return joined DataFrame
   */
  public DataFrame joinRight(DataFrame other, String... onSeries) {
    return this.joinRight(other, onSeries, onSeries);
  }

  /**
   * Performs a right outer join on the {@code onSeriesLeft} columns of this DataFrame and
   * the {@code onSeriesLeft} columns of the {@code other} DataFrame.
   * @see DataFrame#join(DataFrame, DataFrame, String[], String[], Series.JoinType)
   *
   * @param other right DataFrame
   * @return joined DataFrame
   */
  public DataFrame joinRight(DataFrame other, String[] onSeriesLeft, String[] onSeriesRight) {
    return DataFrame.join(this, other, onSeriesLeft, onSeriesRight, Series.JoinType.RIGHT);
  }

  /**
   * Performs a right outer join on the {@code onSeriesLeft} columns of this DataFrame and
   * the {@code onSeriesLeft} columns of the {@code other} DataFrame.
   * @see DataFrame#join(DataFrame, DataFrame, List, List, Series.JoinType)
   *
   * @param other right DataFrame
   * @return joined DataFrame
   */
  public DataFrame joinRight(DataFrame other, List<String> onSeriesLeft, List<String> onSeriesRight) {
    return DataFrame.join(this, other, onSeriesLeft, onSeriesRight, Series.JoinType.RIGHT);
  }

  /**
   * Performs an outer join on the index column of two DataFrames.
   * @see DataFrame#join(DataFrame, DataFrame, String[], String[], Series.JoinType)
   *
   * @param other right DataFrame
   * @return joined DataFrame
   */
  public DataFrame joinOuter(DataFrame other) {
    assertIndex(this, other);
    return this.joinOuter(other, this.indexNames, other.indexNames);
  }

  /**
   * Performs an outer join on the {@code onSeries} columns of two DataFrames.
   * @see DataFrame#join(DataFrame, DataFrame, List, List, Series.JoinType)
   *
   * @param other right DataFrame
   * @return joined DataFrame
   */
  public DataFrame joinOuter(DataFrame other, String... onSeries) {
    return this.joinOuter(other, onSeries, onSeries);
  }

  /**
   * Performs an outer join on the {@code onSeries} columns of two DataFrames.
   * @see DataFrame#join(DataFrame, DataFrame, List, List, Series.JoinType)
   *
   * @param other right DataFrame
   * @return joined DataFrame
   */
  public DataFrame joinOuter(DataFrame other, List<String> onSeries) {
    return this.joinOuter(other, onSeries, onSeries);
  }

  /**
   * Performs an outer join on the {@code onSeriesLeft} columns of this DataFrame and
   * the {@code onSeriesLeft} columns of the {@code other} DataFrame.
   * @see DataFrame#join(DataFrame, DataFrame, String[], String[], Series.JoinType)
   *
   * @param other right DataFrame
   * @return joined DataFrame
   */
  public DataFrame joinOuter(DataFrame other, String[] onSeriesLeft, String[] onSeriesRight) {
    return DataFrame.join(this, other, onSeriesLeft, onSeriesRight, Series.JoinType.OUTER);
  }

  /**
   * Performs an outer join on the {@code onSeriesLeft} columns of this DataFrame and
   * the {@code onSeriesLeft} columns of the {@code other} DataFrame.
   * @see DataFrame#join(DataFrame, DataFrame, List, List, Series.JoinType)
   *
   * @param other right DataFrame
   * @return joined DataFrame
   */
  public DataFrame joinOuter(DataFrame other, List<String> onSeriesLeft, List<String> onSeriesRight) {
    return DataFrame.join(this, other, onSeriesLeft, onSeriesRight, Series.JoinType.OUTER);
  }

  /**
   * Performs a join between two DataFrames and returns the result as a new DataFrame. The join
   * can be performed across multiple series. If (non-joined) series with the same name exist
   * in both DataFrames, their names are appended with {@code COLUMN_JOIN_LEFT} and
   * {@code COLUMN_JOIN_RIGHT} in the returned DataFrame.
   *
   * <b>NOTE:</b> the series names of the left join series survive.
   *
   * @param left left DataFrame
   * @param right right DataFrame
   * @param onSeriesLeft left series names to join on
   * @param onSeriesRight right series names to join on
   * @param joinType type of join to perform
   * @return joined DataFrame
   */
  public static DataFrame join(DataFrame left, DataFrame right, String[] onSeriesLeft, String[] onSeriesRight, Series.JoinType joinType) {
    return join(left, right, Arrays.asList(onSeriesLeft), Arrays.asList(onSeriesRight), joinType);
  }

  /**
   * Performs a join between two DataFrames and returns the result as a new DataFrame. The join
   * can be performed across multiple series. If (non-joined) series with the same name exist
   * in both DataFrames, their names are appended with {@code COLUMN_JOIN_LEFT} and
   * {@code COLUMN_JOIN_RIGHT} in the returned DataFrame.
   *
   * <b>NOTE:</b> the series names of the left join series survive.
   *
   * @param left left DataFrame
   * @param right right DataFrame
   * @param onSeriesLeft left series names to join on
   * @param onSeriesRight right series names to join on
   * @param joinType type of join to perform
   * @return joined DataFrame
   */
  public static DataFrame join(DataFrame left, DataFrame right, List<String> onSeriesLeft, List<String> onSeriesRight, Series.JoinType joinType) {
    if(onSeriesLeft.size() != onSeriesRight.size())
      throw new IllegalArgumentException("Number of series on the left side of the join must equal the number of series on the right side");

    final int numSeries = onSeriesLeft.size();

    // extract source series
    Series[] leftSeries = new Series[numSeries];
    for(int i=0; i<numSeries; i++)
      leftSeries[i] = left.get(onSeriesLeft.get(i));

    Series[] rightSeries = new Series[numSeries];
    for(int i=0; i<numSeries; i++)
      rightSeries[i] = right.get(onSeriesRight.get(i));

    // perform join, generate row pairs
    Series.JoinPairs pairs = filterJoinPairs(Series.hashJoinOuter(leftSeries, rightSeries), joinType);

    // extract projection indices
    int[] fromIndexLeft = new int[pairs.size()];
    for(int i=0; i<pairs.size(); i++)
      fromIndexLeft[i] = pairs.left(i);

    int[] fromIndexRight = new int[pairs.size()];
    for(int i=0; i<pairs.size(); i++)
      fromIndexRight[i] = pairs.right(i);

    byte[] maskValues = new byte[pairs.size()];
    for(int i=0; i<pairs.size(); i++) {
      if (pairs.left(i) == -1)
        maskValues[i] = BooleanSeries.TRUE;
    }

    // perform projection
    DataFrame leftData = left.project(fromIndexLeft);
    DataFrame rightData = right.project(fromIndexRight);

    // merge values of join columns
    Series[] joinKeys = new Series[numSeries];
    for(int i=0; i<numSeries; i++)
      joinKeys[i] = leftData.get(onSeriesLeft.get(i)).set(BooleanSeries.buildFrom(maskValues), rightData.get(onSeriesRight.get(i)));

    // select series names
    Set<String> seriesLeft = new HashSet<>(left.getSeriesNames());
    Set<String> seriesRight = new HashSet<>(right.getSeriesNames());

    seriesLeft.removeAll(onSeriesLeft);
    seriesRight.removeAll(onSeriesRight);

    final List<String> joinKeyNames = onSeriesLeft;

    // construct result
    DataFrame joined = new DataFrame();

    for(int i=0; i<numSeries; i++)
      joined.addSeries(joinKeyNames.get(i), joinKeys[i]);

    List<String> newIndex = new ArrayList<>();
    for (String name : joinKeyNames) {
      if (left.indexNames.contains(name))
        newIndex.add(name);
    }
    joined.setIndex(newIndex);

    for(String name : seriesRight) {
      Series s = rightData.get(name);
      if(!seriesLeft.contains(name)) {
        joined.addSeries(name, s);
      } else {
        joined.addSeries(name + COLUMN_JOIN_RIGHT, s);
      }
    }

    for(String name : seriesLeft) {
      Series s = leftData.get(name);
      if(!seriesRight.contains(name)) {
        joined.addSeries(name, s);
      } else {
        joined.addSeries(name + COLUMN_JOIN_LEFT, s);
      }
    }

    return joined;
  }

  private static Series.JoinPairs filterJoinPairs(Series.JoinPairs pairs, Series.JoinType type) {
    Series.JoinPairs output = new Series.JoinPairs(pairs.size());
    switch(type) {
      case LEFT:
        for(int i=0; i<pairs.size(); i++)
          if(pairs.left(i) != -1)
            output.add(pairs.get(i));
        return output;
      case RIGHT:
        for(int i=0; i<pairs.size(); i++)
          if(pairs.right(i) != -1)
            output.add(pairs.get(i));
        return output;
      case INNER:
        for(int i=0; i<pairs.size(); i++)
          if(pairs.left(i) != -1 && pairs.right(i) != -1)
            output.add(pairs.get(i));
        return output;
      case OUTER:
        return pairs;
      default:
        throw new IllegalArgumentException(String.format("Unknown join type '%s'", type));
    }
  }

  /**
   * Returns a copy of the DataFrame with data from {@code others} appended at the end. Matches
   * series by names and uses the native type of the original (this) DataFrame. If {@code others}
   * do not contain series with matching names, a sequence of {@code nulls} is appended. Any series
   * in {@code other} that are not matched by name are discarded.
   *
   * @param others DataFrames to append in sequence
   * @return copy of the DataFrame with appended data
   */
  public DataFrame append(DataFrame... others) {
    return this.append(Arrays.asList(others));
  }

  /**
   * Returns a copy of the DataFrame with data from {@code others} appended at the end. Matches
   * series by names and uses the native type of the original (this) DataFrame. If {@code others}
   * do not contain series with matching names, a sequence of {@code nulls} is appended. Any series
   * in {@code other} that are not matched by name are discarded.
   *
   * @param others DataFrames to append in sequence
   * @return copy of the DataFrame with appended data
   */
  public DataFrame append(List<DataFrame> others) {
    DataFrame df = new DataFrame(this);
    df.series.clear();

    for(String name : this.getSeriesNames()) {
      Series.Builder builder = this.get(name).getBuilder();
      builder.addSeries(this.get(name));

      for(DataFrame other : others) {
        if (other.contains(name)) {
          builder.addSeries(other.get(name));
        } else {
          builder.addSeries(BooleanSeries.nulls(other.size()));
        }
      }

      df.addSeries(name, builder.build());
    }

    return df;
  }

  /**
   * Returns a new DataFrame concatenated from a list of given DataFrames.
   * Returns an empty DataFrame on empty input, otherwise follows the conventions of {@code append()}.
   *
   * @see DataFrame#append(DataFrame...)
   *
   * @param dataframes DataFrames to concatenate in sequence
   * @return concatenated DataFrame
   */
  public static DataFrame concatenate(DataFrame... dataframes) {
    return concatenate(Arrays.asList(dataframes));
  }

  /**
   * Returns a new DataFrame concatenated from a list of given DataFrames.
   * Returns an empty DataFrame on empty input, otherwise follows the conventions of {@code append()}.
   *
   * @see DataFrame#append(List)
   *
   * @param dataframes DataFrames to concatenate in sequence
   * @return concatenated DataFrame
   */
  public static DataFrame concatenate(List<DataFrame> dataframes) {
    if (dataframes.isEmpty()) {
      return new DataFrame();
    }

    if (dataframes.size() == 1) {
      return dataframes.get(0);
    }

    DataFrame first = dataframes.get(0);
    List<DataFrame> others = dataframes.subList(1, dataframes.size());

    return first.append(others);
  }

  @Override
  public String toString() {
    List<String> names = new ArrayList<>(this.getSeriesNames());
    for (int i=0; i<this.indexNames.size(); i++) {
      names.remove(this.indexNames.get(i));
      names.add(i, this.indexNames.get(i));
    }

    return this.toString(DEFAULT_MAX_COLUMN_WIDTH, names);
  }

  public String toString(String... seriesNames) {
    return this.toString(Arrays.asList(seriesNames));
  }

  public String toString(List<String> seriesNames) {
    return this.toString(DEFAULT_MAX_COLUMN_WIDTH, seriesNames);
  }

  public String toString(int maxColumnWidth, String... seriesNames) {
    return this.toString(maxColumnWidth, Arrays.asList(seriesNames));
  }

  public String toString(int maxColumnWidth, List<String> seriesNames) {
    if(seriesNames.isEmpty())
      return "";

    String[][] values = new String[this.size()][seriesNames.size()];
    int[] width = new int[seriesNames.size()];
    for(int i=0; i<seriesNames.size(); i++) {
      Series s = assertSeriesExists(seriesNames.get(i));

      width[i] = truncateToString(seriesNames.get(i), maxColumnWidth).length();
      for(int j=0; j<this.size(); j++) {
        String itemValue = truncateToString(s.toString(j), maxColumnWidth);
        values[j][i] = itemValue;
        width[i] = Math.max(itemValue.length(), width[i]);
      }
    }

    StringBuilder sb = new StringBuilder();
    // header
    for(int i=0; i<seriesNames.size(); i++) {
      sb.append(String.format("%" + width[i] + "s", truncateToString(seriesNames.get(i), maxColumnWidth)));
      sb.append("  ");
    }
    sb.delete(sb.length() - 2, sb.length());
    sb.append("\n");

    // values
    for(int j=0; j<this.size(); j++) {
      for(int i=0; i<seriesNames.size(); i++) {
        Series s = this.get(seriesNames.get(i));
        String item;
        switch(s.type()) {
          case DOUBLE:
          case LONG:
          case BOOLEAN:
            item = String.format("%" + width[i] + "s", values[j][i]);
            break;
          case STRING:
          case OBJECT:
            item = String.format("%-" + width[i] + "s", values[j][i]);
            break;
          default:
            throw new IllegalArgumentException(String.format("Unknown series type '%s'", s.type()));
        }
        sb.append(item);
        sb.append("  ");
      }
      sb.delete(sb.length() - 2, sb.length());
      sb.append("\n");
    }

    return sb.toString();
  }

  static String truncateToString(String value, int maxWidth) {
    if(value.length() > maxWidth)
      value = value.substring(0, maxWidth - 3) + "...";
    return value;
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

  Series[] names2series(String... names) {
    return names2series(Arrays.asList(names));
  }

  Series[] names2series(List<String> names) {
    Series[] inputSeries = new Series[names.size()];
    for(int i=0; i<names.size(); i++) {
      inputSeries[i] = assertSeriesExists(names.get(i));
    }
    return inputSeries;
  }

  Series assertSeriesExists(String name) {
    if(!series.containsKey(name))
      throw new IllegalArgumentException(String.format("Unknown series '%s'", name));
    return series.get(name);
  }

  List<Series> assertSeriesExist(List<String> names) {
    List<Series> series = new ArrayList<>();
    for (String n : names)
      series.add(assertSeriesExists(n));
    return series;
  }

  void assertSameLength(Series s) {
    if(this.size() != s.size())
      throw new IllegalArgumentException("Series size must be equals to DataFrame size");
  }

  static void assertSameLength(Series... series) {
    for(int i=0; i<series.length-1; i++) {
      if (series[i].size() != series[i+1].size())
        throw new IllegalArgumentException("Series size must be equals to DataFrame size");
    }
  }

  static void assertIndex(DataFrame... dataframes) {
    for(DataFrame d : dataframes)
      if(!d.hasIndex())
        throw new IllegalArgumentException("DataFrames must have a valid index");
  }

  Set<String> extractSeriesNames(String doubleExpression) {
    Matcher m = PATTERN_FORMULA_VARIABLE.matcher(doubleExpression);

    Set<String> variables = new HashSet<>();
    while(m.find()) {
      if(this.series.keySet().contains(m.group(1))) {
        variables.add(m.group(1));
      } else {
        throw new IllegalArgumentException(String.format("Could not resolve variable '%s'", m.group()));
      }
    }

    return variables;
  }

  /* **************************************************************************
   * DataFrame parsers
   ***************************************************************************/

  /**
   * Reads in a CSV structured stream and returns it as a DataFrame. The native series type is
   * chosen to be as specific as possible based on the data ingested.
   * <br/><b>NOTE:</b> Expects the first line to contain
   * column headers. The column headers are transformed into series names by replacing non-word
   * character sequences with underscores ({@code "_"}). Leading digits in series names are also
   * escaped with a leading underscore.
   *
   * @param in input reader
   * @return CSV as DataFrame
   * @throws IOException if a read error is encountered
   * @throws IllegalArgumentException if the column headers cannot be transformed into valid series names
   */
  public static DataFrame fromCsv(Reader in) throws IOException {
    Iterator<CSVRecord> it = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(in).iterator();
    if(!it.hasNext())
      return new DataFrame();

    CSVRecord first = it.next();
    Set<String> headers = first.toMap().keySet();

    // transform column headers into series names
    Map<String, String> header2name = new HashMap<>();
    for(String h : headers) {
      // remove spaces
      String name = Pattern.compile("\\W+").matcher(h).replaceAll("_");

      // underscore escape leading number
      if(Pattern.compile("\\A[0-9]").matcher(name).find())
        name = "_" + name;

      header2name.put(h, name);
    }

    // read first line and initialize builders
    Map<String, StringSeries.Builder> builders = new HashMap<>();
    for(String h : headers) {
      StringSeries.Builder builder = StringSeries.builder();
      builder.addValues(first.get(h));
      builders.put(h, builder);
    }

    while(it.hasNext()) {
      CSVRecord record = it.next();
      for(String h : headers) {
        String value = record.get(h);
        builders.get(h).addValues(value);
      }
    }

    // construct dataframe and detect native data types
    DataFrame df = new DataFrame();
    for(Map.Entry<String, StringSeries.Builder> e : builders.entrySet()) {
      StringSeries s = e.getValue().build();
      Series conv = s.get(s.inferType());
      String name = header2name.get(e.getKey());
      df.addSeries(name, conv);
    }

    return df;
  }

  /**
   * Reads in a Pinot ResultSetGroup and returns it as a DataFrame.
   *
   * @param resultSetGroup pinot query result
   * @return Pinot query result as DataFrame
   * @throws IllegalArgumentException if the result cannot be parsed
   */
  public static DataFrame fromPinotResult(ResultSetGroup resultSetGroup) {
    if (resultSetGroup.getResultSetCount() <= 0)
      throw new IllegalArgumentException("Query did not return any results");

    if (resultSetGroup.getResultSetCount() == 1) {
      ResultSet resultSet = resultSetGroup.getResultSet(0);

      if (resultSet.getColumnCount() == 1 && resultSet.getRowCount() == 0) {
        // empty result
        return new DataFrame();

      } else if (resultSet.getColumnCount() == 1 && resultSet.getRowCount() == 1 && resultSet.getGroupKeyLength() == 0) {
        // aggregation result

        DataFrame df = new DataFrame();
        String function = resultSet.getColumnName(0);
        String value = resultSet.getString(0, 0);
        df.addSeries(function, DataFrame.toSeries(value));
        return df;

      } else if (resultSet.getColumnCount() >= 1 && resultSet.getGroupKeyLength() == 0) {
        // selection result

        DataFrame df = new DataFrame();
        for (int i = 0; i < resultSet.getColumnCount(); i++) {
          df.addSeries(resultSet.getColumnName(i), makeSelectionSeries(resultSet, i));
        }
        return df;

      }
    }

    // group by result
    ResultSet firstResultSet = resultSetGroup.getResultSet(0);
    String[] groupKeyNames = new String[firstResultSet.getGroupKeyLength()];
    for(int i=0; i<firstResultSet.getGroupKeyLength(); i++) {
      groupKeyNames[i] = firstResultSet.getGroupKeyColumnName(i);
    }

    DataFrame df = new DataFrame();
    for (String groupKeyName : groupKeyNames) {
      df.addSeries(groupKeyName, StringSeries.empty());
    }
    df.setIndex(groupKeyNames);

    for(int i=0; i<resultSetGroup.getResultSetCount(); i++) {
      ResultSet resultSet = resultSetGroup.getResultSet(i);
      String function = resultSet.getColumnName(0);

      // group keys
      DataFrame dfColumn = new DataFrame();
      for(int j=0; j<resultSet.getGroupKeyLength(); j++) {
        dfColumn.addSeries(groupKeyNames[j], makeGroupByGroupSeries(resultSet, j));
      }
      dfColumn.setIndex(groupKeyNames);

      // values
      dfColumn.addSeries(function, makeGroupByValueSeries(resultSet));

      df = df.joinOuter(dfColumn);
    }

    return df;
  }

  private static Series makeSelectionSeries(ResultSet resultSet, int colIndex) {
    int rowCount = resultSet.getRowCount();
    if(rowCount <= 0)
      return StringSeries.empty();

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

  private static Set<String> getValidTypes() {
    Set<String> values = new HashSet<>();
    for (Series.SeriesType type : Series.SeriesType.values()) {
      values.add(type.name());
    }
    return values;
  }

  public static class Tuple implements Comparable<Tuple> {
    private final Object[] values;

    public static Tuple buildFrom(Object... values) {
      return new Tuple(values);
    }

    public static Tuple copyFrom(Object... values) {
      return new Tuple(Arrays.copyOf(values, values.length));
    }

    public static Tuple buildFrom(Series[] series, int row) {
      Object[] values = new Object[series.length];
      for(int i=0; i<series.length; i++) {
        values[i] = series[i].getObject(row);
      }
      return new Tuple(values);
    }

    private Tuple(Object... values) {
      this.values = values;
    }

    public Object[] getValues() {
      return values;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Tuple tuple = (Tuple) o;
      return Arrays.equals(values, tuple.values);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(values);
    }

    @Override
    public String toString() {
      return Arrays.toString(values);
    }

    @Override
    public int compareTo(Tuple o) {
      if (this.values.length != o.values.length) {
        return Integer.compare(this.values.length, o.values.length);
      }

      for (int i = 0; i < this.values.length; i++) {
        if (this.values[i] == null && o.values[i] == null) {
          continue;
        }
        if (this.values[i] == null) {
          return -1;
        }
        if (o.values[i] == null) {
          return 1;
        }

        if (this.values[i].equals(o.values[i])) {
          continue;
        }

        int result = ((Comparable<Object>) this.values[i]).compareTo(o.values[i]);
        if (result == 0) {
          continue;
        }

        return result;
      }

      return 0;
    }
  }
}
