/**
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
package org.apache.pinot.tsdb.spi.series;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.pinot.tsdb.spi.TimeBuckets;


/**
 * Logically, a time-series is a list of pairs of time and data values, where time is stored in increasing order.
 * A time-series is identified using its ID, which can be retrieved using {@link #getId()}.
 * A time series typically also has a set of pairs of keys and values which are called tags or labels.
 * We allow a Series to store time either via {@link TimeBuckets} or via a long array as in {@link #getTimeValues()}.
 * Using {@link TimeBuckets} is ideal when your queries are working on evenly spaced time ranges. The other option
 * exists to support use-cases such as "Instant Vectors" in PromQL.
 * <p>
 *   <b>Warning:</b> The time and value arrays passed to the Series are not copied, and can be modified by anyone with
 *   access to them. This is by design, to make it easier to re-use buffers during time-series operations.
 * </p>
 *
 * <h3>Series ID Usage and Semantics</h3>
 * ID of a time-series should uniquely identify a time-series in an execution context. There are languages that
 * allow performing a "union" operation, and to accommodate those cases, we always store a {@link List<TimeSeries>}
 * in {@link TimeSeriesBlock}. Moreover, for the union of series case, series with the same label key-value pairs can
 * have the same ID.
 * <p>
 *   <b>Important:</b> The following points summarize how Series ID should be used:
 *   <ul>
 *     <li>
 *       Series ID should be used in series blocks as the identifier that defines uniqueness. In other words, use it
 *       as the key for the Map&lt;Long, List&lt;TimeSeries&gt;&gt;.
 *     </li>
 *     <li>
 *       The leaf operator creates Series IDs using the tag-values alone, stored in a Object[]. For the Map in series
 *       block, we hash the ID to a Long using {@link TimeSeries#hash(Object[])}. The Object[] array will be empty,
 *       and so will the tags and values, if you do an aggregation without any grouping set.
 *     </li>
 *     <li>
 *       Whenever you have to convert the series ID to a Long, you can use Java hashCode or any other algorithm. The
 *       only reason we use a Long and not the String series ID is to make the Map lookups faster.
 *     </li>
 *   </ul>
 * </p>
 */
public class TimeSeries {
  private final String _id;
  private final Long[] _timeValues;
  private final TimeBuckets _timeBuckets;
  private final Object[] _values;
  private final List<String> _tagNames;
  private final Object[] _tagValues;

  // TODO(timeseries): Time series may also benefit from storing extremal/outlier value traces, similar to Monarch.
  // TODO(timeseries): It may make sense to allow types other than Double and byte[] arrays.
  public TimeSeries(String id, @Nullable Long[] timeValues, @Nullable TimeBuckets timeBuckets, Object[] values,
      List<String> tagNames, Object[] tagValues) {
    Preconditions.checkArgument(values instanceof Double[] || values instanceof byte[][],
        "Time Series can only take Double[] or byte[][] values");
    _id = id;
    _timeValues = timeValues;
    _timeBuckets = timeBuckets;
    _values = values;
    _tagNames = Collections.unmodifiableList(tagNames);
    _tagValues = tagValues;
  }

  public String getId() {
    return _id;
  }

  @Nullable
  public Long[] getTimeValues() {
    return _timeValues;
  }

  @Nullable
  public TimeBuckets getTimeBuckets() {
    return _timeBuckets;
  }

  public Object[] getValues() {
    return _values;
  }

  public Double[] getDoubleValues() {
    return (Double[]) _values;
  }

  public byte[][] getBytesValues() {
    return (byte[][]) _values;
  }

  public List<String> getTagNames() {
    return _tagNames;
  }

  public Object[] getTagValues() {
    return _tagValues;
  }

  public Map<String, String> getTagKeyValuesAsMap() {
    Map<String, String> result = new HashMap<>();
    for (int index = 0; index < _tagNames.size(); index++) {
      String tagValue = _tagValues[index] == null ? "null" : _tagValues[index].toString();
      result.put(_tagNames.get(index), tagValue);
    }
    return result;
  }

  public String getTagsSerialized() {
    if (_tagNames.isEmpty()) {
      return "*";
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < _tagNames.size(); i++) {
      if (i > 0) {
        sb.append(",");
      }
      sb.append(String.format("%s=%s", _tagNames.get(i), _tagValues[i]));
    }
    return sb.toString();
  }

  // TODO: This can be cleaned up
  public static long hash(Object[] tagNamesAndValues) {
    return Objects.hash(tagNamesAndValues);
  }
}
