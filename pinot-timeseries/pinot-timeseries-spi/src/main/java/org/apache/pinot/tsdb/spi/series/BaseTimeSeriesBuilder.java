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

import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.tsdb.spi.TimeBuckets;


/**
 * BaseSeriesBuilder allows language implementations to build their own aggregation and other time-series functions.
 * Each time-series operator would typically call either of {@link #addValue} or {@link #addValueAtIndex}. When
 * the operator is done, it will call {@link #build()} to allow the builder to compute the final {@link TimeSeries}.
 * <br />
 * <b>Important:</b> Refer to {@link TimeSeries} for details on Series ID and how to use it in general.
 */
public abstract class BaseTimeSeriesBuilder {
  public static final List<String> UNINITIALISED_TAG_NAMES = Collections.emptyList();
  public static final Object[] UNINITIALISED_TAG_VALUES = new Object[]{};
  protected final String _id;
  @Nullable
  protected final Long[] _timeValues;
  @Nullable
  protected final TimeBuckets _timeBuckets;
  protected final List<String> _tagNames;
  protected final Object[] _tagValues;

  /**
   * <b>Note:</b> The leaf stage will use {@link #UNINITIALISED_TAG_NAMES} and {@link #UNINITIALISED_TAG_VALUES} during
   * the aggregation. This is because tag values are materialized after the Combine Operator.
   */
  public BaseTimeSeriesBuilder(String id, @Nullable Long[] timeValues, @Nullable TimeBuckets timeBuckets,
      List<String> tagNames, Object[] tagValues) {
    _id = id;
    _timeValues = timeValues;
    _timeBuckets = timeBuckets;
    _tagNames = tagNames;
    _tagValues = tagValues;
  }

  public abstract void addValueAtIndex(int timeBucketIndex, Double value);

  public void addValueAtIndex(int timeBucketIndex, String value) {
    throw new UnsupportedOperationException("This aggregation function does not support string input");
  }

  public abstract void addValue(long timeValue, Double value);

  /**
   * Assumes Double[] values and attempts to merge the given series with this builder. Implementations are
   * recommended to override this to either optimize, or add bytes[][] values from the input Series.
   */
  public void mergeAlignedSeries(TimeSeries series) {
    int numDataPoints = series.getDoubleValues().length;
    for (int i = 0; i < numDataPoints; i++) {
      addValueAtIndex(i, series.getDoubleValues()[i]);
    }
  }

  /**
   * Adds an un-built series-builder to this builder. Implementations may want to override this method, especially for
   * complex aggregations, where the series builder accumulates results in a complex object. (e.g. percentile)
   */
  public void mergeAlignedSeriesBuilder(BaseTimeSeriesBuilder builder) {
    TimeSeries timeSeries = builder.build();
    mergeAlignedSeries(timeSeries);
  }

  public abstract TimeSeries build();

  /**
   * Used by the leaf stage, because the leaf stage materializes tag values very late.
   */
  public abstract TimeSeries buildWithTagOverrides(List<String> tagNames, Object[] tagValues);
}
