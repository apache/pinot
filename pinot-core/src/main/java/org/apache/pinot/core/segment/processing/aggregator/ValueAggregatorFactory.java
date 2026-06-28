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
package org.apache.pinot.core.segment.processing.aggregator;

import org.apache.datasketches.tuple.aninteger.IntegerSummary;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Factory class to create instances of value aggregator from the given name.
 */
public class ValueAggregatorFactory {
  private ValueAggregatorFactory() {
  }

  /// Returns `true` if the given aggregation type requires the rollup rows to be ordered by the original
  /// (pre-rounding) time, i.e. it picks a value based on the original time order of the rows instead of combining the
  /// values, and therefore requires the rollup rows to be sorted on the hidden original time column within each
  /// rollup group.
  public static boolean requiresTimeOrdering(AggregationFunctionType aggregationType) {
    return aggregationType == AggregationFunctionType.FIRSTWITHTIME
        || aggregationType == AggregationFunctionType.LASTWITHTIME;
  }

  /// Returns `true` if the rollup ValueAggregator for the given type stores its aggregated value as a serialized
  /// object (sketch, AvgPair, TDigest, ...) and therefore reads the metric column value as a `byte[]`, i.e. the
  /// column must be a `BYTES` column. Numeric (MIN/MAX/SUM) and time-ordered (FIRSTWITHTIME/LASTWITHTIME) aggregators
  /// keep the column's native type. Keep this in sync with [#getValueAggregator]: a type is bytes-backed iff its
  /// aggregator deserializes the column value as `byte[]`.
  public static boolean isBytesBacked(AggregationFunctionType aggregationType) {
    switch (aggregationType) {
      case AVG:
      case DISTINCTCOUNTHLL:
      case DISTINCTCOUNTRAWHLL:
      case DISTINCTCOUNTTHETASKETCH:
      case DISTINCTCOUNTRAWTHETASKETCH:
      case DISTINCTCOUNTTUPLESKETCH:
      case DISTINCTCOUNTRAWINTEGERSUMTUPLESKETCH:
      case SUMVALUESINTEGERSUMTUPLESKETCH:
      case AVGVALUEINTEGERSUMTUPLESKETCH:
      case DISTINCTCOUNTCPCSKETCH:
      case DISTINCTCOUNTRAWCPCSKETCH:
      case DISTINCTCOUNTULL:
      case DISTINCTCOUNTRAWULL:
      case PERCENTILEKLL:
      case PERCENTILERAWKLL:
      case PERCENTILETDIGEST:
      case PERCENTILERAWTDIGEST:
        return true;
      default:
        return false;
    }
  }

  /// Constructs a ValueAggregator from the given aggregation type.
  ///
  /// When adding entries to this please add them to the Set named AVAILABLE_CORE_VALUE_AGGREGATORS in
  /// org.apache.pinot.core.common.MinionConstants.MergeRollupTask so that they pass the task config validation of the
  /// merge tasks (MergeRollupTask, RealtimeToOfflineSegmentsTask), and update [#isBytesBacked] if the new aggregator
  /// reads/writes the column value as `byte[]`.
  public static ValueAggregator getValueAggregator(AggregationFunctionType aggregationType, DataType dataType) {
    switch (aggregationType) {
      case MIN:
        return new MinValueAggregator(dataType);
      case MAX:
        return new MaxValueAggregator(dataType);
      case SUM:
        return new SumValueAggregator(dataType);
      case AVG:
        return new AvgValueAggregator();
      case DISTINCTCOUNTHLL:
      case DISTINCTCOUNTRAWHLL:
        return new DistinctCountHLLAggregator();
      case DISTINCTCOUNTTHETASKETCH:
      case DISTINCTCOUNTRAWTHETASKETCH:
        return new DistinctCountThetaSketchAggregator();
      case DISTINCTCOUNTTUPLESKETCH:
      case DISTINCTCOUNTRAWINTEGERSUMTUPLESKETCH:
      case SUMVALUESINTEGERSUMTUPLESKETCH:
      case AVGVALUEINTEGERSUMTUPLESKETCH:
        return new IntegerTupleSketchAggregator(IntegerSummary.Mode.Sum);
      case DISTINCTCOUNTCPCSKETCH:
      case DISTINCTCOUNTRAWCPCSKETCH:
        return new DistinctCountCPCSketchAggregator();
      case DISTINCTCOUNTULL:
      case DISTINCTCOUNTRAWULL:
        return new DistinctCountULLAggregator();
      case PERCENTILEKLL:
      case PERCENTILERAWKLL:
        return new PercentileKLLSketchAggregator();
      case PERCENTILETDIGEST:
      case PERCENTILERAWTDIGEST:
        return new PercentileTDigestAggregator();
      case FIRSTWITHTIME:
        return new FirstWithTimeValueAggregator();
      case LASTWITHTIME:
        return new LastWithTimeValueAggregator();
      default:
        throw new IllegalStateException("Unsupported aggregation type: " + aggregationType);
    }
  }
}
