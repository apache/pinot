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

  /**
   * Constructs a ValueAggregator from the given aggregation type.
   *
   * When adding entries to this please add them to the Set in org.apache.pinot.segment.local.utils.TableConfigUtils
   * named AVAILABLE_CORE_VALUE_AGGREGATORS so that they can be used in RealtimeToOfflineTask
   */
  public static ValueAggregator getValueAggregator(AggregationFunctionType aggregationType, DataType dataType) {
    switch (aggregationType) {
      case MIN:
        return new MinValueAggregator(dataType);
      case MAX:
        return new MaxValueAggregator(dataType);
      case SUM:
        return new SumValueAggregator(dataType);
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
      default:
        throw new IllegalStateException("Unsupported aggregation type: " + aggregationType);
    }
  }
}
