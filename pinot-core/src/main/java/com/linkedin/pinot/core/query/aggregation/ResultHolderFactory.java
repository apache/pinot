/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.query.aggregation;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionFactory;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionFactory.AggregationFunctionType;
import com.linkedin.pinot.core.query.aggregation.groupby.DoubleGroupByResultHolder;
import com.linkedin.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import com.linkedin.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;


public class ResultHolderFactory {

  private static final Double DEFAULT_DOUBLE_VALUE = 0.0;
  private static final String DEFAULT_STRING_VALUE = "";
  private static final Double DEFAULT_MAX_DOUBLE = Double.NEGATIVE_INFINITY;
  private static final Double DEFAULT_MIN_DOUBLE = Double.POSITIVE_INFINITY;

  public static AggregationResultHolder getAggregationResultHolder(
      AggregationFunctionType aggregationFunctionType, FieldSpec.DataType dataType) {

    switch (aggregationFunctionType) {

      case COUNT:
      case COUNTMV:
      case SUM:
      case SUMMV:
        return new DoubleAggregationResultHolder(DEFAULT_DOUBLE_VALUE);

      case MIN:
      case MINMV:
        if (dataType.equals(FieldSpec.DataType.STRING)) {
          return new ObjectAggregationResultHolder(DEFAULT_STRING_VALUE);
        } else {
          return new ObjectAggregationResultHolder(DEFAULT_MIN_DOUBLE);
        }
      case MAX:
      case MAXMV:
        if (dataType.equals(FieldSpec.DataType.STRING)) {
          return new ObjectAggregationResultHolder(DEFAULT_STRING_VALUE);
        } else {
          return new ObjectAggregationResultHolder(DEFAULT_MAX_DOUBLE);
        }

      case AVG:
      case MINMAXRANGE:
      case DISTINCTCOUNT:
      case DISTINCTCOUNTHLL:
      case FASTHLL:
      case PERCENTILE10:
      case PERCENTILE20:
      case PERCENTILE30:
      case PERCENTILE40:
      case PERCENTILE50:
      case PERCENTILE60:
      case PERCENTILE70:
      case PERCENTILE80:
      case PERCENTILE90:
      case PERCENTILE95:
      case PERCENTILE99:
      case PERCENTILEEST10:
      case PERCENTILEEST20:
      case PERCENTILEEST30:
      case PERCENTILEEST40:
      case PERCENTILEEST50:
      case PERCENTILEEST60:
      case PERCENTILEEST70:
      case PERCENTILEEST80:
      case PERCENTILEEST90:
      case PERCENTILEEST95:
      case PERCENTILEEST99:
      case AVGMV:
      case MINMAXRANGEMV:
      case DISTINCTCOUNTMV:
      case DISTINCTCOUNTHLLMV:
      case FASTHLLMV:
      case PERCENTILE10MV:
      case PERCENTILE20MV:
      case PERCENTILE30MV:
      case PERCENTILE40MV:
      case PERCENTILE50MV:
      case PERCENTILE60MV:
      case PERCENTILE70MV:
      case PERCENTILE80MV:
      case PERCENTILE90MV:
      case PERCENTILE95MV:
      case PERCENTILE99MV:
      case PERCENTILEEST10MV:
      case PERCENTILEEST20MV:
      case PERCENTILEEST30MV:
      case PERCENTILEEST40MV:
      case PERCENTILEEST50MV:
      case PERCENTILEEST60MV:
      case PERCENTILEEST70MV:
      case PERCENTILEEST80MV:
      case PERCENTILEEST90MV:
      case PERCENTILEEST95MV:
      case PERCENTILEEST99MV:
      default:
        return new ObjectAggregationResultHolder();
    }
  }


  public static GroupByResultHolder getGroupByResultHolder(AggregationFunctionType aggregationFunctionType,
      int initialCapacity, int maxNumResults, int trimSize) {
    switch (aggregationFunctionType) {
      case COUNT:
      case COUNTMV:
      case SUM:
      case SUMMV:
        return new DoubleGroupByResultHolder(initialCapacity, maxNumResults, trimSize, DEFAULT_DOUBLE_VALUE);

      case MIN:
      case MINMV:
      case MAX:
      case MAXMV:
      case AVG:
      case MINMAXRANGE:
      case DISTINCTCOUNT:
      case DISTINCTCOUNTHLL:
      case FASTHLL:
      case PERCENTILE10:
      case PERCENTILE20:
      case PERCENTILE30:
      case PERCENTILE40:
      case PERCENTILE50:
      case PERCENTILE60:
      case PERCENTILE70:
      case PERCENTILE80:
      case PERCENTILE90:
      case PERCENTILE95:
      case PERCENTILE99:
      case PERCENTILEEST10:
      case PERCENTILEEST20:
      case PERCENTILEEST30:
      case PERCENTILEEST40:
      case PERCENTILEEST50:
      case PERCENTILEEST60:
      case PERCENTILEEST70:
      case PERCENTILEEST80:
      case PERCENTILEEST90:
      case PERCENTILEEST95:
      case PERCENTILEEST99:
      case AVGMV:
      case MINMAXRANGEMV:
      case DISTINCTCOUNTMV:
      case DISTINCTCOUNTHLLMV:
      case FASTHLLMV:
      case PERCENTILE10MV:
      case PERCENTILE20MV:
      case PERCENTILE30MV:
      case PERCENTILE40MV:
      case PERCENTILE50MV:
      case PERCENTILE60MV:
      case PERCENTILE70MV:
      case PERCENTILE80MV:
      case PERCENTILE90MV:
      case PERCENTILE95MV:
      case PERCENTILE99MV:
      case PERCENTILEEST10MV:
      case PERCENTILEEST20MV:
      case PERCENTILEEST30MV:
      case PERCENTILEEST40MV:
      case PERCENTILEEST50MV:
      case PERCENTILEEST60MV:
      case PERCENTILEEST70MV:
      case PERCENTILEEST80MV:
      case PERCENTILEEST90MV:
      case PERCENTILEEST95MV:
      case PERCENTILEEST99MV:
      default:
        return new ObjectGroupByResultHolder(initialCapacity, maxNumResults, trimSize);

    }
  }
}
