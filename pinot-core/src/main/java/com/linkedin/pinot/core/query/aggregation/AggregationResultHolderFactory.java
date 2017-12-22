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


public class AggregationResultHolderFactory {

  public static AggregationResultHolder getAggregationResultHolder(
      AggregationFunctionFactory.AggregationFunctionType aggregationFunctionType, FieldSpec.DataType dataType) {

    switch (aggregationFunctionType) {

      case COUNT:
      case COUNTMV:
      case SUM:
      case SUMMV:
        return new DoubleAggregationResultHolder(0.0);

      case MIN:
      case MINMV:
        if (dataType.equals(FieldSpec.DataType.STRING)) {
          return new ObjectAggregationResultHolder("");
        } else {
          return new ObjectAggregationResultHolder(Double.POSITIVE_INFINITY);
        }
      case MAX:
      case MAXMV:
        if (dataType.equals(FieldSpec.DataType.STRING)) {
          return new ObjectAggregationResultHolder("");
        } else {
          return new ObjectAggregationResultHolder(Double.NEGATIVE_INFINITY);
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
        return new ObjectAggregationResultHolder();

      default:
        return new ObjectAggregationResultHolder();
    }
  }
}
