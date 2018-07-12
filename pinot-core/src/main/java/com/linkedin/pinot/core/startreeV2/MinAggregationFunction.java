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

package com.linkedin.pinot.core.startreeV2;

import java.util.List;
import javax.annotation.Nonnull;
import com.linkedin.pinot.common.data.FieldSpec;


public class MinAggregationFunction implements AggregationFunction<Number, Double> {

  @Nonnull
  @Override
  public String getName() {
    return StarTreeV2Constant.AggregateFunctions.MIN;
  }

  @Nonnull
  @Override
  public FieldSpec.DataType getDatatype() {
    return FieldSpec.DataType.DOUBLE;
  }

  @Override
  public Double aggregateRaw(List<Number> data) {
    double max = Double.POSITIVE_INFINITY;
    for (Number number : data) {
      max = Math.min(max, number.doubleValue());
    }
    return max;
  }

  @Override
  public Double aggregatePreAggregated(List<Double> data) {
    double max = Double.POSITIVE_INFINITY;
    for (Double number : data) {
      max = Math.min(max, number);
    }
    return max;
  }
}
