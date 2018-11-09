/*
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.thirdeye.detection.spi.components;

import com.linkedin.thirdeye.dataframe.DoubleSeries;
import com.linkedin.thirdeye.dataframe.Series;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.detection.spec.AbstractSpec;
import com.linkedin.thirdeye.detection.spi.model.InputData;
import com.linkedin.thirdeye.detection.spi.model.InputDataSpec;
import com.linkedin.thirdeye.detection.spi.model.TimeSeries;


/**
 * The baseline provider to calculate predicted baseline.
 */
public interface BaselineProvider<T extends AbstractSpec> extends BaseComponent<T> {
  /**
   * Compute the baseline time series for the metric slice.
   * @return the time series contains predicted baseline.
   */
  TimeSeries computePredictedTimeSeries(InputData data);

  InputDataSpec getInputDataSpec(MetricSlice slice);

  /**
   * Compute the baseline time series for the metric slice.
   * default implementation is to call computePredictedTimeSeries and aggregate
   * @return the predicted value.
   */
  default Double computePredictedAggregates(InputData data){
    // default to average
    return computePredictedAggregates(data, DoubleSeries.MEAN);
  }

  default Double computePredictedAggregates(InputData data, Series.DoubleFunction aggregateFunction){
    // default to be average
    TimeSeries baselineTimeSeries = this.computePredictedTimeSeries(data);
    return baselineTimeSeries.getPredictedBaseline().aggregate(aggregateFunction).getDouble(0);
  }

  default InputDataSpec getAggregateInputDataSpec(MetricSlice slice) {
    return this.getInputDataSpec(slice);
  }

}
