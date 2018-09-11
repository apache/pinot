/**
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

package com.linkedin.thirdeye.rootcause.timeseries;

import com.linkedin.thirdeye.dataframe.DoubleSeries;
import com.linkedin.thirdeye.dataframe.Series;


/**
 * Aggregation types supported by BaselineAggregate.
 *
 * @see BaselineAggregate
 */
public enum BaselineAggregateType {
  SUM(DoubleSeries.SUM),
  PRODUCT(DoubleSeries.PRODUCT),
  MEAN(DoubleSeries.MEAN),
  MEDIAN(DoubleSeries.MEDIAN),
  MIN(DoubleSeries.MIN),
  MAX(DoubleSeries.MAX),
  STD(DoubleSeries.STD);

  final Series.DoubleFunction function;

  BaselineAggregateType(Series.DoubleFunction function) {
    this.function = function;
  }

  public Series.DoubleFunction getFunction() {
    return function;
  }
}

