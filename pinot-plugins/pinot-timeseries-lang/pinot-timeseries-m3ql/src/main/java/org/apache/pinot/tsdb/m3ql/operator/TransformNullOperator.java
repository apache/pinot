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
package org.apache.pinot.tsdb.m3ql.operator;

import java.util.List;
import org.apache.pinot.tsdb.spi.operator.BaseTimeSeriesOperator;
import org.apache.pinot.tsdb.spi.series.TimeSeries;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBlock;


public class TransformNullOperator extends BaseTimeSeriesOperator {
  private final Double _defaultValue;

  public TransformNullOperator(Double defaultValue, List<BaseTimeSeriesOperator> childOperators) {
    super(childOperators);
    _defaultValue = defaultValue;
  }

  @Override
  public TimeSeriesBlock getNextBlock() {
    TimeSeriesBlock seriesBlock = _childOperators.get(0).nextBlock();
    seriesBlock.getSeriesMap().values().parallelStream().forEach(unionOfSeries -> {
      for (TimeSeries series : unionOfSeries) {
        Double[] values = series.getDoubleValues();
        for (int index = 0; index < values.length; index++) {
          values[index] = values[index] == null ? _defaultValue : values[index];
        }
      }
    });
    return seriesBlock;
  }

  @Override
  public String getExplainName() {
    return "TRANSFORM_NULL";
  }
}
