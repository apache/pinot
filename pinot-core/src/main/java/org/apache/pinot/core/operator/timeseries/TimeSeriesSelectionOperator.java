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
package org.apache.pinot.core.operator.timeseries;

import com.google.common.collect.ImmutableList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.blocks.results.TimeSeriesResultsBlock;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBuilderFactory;


public class TimeSeriesSelectionOperator extends BaseOperator<TimeSeriesResultsBlock> {
  private static final String EXPLAIN_NAME = "TIME_SERIES_SELECTION_OPERATOR";
  private final Long _evaluationTimestamp;
  private final TransformOperator _transformOperator;
  private final TimeSeriesBuilderFactory _seriesBuilderFactory;

  public TimeSeriesSelectionOperator(Long evaluationTimestamp,
      TransformOperator transformOperator, TimeSeriesBuilderFactory seriesBuilderFactory) {
    _evaluationTimestamp = evaluationTimestamp;
    _transformOperator = transformOperator;
    _seriesBuilderFactory = seriesBuilderFactory;
  }

  @Override
  protected TimeSeriesResultsBlock getNextBlock() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public List<? extends Operator> getChildOperators() {
    return ImmutableList.of(_transformOperator);
  }

  @Nullable
  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }
}
