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

import java.util.Collections;
import org.apache.pinot.core.operator.blocks.results.TimeSeriesResultsBlock;
import org.apache.pinot.core.operator.combine.TimeSeriesCombineOperator;
import org.apache.pinot.tsdb.spi.operator.BaseTimeSeriesOperator;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBlock;


/**
 * Adapter operator that ties in the Pinot TimeSeriesCombineOperator with the Pinot BaseTimeSeriesOperator.
 */
public class TimeSeriesPassThroughOperator extends BaseTimeSeriesOperator {
  private static final String EXPLAIN_NAME = "TIME_SERIES_PASS_THROUGH_OPERATOR";
  private final TimeSeriesCombineOperator _timeSeriesCombineOperator;

  public TimeSeriesPassThroughOperator(TimeSeriesCombineOperator combineOperator) {
    super(Collections.emptyList());
    _timeSeriesCombineOperator = combineOperator;
  }

  @Override
  public TimeSeriesBlock getNextBlock() {
    TimeSeriesResultsBlock resultsBlock = (TimeSeriesResultsBlock) _timeSeriesCombineOperator.nextBlock();
    return resultsBlock.getTimeSeriesBlock();
  }

  @Override
  public String getExplainName() {
    return EXPLAIN_NAME;
  }
}
