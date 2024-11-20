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
package org.apache.pinot.core.operator.blocks.results;

import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.operator.blocks.TimeSeriesBuilderBlock;
import org.apache.pinot.core.query.request.context.QueryContext;


public class TimeSeriesResultsBlock extends BaseResultsBlock {
  private final TimeSeriesBuilderBlock _timeSeriesBuilderBlock;

  public TimeSeriesResultsBlock(TimeSeriesBuilderBlock timeSeriesBuilderBlock) {
    _timeSeriesBuilderBlock = timeSeriesBuilderBlock;
  }

  @Override
  public int getNumRows() {
    return _timeSeriesBuilderBlock.getSeriesBuilderMap().size();
  }

  @Nullable
  @Override
  public QueryContext getQueryContext() {
    // TODO(timeseries): Implement this when merging with MSE. Only LeafStageTransferableBlockOperator uses this so far.
    throw new UnsupportedOperationException("Time series results block does not support getting QueryContext yet");
  }

  @Nullable
  @Override
  public DataSchema getDataSchema() {
    // TODO(timeseries): Define this when merging with MSE.
    throw new UnsupportedOperationException("Time series results block does not support getting DataSchema yet");
  }

  @Nullable
  @Override
  public List<Object[]> getRows() {
    throw new UnsupportedOperationException("Time series results block does not support getRows yet");
  }

  @Override
  public DataTable getDataTable()
      throws IOException {
    throw new UnsupportedOperationException("Time series results block does not support returning DataTable");
  }

  public TimeSeriesBuilderBlock getTimeSeriesBuilderBlock() {
    return _timeSeriesBuilderBlock;
  }
}
