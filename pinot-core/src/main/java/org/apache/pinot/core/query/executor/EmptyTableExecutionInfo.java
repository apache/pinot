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
package org.apache.pinot.core.query.executor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nullable;
import org.apache.pinot.core.query.pruner.SegmentPrunerService;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.TimerContext;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.spi.data.Schema;


/// The [TableExecutionInfo] of a query that has nothing to query on the server: the table is not set up on the server
/// yet, and the query has no segment the server must have.
///
/// A server sets up a table when it adds the first segment of it, so a server without the table has not loaded any of
/// the optional segments it is asked for. The query runs the way it does over a table whose segments are all skipped.
///
/// Stateless, and thus thread safe.
public final class EmptyTableExecutionInfo implements TableExecutionInfo {
  public static final EmptyTableExecutionInfo INSTANCE = new EmptyTableExecutionInfo();

  private EmptyTableExecutionInfo() {
  }

  @Nullable
  @Override
  public Schema getSchema() {
    return null;
  }

  @Override
  public boolean hasRealtime() {
    return false;
  }

  @Override
  public List<IndexSegment> getIndexSegments() {
    return List.of();
  }

  @Nullable
  @Override
  public Map<IndexSegment, SegmentContext> getProvidedSegmentContexts() {
    return null;
  }

  @Override
  public List<String> getSegmentsToQuery() {
    return List.of();
  }

  @Override
  public List<String> getOptionalSegments() {
    return List.of();
  }

  @Override
  public List<SegmentDataManager> getSegmentDataManagers() {
    return List.of();
  }

  @Override
  public void releaseSegmentDataManagers() {
  }

  @Override
  public List<SegmentContext> getSegmentContexts(List<IndexSegment> selectedSegments,
      Map<String, String> queryOptions) {
    return List.of();
  }

  @Override
  public List<String> getNotAcquiredSegments() {
    return List.of();
  }

  @Override
  public List<String> getMissingSegments() {
    return List.of();
  }

  @Override
  public int getNumSegmentsAcquired() {
    return 0;
  }

  @Override
  public SelectedSegmentsInfo getSelectedSegmentsInfo(QueryContext queryContext, TimerContext timerContext,
      ExecutorService executorService, SegmentPrunerService segmentPrunerService) {
    return new SelectedSegmentsInfo();
  }

  @Override
  public ConsumingSegmentsInfo getConsumingSegmentsInfo() {
    return new ConsumingSegmentsInfo(0, 0, 0, 0);
  }
}
