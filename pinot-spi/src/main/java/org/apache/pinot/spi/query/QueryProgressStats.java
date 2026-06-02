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
package org.apache.pinot.spi.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nullable;


/// Progress for a running query.
///
/// The progress percentage is derived from processed work units over total work units. For single-stage queries, work
/// units map to selected segments. For multi-stage queries, work units include selected segments and stage op-chains,
/// because non-leaf stages can still be running after all leaf segments have been scanned. A negative percentage means
/// the denominator is not known yet. Optional detail rows let multi-stage queries expose component-level progress while
/// keeping single-stage responses compact.
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public final class QueryProgressStats {
  @Nullable
  private final String _label;
  private final long _processedWorkUnits;
  private final long _totalWorkUnits;
  private final long _processedSegments;
  private final long _totalSegmentsToProcess;
  private final boolean _estimated;
  private final double _progressPercent;
  private final List<QueryProgressStats> _details;

  @JsonCreator
  public QueryProgressStats(@JsonProperty("label") @Nullable String label,
      @JsonProperty("processedWorkUnits") @Nullable Long processedWorkUnits,
      @JsonProperty("totalWorkUnits") @Nullable Long totalWorkUnits,
      @JsonProperty("processedSegments") @Nullable Long processedSegments,
      @JsonProperty("totalSegmentsToProcess") @Nullable Long totalSegmentsToProcess,
      @JsonProperty("estimated") @Nullable Boolean estimated,
      @JsonProperty("details") @Nullable List<QueryProgressStats> details) {
    _label = label;
    _processedSegments = processedSegments != null ? processedSegments : 0;
    _totalSegmentsToProcess = totalSegmentsToProcess != null ? totalSegmentsToProcess : -1;
    _processedWorkUnits = processedWorkUnits != null ? processedWorkUnits : _processedSegments;
    _totalWorkUnits = totalWorkUnits != null ? totalWorkUnits : _totalSegmentsToProcess;
    _estimated = estimated != null && estimated;
    _details = details != null ? List.copyOf(details) : List.of();
    if (_totalWorkUnits < 0) {
      _progressPercent = -1.0;
    } else if (_totalWorkUnits == 0) {
      _progressPercent = 100.0;
    } else {
      _progressPercent = Math.min(100.0, _processedWorkUnits * 100.0 / _totalWorkUnits);
    }
  }

  public QueryProgressStats(@Nullable Long processedWorkUnits, @Nullable Long totalWorkUnits,
      @Nullable Long processedSegments, @Nullable Long totalSegmentsToProcess, @Nullable Boolean estimated) {
    this(null, processedWorkUnits, totalWorkUnits, processedSegments, totalSegmentsToProcess, estimated, null);
  }

  public QueryProgressStats(long processedSegments, long totalSegmentsToProcess) {
    this(processedSegments, totalSegmentsToProcess, processedSegments, totalSegmentsToProcess, false);
  }

  public QueryProgressStats(long processedWorkUnits, long totalWorkUnits, long processedSegments,
      long totalSegmentsToProcess, boolean estimated) {
    this(Long.valueOf(processedWorkUnits), Long.valueOf(totalWorkUnits), Long.valueOf(processedSegments),
        Long.valueOf(totalSegmentsToProcess), Boolean.valueOf(estimated));
  }

  public QueryProgressStats(String label, long processedWorkUnits, long totalWorkUnits, long processedSegments,
      long totalSegmentsToProcess, boolean estimated) {
    this(label, Long.valueOf(processedWorkUnits), Long.valueOf(totalWorkUnits), Long.valueOf(processedSegments),
        Long.valueOf(totalSegmentsToProcess), Boolean.valueOf(estimated), null);
  }

  public static QueryProgressStats unknown(String label, boolean estimated) {
    return new QueryProgressStats(label, 0, -1, 0, -1, estimated);
  }

  public static QueryProgressStats aggregate(Collection<QueryProgressStats> progressStats) {
    long processedWorkUnits = 0;
    long totalWorkUnits = 0;
    long processedSegments = 0;
    long totalSegmentsToProcess = 0;
    boolean hasUnknownWorkTotal = false;
    boolean hasUnknownSegmentTotal = false;
    boolean estimated = false;
    for (QueryProgressStats progress : progressStats) {
      processedWorkUnits += progress.getProcessedWorkUnits();
      if (progress.getTotalWorkUnits() >= 0) {
        totalWorkUnits += progress.getTotalWorkUnits();
      } else {
        hasUnknownWorkTotal = true;
      }
      processedSegments += progress.getProcessedSegments();
      if (progress.getTotalSegmentsToProcess() >= 0) {
        totalSegmentsToProcess += progress.getTotalSegmentsToProcess();
      } else {
        hasUnknownSegmentTotal = true;
      }
      estimated |= progress.isEstimated();
    }
    return new QueryProgressStats(processedWorkUnits, hasUnknownWorkTotal ? -1 : totalWorkUnits, processedSegments,
        hasUnknownSegmentTotal ? -1 : totalSegmentsToProcess, estimated);
  }

  public QueryProgressStats withLabel(String label) {
    return new QueryProgressStats(label, _processedWorkUnits, _totalWorkUnits, _processedSegments,
        _totalSegmentsToProcess, _estimated, _details);
  }

  public QueryProgressStats withDetails(List<QueryProgressStats> details) {
    return new QueryProgressStats(_label, _processedWorkUnits, _totalWorkUnits, _processedSegments,
        _totalSegmentsToProcess, _estimated, details);
  }

  @Nullable
  public String getLabel() {
    return _label;
  }

  public long getProcessedWorkUnits() {
    return _processedWorkUnits;
  }

  public long getTotalWorkUnits() {
    return _totalWorkUnits;
  }

  public long getProcessedSegments() {
    return _processedSegments;
  }

  public long getTotalSegmentsToProcess() {
    return _totalSegmentsToProcess;
  }

  public boolean isEstimated() {
    return _estimated;
  }

  public double getProgressPercent() {
    return _progressPercent;
  }

  public List<QueryProgressStats> getDetails() {
    return _details;
  }
}
