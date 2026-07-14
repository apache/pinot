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
package org.apache.pinot.controller.util;

import com.google.common.collect.BiMap;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.util.Timeout;
import org.apache.pinot.common.compression.ColumnCompressionStatsAccumulator;
import org.apache.pinot.common.http.MultiHttpRequest;
import org.apache.pinot.common.http.MultiHttpRequestBufferedResponse;
import org.apache.pinot.common.restlet.resources.ColumnCompressionStatsContribution;
import org.apache.pinot.common.restlet.resources.ColumnCompressionStatsInfo;
import org.apache.pinot.common.restlet.resources.CompressionStatsSummary;
import org.apache.pinot.common.restlet.resources.SegmentCompressionStatsContribution;
import org.apache.pinot.common.restlet.resources.ServerCompressionStatsResponse;
import org.apache.pinot.common.restlet.resources.TableSegments;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.UrlBuilderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Reads per-segment compression statistics from deterministic preferred replicas with bounded batching and fallback.
///
/// One instance may be called concurrently. Each call owns its scheduling state and HTTP client while sharing the
/// injected executor and connection manager. Requests containing segments with another candidate use a proportional
/// slice of the remaining deadline. Final-candidate requests keep the overall deadline, and the scheduler reserves
/// capacity for both classes so neither can starve the other.
final class ServerCompressionStatsReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerCompressionStatsReader.class);
  private static final int MAX_SEGMENTS_PER_REQUEST = 500;
  private static final int MAX_SUMMARY_REQUESTS_IN_FLIGHT = 8;
  private static final int MAX_DETAIL_REQUESTS_IN_FLIGHT = 4;
  private static final int ESTIMATED_COLUMNS_WHEN_UNFILTERED = 1_000;
  private static final int REPLICA_ROTATION_SALT = 0x9e3779b9;

  private final Executor _executor;
  private final HttpClientConnectionManager _connectionManager;

  ServerCompressionStatsReader(Executor executor, HttpClientConnectionManager connectionManager) {
    _executor = executor;
    _connectionManager = connectionManager;
  }

  CompressionStatsResult read(String tableNameWithType, Map<String, List<String>> serverToSegments,
      BiMap<String, String> serverToEndpoints, @Nullable List<String> columns,
      boolean includeColumnCompressionStats, long deadlineNanos) {
    return read(tableNameWithType, serverToSegments, serverToEndpoints, columns, includeColumnCompressionStats,
        deadlineNanos, false);
  }

  CompressionStatsResult readWithSelectedSegments(String tableNameWithType,
      Map<String, List<String>> serverToSegments, BiMap<String, String> serverToEndpoints,
      @Nullable List<String> columns, boolean includeColumnCompressionStats, long deadlineNanos) {
    return read(tableNameWithType, serverToSegments, serverToEndpoints, columns, includeColumnCompressionStats,
        deadlineNanos, true);
  }

  private CompressionStatsResult read(String tableNameWithType, Map<String, List<String>> serverToSegments,
      BiMap<String, String> serverToEndpoints, @Nullable List<String> columns,
      boolean includeColumnCompressionStats, long deadlineNanos, boolean includeSelectedSegments) {
    SegmentStates segmentStates = createSegmentStates(serverToSegments, serverToEndpoints, deadlineNanos);
    Collection<SegmentState> states = segmentStates._states;
    CompressionStatsAggregation aggregation =
        new CompressionStatsAggregation(segmentStates._totalSegments, includeColumnCompressionStats,
            includeSelectedSegments);
    if (states.isEmpty() || System.nanoTime() >= deadlineNanos) {
      return aggregation.result();
    }

    int segmentsPerRequest = segmentsPerRequest(columns, includeColumnCompressionStats);
    int maxRequestsInFlight = includeColumnCompressionStats
        ? MAX_DETAIL_REQUESTS_IN_FLIGHT : MAX_SUMMARY_REQUESTS_IN_FLIGHT;
    Deque<RequestBatch> retryableBatches = new ArrayDeque<>();
    Deque<RequestBatch> finalBatches = new ArrayDeque<>();
    enqueue(states, retryableBatches, finalBatches, segmentsPerRequest);
    Map<Future<MultiHttpRequestBufferedResponse>, ActiveRequest> activeRequests = new HashMap<>();
    boolean preferFinal = true;

    int executionTimeoutMs = toTimeoutMs(deadlineNanos - System.nanoTime());
    try (MultiHttpRequest.BufferedRequestExecution execution =
        new MultiHttpRequest(_executor, _connectionManager).createBufferedExecution(executionTimeoutMs)) {
      while (System.nanoTime() < deadlineNanos
          && (!retryableBatches.isEmpty() || !finalBatches.isEmpty() || !activeRequests.isEmpty())) {
        preferFinal = fillAvailableSlots(tableNameWithType, columns, includeColumnCompressionStats, deadlineNanos,
            retryableBatches, finalBatches, execution, activeRequests, maxRequestsInFlight, preferFinal);
        if (activeRequests.isEmpty()) {
          break;
        }

        long now = System.nanoTime();
        long nextDeadline = Math.min(deadlineNanos, activeRequests.values().stream()
            .mapToLong(request -> request._deadlineNanos).min().orElse(deadlineNanos));
        long waitNanos = Math.max(1, nextDeadline - now);
        Future<MultiHttpRequestBufferedResponse> completed = execution.poll(waitNanos, TimeUnit.NANOSECONDS);
        if (completed != null) {
          ActiveRequest active = activeRequests.remove(completed);
          if (active != null && !processCompleted(completed, active, aggregation, retryableBatches, finalBatches,
              segmentsPerRequest)) {
            break;
          }
        }
        expireRequests(System.nanoTime(), activeRequests, retryableBatches, finalBatches, execution,
            segmentsPerRequest);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    CompressionStatsResult result = aggregation.result();
    if (result._summary != null && result._summary.isPartialCoverage()) {
      LOGGER.warn("Finished reading compression statistics for table: {} with {}/{} covered segments",
          tableNameWithType, result._summary.getSegmentsWithCompleteStats(), result._summary.getTotalSegments());
    } else {
      LOGGER.info("Finished reading compression statistics for table: {}", tableNameWithType);
    }
    return result;
  }

  private boolean fillAvailableSlots(String tableNameWithType, @Nullable List<String> columns,
      boolean includeColumnCompressionStats, long deadlineNanos, Deque<RequestBatch> retryableBatches,
      Deque<RequestBatch> finalBatches, MultiHttpRequest.BufferedRequestExecution execution,
      Map<Future<MultiHttpRequestBufferedResponse>, ActiveRequest> activeRequests, int maxRequestsInFlight,
      boolean preferFinal) {
    int reservedRequestsPerClass = maxRequestsInFlight / 2;
    while (activeRequests.size() < maxRequestsInFlight) {
      int activeFinal = 0;
      int activeRetryable = 0;
      for (ActiveRequest request : activeRequests.values()) {
        if (request._batch.isFinalAttempt()) {
          activeFinal++;
        } else {
          activeRetryable++;
        }
      }
      RequestBatch batch;
      if (!finalBatches.isEmpty() && !retryableBatches.isEmpty()) {
        if (activeFinal >= reservedRequestsPerClass) {
          batch = retryableBatches.pollFirst();
        } else if (activeRetryable >= reservedRequestsPerClass) {
          batch = finalBatches.pollFirst();
        } else if (preferFinal) {
          batch = finalBatches.pollFirst();
          preferFinal = false;
        } else {
          batch = retryableBatches.pollFirst();
          preferFinal = true;
        }
      } else if (!retryableBatches.isEmpty()) {
        batch = retryableBatches.pollFirst();
      } else if (!finalBatches.isEmpty()) {
        batch = finalBatches.pollFirst();
      } else {
        break;
      }
      if (!submit(batch, tableNameWithType, columns, includeColumnCompressionStats, deadlineNanos,
          execution, activeRequests)) {
        break;
      }
    }
    return preferFinal;
  }

  private boolean submit(RequestBatch batch, String tableNameWithType, @Nullable List<String> columns,
      boolean includeColumnCompressionStats, long overallDeadlineNanos,
      MultiHttpRequest.BufferedRequestExecution execution,
      Map<Future<MultiHttpRequestBufferedResponse>, ActiveRequest> activeRequests) {
    long now = System.nanoTime();
    long remainingNanos = overallDeadlineNanos - now;
    if (remainingNanos <= 0) {
      return false;
    }
    long requestBudgetNanos = batch.isFinalAttempt() ? remainingNanos
        : Math.max(1, remainingNanos / batch._attemptsRemaining);
    long requestDeadlineNanos = Math.min(overallDeadlineNanos, now + requestBudgetNanos);
    int timeoutMs = toTimeoutMs(requestBudgetNanos);
    String url = buildUrl(tableNameWithType, columns, includeColumnCompressionStats,
        batch._endpoint);
    HttpPost request = new HttpPost(url);
    request.setHeader("Content-Type", "application/json");
    request.setConfig(RequestConfig.custom()
        .setConnectionRequestTimeout(Timeout.of(timeoutMs, TimeUnit.MILLISECONDS))
        .setResponseTimeout(Timeout.of(timeoutMs, TimeUnit.MILLISECONDS))
        .build());
    try {
      request.setEntity(new StringEntity(JsonUtils.objectToString(new TableSegments(batch.segmentNames()))));
    } catch (IOException e) {
      throw new IllegalStateException("Failed to serialize compression segment batch", e);
    }
    Future<MultiHttpRequestBufferedResponse> future = execution.submit(request);
    activeRequests.put(future, new ActiveRequest(batch, requestDeadlineNanos));
    return true;
  }

  private boolean processCompleted(Future<MultiHttpRequestBufferedResponse> completed, ActiveRequest active,
      CompressionStatsAggregation aggregation, Deque<RequestBatch> retryableBatches,
      Deque<RequestBatch> finalBatches, int segmentsPerRequest) {
    try {
      MultiHttpRequestBufferedResponse response = completed.get();
      if (response.getStatusCode() == 413 && active._batch._states.size() > 1) {
        split(active._batch, retryableBatches, finalBatches);
        return true;
      }
      if (response.getStatusCode() >= 300) {
        LOGGER.error("Server returned error: {}, reason: {} for uri: {}", response.getStatusCode(),
            response.getReasonPhrase(), response.getURI());
        retry(active._batch, retryableBatches, finalBatches, segmentsPerRequest);
        return true;
      }
      if (response.getResponseBody() == null) {
        throw new IOException("Response body is empty");
      }
      ServerCompressionStatsResponse statsResponse =
          JsonUtils.stringToObject(response.getResponseBody(), ServerCompressionStatsResponse.class);
      if (statsResponse.getMetadataVersion() < ServerCompressionStatsResponse.CURRENT_METADATA_VERSION) {
        retry(active._batch, retryableBatches, finalBatches, segmentsPerRequest);
        return true;
      }
      Map<String, SegmentState> statesByName = active._batch.statesByName();
      for (SegmentCompressionStatsContribution contribution : statsResponse.getSegmentCompressionStats()) {
        SegmentState state = statesByName.get(contribution.getSegmentName());
        if (state == null || state._complete) {
          continue;
        }
        aggregation.add(state, contribution);
      }
      retry(active._batch, retryableBatches, finalBatches, segmentsPerRequest);
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (CancellationException | ExecutionException | IOException e) {
      Throwable cause = e instanceof ExecutionException && e.getCause() != null ? e.getCause() : e;
      LOGGER.error("Connection error in 'compression statistics'. Details: {}", cause.getMessage());
      retry(active._batch, retryableBatches, finalBatches, segmentsPerRequest);
      return true;
    }
  }

  private static void expireRequests(long nowNanos,
      Map<Future<MultiHttpRequestBufferedResponse>, ActiveRequest> activeRequests,
      Deque<RequestBatch> retryableBatches, Deque<RequestBatch> finalBatches,
      MultiHttpRequest.BufferedRequestExecution execution, int segmentsPerRequest) {
    List<Map.Entry<Future<MultiHttpRequestBufferedResponse>, ActiveRequest>> expired = new ArrayList<>();
    for (Map.Entry<Future<MultiHttpRequestBufferedResponse>, ActiveRequest> entry : activeRequests.entrySet()) {
      if (entry.getValue()._deadlineNanos <= nowNanos) {
        expired.add(entry);
      }
    }
    for (Map.Entry<Future<MultiHttpRequestBufferedResponse>, ActiveRequest> entry : expired) {
      activeRequests.remove(entry.getKey());
      execution.cancel(entry.getKey());
      retry(entry.getValue()._batch, retryableBatches, finalBatches, segmentsPerRequest);
    }
  }

  private static void retry(RequestBatch batch, Deque<RequestBatch> retryableBatches,
      Deque<RequestBatch> finalBatches, int segmentsPerRequest) {
    List<SegmentState> nextAttempts = new ArrayList<>();
    for (SegmentState state : batch._states) {
      if (!state._complete && ++state._candidateIndex < state.candidateCount()) {
        nextAttempts.add(state);
      }
    }
    enqueue(nextAttempts, retryableBatches, finalBatches, segmentsPerRequest);
  }

  private static void enqueue(Iterable<SegmentState> states, Deque<RequestBatch> retryableBatches,
      Deque<RequestBatch> finalBatches, int segmentsPerRequest) {
    Map<RequestKey, List<SegmentState>> grouped = new TreeMap<>();
    for (SegmentState state : states) {
      if (state._complete || state._candidateIndex >= state.candidateCount()) {
        continue;
      }
      int attemptsRemaining = state.candidateCount() - state._candidateIndex;
      String server = state.currentServer();
      grouped.computeIfAbsent(new RequestKey(server, attemptsRemaining), ignored -> new ArrayList<>()).add(state);
    }
    for (Map.Entry<RequestKey, List<SegmentState>> entry : grouped.entrySet()) {
      List<SegmentState> groupedStates = entry.getValue();
      groupedStates.sort(Comparator.comparing(state -> state._segmentName));
      for (int start = 0; start < groupedStates.size(); start += segmentsPerRequest) {
        int end = Math.min(start + segmentsPerRequest, groupedStates.size());
        RequestBatch batch = new RequestBatch(entry.getKey()._attemptsRemaining,
            new ArrayList<>(groupedStates.subList(start, end)));
        (batch.isFinalAttempt() ? finalBatches : retryableBatches).addLast(batch);
      }
    }
  }

  private static void split(RequestBatch batch, Deque<RequestBatch> retryableBatches,
      Deque<RequestBatch> finalBatches) {
    int midpoint = batch._states.size() / 2;
    Deque<RequestBatch> destination = batch.isFinalAttempt() ? finalBatches : retryableBatches;
    destination.addFirst(new RequestBatch(batch._attemptsRemaining,
        new ArrayList<>(batch._states.subList(midpoint, batch._states.size()))));
    destination.addFirst(new RequestBatch(batch._attemptsRemaining,
        new ArrayList<>(batch._states.subList(0, midpoint))));
  }

  private static int segmentsPerRequest(@Nullable List<String> columns, boolean includeColumnCompressionStats) {
    if (!includeColumnCompressionStats) {
      return MAX_SEGMENTS_PER_REQUEST;
    }
    int estimatedColumns = ESTIMATED_COLUMNS_WHEN_UNFILTERED;
    if (columns != null && !columns.isEmpty() && !columns.contains("*")) {
      estimatedColumns = Math.max(1, new HashSet<>(columns).size());
    }
    return Math.max(1, Math.min(MAX_SEGMENTS_PER_REQUEST,
        ServerCompressionStatsResponse.MAX_COLUMN_CONTRIBUTIONS_PER_RESPONSE / estimatedColumns));
  }

  private static int toTimeoutMs(long timeoutNanos) {
    return (int) Math.min(Integer.MAX_VALUE,
        Math.max(1, TimeUnit.NANOSECONDS.toMillis(timeoutNanos) + 1));
  }

  private static SegmentStates createSegmentStates(Map<String, List<String>> serverToSegments,
      BiMap<String, String> serverToEndpoints, long deadlineNanos) {
    Map<String, SegmentState> states = new HashMap<>();
    Set<String> segmentsWithoutState = null;
    boolean collectCandidates = System.nanoTime() < deadlineNanos;
    int segmentsUntilDeadlineCheck = 1_024;
    for (Map.Entry<String, List<String>> entry : serverToSegments.entrySet()) {
      boolean endpointAvailable = serverToEndpoints.containsKey(entry.getKey());
      for (String segment : entry.getValue()) {
        if (collectCandidates) {
          SegmentState state = states.computeIfAbsent(segment,
              ignored -> new SegmentState(segment, serverToEndpoints));
          if (endpointAvailable) {
            state.addCandidate(entry.getKey());
          }
          if (--segmentsUntilDeadlineCheck == 0) {
            collectCandidates = System.nanoTime() < deadlineNanos;
            segmentsUntilDeadlineCheck = 1_024;
          }
        } else if (!states.containsKey(segment)) {
          if (segmentsWithoutState == null) {
            segmentsWithoutState = new HashSet<>();
          }
          segmentsWithoutState.add(segment);
        }
      }
    }
    if (collectCandidates) {
      for (SegmentState state : states.values()) {
        state.finishCandidates();
      }
    }
    int totalSegments = states.size() + (segmentsWithoutState != null ? segmentsWithoutState.size() : 0);
    return new SegmentStates(states.values(), totalSegments);
  }

  private static String buildUrl(String tableNameWithType, @Nullable List<String> columns,
      boolean includeColumnCompressionStats, String endpoint) {
    String columnsParam = UrlBuilderUtils.generateColumnsParam(columns);
    StringBuilder url = new StringBuilder(String.format("%s/tables/%s/compression-stats", endpoint,
        URLEncoder.encode(tableNameWithType, StandardCharsets.UTF_8)));
    if (columnsParam != null) {
      url.append('?').append(columnsParam);
    }
    if (includeColumnCompressionStats) {
      url.append(columnsParam != null ? '&' : '?').append("includeColumnCompressionStats=true");
    }
    return url.toString();
  }

  /// Owns controller-side replica selection and aggregation for one read. Segment state and its selected columns stay
  /// together so retries cannot update one without the other.
  private static final class CompressionStatsAggregation {
    private final int _totalSegments;
    private final boolean _includeColumnStats;
    private final boolean _includeSelectedSegments;
    private final Map<String, ColumnCompressionStatsAccumulator> _columnAccumulators = new HashMap<>();
    private final Map<String, SelectedSegmentCompressionStats> _selectedSegments = new HashMap<>();
    private long _uncompressedValueSizeInBytes;
    private long _forwardIndexAndDictionaryStorageSizeInBytes;
    private int _segmentsWithCompleteStats;

    private CompressionStatsAggregation(int totalSegments, boolean includeColumnStats,
        boolean includeSelectedSegments) {
      _totalSegments = totalSegments;
      _includeColumnStats = includeColumnStats;
      _includeSelectedSegments = includeSelectedSegments;
    }

    private void add(SegmentState state, SegmentCompressionStatsContribution contribution) {
      boolean selectedCompleteContribution = contribution.isComplete()
          && contribution.getUncompressedValueSizeInBytes() >= 0
          && contribution.getForwardIndexAndDictionaryStorageSizeInBytes() >= 0;
      if (selectedCompleteContribution) {
        _uncompressedValueSizeInBytes += contribution.getUncompressedValueSizeInBytes();
        _forwardIndexAndDictionaryStorageSizeInBytes +=
            contribution.getForwardIndexAndDictionaryStorageSizeInBytes();
        _segmentsWithCompleteStats++;
        if (_includeSelectedSegments) {
          _selectedSegments.put(state._segmentName,
              new SelectedSegmentCompressionStats(state.currentServer(), contribution));
        }
      }

      Map<String, ColumnCompressionStatsContribution> columnStats = contribution.getColumnCompressionStats();
      if (_includeColumnStats && columnStats != null) {
        Set<String> selectedColumns = state._selectedColumns;
        for (Map.Entry<String, ColumnCompressionStatsContribution> entry : columnStats.entrySet()) {
          if (!isUsable(entry.getValue())) {
            continue;
          }
          if (selectedColumns == null && !selectedCompleteContribution) {
            selectedColumns = new HashSet<>();
            state._selectedColumns = selectedColumns;
          }
          if (selectedColumns == null || selectedColumns.add(entry.getKey())) {
            _columnAccumulators.computeIfAbsent(entry.getKey(), ignored -> new ColumnCompressionStatsAccumulator())
                .add(entry.getValue());
          }
        }
      }

      if (selectedCompleteContribution) {
        state._complete = true;
        state._selectedColumns = null;
      }
    }

    private CompressionStatsResult result() {
      double ratio = _forwardIndexAndDictionaryStorageSizeInBytes > 0
          ? (double) _uncompressedValueSizeInBytes / _forwardIndexAndDictionaryStorageSizeInBytes : 0;
      CompressionStatsSummary summary = new CompressionStatsSummary(_uncompressedValueSizeInBytes,
          _forwardIndexAndDictionaryStorageSizeInBytes, ratio, _segmentsWithCompleteStats, _totalSegments,
          _segmentsWithCompleteStats < _totalSegments);
      if (!_includeColumnStats || _columnAccumulators.isEmpty()) {
        return new CompressionStatsResult(summary, null, _selectedSegments);
      }
      List<ColumnCompressionStatsInfo> columnStats = new ArrayList<>(_columnAccumulators.size());
      for (Map.Entry<String, ColumnCompressionStatsAccumulator> entry : _columnAccumulators.entrySet()) {
        columnStats.add(entry.getValue().toColumnCompressionStatsInfo(entry.getKey()));
      }
      columnStats.sort(Comparator.comparing(ColumnCompressionStatsInfo::getColumn));
      return new CompressionStatsResult(summary, columnStats, _selectedSegments);
    }

    private static boolean isUsable(ColumnCompressionStatsContribution candidate) {
      return candidate.getUncompressedValueSizeInBytes() >= 0
          && candidate.getForwardIndexAndDictionaryStorageSizeInBytes() >= 0
          && candidate.getNumSegments() > 0 && !candidate.getEncodingBreakdown().isEmpty();
    }
  }

  /// Immutable aggregate plus optional per-segment selections from one compression read.
  static final class CompressionStatsResult {
    @Nullable
    private final CompressionStatsSummary _summary;
    @Nullable
    private final List<ColumnCompressionStatsInfo> _columnStats;
    private final Map<String, SelectedSegmentCompressionStats> _selectedSegments;

    private CompressionStatsResult(@Nullable CompressionStatsSummary summary,
        @Nullable List<ColumnCompressionStatsInfo> columnStats,
        Map<String, SelectedSegmentCompressionStats> selectedSegments) {
      _summary = summary;
      _columnStats = columnStats;
      _selectedSegments = Map.copyOf(selectedSegments);
    }

    @Nullable
    CompressionStatsSummary getSummary() {
      return _summary;
    }

    @Nullable
    List<ColumnCompressionStatsInfo> getColumnStats() {
      return _columnStats;
    }

    Map<String, SelectedSegmentCompressionStats> getSelectedSegments() {
      return _selectedSegments;
    }
  }

  /// Complete statistics selected from one source replica for a logical segment.
  static final class SelectedSegmentCompressionStats {
    private final String _server;
    private final SegmentCompressionStatsContribution _contribution;

    private SelectedSegmentCompressionStats(String server,
        SegmentCompressionStatsContribution contribution) {
      _server = server;
      _contribution = contribution;
    }

    String getServer() {
      return _server;
    }

    SegmentCompressionStatsContribution getContribution() {
      return _contribution;
    }
  }

  private static final class SegmentState {
    private final String _segmentName;
    private final List<String> _candidates = new ArrayList<>();
    private final BiMap<String, String> _serverToEndpoints;
    private int _candidateOffset;
    private int _candidateIndex;
    private boolean _complete;
    @Nullable
    private Set<String> _selectedColumns;

    private SegmentState(String segmentName, BiMap<String, String> serverToEndpoints) {
      _segmentName = segmentName;
      _serverToEndpoints = serverToEndpoints;
    }

    private void addCandidate(String candidate) {
      if (_candidates.isEmpty() || !_candidates.get(_candidates.size() - 1).equals(candidate)) {
        _candidates.add(candidate);
      }
    }

    private void finishCandidates() {
      _candidates.sort(String::compareTo);
      if (_candidates.size() > 1) {
        _candidateOffset = Math.floorMod(_segmentName.hashCode() ^ REPLICA_ROTATION_SALT, _candidates.size());
      }
    }

    private int candidateCount() {
      return _candidates.size();
    }

    private String currentServer() {
      return _candidates.get((_candidateOffset + _candidateIndex) % _candidates.size());
    }

    private String endpoint() {
      return _serverToEndpoints.get(currentServer());
    }
  }

  private static final class SegmentStates {
    private final Collection<SegmentState> _states;
    private final int _totalSegments;

    private SegmentStates(Collection<SegmentState> states, int totalSegments) {
      _states = states;
      _totalSegments = totalSegments;
    }
  }

  private static final class RequestKey implements Comparable<RequestKey> {
    private final String _server;
    private final int _attemptsRemaining;

    private RequestKey(String server, int attemptsRemaining) {
      _server = server;
      _attemptsRemaining = attemptsRemaining;
    }

    @Override
    public int compareTo(RequestKey other) {
      int serverComparison = _server.compareTo(other._server);
      return serverComparison != 0 ? serverComparison : Integer.compare(_attemptsRemaining, other._attemptsRemaining);
    }
  }

  private static final class RequestBatch {
    private final String _endpoint;
    private final int _attemptsRemaining;
    private final List<SegmentState> _states;

    private RequestBatch(int attemptsRemaining, List<SegmentState> states) {
      _endpoint = states.get(0).endpoint();
      _attemptsRemaining = attemptsRemaining;
      _states = states;
    }

    private boolean isFinalAttempt() {
      return _attemptsRemaining == 1;
    }

    private List<String> segmentNames() {
      List<String> names = new ArrayList<>(_states.size());
      for (SegmentState state : _states) {
        names.add(state._segmentName);
      }
      return names;
    }

    private Map<String, SegmentState> statesByName() {
      Map<String, SegmentState> states = new HashMap<>();
      for (SegmentState state : _states) {
        states.put(state._segmentName, state);
      }
      return states;
    }
  }

  private static final class ActiveRequest {
    private final RequestBatch _batch;
    private final long _deadlineNanos;

    private ActiveRequest(RequestBatch batch, long deadlineNanos) {
      _batch = batch;
      _deadlineNanos = deadlineNanos;
    }
  }
}
