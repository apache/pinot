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
package org.apache.pinot.core.util.trace;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * The main entry point for servers to record the trace information.
 * <p>
 * To enable tracing, the request handler thread should register the requestId by calling
 * {@link #register(long requestId)}.
 * <p>
 * To trace the {@link Runnable} or {@link java.util.concurrent.Callable} jobs the request handler creates and will be
 * executed in other threads, use {@link TraceRunnable} or {@link TraceCallable} instead.
 * <p>
 * At the end of tracing a request, the request handler thread should call {@link #unregister()} to un-register the
 * request from tracing to prevent resource leak.
 */
public final class TraceContext {
  private TraceContext() {
  }

  /**
   * Trace represents the logs for a single thread.
   */
  static class Trace {
    static class LogEntry {
      final String _key;
      final Object _value;

      LogEntry(String key, Object value) {
        _key = key;
        _value = value;
      }

      JsonNode toJson() {
        return JsonUtils.newObjectNode().set(_key, JsonUtils.objectToJsonNode(_value));
      }
    }

    final String _traceId;
    final List<LogEntry> _logs = new ArrayList<>();
    final AtomicInteger _numChildren = new AtomicInteger(0);

    Trace(@Nullable Trace parent) {
      if (parent == null) {
        _traceId = "0";
      } else {
        _traceId = parent.getChildTraceId();
      }
    }

    void log(String key, Object value) {
      _logs.add(new LogEntry(key, value));
    }

    String getChildTraceId() {
      return _traceId + "_" + _numChildren.getAndIncrement();
    }

    JsonNode toJson() {
      ArrayNode jsonLogs = JsonUtils.newArrayNode();
      for (LogEntry log : _logs) {
        jsonLogs.add(log.toJson());
      }
      return JsonUtils.newObjectNode().set(_traceId, jsonLogs);
    }
  }

  /**
   * TraceEntry is a wrapper on the trace and the request Id it belongs to.
   */
  static class TraceEntry {
    final long _requestId;
    final Trace _trace;

    TraceEntry(long requestId, Trace trace) {
      _requestId = requestId;
      _trace = trace;
    }
  }

  private static final ThreadLocal<TraceEntry> TRACE_ENTRY_THREAD_LOCAL = new ThreadLocal<TraceEntry>() {
    @Override
    protected TraceEntry initialValue() {
      return null;
    }
  };

  /**
   * Map from request Id to traces associated with the request.
   * <p>Requests may arrive simultaneously, so we need a concurrent map to manage these requests.
   * <p>Each request may use multiple threads, so the queue should be thread-safe as well.
   */
  @VisibleForTesting
  static final Map<Long, Queue<Trace>> REQUEST_TO_TRACES_MAP = new ConcurrentHashMap<>();

  /**
   * Register a request to the trace.
   * <p>Should be called before logging any trace information.
   */
  public static void register(long requestId) {
    REQUEST_TO_TRACES_MAP.put(requestId, new ConcurrentLinkedQueue<Trace>());
    registerThreadToRequest(new TraceEntry(requestId, null));
  }

  /**
   * Register a thread to the request.
   */
  static void registerThreadToRequest(TraceEntry parentTraceEntry) {
    Trace trace = new Trace(parentTraceEntry._trace);
    TRACE_ENTRY_THREAD_LOCAL.set(new TraceEntry(parentTraceEntry._requestId, trace));
    Queue<Trace> traces = REQUEST_TO_TRACES_MAP.get(parentTraceEntry._requestId);
    if (traces != null) {
      traces.add(trace);
    }
  }

  /**
   * Un-register a request from the trace.
   * <p>Should be called after all trace information being saved.
   */
  public static void unregister() {
    TraceEntry traceEntry = TRACE_ENTRY_THREAD_LOCAL.get();
    REQUEST_TO_TRACES_MAP.remove(traceEntry._requestId);
    unregisterThreadFromRequest();
  }

  /**
   * Un-register a thread from the request.
   */
  static void unregisterThreadFromRequest() {
    TRACE_ENTRY_THREAD_LOCAL.remove();
  }

  /**
   * Return whether the trace is enabled.
   */
  public static boolean traceEnabled() {
    return TRACE_ENTRY_THREAD_LOCAL.get() != null;
  }

  /**
   * Log the time spent in a specific operator.
   * <p>Should be called after calling {@link #traceEnabled()} and ensure trace is enabled.
   */
  public static void logTime(String operatorName, long timeMs) {
    TRACE_ENTRY_THREAD_LOCAL.get()._trace.log(operatorName + " Time", timeMs);
  }

  /**
   * Log a key-value pair trace information.
   * <p>Should be called after calling {@link #traceEnabled()} and ensure trace is enabled.
   */
  public static void logInfo(String key, Object value) {
    TRACE_ENTRY_THREAD_LOCAL.get()._trace.log(key, value);
  }

  /**
   * Get the trace information added so far.
   */
  public static String getTraceInfo() {
    ArrayNode jsonTraces = JsonUtils.newArrayNode();
    Queue<Trace> traces = REQUEST_TO_TRACES_MAP.get(TRACE_ENTRY_THREAD_LOCAL.get()._requestId);
    if (traces != null && !traces.isEmpty()) {
      for (Trace trace : traces) {
        jsonTraces.add(trace.toJson());
      }
    }
    return jsonTraces.toString();
  }

  /**
   * Get the {@link TraceEntry} for the current thread.
   */
  @Nullable
  static TraceEntry getTraceEntry() {
    return TRACE_ENTRY_THREAD_LOCAL.get();
  }
}
