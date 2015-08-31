/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.trace;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import com.linkedin.pinot.common.request.InstanceRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The main entry point for servers to record the trace information.
 *
 * To enable tracing, the request handler thread should register the requestId by calling {@link #register(InstanceRequest)}.
 *
 * For any other {@link Runnable} jobs the request handler creates and will be executed in other threads,
 * They need to be changed to {@link TraceRunnable} interface which handles register/unregister automatically.
 *
 * At the end of tracing a request, the request handler thread should unregister by calling
 * {@link #unregister(InstanceRequest)} to avoid any resource leaks.
 *
 * For place where trace info needs to be recorded, just call {@link TraceContext#log(String, Object)}.
 *
 */
public class TraceContext {

  /**
   * Only for tests
   */
  protected static class Info {
    public CONSTANT message;
    public Long requestId;

    public Info(CONSTANT message, Long requestId) {
      this.message = message;
      this.requestId = requestId;
    }

    @Override
    public String toString() {
      return "[" + requestId + "] " + message;
    }
  }

  /**
   * Enum order matters!
   */
  protected enum CONSTANT {
    // control constants
    REGISTER_REQUEST,
    REGISTER_THREAD_TO_REQUEST,
    UNREGISTER_THREAD_FROM_REQUEST,
    UNREGISTER_REQUEST,

    // info constants
    START_NEW_TRACE,

    // failure constants
    UNREGISTER_REQUEST_FAILURE,
    REGISTER_THREAD_FAILURE,

    // trace log failure constants
    REQUEST_FOR_THREAD_NOT_FOUND;
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(TraceContext.class);

  public static boolean TEST_ENABLED = false; // whether to log test-related info for this class

  private static final ThreadLocal<Trace> _localTrace = new ThreadLocal<Trace>() {
    @Override
    protected Trace initialValue() {
      return null;
    }
  };
  private static final ThreadLocal<Boolean> _isActive = new ThreadLocal<Boolean>() {
    @Override
    protected Boolean initialValue() {
      return false;
    }
  };
  private static final ThreadLocal<InstanceRequest> _request = new ThreadLocal<InstanceRequest>() {
    @Override
    protected InstanceRequest initialValue() {
      return null;
    }
  };

  /**
   * Requests may arrive simultaneously, so we need a concurrent collection to manage these requests.
   *
   * Also, each requests may issue a number of concurrent {@link Runnable} jobs,
   * so a thread-safe queue is needed for collecting trace info from threads.
   */
  private static ConcurrentHashMap<Long, ConcurrentLinkedDeque<Trace>> allTraceInfoMap;

  /**
   * Simple way to log all info into a same concurrent list, only used when {@link #TEST_ENABLED}
   * Map a thread to all info it logged in a linked deque.
   */
  private static ConcurrentHashMap<Long, ConcurrentLinkedDeque<Info>> tidToTestInfoMap;

  static {
    reset();
  }

  /**
   * This method is served for debugging purpose, no need to explicitly call it.
   */
  protected static void reset() {
    allTraceInfoMap = new ConcurrentHashMap<Long, ConcurrentLinkedDeque<Trace>>();
    if (TEST_ENABLED) {
      tidToTestInfoMap = new ConcurrentHashMap<Long, ConcurrentLinkedDeque<Info>>();
    }
  }

  /**
   * call this method to log info for this class, requestId -1 means requestId is invalid.
   *
   * @param info
   * @param request
   */
  private static void logInfo(CONSTANT info, InstanceRequest request) {
    long requestId = (request == null) ? -1 : request.getRequestId();
    Long tid = Thread.currentThread().getId();
    LOGGER.debug("[TID: {}] {} in Request: {}", tid, info, requestId);
    if (TEST_ENABLED) {
      tidToTestInfoMap.putIfAbsent(tid, new ConcurrentLinkedDeque<Info>());
      tidToTestInfoMap.get(tid).offerLast(new Info(info, requestId));
    }
  }

  /**
   * Register current thread to a given requestId.
   * It must be called as the first statement in any job if trace needed.
   *
   * @param request
   * @param parent
   */
  protected static void registerThreadToRequest(InstanceRequest request, Trace parent) {
    ConcurrentLinkedDeque<Trace> traceInfo = allTraceInfoMap.get(request.getRequestId());

    if (traceInfo != null && _request.get() == null) {
      logInfo(CONSTANT.REGISTER_THREAD_TO_REQUEST, request);
      long threadId = Thread.currentThread().getId();
      Trace trace = new Trace(threadId, parent);
      _localTrace.set(trace);
      _request.set(request);
      traceInfo.offerLast(trace);
    } else {
      logInfo(CONSTANT.REGISTER_THREAD_FAILURE, request);
    }
  }

  /**
   * Threads are managed by {@link ExecutorService}, so that a thread may be reused and assigned to different requests.
   * It is always necessary to call this method at the end of tracing so any ThreadLocal fields are clean.
   *
   */
  protected static void unregisterThreadFromRequest() {
    logInfo(CONSTANT.UNREGISTER_THREAD_FROM_REQUEST, _request.get());
    _request.remove();
    _isActive.remove();
    _localTrace.remove();
  }

  protected static ConcurrentHashMap<Long, ConcurrentLinkedDeque<Info>> getTestInfoMap() {
    return tidToTestInfoMap;
  }

  protected static ConcurrentHashMap<Long, ConcurrentLinkedDeque<Trace>> getAllTraceInfoMap() {
    return allTraceInfoMap;
  }

  protected static InstanceRequest getRequestForCurrentThread() {
    return _request.get();
  }

  protected static Trace getLocalTraceForCurrentThread() {
    return _localTrace.get();
  }

  // ------ Public APIs ------

  /**
   * We do not want to mix one request with others, so each request should start the trace by calling this method.
   *
   * Test whether the requestId exists in the ConcurrentHashMap is meaningless since the return may not up-to-date.
   * As a result, just make sure NOT to call this method for same requestId in the same request handler thread twice,
   * we also register the calling thread to the request.
   *
   */
  public static void register(InstanceRequest request) {
    logInfo(CONSTANT.REGISTER_REQUEST, request);
    allTraceInfoMap.put(request.getRequestId(), new ConcurrentLinkedDeque<Trace>());
    registerThreadToRequest(request, null);
  }

  /**
   * Call this method at the end of request processing to avoid resource leak!
   * Remember to save trace info of current request before calling,
   * we also unregister the calling thread from the request.
   *
   * @param request
   */
  public static void unregister(InstanceRequest request) {
    unregisterThreadFromRequest();
    logInfo(CONSTANT.UNREGISTER_REQUEST, request);
    if (!TEST_ENABLED) {
      ConcurrentLinkedDeque<Trace> traces = allTraceInfoMap.remove(request.getRequestId());
      if (traces == null) {
        logInfo(CONSTANT.UNREGISTER_REQUEST_FAILURE, request);
        return;
      }
      traces.clear(); // release all references, this may be optional
    }
  }

  /**
   * Thread-safe log operation, all fields referenced in this method should be ThreadLocal.
   *
   * @param key
   * @param value
   */
  public static void log(String key, Object value){
    if (_request.get() == null) {
      logInfo(CONSTANT.REQUEST_FOR_THREAD_NOT_FOUND, null);
      return;
    }

    if (!_request.get().isEnableTrace()) {
      return;
    }

    if (!_isActive.get()) {
      long threadId = Thread.currentThread().getId();
      logInfo(CONSTANT.START_NEW_TRACE, _request.get());
      _isActive.set(true);
    }

    _localTrace.get().log(key, value);
  }

  /**
   * This method does not guarantee it returns the complete trace info.
   * It is caller's responsibility to make sure all threads belong to requestId
   * finish their work before this method is called.
   *
   * Eg. the caller could use a {@link CountDownLatch} to wait for all jobs finished.
   *
   * TODO: modify this method to return serialized/compact results from ConcurrentLinkedDeque only.
   *
   * @param requestId
   * @return
   */
  public static String getTraceInfoOfRequestId(Long requestId) {
    ConcurrentLinkedDeque<Trace> deque = allTraceInfoMap.get(requestId);
    return (deque == null) ? "trace for " + requestId + " not found" : TraceUtils.getTraceString(deque);
  }

}