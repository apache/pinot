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
package com.linkedin.pinot.transport.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.pinot.common.response.ServerInstance;


/**
 * A conjunctive type Composite future.
 *
 * Future completes only when
 *  (a) All underlying futures completes successfully or (GatherModeOnError config : For both AND and SHORTCIRCUIT_AND
 *  (b) When any underlying future fails or (GatherModeOnError config : SHORTCIRCUIT_AND)
 *  (c) When all underlying futures completes successfully or with error ( GatherModeOnError Config : AND)
 *
 * The error gather mode determines if (b) or (c) has to be employed in case of errors in underlying futures.
 * This future is static in the sense that all the underlying futures have to added before we call any operations
 * on them.
 *
 * This future's value will be a map of each future's key and the corresponding underlying future's value.
 *
 * @param <V> Value type of the underlying future
 */
public class CompositeFuture<V> extends AbstractCompositeListenableFuture<V> {
  protected static Logger LOGGER = LoggerFactory.getLogger(CompositeFuture.class);

  public static enum GatherModeOnError {
    /* Future completes only when all underlying futures complete or any one underlying future fails */
    SHORTCIRCUIT_AND,
    /* Future completes only when all underlying futures completes successfully or fails. */
    AND,
  };

  private final Collection<ServerResponseFuture<V>> _futures;

  // Composite Response
  private final ConcurrentMap<ServerInstance, V> _delayedResponseMap;

  private final ConcurrentMap<ServerInstance, Long> _responseTimeMap = new ConcurrentHashMap<>(10);

  // Exception in case of error
  private final ConcurrentMap<ServerInstance, Throwable> _errorMap;

  private final GatherModeOnError _gatherMode;

  // Descriptive name of the future
  private final String _name;

  public CompositeFuture(String name, GatherModeOnError mode) {
    _name = name;
    _futures = new ArrayList<ServerResponseFuture<V>>();
    _delayedResponseMap = new ConcurrentHashMap<ServerInstance, V>();
    _errorMap = new ConcurrentHashMap<ServerInstance, Throwable>();
    _gatherMode = mode;
  }

  /**
   * Start the future. This will add listener to the underlying futures. This method needs to be called
   * as soon the composite future is constructed and before any other method is invoked.
   */
  public void start(Collection<ServerResponseFuture<V>> futureList) {
    boolean started = super.start();

    if (!started) {
      String msg = "Unable to start the future. State is already : " + _state;
      LOGGER.error(msg);
      throw new IllegalStateException(msg);
    }

    if ( null !=  futureList )
    {
      _futures.addAll(futureList);
      _latch = new CountDownLatch(futureList.size());
    } else {
      _latch = new CountDownLatch(0);
    }
    for (ServerResponseFuture<V> entry : _futures) {
      if (null != entry) {
        addResponseFutureListener(entry);
      }
    }
  }

  /**
   * Call cancel on underlying futures. Dont worry if they are completed.
   * If they are already completed, cancel will be discarded. THis is best-effort only !!.
   */
  @Override
  protected void cancelUnderlyingFutures() {
    LOGGER.info("Cancelling all underlying futures for {}", getName());
    for (ServerResponseFuture<V> entry : _futures) {
      LOGGER.info("Cancelling future {}", entry.getName());
      entry.cancel(true);
    }
  }

  @Override
  /**
   *
   */
  public Map<ServerInstance, V> get() throws InterruptedException, ExecutionException {
    _latch.await();
    return _delayedResponseMap;
  }

  @Override
  public V getOne() throws InterruptedException, ExecutionException {
    _latch.await();
    if (_delayedResponseMap.isEmpty()) {
      return null;
    }
    return _delayedResponseMap.values().iterator().next();
  }

  @Override
  public V getOne(long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    boolean notElapsed = _latch.await(timeout,unit);

    if (!notElapsed)
      throw new TimeoutException("Timedout waiting for async result for composite ");

    if (_delayedResponseMap.isEmpty()) {
      return null;
    }
    return _delayedResponseMap.values().iterator().next();
  }

  @Override
  public Map<ServerInstance, Throwable> getError() {
    return _errorMap;
  }

  @Override
  public long getDurationMillis() {
    // The duration for a composite future (if it is called), may be defined as the max of all the durations of
    // the individual futures that have completed. Typically, we are interested in getting the individual completion
    // times, however.
    long maxDuration = -1;
    for (Map.Entry<ServerInstance, Long> entry:_responseTimeMap.entrySet()) {
      if (entry.getValue() > maxDuration) {
        maxDuration = entry.getValue();
      }
    }
    return maxDuration;
  }

  @Override
  public Map<ServerInstance, V> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    _latch.await(timeout, unit);
    return _delayedResponseMap;
  }

  /**
   * This method must be called after the 'get' is called, so that all response times are recorded.
   * For now, this method has not been added to the interface.
   * @return A map of response times of each response in _delayedResponseMap.
   */
  public Map<ServerInstance, Long> getResponseTimes() {
    return Collections.unmodifiableMap(_responseTimeMap);
  }

  @Override
  public ServerInstance getServerInstance() {
    throw new RuntimeException("Invalid API call on a composite future");
  }

  @Override
  protected boolean processFutureResult(ServerInstance server, Map<ServerInstance, V> response, Map<ServerInstance, Throwable> error, long durationMillis) {
    // Get the response time and create another map that can be invoked to get the end time when responses were received for each server.
    boolean ret = false;
    if (null != response) {
      LOGGER.debug("Response from {} is {}", server, response);
      _delayedResponseMap.putAll(response);
    } else if (null != error) {
      LOGGER.debug("Error from {} is : {}", server, error);
      _errorMap.putAll(error);

      if (_gatherMode == GatherModeOnError.SHORTCIRCUIT_AND) {
        ret = true; // We are done as we got an error
      }
    }
    // TODO May be limit the number of entries here to 10? We don't want to create too much garbage on the broker.
    _responseTimeMap.put(server, durationMillis);
    return ret;
  }

  @Override
  public String getName() {
    return _name;
  }

  public int getNumFutures() {
    return _futures.size();
  }
}
