/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.pinot.common.response.ServerInstance;


/**
 * A selecting future which completes when
 *  (a) Any underlying future completes successfully with a response or
 *  (b) all underlying futures fail.
 *
 * This construct can be used in case where you want to run speculative execution. When
 * one of those completes successfully, we dont have to wait for other options.
 *
 * This future holds the first successfully completed future that notified (if any present) or
 * the last error (if all underlying futures failed)
 * @param <T> Response object.
 */
public class SelectingFuture<T> extends AbstractCompositeListenableFuture<T> {
  protected static Logger LOGGER = LoggerFactory.getLogger(SelectingFuture.class);

  private final List<ServerResponseFuture<T>> _futuresList;

  // First successful Response
  private volatile Map<ServerInstance, T> _delayedResponse;

  // Last Exception in case of error
  private volatile Map<ServerInstance, Throwable> _error;

  private final String _name;

  private long _durationMillis = -1;

  public SelectingFuture(String name) {
    _name = name;
    _futuresList = new ArrayList<ServerResponseFuture<T>>();
    _delayedResponse = null;
    _error = null;
  }

  /**
   * Start the future. This will add listener to the underlying futures. This method needs to be called
   * as soon the composite future is constructed and before any other method is invoked.
   */
  public void start(Collection<ServerResponseFuture<T>> futuresList) {
    boolean started = super.start();

    if (!started) {
      String msg = "Unable to start the future. State is already : " + _state;
      LOGGER.error(msg);
      throw new IllegalStateException(msg);
    }

    _futuresList.addAll(futuresList);
    _latch = new CountDownLatch(futuresList.size());
    for (ServerResponseFuture<T> entry : _futuresList) {
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
    for (ServerResponseFuture<T> entry : _futuresList) {
      entry.cancel(true);
    }
  }

  @Override
  public Map<ServerInstance, T> get() throws InterruptedException, ExecutionException {
    _latch.await();
    return _delayedResponse;
  }

  @Override
  public Map<ServerInstance, Throwable> getError() {
    return _error;
  }

  @Override
  public Map<ServerInstance, T> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    _latch.await(timeout, unit);
    return _delayedResponse;
  }

  @Override
  public T getOne() throws InterruptedException, ExecutionException {
    _latch.await();
    if ((null == _delayedResponse) || (_delayedResponse.isEmpty())) {
      return null;
    }
    return _delayedResponse.values().iterator().next();
  }

  @Override
  public T getOne(long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    boolean notElapsed = _latch.await(timeout,unit);

    if (!notElapsed)
      throw new TimeoutException("Timedout waiting for async result for selecting future " + _name);

    if ((null == _delayedResponse) || (_delayedResponse.isEmpty())) {
      return null;
    }
    return _delayedResponse.values().iterator().next();
  }

  @Override
  public long getDurationMillis() {
    return _durationMillis;
  }

  @Override
  protected boolean processFutureResult(ServerInstance server, Map<ServerInstance, T> response, Map<ServerInstance, Throwable> error, long durationMillis) {
    // Add an argument here to get the time of completion of the future.
    boolean done = false;
    if ((null != response)) {
      LOGGER.debug("Error got from {} is : {}", server, response);

      _delayedResponse = response;
      _error = null;
      done = true;
    } else if (null != error) {
      LOGGER.debug("Error got from {} is : {}", server, error);
      _error = error;
    }
    _durationMillis = durationMillis;
    return done;
  }

  @Override
  public ServerInstance getServerInstance() {
    throw new RuntimeException("Invalid API call on selecting future");
  }

  @Override
  public String getName() {
    return _name;
  }
}
