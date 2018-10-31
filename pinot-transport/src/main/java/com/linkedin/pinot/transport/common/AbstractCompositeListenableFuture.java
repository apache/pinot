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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.pinot.common.response.ServerInstance;


public abstract class AbstractCompositeListenableFuture<T> implements ServerResponseFuture<T> {
  protected static Logger LOGGER = LoggerFactory.getLogger(CompositeFuture.class);

  /**
   * Response Future State
   */
  private enum State {
    PENDING,
    STARTED,
    CANCELLED,
    DONE;

    public boolean isCompleted() {
      return (this != PENDING) && (this != STARTED);
    }
  }

  // Lock for mutex
  private final Lock _futureLock = new ReentrantLock();

  // Latch to wait for the response
  protected volatile CountDownLatch _latch;

  // State of the future
  protected State _state;

  // List of runnables that needs to be executed on completion
  private final List<Runnable> _pendingRunnable = new ArrayList<Runnable>();
  //List of executors that needs to run the runnables.
  private final List<Executor> _pendingRunnableExecutors = new ArrayList<Executor>();

  public AbstractCompositeListenableFuture() {
    _state = State.PENDING;
  }

  protected synchronized boolean start() {
    if (_state != State.PENDING) {
      return false;
    }

    _state = State.STARTED;
    return true;
  }

  @Override
  /**
   * Does best-effort cancellation of futures. If one of the underlying futures are cancelled, the others
   * are still cancelled. THere is no undo here. The composite future will be marked cancelled. If there are
   * listeners to the non-cancelled underlying future, it can still see the future getting complete.
   * If using Composite futures, only listen to the composite instance and not to the underlying futures directly.
   *
   */
  public boolean cancel(boolean mayInterruptIfRunning) {
    try {
      _futureLock.lock();
      if (_state.isCompleted()) {
        LOGGER.info("Request is no longer pending. Cannot cancel !!");
        return false;
      }
      setDone(State.CANCELLED);
    } finally {
      _futureLock.unlock();
    }

    cancelUnderlyingFutures();
    return true;
  }

  /**
   * Call cancel on underlying futures. Dont worry if they are completed.
   * If they are already completed, cancel will be discarded. THis is best-effort only !!.
   */
  protected abstract void cancelUnderlyingFutures();

  @Override
  public boolean isCancelled() {
    return _state == State.CANCELLED;
  }

  /**
   * Mark complete and notify threads waiting for this condition
   */
  private void setDone(State state) {
    LOGGER.debug("Setting state to :" + state + ", Current State :" + _state);
    try {
      _futureLock.lock();
      _state = state;

      // Drain the latch
      long count = _latch.getCount();
      for (long i = 0; i < count; i++) {
        _latch.countDown();
      }
    } finally {
      _futureLock.unlock();
    }

    for (int i = 0; i < _pendingRunnable.size(); i++) {
      LOGGER.info("Running pending runnable :" + i);
      Executor e = _pendingRunnableExecutors.get(i);
      if (null != e) {
        e.execute(_pendingRunnable.get(i));
      } else {
        _pendingRunnable.get(i).run(); // run in the current thread.
      }
    }
    _pendingRunnable.clear();
    _pendingRunnableExecutors.clear();
  }

  @Override
  public boolean isDone() {
    return _state.isCompleted();
  }

  @Override
  public void addListener(Runnable listener, Executor executor) {
    boolean processed = false;
    try {
      _futureLock.lock();
      if (!_state.isCompleted()) {
        _pendingRunnable.add(listener);
        _pendingRunnableExecutors.add(executor);
        processed = true;
      }
    } finally {
      _futureLock.unlock();
    }

    if (!processed) {
      LOGGER.info("Executing the listener as the future event is already done !!");
      if (null != executor) {
        executor.execute(listener);
      } else {
        listener.run(); // run in the same thread
      }
    }
  }

  /**
   * Process underlying futures. Returns true if all the processing is done
   * The framework will go ahead and cancel any outstanding futures.
   *
   * @param server ServerInstance that is responding
   * @param responses Response
   * @param error Error
   * @param durationMillis
   * @return true if processing done
   */
  protected abstract boolean processFutureResult(ServerInstance server, Map<ServerInstance, T> responses, Map<ServerInstance, Throwable> error,
      long durationMillis);

  protected void addResponseFutureListener(ServerResponseFuture<T> future) {
    future.addListener(new ResponseFutureListener(future), null); // no need for separate Executors
  }

  /**
   * Underlying Response future listener. This will count down the latch.
   */
  private class ResponseFutureListener implements Runnable {
    private final ServerResponseFuture<T> _future;

    public ResponseFutureListener(ServerResponseFuture<T> future) {
      _future = future;
    }

    @Override
    public void run() {
      LOGGER.debug("Running Future Listener for underlying future for {}", _future.getName());
      try {
        _futureLock.lock();
        if (_state.isCompleted()) {
          return; // We are marked done. Dont take in anything.
        }
      } finally {
        _futureLock.unlock();
      }

      Map<ServerInstance, T> response = null;
      try {
        response = _future.get();
      } catch (InterruptedException e) {
        LOGGER.info("Got interrupted waiting for response", e);
      } catch (ExecutionException e) {
        LOGGER.info("Got execution exception waiting for response", e);
      }

      // Since the future is done, it is safe to look at error results
      Map<ServerInstance, Throwable> error = _future.getError();
      boolean done = processFutureResult(_future.getServerInstance(), response, error, _future.getDurationMillis());

      if (done) {
        setDone(State.DONE);
        cancelUnderlyingFutures(); //Cancel underlying futures if they have not completed.
      }

      try {
        _futureLock.lock();
        if ((_latch.getCount() == 1) && !done) {
          setDone(State.DONE);
        } else if (!done) {
          _latch.countDown();
        }
      } finally {
        _futureLock.unlock();
      }
    }
  }
}
