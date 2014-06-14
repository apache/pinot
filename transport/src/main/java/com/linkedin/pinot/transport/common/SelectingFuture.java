package com.linkedin.pinot.transport.common;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
 * @param <K> Key type used in underlying response futures
 * @param <V> Response object.
 */
public class SelectingFuture<K,V> extends AbstractCompositeListenableFuture<K, V, V>{
  protected static Logger LOG = LoggerFactory.getLogger(SelectingFuture.class);

  private final List<AsyncResponseFuture<K, V>> _futuresList;

  // First successful Response
  private volatile V _delayedResponse;

  // Last Exception in case of error
  private volatile Throwable _error;


  public SelectingFuture()
  {
    _futuresList = new ArrayList<AsyncResponseFuture<K, V>>();
    _delayedResponse = null;
    _error = null;
  }

  /**
   * Start the future. This will add listener to the underlying futures. This method needs to be called
   * as soon the composite future is constructed and before any other method is invoked.
   */
  public void start(List<AsyncResponseFuture<K, V>> futuresList)
  {
    _futuresList.addAll(futuresList);
    _latch = new CountDownLatch(futuresList.size());
    for (AsyncResponseFuture<K, V> entry : _futuresList)
    {
      addResponseFutureListener(entry);
    }
  }

  /**
   * Call cancel on underlying futures. Dont worry if they are completed.
   * If they are already completed, cancel will be discarded. THis is best-effort only !!.
   */
  @Override
  protected void cancelUnderlyingFutures() {
    for (AsyncResponseFuture<K, V> entry : _futuresList) {
      entry.cancel(true);
    }
  }
  @Override
  public V get() throws InterruptedException, ExecutionException {
    _latch.await();
    return _delayedResponse;
  }

  public Throwable getError()
  {
    return _error;
  }

  @Override
  public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    _latch.await(timeout, unit);
    return _delayedResponse;
  }

  @Override
  protected boolean processFutureResult(K key, V response, Throwable error) {
    boolean done = false;
    if ( null != response)
    {
      LOG.debug("Response from %s is %s", key, response);
      _delayedResponse = response;
      _error = null;
      done = true;
    } else if ( null != error ) {
      LOG.debug("Error from %s is : %s", key, error);
      _error = error;
    }
    return done;
  }
}
