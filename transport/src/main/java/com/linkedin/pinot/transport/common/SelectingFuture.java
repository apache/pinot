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
 * @param <T> Response object.
 */
public class SelectingFuture<K,T> extends AbstractCompositeListenableFuture<K, T>{
  protected static Logger LOG = LoggerFactory.getLogger(SelectingFuture.class);

  private final List<KeyedFuture<K, T>> _futuresList;

  // First successful Response
  private volatile Map<K,T>  _delayedResponse;

  // Last Exception in case of error
  private volatile Map<K,Throwable> _error;

  private final String _name;

  public SelectingFuture(String name)
  {
    _name = name;
    _futuresList = new ArrayList<KeyedFuture<K, T>>();
    _delayedResponse = null;
    _error = null;
  }

  /**
   * Start the future. This will add listener to the underlying futures. This method needs to be called
   * as soon the composite future is constructed and before any other method is invoked.
   */
  public void start(Collection<KeyedFuture<K, T>> futuresList)
  {
    boolean started = super.start();

    if ( !started)
    {
      String msg = "Unable to start the future. State is already : " + _state;
      LOG.error(msg);
      throw new IllegalStateException(msg);
    }

    _futuresList.addAll(futuresList);
    _latch = new CountDownLatch(futuresList.size());
    for (KeyedFuture<K, T> entry : _futuresList)
    {
      if ( null != entry) {
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
    for (KeyedFuture<K, T> entry : _futuresList) {
      entry.cancel(true);
    }
  }
  @Override
  public Map<K, T> get() throws InterruptedException, ExecutionException {
    _latch.await();
    return _delayedResponse;
  }

  @Override
  public Map<K,Throwable> getError()
  {
    return _error;
  }

  @Override
  public Map<K, T> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    _latch.await(timeout, unit);
    return _delayedResponse;
  }

  @Override
  public T getOne() throws InterruptedException, ExecutionException {
    _latch.await();
    if ( (null == _delayedResponse) || (_delayedResponse.isEmpty())) {
      return null;
    }
    return _delayedResponse.values().iterator().next();
  }

  @Override
  protected boolean processFutureResult(String name, Map<K,T> response, Map<K,Throwable> error) {
    boolean done = false;
    if ( (null != response) )
    {
      LOG.debug("Error got from {} is : {}", name, response);

      _delayedResponse = response;
      _error = null;
      done = true;
    } else if ( null != error ) {
      LOG.debug("Error got from {} is : {}", name, error);
      _error = error;
    }
    return done;
  }

  @Override
  public String getName() {
    return _name;
  }
}
