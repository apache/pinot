package com.linkedin.pinot.transport.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * @param <K> Key to locate the specific future's value
 * @param <V> Value type of the underlying future
 */
public class CompositeFuture<K, V> extends AbstractCompositeListenableFuture<K, V >
{
  protected static Logger LOG = LoggerFactory.getLogger(CompositeFuture.class);

  public static enum GatherModeOnError
  {
    /* Future completes only when all underlying futures complete or any one underlying future fails */
    SHORTCIRCUIT_AND,
    /* Future completes only when all underlying futures completes successfully or fails. */
    AND,
  };

  private final Collection<KeyedFuture<K,V>> _futures;

  // Composite Response
  private final  ConcurrentMap<K,V> _delayedResponseMap;

  // Exception in case of error
  private final ConcurrentMap<K,Throwable> _errorMap;

  private final GatherModeOnError _gatherMode;

  // Descriptive name of the future
  private final String _name;

  public CompositeFuture(String name, GatherModeOnError mode)
  {
    _name = name;
    _futures = new ArrayList<KeyedFuture<K,V>>();
    _delayedResponseMap = new ConcurrentHashMap<K, V>();
    _errorMap = new ConcurrentHashMap<K, Throwable>();
    _gatherMode = mode;
  }

  /**
   * Start the future. This will add listener to the underlying futures. This method needs to be called
   * as soon the composite future is constructed and before any other method is invoked.
   */
  public void start(Collection<KeyedFuture<K,V>> futureList)
  {
    boolean started = super.start();

    if ( !started)
    {
      String msg = "Unable to start the future. State is already : " + _state;
      LOG.error(msg);
      throw new IllegalStateException(msg);
    }

    _futures.addAll(futureList);
    _latch = new CountDownLatch(futureList.size());
    for (KeyedFuture<K,V> entry : _futures)
    {
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
    for (KeyedFuture<K,V> entry : _futures) {
      entry.cancel(true);
    }
  }

  @Override
  /**
   * 
   */
  public Map<K, V> get() throws InterruptedException, ExecutionException {
    _latch.await();
    return _delayedResponseMap;
  }

  @Override
  public V getOne() throws InterruptedException, ExecutionException {
    _latch.await();
    if ( _delayedResponseMap.isEmpty()) {
      return null;
    }
    return _delayedResponseMap.values().iterator().next();
  }

  @Override
  public Map<K,Throwable> getError()
  {
    return _errorMap;
  }

  @Override
  public Map<K, V> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    _latch.await(timeout, unit);
    return _delayedResponseMap;
  }

  @Override
  protected boolean processFutureResult(String name, Map<K,V> response, Map<K,Throwable> error) {
    boolean ret = false;
    if ( null != response)
    {
      LOG.debug("Response from {} is {}", name, response);
      _delayedResponseMap.putAll(response);
    } else if (null != error) {
      LOG.debug("Error from {} is : {}", name, error);
      _errorMap.putAll(error);

      if ( _gatherMode == GatherModeOnError.SHORTCIRCUIT_AND)
      {
        ret = true; // We are done as we got an error
      }
    }
    return ret;
  }

  @Override
  public String getName() {
    return _name;
  }
}
