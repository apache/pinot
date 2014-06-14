package com.linkedin.pinot.transport.common;

import java.util.Map;
import java.util.Map.Entry;
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
public class ConjunctiveCompositeFuture<K, V> extends AbstractCompositeListenableFuture<K, V, Map<K,V>>
{
  protected static Logger LOG = LoggerFactory.getLogger(ConjunctiveCompositeFuture.class);

  public static enum GatherModeOnError
  {
    /* Future completes only when all underlying futures complete or any one underlying future fails */
    SHORTCIRCUIT_AND,
    /* Future completes only when all underlying futures completes successfully or fails. */
    AND,
  };

  private final ConcurrentMap<K, AsyncResponseFuture<K, V>> _futureMap;

  // Composite Response
  private final  ConcurrentMap<K,V> _delayedResponseMap;

  // Exception in case of error
  private final ConcurrentMap<K,Throwable> _errorMap;

  private final GatherModeOnError _gatherMode;

  public ConjunctiveCompositeFuture(GatherModeOnError mode)
  {
    _futureMap = new ConcurrentHashMap<K, AsyncResponseFuture<K, V>>();
    _delayedResponseMap = new ConcurrentHashMap<K, V>();
    _errorMap = new ConcurrentHashMap<K, Throwable>();
    _gatherMode = mode;
  }

  /**
   * Start the future. This will add listener to the underlying futures. This method needs to be called
   * as soon the composite future is constructed and before any other method is invoked.
   */
  public void start(Map<K, AsyncResponseFuture<K, V>> futureMap)
  {
    _futureMap.putAll(futureMap);
    _latch = new CountDownLatch(futureMap.size());
    for (Entry<K, AsyncResponseFuture<K, V>> entry : _futureMap.entrySet())
    {
      addResponseFutureListener(entry.getValue());
    }
  }


  /**
   * Call cancel on underlying futures. Dont worry if they are completed.
   * If they are already completed, cancel will be discarded. THis is best-effort only !!.
   */
  @Override
  protected void cancelUnderlyingFutures() {
    for (Entry<K, AsyncResponseFuture<K, V>> entry : _futureMap.entrySet()) {
      entry.getValue().cancel(true);
    }
  }

  @Override
  public Map<K, V> get() throws InterruptedException, ExecutionException {
    _latch.await();
    return _delayedResponseMap;
  }

  public Map<K, Throwable> getError()
  {
    return _errorMap;
  }

  @Override
  public Map<K, V> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    _latch.await(timeout, unit);
    return _delayedResponseMap;
  }

  @Override
  protected boolean processFutureResult(K key, V response, Throwable error) {
    boolean ret = false;
    if ( null != response)
    {
      LOG.debug("Response from {} is {}", key, response);
      _delayedResponseMap.put(key, response);
    } else if (null != error) {
      LOG.debug("Error from {} is : {}", key, error);
      _errorMap.put(key, error);

      if ( _gatherMode == GatherModeOnError.SHORTCIRCUIT_AND)
      {
        ret = true; // We are done as we got an error
      }
    }
    return ret;
  }
}
