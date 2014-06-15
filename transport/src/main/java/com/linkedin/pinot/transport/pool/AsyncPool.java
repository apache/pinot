package com.linkedin.pinot.transport.pool;

import java.util.Collection;

import com.linkedin.pinot.transport.common.Callback;
import com.linkedin.pinot.transport.common.Cancellable;
import com.linkedin.pinot.transport.common.NoneType;
import com.linkedin.pinot.transport.metrics.PoolStats;
import com.linkedin.pinot.transport.metrics.PoolStatsProvider;

/**
 * This implementation is mostly copied from R2's AsyncPool. The AsyncPool is just a  very small part
 * of R2 jar which pulls in a lot of dependent jar. Hence copying to keep the dependency thin.
 *
 * Original Author information:
 * 
 * @author Steven Ihde
 * @version $Revision: $
 *
 * @param <T> Resource type to be managed
 */
public interface AsyncPool<T> extends PoolStatsProvider {

  /**
   * Get the pool's name.
   *
   * @return The pool's name.
   */
  String getName();

  /**
   * Start the pool.
   */
  void start();

  /**
   * Cancels all pending requests (i.e., those that are waiting for objects).
   * @return all pending requests that were cancelled
   */
  Collection<Callback<T>> cancelWaiters();

  /**
   * Get an object from the pool.
   *
   * If a valid object is available, it will be passed to the callback (possibly by the thread
   * that invoked <code>get</code>.
   *
   * The pool will determine if an idle object is valid by calling the Lifecycle's
   * <code>validate</code> method.
   *
   * If none is available, the method returns immediately.  If the pool is not yet at
   * max capacity, object creation will be initiated.
   *
   * Callbacks will be executed in FIFO order as objects are returned to the pool (either
   * by other users, or as new object creation completes) or as the timeout expires.
   *
   * After finishing with the object, the user must return the object to the pool with
   * <code>put</code>.
   *
   * @param callback the callback to receive the checked out object
   * @return A {@link Cancellable} which, if invoked before the callback, will cancel
   * the pending get request.  If the caller abandons a request without cancelling it, the
   * pool will retain the callback reference until an object is available and the callback is
   * invoked, which may never occur if all pooled objects are busy indefinitely and no more
   * can be created.
   */
  Cancellable get(Callback<T> callback);

  /**
   * Return a previously checked out object to the pool.  It is an error to return an object to
   * the pool that is not currently checked out from the pool.
   *
   * @param obj the object to be returned
   */
  void put(T obj);

  /**
   * Dispose of a checked out object which is not operating correctly.  It is an error to
   * <code>dispose</code> an object which is not currently checked out from the pool.
   *
   * @param obj the object to be disposed
   */
  void dispose(T obj);

  /**
   * Initiate an orderly shutdown of the pool.  The pool will immediately stop accepting
   * new {@link #get(com.linkedin.pinot.transport.common.Callback)} requests.  Shutdown is complete when
   * <ul>
   *   <li>No pending requests are waiting for objects</li>
   *   <li>All objects have been returned to the pool, via either {@link #put(Object)} or {@link #dispose(Object)}</li>
   * </ul>
   *
   * @param callback A callback that is invoked when the conditions above are satisfied
   */
  void shutdown(Callback<NoneType> callback);


  public interface Lifecycle<T>
  {
    void create(Callback<T> callback);
    boolean validateGet(T obj);
    boolean validatePut(T obj);
    void destroy(T obj, boolean error, Callback<T> callback);
    PoolStats.LifecycleStats getStats();
  }
}
