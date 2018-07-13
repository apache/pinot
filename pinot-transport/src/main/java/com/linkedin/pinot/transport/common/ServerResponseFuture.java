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

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.util.concurrent.ListenableFuture;
import com.linkedin.pinot.common.response.ServerInstance;


/**
 *
 * This provides a composite listenable future interface with each response
 * and errors keyed by an id.
 *
 * For example, in the case of future which holds scatter requests, this will
 * be the future for a request processing done at server "Si". The key in this
 * case would be the unique host-port where the server is serving data.
 *
 * Please note that both single and composite future implementations derive from this
 * interface as this would provide a cleaner way to support arbitrary composition
 * of futures ( multi-level aggregations). This is needed for scatter-gather type
 * request dispatch with speculative (duplicate) request dispatching.
 *
 *
 * @param <V> Response Type
 */
public interface ServerResponseFuture<V> extends ListenableFuture<Map<ServerInstance, V>> {

  /**
   * Returns a name of the future. The name of the future is not expected
   * to be unique. This is just used for logging to provide more context
   * @return
   */
  public String getName();

  /**
   * Returns the duration between when the future was created and when its result was available.
   * @return
   */
  public long getDurationMillis();

  /**
   * Blocking call. Similar to {@link Future#get()} But returns any one of the keyed
   * response. Useful when we know the only one response is expected.
   * @return
   */
  public V getOne() throws InterruptedException, ExecutionException;

  /**
   * Waits if necessary for at most the given time for the computation
   * to complete, and then retrieves its result, if available.
   * Useful when we know the only one response is expected.
   *
   * @param timeout the maximum time to wait
   * @param unit the time unit of the timeout argument
   * @return the computed result
   * @throws ExecutionException if the computation threw an
   * exception
   * @throws InterruptedException if the current thread was interrupted
   * while waiting
   * @throws TimeoutException if the wait timed out
   */
  V getOne(long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException;

  /**
   * Returns the current error results. If there are no errors, this method can
   * return null or empty map. This method is non-blocking and will not wait
   * for the future to complete. It will just return the current error results
   * @return
   */
  public Map<ServerInstance, Throwable> getError();

  public ServerInstance getServerInstance();
}
