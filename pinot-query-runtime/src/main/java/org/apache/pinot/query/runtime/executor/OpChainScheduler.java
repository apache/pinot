/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.query.runtime.executor;

import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.apache.pinot.query.runtime.operator.OpChainId;


/**
 * An interface that defines different scheduling strategies to work with the {@link OpChainSchedulerService}.
 */
@ThreadSafe
public interface OpChainScheduler {
  /**
   * Registers a new OpChain with the scheduler.
   * @param operatorChain the operator chain to register
   */
  void register(OpChain operatorChain);

  /**
   * When the OpChain is finished, error or otherwise, deregister is called for it so the scheduler can do any required
   * cleanup. After an OpChain is de-registered, the scheduler service will never call any other method for it.
   * However, the {@link #onDataAvailable} callback may be called even after an OpChain is de-registered, and the
   * scheduler should handle that scenario.
   * @param operatorChain an operator chain that is finished (error or otherwise).
   */
  void deregister(OpChain operatorChain);

  /**
   * Used by {@link OpChainSchedulerService} to indicate that a given OpChain can be suspended until it receives some
   * data. Note that this method is only used by the scheduler service to "indicate" that an OpChain can be suspended.
   * The decision on whether to actually suspend or not can be taken by the scheduler.
   */
  void yield(OpChain opChain);

  /**
   * A callback called whenever data is received for the given opChain. This can be used by the scheduler
   * implementations to re-scheduled suspended OpChains. This method may be called for an OpChain that has not yet
   * been scheduled, or an OpChain that has already been de-registered.
   * @param opChainId the {@link OpChain} ID
   */
  void onDataAvailable(OpChainId opChainId);

  /**
   * Returns an OpChain that is ready to be run by {@link OpChainSchedulerService}, waiting for the given time if
   * there are no such OpChains ready yet. Will return null if there's no ready OpChains even after the specified time.
   *
   * @param time non-negative value that determines the time the scheduler will wait for new OpChains to be ready.
   * @param timeUnit TimeUnit for the await time.
   * @return a non-null OpChain that's ready to be run, or null if there's no OpChain ready even after waiting for the
   *         given time.
   * @throws InterruptedException if the wait for a ready OpChain was interrupted.
   */
  @Nullable
  OpChain next(long time, TimeUnit timeUnit)
      throws InterruptedException;

  /**
   * @return the number of operator chains registered with the scheduler
   */
  int size();

  /**
   * TODO: Figure out shutdown flow in context of graceful shutdown.
   */
  void shutdownNow();
}
