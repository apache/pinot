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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.core.util.trace.TraceRunnable;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.apache.pinot.query.runtime.operator.OpChainId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class provides the implementation for scheduling multistage queries on a single node based
 * on the {@link OpChainScheduler} logic that is passed in. Multistage queries support partial execution
 * and will return a NOOP metadata block as a "yield" signal, indicating that the next operator
 * chain ({@link OpChainScheduler#next} will be requested.
 */
@SuppressWarnings("UnstableApiUsage")
public class OpChainSchedulerService extends AbstractExecutionThreadService {
  private static final Logger LOGGER = LoggerFactory.getLogger(OpChainSchedulerService.class);
  /**
   * Default time scheduler is allowed to wait for a runnable OpChain to be available.
   */
  private static final long DEFAULT_SCHEDULER_NEXT_WAIT_MS = 100;
  /**
   * Default cancel signal retention, this should be set to several times larger than
   * {@link org.apache.pinot.query.service.QueryConfig#DEFAULT_SCHEDULER_RELEASE_TIMEOUT_MS}.
   */
  private static final long SCHEDULER_CANCELLATION_SIGNAL_RETENTION_MS = 60_000L;

  private final OpChainScheduler _scheduler;
  private final ExecutorService _workerPool;
  private final Cache<Long, Long> _cancelledRequests = CacheBuilder.newBuilder()
      .expireAfterWrite(SCHEDULER_CANCELLATION_SIGNAL_RETENTION_MS, TimeUnit.MILLISECONDS).build();

  public OpChainSchedulerService(OpChainScheduler scheduler, ExecutorService workerPool) {
    _scheduler = scheduler;
    _workerPool = workerPool;
  }

  @Override
  protected void triggerShutdown() {
    // TODO: Figure out shutdown lifecycle with graceful shutdown in mind.
    LOGGER.info("Triggered shutdown on OpChainScheduler...");
  }

  @Override
  protected void run()
      throws Exception {
    while (isRunning()) {
      OpChain operatorChain = _scheduler.next(DEFAULT_SCHEDULER_NEXT_WAIT_MS, TimeUnit.MILLISECONDS);
      if (operatorChain == null) {
        continue;
      }
      LOGGER.trace("({}): Scheduling", operatorChain);
      _workerPool.submit(new TraceRunnable() {
        @Override
        public void runJob() {
          boolean isFinished = false;
          boolean returnedErrorBlock = false;
          Throwable thrown = null;
          try {
            LOGGER.trace("({}): Executing", operatorChain);
            // throw if the operatorChain is cancelled.
            if (_cancelledRequests.asMap().containsKey(operatorChain.getId().getRequestId())) {
              throw new InterruptedException("Query was cancelled!");
            }
            operatorChain.getStats().executing();
            // so long as there's work to be done, keep getting the next block
            // when the operator chain returns a NOOP block, then yield the execution
            // of this to another worker
            TransferableBlock result = operatorChain.getRoot().nextBlock();
            while (!result.isNoOpBlock() && !result.isEndOfStreamBlock()) {
              result = operatorChain.getRoot().nextBlock();
            }

            if (result.isNoOpBlock()) {
              // TODO: There should be a waiting-for-data state in OpChainStats.
              operatorChain.getStats().queued();
              _scheduler.yield(operatorChain);
            } else {
              isFinished = true;
              if (result.isErrorBlock()) {
                returnedErrorBlock = true;
                LOGGER.error("({}): Completed erroneously {} {}", operatorChain, operatorChain.getStats(),
                    result.getDataBlock().getExceptions());
              } else {
                LOGGER.debug("({}): Completed {}", operatorChain, operatorChain.getStats());
              }
            }
          } catch (Exception e) {
            LOGGER.error("({}): Failed to execute operator chain! {}", operatorChain, operatorChain.getStats(), e);
            thrown = e;
          } finally {
            if (returnedErrorBlock || thrown != null) {
              cancelOpChain(operatorChain, thrown);
            } else if (isFinished) {
              closeOpChain(operatorChain);
            }
          }
        }
      });
    }
  }

  /**
   * Register a new operator chain with the scheduler.
   *
   * @param operatorChain the chain to register
   */
  public final void register(OpChain operatorChain) {
    operatorChain.getStats().queued();
    _scheduler.register(operatorChain);
    LOGGER.debug("({}): Scheduler is now handling operator chain listening to mailboxes {}. "
            + "There are a total of {} chains awaiting execution.", operatorChain,
        operatorChain.getReceivingMailboxIds(),
        _scheduler.size());
  }

  /**
   * Async cancel a request. Request will not be fully cancelled until the next time opChain is being polled.
   *
   * @param requestId requestId to be cancelled.
   */
  public final void cancel(long requestId) {
    _cancelledRequests.put(requestId, requestId);
  }

  /**
   * This method should be called whenever data is available for an {@link OpChain} to consume.
   * Implementations of this method should be idempotent, it may be called in the scenario that no data is available.
   *
   * @param opChainId the identifier of the operator chain
   */
  public final void onDataAvailable(OpChainId opChainId) {
    _scheduler.onDataAvailable(opChainId);
  }

  // TODO: remove this method after we pipe down the proper executor pool to the v1 engine
  public ExecutorService getWorkerPool() {
    return _workerPool;
  }

  private void closeOpChain(OpChain opChain) {
    try {
      opChain.close();
    } finally {
      _scheduler.deregister(opChain);
    }
  }

  private void cancelOpChain(OpChain opChain, Throwable e) {
    try {
      opChain.cancel(e);
    } finally {
      _scheduler.deregister(opChain);
    }
  }
}
