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

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.Monitor;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.core.util.trace.TraceRunnable;
import org.apache.pinot.query.mailbox.MailboxIdentifier;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class provides the implementation for scheduling multistage queries on a single node based
 * on the {@link OpChainScheduler} logic that is passed in. Multistage queries support partial execution
 * and will return a NOOP metadata block as a "yield" signal, indicating that the next operator
 * chain ({@link OpChainScheduler#next()} will be requested.
 *
 * <p>Note that a yielded operator chain will be re-registered with the underlying scheduler.
 */
@SuppressWarnings("UnstableApiUsage")
public class OpChainSchedulerService extends AbstractExecutionThreadService {

  private static final Logger LOGGER = LoggerFactory.getLogger(OpChainSchedulerService.class);

  private static final int TERMINATION_TIMEOUT_SEC = 60;

  private final OpChainScheduler _scheduler;
  private final ExecutorService _workerPool;
  private final long _pollIntervalMs;

  // anything that is guarded by this monitor should be non-blocking
  private final Monitor _monitor = new Monitor();
  private final Monitor.Guard _hasNextOrClosing = new Monitor.Guard(_monitor) {
    @Override
    public boolean isSatisfied() {
      return _scheduler.hasNext() || !isRunning();
    }
  };

  // Note that workerPool is shut down in this class.
  public OpChainSchedulerService(OpChainScheduler scheduler, ExecutorService workerPool) {
    this(scheduler, workerPool, -1);
  }

  public OpChainSchedulerService(OpChainScheduler scheduler, ExecutorService workerPool, long pollIntervalMs) {
    _scheduler = scheduler;
    _workerPool = workerPool;
    _pollIntervalMs = pollIntervalMs;
  }

  @Override
  protected void triggerShutdown() {
    LOGGER.info("Triggered shutdown on OpChainScheduler...");
    // this will just notify all waiters that the scheduler is shutting down
    _monitor.enter();
    _monitor.leave();
    if (!MoreExecutors.shutdownAndAwaitTermination(_workerPool, TERMINATION_TIMEOUT_SEC, TimeUnit.SECONDS)) {
      LOGGER.error("Failed to shut down and terminate OpChainScheduler.");
    }
    _scheduler.shutDown();
  }

  @Override
  protected void run()
      throws Exception {
    while (isRunning()) {
      if (enterMonitor()) {
        try {
          if (!isRunning()) {
            return;
          }

          OpChain operatorChain = _scheduler.next();
          LOGGER.trace("({}): Scheduling", operatorChain);
          _workerPool.submit(new TraceRunnable() {
            @Override
            public void runJob() {
              try {
                LOGGER.trace("({}): Executing", operatorChain);
                operatorChain.getStats().executing();

                // so long as there's work to be done, keep getting the next block
                // when the operator chain returns a NOOP block, then yield the execution
                // of this to another worker
                TransferableBlock result = operatorChain.getRoot().nextBlock();
                while (!result.isNoOpBlock() && !result.isEndOfStreamBlock()) {
                  result = operatorChain.getRoot().nextBlock();
                }

                if (!result.isEndOfStreamBlock()) {
                  // not complete, needs to re-register for scheduling
                  register(operatorChain, false);
                } else {
                  if (result.isErrorBlock()) {
                    operatorChain.getRoot().toExplainString();
                    LOGGER.error("({}): Completed erroneously {} {}", operatorChain, operatorChain.getStats(),
                        result.getDataBlock().getExceptions());
                  } else {
                    operatorChain.getRoot().toExplainString();
                    operatorChain.getStats().setOperatorStatsMap(result.getResultMetadata());
                    LOGGER.debug("({}): Completed {}", operatorChain, operatorChain.getStats());
                  }
                  operatorChain.close();
                }
              } catch (Exception e) {
                operatorChain.close();
                operatorChain.getRoot().toExplainString();
                LOGGER.error("({}): Failed to execute operator chain! {}", operatorChain, operatorChain.getStats(), e);
              }
            }
          });
        } finally {
          _monitor.leave();
        }
      }
    }
  }

  private boolean enterMonitor()
      throws InterruptedException {
    if (_pollIntervalMs >= 0) {
      return _monitor.enterWhen(_hasNextOrClosing, _pollIntervalMs, TimeUnit.MILLISECONDS);
    } else {
      _monitor.enterWhen(_hasNextOrClosing);
      return true;
    }
  }

  /**
   * Register a new operator chain with the scheduler.
   *
   * @param operatorChain the chain to register
   */
  public final void register(OpChain operatorChain) {
    register(operatorChain, true);
    LOGGER.debug("({}): Scheduler is now handling operator chain listening to mailboxes {}. "
            + "There are a total of {} chains awaiting execution.", operatorChain, operatorChain.getReceivingMailbox(),
        _scheduler.size());
  }

  public final void register(OpChain operatorChain, boolean isNew) {
    _monitor.enter();
    try {
      LOGGER.trace("({}): Registered operator chain (new: {}). Total: {}", operatorChain, isNew, _scheduler.size());

      _scheduler.register(operatorChain, isNew);
    } finally {
      operatorChain.getStats().queued();
      _monitor.leave();
    }
  }

  /**
   * This method should be called whenever data is available in a given mailbox.
   * Implementations of this method should be idempotent, it may be called in the
   * scenario that no mail is available.
   *
   * @param mailbox the identifier of the mailbox that now has data
   */
  public final void onDataAvailable(MailboxIdentifier mailbox) {
    _monitor.enter();
    try {
      LOGGER.trace("Notified onDataAvailable for mailbox {}", mailbox);
      _scheduler.onDataAvailable(mailbox);
    } finally {
      _monitor.leave();
    }
  }

  // TODO: remove this method after we pipe down the proper executor pool to the v1 engine
  public ExecutorService getWorkerPool() {
    return _workerPool;
  }
}
