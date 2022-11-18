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
import java.util.concurrent.ExecutorService;
import org.apache.pinot.core.util.trace.TraceRunnable;
import org.apache.pinot.query.mailbox.MailboxIdentifier;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.apache.pinot.spi.accounting.ThreadResourceUsageProvider;
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

  private final OpChainScheduler _scheduler;
  private final ExecutorService _workerPool;

  // anything that is guarded by this monitor should be non-blocking
  private final Monitor _monitor = new Monitor();
  private final Monitor.Guard _hasNextOrClosing = new Monitor.Guard(_monitor) {
    @Override
    public boolean isSatisfied() {
      return _scheduler.hasNext() || !isRunning();
    }
  };

  public OpChainSchedulerService(OpChainScheduler scheduler, ExecutorService workerPool) {
    _scheduler = scheduler;
    _workerPool = workerPool;
  }

  @Override
  protected void triggerShutdown() {
    // this wil just notify all waiters that the scheduler is shutting down
    _monitor.enter();
    _monitor.leave();
  }

  @Override
  protected void run()
      throws Exception {
    while (isRunning()) {
      _monitor.enterWhen(_hasNextOrClosing);
      try {
        if (!isRunning()) {
          return;
        }

        OpChain operatorChain = _scheduler.next();
        _workerPool.submit(new TraceRunnable() {
          @Override
          public void runJob() {
            try {
              ThreadResourceUsageProvider timer = operatorChain.getAndStartTimer();

              // so long as there's work to be done, keep getting the next block
              // when the operator chain returns a NOOP block, then yield the execution
              // of this to another worker
              TransferableBlock result = operatorChain.getRoot().nextBlock();
              while (!result.isNoOpBlock() && !result.isEndOfStreamBlock()) {
                LOGGER.debug("Got block with {} rows.", result.getNumRows());
                result = operatorChain.getRoot().nextBlock();
              }

              if (!result.isEndOfStreamBlock()) {
                // not complete, needs to re-register for scheduling
                register(operatorChain);
              } else {
                LOGGER.info("Execution time: " + timer.getThreadTimeNs());
              }
            } catch (Exception e) {
              LOGGER.error("Failed to execute query!", e);
            }
          }
        });
      } finally {
        _monitor.leave();
      }
    }
  }

  /**
   * Register a new operator chain with the scheduler.
   *
   * @param operatorChain the chain to register
   */
  public final void register(OpChain operatorChain) {
    _monitor.enter();
    try {
      _scheduler.register(operatorChain);
    } finally {
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
