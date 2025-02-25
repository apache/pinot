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

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.pinot.core.util.trace.TraceRunnable;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.apache.pinot.query.runtime.operator.OpChainId;
import org.apache.pinot.spi.accounting.ThreadExecutionContext;
import org.apache.pinot.spi.accounting.ThreadResourceUsageProvider;
import org.apache.pinot.spi.trace.Tracing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OpChainSchedulerService {
  private static final Logger LOGGER = LoggerFactory.getLogger(OpChainSchedulerService.class);

  private final ExecutorService _executorService;
  private final ConcurrentHashMap<OpChainId, Future<?>> _submittedOpChainMap;

  public OpChainSchedulerService(ExecutorService executorService) {
    _executorService = executorService;
    _submittedOpChainMap = new ConcurrentHashMap<>();
  }

  public void register(OpChain operatorChain) {
    Future<?> scheduledFuture = _executorService.submit(new TraceRunnable() {
      @Override
      public void runJob() {
        TransferableBlock returnedErrorBlock = null;
        Throwable thrown = null;
        // try-with-resources to ensure that the operator chain is closed
        // TODO: Change the code so we ownership is expressed in the code in a better way
        try (OpChain closeMe = operatorChain) {
          ThreadResourceUsageProvider threadResourceUsageProvider = new ThreadResourceUsageProvider();
          Tracing.ThreadAccountantOps.setupWorker(operatorChain.getId().getStageId(),
              ThreadExecutionContext.TaskType.MSE, threadResourceUsageProvider,
              operatorChain.getParentContext());
          LOGGER.trace("({}): Executing", operatorChain);
          TransferableBlock result = operatorChain.getRoot().nextBlock();
          while (!result.isEndOfStreamBlock()) {
            result = operatorChain.getRoot().nextBlock();
          }
          if (result.isErrorBlock()) {
            returnedErrorBlock = result;
            LOGGER.error("({}): Completed erroneously {} {}", operatorChain, result.getQueryStats(),
                result.getExceptions());
          } else {
            LOGGER.debug("({}): Completed {}", operatorChain, result.getQueryStats());
          }
        } catch (Exception e) {
          LOGGER.error("({}): Failed to execute operator chain!", operatorChain, e);
          thrown = e;
        } finally {
          _submittedOpChainMap.remove(operatorChain.getId());
          if (returnedErrorBlock != null || thrown != null) {
            if (thrown == null) {
              thrown = new RuntimeException("Error block " + returnedErrorBlock.getExceptions());
            }
            operatorChain.cancel(thrown);
          }
          Tracing.ThreadAccountantOps.clear();
        }
      }
    });
    _submittedOpChainMap.put(operatorChain.getId(), scheduledFuture);
  }

  public void cancel(long requestId) {
    // simple cancellation. for leaf stage this cannot be a dangling opchain b/c they will eventually be cleared up
    // via query timeout.
    Iterator<Map.Entry<OpChainId, Future<?>>> iterator = _submittedOpChainMap.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<OpChainId, Future<?>> entry = iterator.next();
      if (entry.getKey().getRequestId() == requestId) {
        entry.getValue().cancel(true);
        iterator.remove();
      }
    }
  }
}
