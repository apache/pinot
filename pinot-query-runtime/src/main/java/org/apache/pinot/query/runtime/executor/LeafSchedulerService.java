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
import com.google.common.cache.RemovalListener;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.pinot.core.util.trace.TraceRunnable;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.apache.pinot.query.runtime.operator.OpChainId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LeafSchedulerService {
  private static final int DANGLING_LEAF_OP_CHAIN_EXPIRY_SECONDS = 300;
  private static final long SCHEDULER_CANCELLATION_SIGNAL_RETENTION_MS = 60_000L;
  private static final Logger LOGGER = LoggerFactory.getLogger(LeafSchedulerService.class);

  private final ExecutorService _executorService;
  private final Cache<OpChainId, Future<?>> _submittedOpChainCache =
      CacheBuilder.newBuilder().expireAfterAccess(DANGLING_LEAF_OP_CHAIN_EXPIRY_SECONDS, TimeUnit.SECONDS)
          .removalListener((RemovalListener<OpChainId, Future<?>>) notification -> {
            if (notification.wasEvicted()) {
              Future<?> future = notification.getValue();
              if (!future.isDone()) {
                LOGGER.warn("Evicting dangling leaf opChain: {}", notification.getKey());
                future.cancel(true);
              }
            }
          }).build();
  private final Cache<Long, Long> _cancelledRequests = CacheBuilder.newBuilder()
      .expireAfterWrite(SCHEDULER_CANCELLATION_SIGNAL_RETENTION_MS, TimeUnit.MILLISECONDS).build();

  public LeafSchedulerService(ExecutorService executorService) {
    _executorService = executorService;
  }

  public void register(OpChain operatorChain) {
    if (!_cancelledRequests.asMap().containsKey(operatorChain.getId().getRequestId())) {
      Future<?> scheduledFuture = _executorService.submit(new TraceRunnable() {
        @Override
        public void runJob() {
          boolean isFinished = false;
          boolean returnedErrorBlock = false;
          Throwable thrown = null;
          try {
            LOGGER.trace("({}): Executing", operatorChain);
            operatorChain.getStats().executing();
            TransferableBlock result = operatorChain.getRoot().nextBlock();
            while (!result.isEndOfStreamBlock()) {
              result = operatorChain.getRoot().nextBlock();
            }
            isFinished = true;
            if (result.isErrorBlock()) {
              returnedErrorBlock = true;
              LOGGER.error("({}): Completed erroneously {} {}", operatorChain, operatorChain.getStats(),
                  result.getDataBlock().getExceptions());
            } else {
              LOGGER.debug("({}): Completed {}", operatorChain, operatorChain.getStats());
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
      _submittedOpChainCache.put(operatorChain.getId(), scheduledFuture);
    }
  }

  public void cancel(long requestId) {
    _cancelledRequests.put(requestId, requestId);
    List<OpChainId> opChainIdsToCancel = _submittedOpChainCache.asMap().keySet()
        .stream().filter(opChainId -> opChainId.getRequestId() == requestId).collect(Collectors.toList());
    for (OpChainId opChainId : opChainIdsToCancel) {
      Future<?> future = _submittedOpChainCache.getIfPresent(opChainId);
      if (future != null) {
        future.cancel(true);
        _submittedOpChainCache.invalidate(opChainId);
      }
    }
  }

  private void closeOpChain(OpChain opChain) {
    opChain.close();
  }

  private void cancelOpChain(OpChain opChain, Throwable t) {
    opChain.cancel(t);
  }
}
