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
package org.apache.pinot.query.runtime.operator.exchange;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.OpChainId;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Exchange service provides an {@link ExecutorService} for running {@link BlockExchange}.
 *
 * {@link BlockExchange} is used by {@link org.apache.pinot.query.runtime.operator.MailboxSendOperator} to
 * exchange data between underlying {@link org.apache.pinot.query.mailbox.MailboxService} and the query stage execution
 * engine running the actual {@link org.apache.pinot.query.runtime.operator.OpChain}.
 *
 * This exchange service is used to run the actual underlying connection and data transfer on a separate thread so that
 * the query stage execution can properly decoupled with the data transfer mechanism.
 */
public class ExchangeService {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExchangeService.class);
  private static final int DANGLING_EXCHANGE_EXPIRY_SECONDS = 300;

  private final Cache<OpChainId, Future<?>> _submittedExchangeCache =
      CacheBuilder.newBuilder().expireAfterAccess(DANGLING_EXCHANGE_EXPIRY_SECONDS, TimeUnit.SECONDS)
          .removalListener((RemovalListener<OpChainId, Future<?>>) notification -> {
            if (notification.wasEvicted()) {
              Future<?> future = notification.getValue();
              if (!future.isDone()) {
                LOGGER.warn("Evicting dangling exchange request for {}}", notification.getKey());
                future.cancel(true);
              }
            }
          }).build();
  private final ExecutorService _exchangeExecutor;

  public ExchangeService(String hostname, int port, PinotConfiguration config) {
    _exchangeExecutor = Executors.newCachedThreadPool();
    LOGGER.info("Initialized ExchangeService with hostname: {}, port: {}", hostname, port);
  }

  /**
   * submit a block exchange to sending service for a single OpChain.
   *
   * Notice that the logic inside the {@link BlockExchange#send()} should guarantee the submitted Runnable object
   *     to be terminated successfully or after opChain timeout.
   *
   * @param blockExchange the exchange object of the OpChain with all the pending data to be sent.
   */
  public void submitExchangeRequest(OpChainId opChainId, BlockExchange blockExchange) {
    _submittedExchangeCache.put(opChainId, _exchangeExecutor.submit(() -> {
      TransferableBlock block = blockExchange.send();
      while (!TransferableBlockUtils.isEndOfStream(block)) {
        block = blockExchange.send();
      }
    }));
  }

  public void cancelExchangeRequest(OpChainId opChainId, Throwable t) {
    _submittedExchangeCache.invalidate(opChainId);
  }
}
