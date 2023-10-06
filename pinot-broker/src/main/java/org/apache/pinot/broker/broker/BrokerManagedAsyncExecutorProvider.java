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
package org.apache.pinot.broker.broker;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import javax.ws.rs.ServiceUnavailableException;
import javax.ws.rs.core.Response;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.glassfish.jersey.server.ManagedAsyncExecutor;
import org.glassfish.jersey.spi.ThreadPoolExecutorProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * BrokerManagedAsyncExecutorProvider provides a bounded thread pool.
 */
@ManagedAsyncExecutor
public class BrokerManagedAsyncExecutorProvider extends ThreadPoolExecutorProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerManagedAsyncExecutorProvider.class);

  private static final String NAME = "broker-managed-async-executor";

  private final BrokerMetrics _brokerMetrics;

  @Getter(value = AccessLevel.PROTECTED)
  private final int _maximumPoolSize;
  @Getter(value = AccessLevel.PROTECTED)
  private final int _corePoolSize;
  private final int _queueSize;

  public BrokerManagedAsyncExecutorProvider(int corePoolSize, int maximumPoolSize, int queueSize,
      BrokerMetrics brokerMetrics) {
    super(NAME);
    _corePoolSize = corePoolSize;
    _maximumPoolSize = maximumPoolSize;
    _queueSize = queueSize;
    _brokerMetrics = brokerMetrics;
  }

  @Override
  protected BlockingQueue<Runnable> getWorkQueue() {
    if (_queueSize == Integer.MAX_VALUE) {
      return new LinkedBlockingQueue();
    }
    return new ArrayBlockingQueue(_queueSize);
  }

  @Override
  protected RejectedExecutionHandler getRejectedExecutionHandler() {
    return new BrokerThreadPoolRejectExecutionHandler(_brokerMetrics);
  }

  static class BrokerThreadPoolRejectExecutionHandler implements RejectedExecutionHandler {
    private final BrokerMetrics _brokerMetrics;

    public BrokerThreadPoolRejectExecutionHandler(BrokerMetrics brokerMetrics) {
      _brokerMetrics = brokerMetrics;
    }

    /**
     * Reject the runnable if it can’t be accommodated by the thread pool.
     *
     * <p> Response returned will have SERVICE_UNAVAILABLE(503) error code with error msg.
     *
     * @param r Runnable
     * @param executor ThreadPoolExecutor
     */
    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.QUERY_REJECTED_EXCEPTIONS, 1L);
      LOGGER.error("Task " + r + " rejected from " + executor);

      throw new ServiceUnavailableException(Response.status(Response.Status.SERVICE_UNAVAILABLE).entity(
          "Pinot Broker thread pool can not accommodate more requests now. " + "Request is rejected from " + executor)
          .build());
    }
  }
}
