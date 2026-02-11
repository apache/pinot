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
package org.apache.pinot.server.starter.helix;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.helix.messaging.handling.MessageTask;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.pinot.core.data.manager.SegmentOperationsTaskContext;
import org.apache.pinot.core.data.manager.SegmentOperationsTaskType;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * Default state transition thread pool manager backed by a fixed-size pool.
 *
 * <p>This replaces the Helix-managed state transition thread pool to provide explicit context tracking
 * for segment operations. The pool size is configurable via
 * {@link org.apache.pinot.spi.utils.CommonConstants.Server#CONFIG_OF_STATE_TRANSITION_THREAD_POOL_SIZE}.</p>
 *
 * <p><b>Migration from Helix configuration:</b> Previously, the thread pool size was configured in ZooKeeper
 * using the key "STATE_TRANSITION.maxThreads" at either the participant or cluster config level.
 * This configuration replaces that mechanism.</p>
 */
public class DefaultStateTransitionThreadPoolManager implements StateTransitionThreadPoolManager {
  private final ExecutorService _executorService;

  /**
   * Creates a state transition thread pool manager with the default pool size configuration.
   * The pool size is read from the server configuration.
   *
   * @param serverConf the server configuration
   */
  public DefaultStateTransitionThreadPoolManager(PinotConfiguration serverConf) {
    int poolSize = serverConf.getProperty(
        org.apache.pinot.spi.utils.CommonConstants.Server.CONFIG_OF_STATE_TRANSITION_THREAD_POOL_SIZE,
        org.apache.pinot.spi.utils.CommonConstants.Server.DEFAULT_STATE_TRANSITION_THREAD_POOL_SIZE);
    _executorService = new ContextWrappingExecutor(poolSize, poolSize, 0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(), new ThreadFactoryBuilder().setNameFormat("state-transition-%d").build());
  }

  @Override
  @Nullable
  public StateModelFactory.CustomizedExecutorService getExecutorService(Message.MessageInfo messageInfo) {
    return null;
  }

  @Override
  @Nullable
  public ExecutorService getExecutorService(String resourceName, String fromState, String toState) {
    return null;
  }

  @Override
  @Nullable
  public ExecutorService getExecutorService(String resourceName) {
    return _executorService;
  }

  @Override
  public void shutdown() {
    _executorService.shutdown();
  }

  private static class ContextWrappingExecutor extends ThreadPoolExecutor {
    ContextWrappingExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
        LinkedBlockingQueue<Runnable> workQueue, java.util.concurrent.ThreadFactory threadFactory) {
      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
      Message message = task instanceof MessageTask ? ((MessageTask) task).getMessage() : null;
      String tableNameWithType = message != null ? message.getResourceName() : null;
      return super.submit(
          SegmentOperationsTaskContext.wrap(task, SegmentOperationsTaskType.STATE_TRANSITION, tableNameWithType));
    }
  }
}
