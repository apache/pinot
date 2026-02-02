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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.pinot.core.data.manager.SegmentOperationsTaskType;
import org.apache.pinot.core.data.manager.SegmentOperationsThrottlerContextRegistry;


/**
 * Default state transition thread pool manager backed by a fixed-size pool.
 */
public class DefaultStateTransitionThreadPoolManager implements StateTransitionThreadPoolManager {
  private static final int DEFAULT_POOL_SIZE = 40;

  private final ExecutorService _executorService;

  public DefaultStateTransitionThreadPoolManager() {
    _executorService = new ContextWrappingExecutor(DEFAULT_POOL_SIZE, DEFAULT_POOL_SIZE, 0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(), new ThreadFactoryBuilder().setNameFormat("state-transition-%d").build());
  }

  @Override
  @Nullable
  public StateModelFactory.CustomizedExecutorService getExecutorService(Message.MessageInfo messageInfo) {
    return new StateModelFactory.CustomizedExecutorService(Message.MessageInfo.MessageIdentifierBase.PER_RESOURCE,
        _executorService);
  }

  @Override
  @Nullable
  public ExecutorService getExecutorService(String resourceName, String fromState, String toState) {
    return _executorService;
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
    public void execute(Runnable command) {
      super.execute(
          SegmentOperationsThrottlerContextRegistry.get().wrap(command, SegmentOperationsTaskType.STATE_TRANSITION));
    }
  }
}
