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

import java.util.concurrent.ExecutorService;
import javax.annotation.Nullable;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModelFactory;


/**
 * Manages the custom Helix state transition thread pools for Pinot server.
 * {@link StateTransitionThreadPoolManager#getExecutorService} may only be called once for each argument combination,
 * and the returned {@link ExecutorService} would then be cached in Helix to serve all subsequent state transition
 * messages of that argument combination. See {@link StateModelFactory#getExecutorService(Message.MessageInfo)}
 */
public interface StateTransitionThreadPoolManager {
  @Nullable
  StateModelFactory.CustomizedExecutorService getExecutorService(Message.MessageInfo messageInfo);

  @Nullable
  ExecutorService getExecutorService(String resourceName, String fromState, String toState);

  @Nullable
  ExecutorService getExecutorService(String resourceName);

  void shutdown();
}
