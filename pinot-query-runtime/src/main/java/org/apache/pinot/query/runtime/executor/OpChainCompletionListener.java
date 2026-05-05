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

import javax.annotation.Nullable;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.OpChainId;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;


/**
 * Listener invoked by {@link OpChainSchedulerService} once an opchain finishes (either successfully or with an
 * error). Used by the stream-mode stats-reporting path to encode and emit
 * {@code OpChainComplete} messages on the corresponding broker→server gRPC stream.
 *
 * <p>Listeners are registered against a request id and fire for every opchain that runs on this server for that
 * request. The listener implementation is responsible for cleaning itself up (calling
 * {@link OpChainSchedulerService#unregisterCompletionListener(long)}) once it has received all the events it
 * expects, typically when the per-request opchain count reaches the expected total.
 *
 * <p>The listener is invoked on the gRPC opchain executor thread that ran the opchain. Implementations must not
 * block.
 */
@FunctionalInterface
public interface OpChainCompletionListener {

  /**
   * Invoked once per opchain completion.
   *
   * @param opChainId stage + worker + request id of the finished opchain
   * @param rootOperator the root operator of the finished opchain — still alive at the time of the call but the
   *     scheduler will close it as soon as the listener returns. The listener may walk it but must not retain a
   *     reference past the call
   * @param stats the stats accumulated by this opchain. {@code null} when the opchain failed before reaching the
   *     point where stats are calculated
   * @param context the execution context for the opchain (carries the op→PlanNode map for stats encoding)
   * @param error {@code null} on success, the failure cause on error
   */
  void onOpChainComplete(OpChainId opChainId, MultiStageOperator rootOperator,
      @Nullable MultiStageQueryStats stats, OpChainExecutionContext context, @Nullable Throwable error);
}
