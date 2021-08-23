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
package org.apache.pinot.core.query.scheduler;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.pinot.core.query.request.ServerQueryRequest;


/**
 * Query specific scheduling context
 */
public class SchedulerQueryContext {

  private final ServerQueryRequest _queryRequest;
  private final SettableFuture<byte[]> _resultFuture;
  private SchedulerGroup _schedulerGroup;

  public SchedulerQueryContext(@Nonnull ServerQueryRequest queryRequest) {
    Preconditions.checkNotNull(queryRequest);
    _queryRequest = queryRequest;
    _resultFuture = SettableFuture.create();
  }

  @Nonnull
  public ServerQueryRequest getQueryRequest() {
    return _queryRequest;
  }

  @Nonnull
  public SettableFuture<byte[]> getResultFuture() {
    return _resultFuture;
  }

  public void setResultFuture(ListenableFuture<byte[]> f) {
    _resultFuture.setFuture(f);
  }

  public void setSchedulerGroupContext(SchedulerGroup schedulerGroup) {
    _schedulerGroup = schedulerGroup;
  }

  @Nullable
  public SchedulerGroup getSchedulerGroup() {
    return _schedulerGroup;
  }

  /**
   * Convenience method to get query arrival time
   * @return
   */
  public long getArrivalTimeMs() {
    return _queryRequest.getTimerContext().getQueryArrivalTimeMs();
  }
}
