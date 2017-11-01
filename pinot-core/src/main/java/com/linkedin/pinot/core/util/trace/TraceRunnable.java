/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.util.trace;

import com.linkedin.pinot.common.request.InstanceRequest;
import javax.annotation.Nullable;


/**
 * Wrap a {@link Runnable} so that the thread executes this job
 * will be automatically registered/unregistered to/from a request.
 *
 */
public abstract class TraceRunnable implements Runnable {
  private final InstanceRequest _instanceRequest;
  private final Trace _parent;

  public TraceRunnable() {
    this(TraceContext.getRequestForCurrentThread(), TraceContext.getLocalTraceForCurrentThread());
  }

  /**
   * If trace is not enabled, both instanceRequest and parent will be null.
   */
  private TraceRunnable(@Nullable InstanceRequest instanceRequest, @Nullable Trace parent) {
    _instanceRequest = instanceRequest;
    _parent = parent;
  }

  @Override
  public void run() {
    if (_instanceRequest != null) {
      TraceContext.registerThreadToRequest(_instanceRequest, _parent);
    }
    try {
      runJob();
    } finally {
      if (_instanceRequest != null) {
        TraceContext.unregisterThreadFromRequest();
      }
    }
  }

  public abstract void runJob();
}
