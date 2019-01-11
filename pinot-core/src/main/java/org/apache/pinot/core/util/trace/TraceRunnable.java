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
package org.apache.pinot.core.util.trace;

/**
 * Wrapper class for {@link Runnable} to automatically register/un-register itself to/from a request.
 */
public abstract class TraceRunnable implements Runnable {
  private final TraceContext.TraceEntry _parentTraceEntry;

  /**
   * If trace is not enabled, parent trace entry will be null.
   */
  public TraceRunnable() {
    _parentTraceEntry = TraceContext.getTraceEntry();
  }

  @Override
  public void run() {
    if (_parentTraceEntry != null) {
      TraceContext.registerThreadToRequest(_parentTraceEntry);
    }
    try {
      runJob();
    } finally {
      if (_parentTraceEntry != null) {
        TraceContext.unregisterThreadFromRequest();
      }
    }
  }

  public abstract void runJob();
}
