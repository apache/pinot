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

import java.util.ArrayDeque;
import java.util.Deque;
import org.apache.pinot.spi.trace.InvocationRecording;
import org.apache.pinot.spi.trace.TraceState;


final class TracedThread extends Thread implements TraceState {

  private long _traceId = Long.MIN_VALUE;
  private int _counter;
  private final Deque<InvocationRecording> _stack = new ArrayDeque<>();

  public TracedThread(Runnable target) {
    super(target);
  }

  @Override
  public void setTraceId(long traceId) {
    _traceId = traceId;
  }

  @Override
  public int getAndIncrementCounter() {
    return _counter++;
  }

  @Override
  public void resetCounter() {
    _counter = 0;
  }

  @Override
  public Deque<InvocationRecording> getRecordings() {
    return _stack;
  }

  @Override
  public long getTraceId() {
    return _traceId;
  }
}
