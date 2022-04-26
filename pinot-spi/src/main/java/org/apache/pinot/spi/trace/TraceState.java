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
package org.apache.pinot.spi.trace;

import java.util.Deque;


/**
 * Encapsulation of thread-local state used for tracing. The implementor must ensure this is not passed
 * across threads.
 */
public interface TraceState {

  /**
   * The trace ID - corresponds to a single request or query
   */
  long getTraceId();

  /**
   * Set the trace ID
   */
  void setTraceId(long traceId);

  /**
   * returns and increments a counter which can be used for labeling events.
   *
   */
  int getAndIncrementCounter();

  /**
   * Sets the counter to its undefined base value.
   */
  void resetCounter();

  /**
   * Get the stack of recordings. The implementor is responsible for ensuring only recordings started on the current
   * thread are added to this stack.
   */
  Deque<InvocationRecording> getRecordings();
}
