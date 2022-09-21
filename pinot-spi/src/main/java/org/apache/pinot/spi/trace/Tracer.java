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

/**
 * Tracer responsible for trace state management. Users can implement this to filter
 * what data is recorded, choose what format it is reported in (e.g. logs, JFR events, in-memory)
 * and where it goes.
 */
public interface Tracer {

  /**
   * Registers the requestId on the current thread. This means the request will be traced.
   * TODO: Consider using string id or random id. Currently different broker might send query with the same request id.
   *
   * @param requestId the requestId
   */
  void register(long requestId);

  /**
   * Detach a trace from the current thread.
   */
  void unregister();

  /**
   *
   * @param clazz the enclosing context, e.g. Operator, PlanNode, BlockValSet...
   * @return a new scope which MUST be closed on the current thread.
   */
  InvocationScope createScope(Class<?> clazz);

  /**
   * Starts
   * @return the request record
   */
  default RequestScope createRequestScope() {
    return new DefaultRequestContext();
  }

  /**
   * @return the active recording
   */
  InvocationRecording activeRecording();
}
