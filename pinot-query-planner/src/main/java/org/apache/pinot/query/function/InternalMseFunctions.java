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
package org.apache.pinot.query.function;

import org.apache.pinot.query.MseWorkerThreadContext;
import org.apache.pinot.spi.annotations.ScalarFunction;


public class InternalMseFunctions {
  private InternalMseFunctions() {
  }

  /// Returns the [stage id][MseWorkerThreadContext#getStageId()] of the query or -1 if the MSE worker context is not,
  /// which is normal in SSE queries or when the expression is executed in the broker (ie during simplification)
  ///
  /// The input value is not directly used. Instead it is here to control whether the function is called during query
  /// optimization or execution. In order to do the latter, a non-constant value (like a column) should be passed as
  /// input.
  ///
  /// This is mostly useful for test and internal usage
  @ScalarFunction
  public static int stageId(String input) {
    if (!MseWorkerThreadContext.isInitialized()) {
      return -1;
    }
    return MseWorkerThreadContext.getStageId();
  }

  /// Returns the [worker id][MseWorkerThreadContext#getWorkerId()] of the query or -1 if the MSE worker context is not,
  /// which is normal in SSE queries or when the expression is executed in the broker (ie during simplification)
  ///
  /// The input value is not directly used. Instead it is here to control whether the function is called during query
  /// optimization or execution. In order to do the latter, a non-constant value (like a column) should be passed as
  /// input.
  ///
  /// This is mostly useful for test and internal usage
  @ScalarFunction
  public static int workerId(String input) {
    if (!MseWorkerThreadContext.isInitialized()) {
      return -1;
    }
    return MseWorkerThreadContext.getWorkerId();
  }
}
