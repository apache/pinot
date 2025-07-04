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
package org.apache.pinot.common.function.scalar;

import javax.annotation.Nullable;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.spi.query.QueryThreadContext;


public class InternalFunctions {
  private InternalFunctions() {
  }

  /// Returns the [correlation id][QueryThreadContext#getCid] of the query.
  ///
  /// The input value is not directly used. Instead it is here to control whether the function is called during query
  /// optimization or execution. In order to do the latter, a non-constant value (like a column) should be passed as
  /// input.
  ///
  /// This is mostly useful for test and internal usage
  @ScalarFunction
  public static String cid(String input) {
    return QueryThreadContext.getCid();
  }

  /// Returns the [request id][QueryThreadContext#getRequestId] of the query.
  ///
  /// The input value is not directly used. Instead it is here to control whether the function is called during query
  /// optimization or execution. In order to do the latter, a non-constant value (like a column) should be passed as
  /// input.
  ///
  /// This is mostly useful for test and internal usage
  @ScalarFunction
  public static long reqId(String input) {
    return QueryThreadContext.getRequestId();
  }

  /// Returns the [start time][QueryThreadContext#getStartTimeMs] of the query.
  ///
  /// The input value is not directly used. Instead it is here to control whether the function is called during query
  /// optimization or execution. In order to do the latter, a non-constant value (like a column) should be passed as
  /// input.
  ///
  /// This is mostly useful for test and internal usage and should be close to now()
  @ScalarFunction
  public static long startTime(String input) {
    return QueryThreadContext.getStartTimeMs();
  }

  /// Returns the [deadline][QueryThreadContext#getDeadlineMs] of the query.
  ///
  /// The input value is not directly used. Instead it is here to control whether the function is called during query
  /// optimization or execution. In order to do the latter, a non-constant value (like a column) should be passed as
  /// input.
  ///
  /// This is mostly useful for test and internal usage
  @ScalarFunction
  public static long endTime(String input) {
    return QueryThreadContext.getDeadlineMs();
  }

  ///  Returns the [broker id][QueryThreadContext#getBrokerId] of the query.
  ///
  /// The input value is not directly used. Instead it is here to control whether the function is called during query
  /// optimization or execution. In order to do the latter, a non-constant value (like a column) should be passed as
  /// input.
  ///
  /// This is mostly useful for test and internal usage
  @ScalarFunction
  @Nullable
  public static String brokerId(String input) {
    return QueryThreadContext.getBrokerId();
  }

  /// Returns the [query engine][QueryThreadContext#getQueryEngine] of the query.
  ///
  /// This is mostly useful for test and internal usage
  @ScalarFunction
  @Nullable
  public static String queryEngine(String input) {
    return QueryThreadContext.getQueryEngine();
  }
}
