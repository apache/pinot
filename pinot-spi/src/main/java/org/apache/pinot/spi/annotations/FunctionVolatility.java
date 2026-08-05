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
package org.apache.pinot.spi.annotations;


/// Describes how a scalar function's result or side effects can vary across invocations.
///
/// The categories follow
/// [PostgreSQL's function volatility model](https://www.postgresql.org/docs/current/xfunc-volatility.html):
///
/// - `IMMUTABLE`: the result depends only on the explicit arguments and never changes for the same inputs.
/// - `STABLE`: the result is constant for the same arguments within one query, but can change between queries.
/// - `VOLATILE`: the result can change on every invocation, or the function has side effects.
///
/// This semantic metadata is interpreted by context-specific policies and does not replace
/// [ScalarFunction#isDeterministic()], Pinot's existing compile-time query evaluation hint.
public enum FunctionVolatility {
  IMMUTABLE,
  STABLE,
  VOLATILE
}
