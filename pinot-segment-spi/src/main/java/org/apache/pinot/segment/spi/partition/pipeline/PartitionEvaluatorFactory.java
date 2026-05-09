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
package org.apache.pinot.segment.spi.partition.pipeline;

import org.apache.pinot.spi.function.FunctionEvaluator;


/// SPI for compiling a partition expression string into a [FunctionEvaluator].
///
/// Implementations are loaded via [java.util.ServiceLoader]. At least one implementation must be present on
/// the classpath at runtime. If multiple are found (common in shaded-jar / multi-module test setups), the loader
/// prefers `InbuiltPartitionEvaluatorFactory` when present; otherwise it picks the first discovered factory and
/// logs the choice. The default implementation in `pinot-common` is
/// `org.apache.pinot.common.evaluator.InbuiltPartitionEvaluatorFactory`, which delegates to
/// `org.apache.pinot.common.evaluator.PartitionFunctionEvaluator`.
public interface PartitionEvaluatorFactory {

  /// Compiles a partition expression into a [FunctionEvaluator].
  ///
/// Validates that the expression:
///
/// - References exactly one column: `rawColumn`
/// - Uses only deterministic scalar functions
///
/// @throws IllegalArgumentException if the expression is invalid
  FunctionEvaluator compile(String rawColumn, String expression);
}
