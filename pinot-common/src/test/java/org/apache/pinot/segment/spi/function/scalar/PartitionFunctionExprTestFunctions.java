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
package org.apache.pinot.segment.spi.function.scalar;

import java.math.BigDecimal;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.pinot.spi.annotations.ScalarFunction;


/// Test-only scalar functions used to exercise the partition-expression compiler in `pinot-common` tests.
///
/// Only functions that are NOT already registered in Pinot's production
/// [org.apache.pinot.common.function.FunctionRegistry] should be added here. Hash functions
/// (`md5`, `murmur2`, `fnv1a_32`, etc.) are provided by `HashFunctions` and must not be
/// re-registered.
public final class PartitionFunctionExprTestFunctions {
  private PartitionFunctionExprTestFunctions() {
  }

  @ScalarFunction
  public static int identity(int value) {
    return value;
  }

  @ScalarFunction
  public static long bucket(long value, long divisor) {
    return value / divisor;
  }

  @ScalarFunction
  public static BigDecimal fractionalBigDecimal(long value) {
    return BigDecimal.valueOf(value).add(new BigDecimal("0.5"));
  }

  @ScalarFunction(isDeterministic = false)
  public static long randomBucket(long value) {
    return value + ThreadLocalRandom.current().nextLong();
  }
}
