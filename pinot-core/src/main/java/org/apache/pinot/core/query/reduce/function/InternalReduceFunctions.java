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

package org.apache.pinot.core.query.reduce.function;

import org.apache.pinot.spi.annotations.ScalarFunction;


/**
 * This class contains functions that are necessary for the multistage engine
 * aggregations that need to be reduced after the initial aggregation to get
 * the final result.
 */
public class InternalReduceFunctions {

  private InternalReduceFunctions() {
  }

  @ScalarFunction
  public static Double avgReduce(Double intermediateResultSum, Long intermediateResultCount) {
    if (intermediateResultCount == null || intermediateResultCount == 0L || intermediateResultSum == null) {
      return null;
    }
    return intermediateResultSum / intermediateResultCount;
  }
}
