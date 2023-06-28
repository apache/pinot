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
package org.apache.calcite.rel.logical;

/** Type of exchange. */
public enum PinotRelExchangeType {

  /**
   * A streaming exchange is one that data will be sent and received in a streaming fashion.
   */
  STREAMING,

  /**
   * A sub-plan exchange is one that multi-stage query plan will execute the sub-tree below the exchange first; then
   * treat the result as a {@link org.apache.calcite.rex.RexLiteral}; and put back into the logical plan for further
   * optimization from the {@link org.apache.calcite.plan.RelOptPlanner}.
   *
   * <p>This is useful when plan can be further optimized if a sub plan is executed first from the broker.</p>
   */
  SUB_PLAN,

  /**
   * A pipeline-breaker is one that data will be sent streamingly from sender, but receiver will not start execution
   * until all data has been received successfully.
   *
   * <p>This is useful when logical plan is fixed, but physical plan can be further optimized on the server.</p>
   */
  PIPELINE_BREAKER;

  public static PinotRelExchangeType getDefaultExchangeType() {
    return STREAMING;
  }
}
