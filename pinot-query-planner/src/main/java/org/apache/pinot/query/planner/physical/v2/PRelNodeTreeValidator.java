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
package org.apache.pinot.query.planner.physical.v2;

import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Window;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;


/**
 * Centralizes validations for the optimized PRelNode tree.
 */
public class PRelNodeTreeValidator {
  private static final BrokerMetrics BROKER_METRICS = BrokerMetrics.get();

  private PRelNodeTreeValidator() {
  }

  /**
   * Validate the tree rooted at the given PRelNode. Ideally all issues with the plan should be caught even with an
   * EXPLAIN, hence this method should be called as part of query compilation itself.
   */
  public static void validate(PRelNode rootNode) {
    // TODO(mse-physical): Add plan validations here.
  }

  /**
   * Emit metrics about the given plan tree. This should be avoided for Explain statements since metrics are not really
   * helpful there and can be misleading.
   */
  public static void emitMetrics(PRelNode pRelNode) {
    Context context = new Context();
    walk(pRelNode, context);
    if (context._joinCount > 0) {
      BROKER_METRICS.addMeteredGlobalValue(BrokerMeter.JOIN_COUNT, context._joinCount);
      BROKER_METRICS.addMeteredGlobalValue(BrokerMeter.QUERIES_WITH_JOINS, 1);
    }
    if (context._windowCount > 0) {
      BROKER_METRICS.addMeteredGlobalValue(BrokerMeter.WINDOW_COUNT, context._windowCount);
      BROKER_METRICS.addMeteredGlobalValue(BrokerMeter.QUERIES_WITH_WINDOW, 1);
    }
  }

  private static void walk(PRelNode pRelNode, Context context) {
    if (pRelNode.unwrap() instanceof Join) {
      context._joinCount++;
    } else if (pRelNode.unwrap() instanceof Window) {
      context._windowCount++;
    }
    for (PRelNode input : pRelNode.getPRelInputs()) {
      walk(input, context);
    }
  }

  private static class Context {
    int _joinCount = 0;
    int _windowCount = 0;
  }
}
