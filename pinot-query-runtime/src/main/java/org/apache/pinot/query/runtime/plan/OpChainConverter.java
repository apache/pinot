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
package org.apache.pinot.query.runtime.plan;

import com.google.auto.service.AutoService;
import java.util.function.BiConsumer;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.OpChain;


/**
 * Extension point for alternative plan-to-{@link OpChain} converters. The active implementation is chosen by
 * {@link OpChainConverterDispatcher}: highest {@link #priority()} wins unless an override is set (for product
 * extensions that pin a specific converter at process startup).
 */
public interface OpChainConverter {
  /**
   * Returns the converter identifier handled by this provider (case-insensitive for lookup; typically lowercase).
   */
  String converterId();

  /**
   * Relative preference when more than one converter is registered. The converter with the largest value is used
   * unless {@link OpChainConverterDispatcher#setActiveConverterIdOverride} is set. Ties break lexicographically by
   * {@link #converterId()} (ascending).
   *
   * It is recommended to use multiples of 1000 for the priority, as that gives more flexibility to other
   * converters to be registered with different priorities.
   */
  default int priority() {
    return 0;
  }

  /**
   * Converts the plan node into an opchain for this converter.
   *
   * @param context The context of the conversion, which includes metadata and other information that may be needed
   *                during the conversion process.
   * @param tracker A tracker that must be called every time a PlanNode is converted into an operator, so the caller can
   *                keep track of this mapping.
   */
  OpChain convert(PlanNode node, OpChainExecutionContext context, BiConsumer<PlanNode, MultiStageOperator> tracker);

  @AutoService(OpChainConverter.class)
  class DefaultOpChainConverter implements OpChainConverter {
    public static final String CONVERTER_ID = "DEFAULT";

    @Override
    public String converterId() {
      return CONVERTER_ID;
    }

    @Override
    public OpChain convert(PlanNode node, OpChainExecutionContext context,
        BiConsumer<PlanNode, MultiStageOperator> tracker) {
      return PlanNodeToOpChain.convert(node, context, tracker);
    }
  }
}
