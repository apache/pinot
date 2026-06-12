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

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.routing.StagePlan;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Dispatches physical plan conversion to the active {@link OpChainConverter}. The active instance is resolved from
 * {@link ServiceLoader} when the JVM loads this class and whenever {@link #setActiveConverterIdOverride} runs (intended
 * to be rare, e.g. on config change).
 */
public final class OpChainConverterDispatcher {
  private static final Logger LOGGER = LoggerFactory.getLogger(OpChainConverterDispatcher.class);

  private static volatile OpChainConverter _active;

  static {
    _active = resolveActiveConverter(null);
    LOGGER.info("Active OpChainConverter: {}", _active.converterId());
  }

  private OpChainConverterDispatcher() {
  }

  public static OpChain convert(PlanNode node, OpChainExecutionContext context) {
    return convert(node, context, (planNode, operator) -> {
      // no-op tracker
    });
  }

  /**
   * Re-resolves converters from {@link ServiceLoader} and sets the active one to the given
   * {@link OpChainConverter#converterId()} (case-insensitive), or to the highest-{@link OpChainConverter#priority()}
   * implementation when {@code null}.
   */
  public static void setActiveConverterIdOverride(@Nullable String converterId) {
    _active = resolveActiveConverter(converterId);
    if (converterId != null) {
      LOGGER.info("Active OpChainConverter: {} (pinned by override)", _active.converterId());
    } else if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Active OpChainConverter: {} (priority selection)", _active.converterId());
    }
  }

  public static OpChain convert(PlanNode node, OpChainExecutionContext context,
      BiConsumer<PlanNode, MultiStageOperator> tracker) {
    return _active.convert(node, context, tracker);
  }

  public static OpChain sendEarlyError(OpChainExecutionContext context, StagePlan stagePlan,
      ErrorMseBlock errorBlock) {
    return _active.sendEarlyError(context, stagePlan, errorBlock);
  }

  private static OpChainConverter resolveActiveConverter(@Nullable String overrideConverterId) {
    Map<String, OpChainConverter> byId = loadConvertersById();
    String selectedId;
    if (overrideConverterId == null) {
      selectedId = selectConverterIdByPriority(byId);
    } else {
      String normalized = overrideConverterId.toLowerCase(Locale.ENGLISH);
      if (!byId.containsKey(normalized)) {
        throw new IllegalArgumentException("No OpChainConverter registered for converterId=" + overrideConverterId);
      }
      selectedId = normalized;
    }
    return byId.get(selectedId);
  }

  private static Map<String, OpChainConverter> loadConvertersById() {
    Map<String, OpChainConverter> converters = new HashMap<>();
    for (OpChainConverter converter : ServiceLoader.load(OpChainConverter.class)) {
      String converterId = converter.converterId();
      if (StringUtils.isBlank(converterId)) {
        LOGGER.warn("Skipping OpChainConverter {} because converterId() is blank",
            converter.getClass().getName());
        continue;
      }
      String key = converterId.toLowerCase(Locale.ENGLISH);
      converters.putIfAbsent(key, converter);
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Loaded {} OpChainConverter binding(s)", converters.size());
    }
    return converters;
  }

  private static String selectConverterIdByPriority(Map<String, OpChainConverter> convertersById) {
    String bestId = null;
    int bestPriority = Integer.MIN_VALUE;
    for (Map.Entry<String, OpChainConverter> entry : convertersById.entrySet()) {
      String id = entry.getKey();
      int priority = entry.getValue().priority();
      if (bestId == null || priority > bestPriority
          || (priority == bestPriority && id.compareTo(bestId) < 0)) {
        bestId = id;
        bestPriority = priority;
      }
    }
    if (bestId == null) {
      throw new IllegalStateException("No OpChainConverter implementations registered");
    }
    return bestId;
  }
}
