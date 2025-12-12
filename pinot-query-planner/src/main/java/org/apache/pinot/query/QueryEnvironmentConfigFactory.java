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
package org.apache.pinot.query;

import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.query.routing.WorkerManager;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;

/// A factory class to create QueryEnvironment.Config instances.
///
/// This includes the ability to cus
public class QueryEnvironmentConfigFactory {
  private QueryEnvironmentConfigFactory() {
  }

  public static ImmutableQueryEnvironment.Config create(
      String database, Map<String, String> queryOptions,
      long requestId, PinotConfiguration config, TableCache tableCache, WorkerManager workerManager,
      @Nullable Set<String> defaultDisabledPlannerRules) {
    boolean inferPartitionHint = config.getProperty(CommonConstants.Broker.CONFIG_OF_INFER_PARTITION_HINT,
        CommonConstants.Broker.DEFAULT_INFER_PARTITION_HINT);
    boolean defaultUseSpool = config.getProperty(CommonConstants.Broker.CONFIG_OF_SPOOLS,
        CommonConstants.Broker.DEFAULT_OF_SPOOLS);
    boolean defaultUseLeafServerForIntermediateStage = config.getProperty(
        CommonConstants.Broker.CONFIG_OF_USE_LEAF_SERVER_FOR_INTERMEDIATE_STAGE,
        CommonConstants.Broker.DEFAULT_USE_LEAF_SERVER_FOR_INTERMEDIATE_STAGE);
    boolean defaultEnableGroupTrim = config.getProperty(CommonConstants.Broker.CONFIG_OF_MSE_ENABLE_GROUP_TRIM,
        CommonConstants.Broker.DEFAULT_MSE_ENABLE_GROUP_TRIM);
    boolean defaultEnableDynamicFilteringSemiJoin = config.getProperty(
        CommonConstants.Broker.CONFIG_OF_BROKER_ENABLE_DYNAMIC_FILTERING_SEMI_JOIN,
        CommonConstants.Broker.DEFAULT_ENABLE_DYNAMIC_FILTERING_SEMI_JOIN);
    boolean defaultUsePhysicalOptimizer = config.getProperty(
        CommonConstants.Broker.CONFIG_OF_USE_PHYSICAL_OPTIMIZER,
        CommonConstants.Broker.DEFAULT_USE_PHYSICAL_OPTIMIZER);
    boolean defaultUseLiteMode = config.getProperty(
        CommonConstants.Broker.CONFIG_OF_USE_LITE_MODE,
        CommonConstants.Broker.DEFAULT_USE_LITE_MODE);
    boolean defaultRunInBroker = config.getProperty(
        CommonConstants.Broker.CONFIG_OF_RUN_IN_BROKER,
        CommonConstants.Broker.DEFAULT_RUN_IN_BROKER);
    boolean defaultUseBrokerPruning = config.getProperty(
        CommonConstants.Broker.CONFIG_OF_USE_BROKER_PRUNING,
        CommonConstants.Broker.DEFAULT_USE_BROKER_PRUNING);
    int defaultLiteModeLeafStageLimit = config.getProperty(
        CommonConstants.Broker.CONFIG_OF_LITE_MODE_LEAF_STAGE_LIMIT,
        CommonConstants.Broker.DEFAULT_LITE_MODE_LEAF_STAGE_LIMIT);
    int defaultLiteModeFanoutAdjustedLimit = config.getProperty(
        CommonConstants.Broker.CONFIG_OF_LITE_MODE_LEAF_STAGE_FANOUT_ADJUSTED_LIMIT,
        CommonConstants.Broker.DEFAULT_LITE_MODE_LEAF_STAGE_FAN_OUT_ADJUSTED_LIMIT);
    boolean defaultLiteModeEnableJoins = config.getProperty(
        CommonConstants.Broker.CONFIG_OF_LITE_MODE_ENABLE_JOINS,
        CommonConstants.Broker.DEFAULT_LITE_MODE_ENABLE_JOINS);
    String defaultHashFunction = config.getProperty(
        CommonConstants.Broker.CONFIG_OF_BROKER_DEFAULT_HASH_FUNCTION,
        CommonConstants.Broker.DEFAULT_BROKER_DEFAULT_HASH_FUNCTION);
    boolean caseSensitive = !config.getProperty(
        CommonConstants.Helix.ENABLE_CASE_INSENSITIVE_KEY,
        CommonConstants.Helix.DEFAULT_ENABLE_CASE_INSENSITIVE
    );
    int sortExchangeCopyThreshold = getValue(queryOptions, config,
        CommonConstants.Broker.Request.QueryOptionKey.SORT_EXCHANGE_COPY_THRESHOLD,
        CommonConstants.Broker.CONFIG_OF_SORT_EXCHANGE_COPY_THRESHOLD, -1);

    return QueryEnvironment.configBuilder()
        .requestId(requestId)
        .database(database)
        .tableCache(tableCache)
        .workerManager(workerManager)
        .isCaseSensitive(caseSensitive)
        .isNullHandlingEnabled(QueryOptionsUtils.isNullHandlingEnabled(queryOptions))
        .defaultInferPartitionHint(inferPartitionHint)
        .defaultUseSpools(defaultUseSpool)
        .defaultUseLeafServerForIntermediateStage(defaultUseLeafServerForIntermediateStage)
        .defaultEnableGroupTrim(defaultEnableGroupTrim)
        .defaultEnableDynamicFilteringSemiJoin(defaultEnableDynamicFilteringSemiJoin)
        .defaultUsePhysicalOptimizer(defaultUsePhysicalOptimizer)
        .defaultUseLiteMode(defaultUseLiteMode)
        .defaultRunInBroker(defaultRunInBroker)
        .defaultUseBrokerPruning(defaultUseBrokerPruning)
        .defaultLiteModeLeafStageLimit(defaultLiteModeLeafStageLimit)
        .defaultLiteModeLeafStageFanOutAdjustedLimit(defaultLiteModeFanoutAdjustedLimit)
        .defaultLiteModeEnableJoins(defaultLiteModeEnableJoins)
        .defaultHashFunction(defaultHashFunction)
        .defaultDisabledPlannerRules(defaultDisabledPlannerRules)
        .sortExchangeCopyLimit(sortExchangeCopyThreshold)
        .build();
  }

  private static int getValue(Map<String, String> queryOptions, PinotConfiguration config,
      String optionKey, String confKey, int defaultValue) {
    return queryOptions.containsKey(optionKey)
        ? Integer.parseInt(queryOptions.get(optionKey))
        : config.getProperty(confKey, defaultValue);
  }
}
