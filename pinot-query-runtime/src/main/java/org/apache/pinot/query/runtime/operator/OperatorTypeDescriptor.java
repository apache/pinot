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
package org.apache.pinot.query.runtime.operator;

import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.response.broker.BrokerResponseNativeV2;


/**
 * SPI for MSE operator type descriptors. Built-in types are the entries of {@link MultiStageOperator.Type}. Plugin
 * authors can implement this interface and register descriptors via
 * {@code META-INF/services/org.apache.pinot.query.runtime.operator.OperatorTypeDescriptor} to extend the set of known
 * operator types without modifying core.
 *
 * <p>Instances are registered in {@link OperatorTypeRegistry} at startup. Descriptors should be effectively singletons
 * within a JVM — the registry stores one instance per id, and stage-stats equality checks
 * ({@link org.apache.pinot.query.runtime.plan.MultiStageQueryStats.StageStats#equals}) rely on
 * {@link Object#equals} comparing the two descriptor lists element-by-element. Built-in enum constants satisfy this
 * naturally via identity equality; plugin implementations should either be singletons or override {@link #equals}.
 */
public interface OperatorTypeDescriptor {

  /**
   * First id available to plugin-defined operator types. Built-in types ({@link MultiStageOperator.Type}) use ids 0–15;
   * ids 16–255 are reserved for future built-ins. Plugin ids must be &ge; this value.
   */
  int PLUGIN_ID_FLOOR = 256;

  /**
   * Stable numeric id used in the gRPC wire format and the legacy binary stat format. Must be unique across all
   * descriptors loaded in the same JVM.
   *
   * <p>Built-in types use ids 0–255. Plugin types must use ids &ge; {@value #PLUGIN_ID_FLOOR}.
   */
  int getId();

  /** Display name used in logs and stats JSON output. */
  String name();

  /**
   * Returns the key class for the operator's {@link StatMap}.
   *
   * <p>The returned class must be {@code Class<K>} where {@code K extends Enum<K> & StatMap.Key}. The generic
   * parameter is erased at runtime, so the raw {@code Class} type is used here to remain compatible with the
   * {@link MultiStageOperator.Type} enum's existing {@code getStatKeyClass()} signature.
   */
  @SuppressWarnings("rawtypes")
  Class getStatKeyClass();

  /**
   * Merges this operator's stats into the broker response. Each implementation casts {@code map} to its specific
   * {@link StatMap} key type (which is guaranteed by the serialization contract that the key class matches
   * {@link #getStatKeyClass()}).
   */
  void mergeInto(BrokerResponseNativeV2 response, StatMap<?> map);

  /**
   * Updates server-side metrics from this operator's stats. Default implementation is a no-op; override when the
   * operator produces metrics that should be recorded on the server at query completion.
   */
  default void updateServerMetrics(StatMap<?> map, ServerMetrics serverMetrics) {
  }
}
