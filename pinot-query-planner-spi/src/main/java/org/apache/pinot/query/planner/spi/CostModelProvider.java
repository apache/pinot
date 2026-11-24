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
package org.apache.pinot.query.planner.spi;

import java.util.Map;
import org.apache.calcite.plan.RelOptCostFactory;


/// Supplies the cost factory and per-query statistics used by a cost-based
/// optimizer.
///
/// Instances are created per query via
/// [QueryOptimizerProvider#createCostModelProvider()] and configured with the
/// query's options before planning. This isolates per-query state from other
/// concurrent queries.
///
/// A configured instance is exposed to the optimizer's metadata handlers via
/// the planner context: `context.unwrap(CostModelProvider.class)`. Handlers
/// should read per-query statistics from it at invocation time, keeping the
/// handler instances themselves query-agnostic so Calcite's metadata cache
/// stays warm (see [QueryOptimizerProvider#getMetadataProviders()]).
public interface CostModelProvider {

  /// Cost factory used by the optimizer's planner to compute node costs.
  RelOptCostFactory getCostFactory();

  /// Configures the cost model for a single query. The SPI deliberately does
  /// not define a statistics schema; each implementation parses the query
  /// options it understands.
  void configure(Map<String, String> queryOptions);
}
