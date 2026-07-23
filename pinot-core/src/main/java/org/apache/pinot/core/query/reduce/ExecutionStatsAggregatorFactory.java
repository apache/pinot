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
package org.apache.pinot.core.query.reduce;

/// Factory seam for [ExecutionStatsAggregator], letting downstream projects supply an aggregator
/// that reduces additional, project-specific per-query statistics carried in the server
/// [org.apache.pinot.common.datatable.DataTable] metadata. The default implementation returns the
/// stock aggregator, preserving core behavior when no custom factory is installed.
///
/// Install a custom factory via [#setInstance(ExecutionStatsAggregatorFactory)] during broker
/// startup; the reduce services obtain aggregators through [#create(boolean)].
public interface ExecutionStatsAggregatorFactory {
  ExecutionStatsAggregatorFactory DEFAULT = ExecutionStatsAggregator::new;

  ExecutionStatsAggregator create(boolean enableTrace);

  final class Holder {
    private static volatile ExecutionStatsAggregatorFactory _instance = DEFAULT;

    private Holder() {
    }

    static void set(ExecutionStatsAggregatorFactory instance) {
      _instance = instance;
    }

    static ExecutionStatsAggregatorFactory get() {
      return _instance;
    }
  }

  static void setInstance(ExecutionStatsAggregatorFactory instance) {
    Holder.set(instance);
  }

  static ExecutionStatsAggregator createAggregator(boolean enableTrace) {
    return Holder.get().create(enableTrace);
  }
}
