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
package org.apache.pinot.calcite.rel.metadata;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;


/// Metadata provider for Pinot query planning that places Pinot-aware handlers ahead of Calcite's
/// [DefaultRelMetadataProvider].
///
/// ### Provider chain
/// 1. [PinotRelMdSelectivity] — time-range and NDV-based selectivity estimation.
/// 1. [DefaultRelMetadataProvider#INSTANCE] — Calcite's built-in fallback for all other
///    metadata (including row counts, collation, uniqueness, etc.).
///
/// ### Row-count
/// No custom `RelMdRowCount` is needed: Calcite's default
/// `RelMdRowCount.getRowCount(TableScan, mq)` calls `TableScan.estimateRowCount(mq)`
/// → `RelOptTableImpl.getRowCount()` → `PinotTable.getStatistic().getRowCount()`,
/// which already surfaces the statistics-provider value.
///
/// ### NoOp guard
/// When the provider is a no-op (methods return `null` / empty), all enhanced paths fall
/// back to Calcite defaults — there is no behavior change compared to using
/// [DefaultRelMetadataProvider#INSTANCE] alone.
///
/// ### Janino / caching notes
/// The [PinotRelMdSelectivity] handler is stateless; its [PinotStatisticsProvider]
/// is resolved at call time via [org.apache.pinot.query.catalog.PinotTable], which is
/// obtained from the scan's [org.apache.calcite.plan.RelOptTable]. Accordingly a single
/// [#INSTANCE] is safe to share globally, and Janino only compiles handler classes once.
///
/// ### Thread-safety
/// The singleton instance is effectively immutable once constructed. Safe for concurrent use.
public final class PinotDefaultRelMetadataProvider extends ChainedRelMetadataProvider {

  /// Global singleton. Because [PinotRelMdSelectivity] is stateless, all query environments
  /// share this single provider — Janino compiles the handler bytecode only once.
  public static final PinotDefaultRelMetadataProvider INSTANCE =
      new PinotDefaultRelMetadataProvider();

  private PinotDefaultRelMetadataProvider() {
    super(ImmutableList.of(
        ReflectiveRelMetadataProvider.reflectiveSource(
            new PinotRelMdSelectivity(),
            BuiltInMetadata.Selectivity.Handler.class),
        DefaultRelMetadataProvider.INSTANCE));
  }
}
