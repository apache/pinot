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
package org.apache.pinot.common.materializedview;

import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/// Single source of truth for the aggregation functions an MV definedSQL is allowed to use,
/// and the {@link DataType} the MV's storage column for that aggregation MUST be declared as.
///
/// This catalog is consumed by:
///
///   - the MV analyzer (allow-list check at create / update time), and
///   - the MV schema inferer (picks the column DataType when the user omits explicit column
///     definitions in {@code CREATE MATERIALIZED VIEW}).
///
/// Both consumers must agree on the supported set, otherwise an MV that the inferer happily
/// builds could later be rejected by the analyzer (or, worse, accepted and then never picked
/// up by the rewrite engine because the MV column type doesn't match what the rewrite expects).
///
/// <h3>Why it lives in {@code pinot-common}</h3>
///
/// The schema inferer lives in {@code pinot-sql-ddl}, which must not depend on
/// {@code pinot-materialized-view} in production code. {@code pinot-common} is the
/// lowest-level module both consumers already depend on.
///
/// <h3>Sketch types: deliberate divergence from both engines' surface types</h3>
///
/// Both Pinot engines surface {@code STRING} for raw-sketch aggregations:
///
///   - The v1 (single-stage) engine's {@code DistinctCountRaw{HLL,HLLPlus,ThetaSketch}AggregationFunction}
///     return {@code STRING} from {@code getFinalResultColumnType()}.
///   - The v2 (multi-stage) engine's Calcite return-type inference (via
///     {@code PinotReturnTypeInferenceUtils} / {@code SqlReturnTypeInference}) likewise reports
///     {@code STRING} for these aggregations.
///
/// In both cases the {@code STRING} is the *display* type — the hex-encoded scalar response a
/// SQL client receives. It is not the type of the underlying sketch the MV needs to store.
///
/// For an MV column the right type is {@code BYTES}: that is the only shape the rewrite engine
/// can re-aggregate (each row's stored value is a serialized sketch that gets merged with the
/// running accumulator). Storing {@code STRING} would silently route the rewrite through the
/// "hash the literal" branch of {@code DistinctCountHLLAggregationFunction}, producing wrong
/// answers without any error.
///
/// This is the source of truth: the
/// {@code MaterializedViewSchemaInferer} consults this catalog rather than reading
/// the engine's surface type for each aggregation, so the inferer is correct even though the
/// MSE validator would naively report {@code STRING} for these three operators.
///
/// {@code MaterializedViewTaskExecutor#decodeBytesValue} already converts the incoming
/// hex-encoded string from the source query response into raw bytes when the destination
/// schema column is {@code BYTES}, so this divergence is end-to-end consistent — but it is
/// load-bearing and must not be "fixed" to match either engine's surface type.
///
/// The static regression test on this class pins both the supported-set and the type mapping
/// so that the divergence cannot be silently undone by a future refactor.
public final class MaterializedViewAggregationCatalog {

  private MaterializedViewAggregationCatalog() {
  }

  /// Maps canonical (uppercase) aggregation operator name to the {@link DataType} an MV
  /// schema column produced by that aggregation MUST be declared as, for MV ingestion AND
  /// rewrite to remain semantically correct.
  ///
  /// Iteration order is insertion order (see {@link Map#of} on Java 11+: no guarantee, but
  /// callers should not rely on it). Use {@link #getSupportedOperators()} for the allow-list
  /// check, {@link #getInferredDataType(String)} for the type lookup.
  ///
  /// See the class javadoc for the BYTES-vs-STRING explanation on the sketch entries.
  public static final Map<String, DataType> SUPPORTED_AGGREGATIONS = Map.of(
      "SUM", DataType.DOUBLE,
      "MIN", DataType.DOUBLE,
      "MAX", DataType.DOUBLE,
      "COUNT", DataType.LONG,
      "DISTINCTCOUNTRAWHLL", DataType.BYTES,
      "DISTINCTCOUNTRAWHLLPLUS", DataType.BYTES,
      "DISTINCTCOUNTRAWTHETASKETCH", DataType.BYTES
  );

  /// Returns the canonicalised set of aggregation operator names this catalog supports.
  /// Suitable as the allow-list for an MV analyzer's "is this aggregation re-aggregatable?"
  /// check.
  public static Set<String> getSupportedOperators() {
    return SUPPORTED_AGGREGATIONS.keySet();
  }

  /// Returns true iff {@code operator} (case-insensitive) is one of the catalog's supported
  /// aggregation function names.
  public static boolean isSupported(String operator) {
    if (operator == null) {
      return false;
    }
    return SUPPORTED_AGGREGATIONS.containsKey(operator.toUpperCase(Locale.ROOT));
  }

  /// Returns the {@link DataType} an MV schema column produced by {@code operator} must be
  /// declared as, or {@code null} if {@code operator} is not in the catalog.
  ///
  /// Callers should typically pre-check membership with {@link #isSupported(String)} so the
  /// "unsupported aggregation" error path can carry a clear, single-shot message rather than
  /// a downstream {@link NullPointerException}.
  public static DataType getInferredDataType(String operator) {
    if (operator == null) {
      return null;
    }
    return SUPPORTED_AGGREGATIONS.get(operator.toUpperCase(Locale.ROOT));
  }
}
