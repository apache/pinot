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
package org.apache.pinot.sql.ddl.compile;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// End-to-end DDL-compiler tests for the inferred-column path of `CREATE MATERIALIZED
/// VIEW` that exercise wiring without requiring the full Calcite/TableCache fixture.
///
/// Verifies that:
///
///   1. Errors from the inferer surface as {@link DdlCompilationException} (not raw
///      RuntimeException) so the controller layer can return a clean 400.
///   2. The legacy explicit-column path still works under the new entry point — i.e.
///      the dispatch in `compileCreateMaterializedView` is "branches on emptiness of
///      the column list" and not "always go through the inferer".
///
/// Happy-path inferred-column tests with full Calcite validation live alongside the
/// {@code MaterializedViewSchemaInferer} unit tests; they require a
/// {@code TableCache}-backed Calcite catalog that is too heavyweight for these
/// DDL-compiler entry-point smoke tests.
public class DdlCompilerMaterializedViewInferenceTest {

  // --------------------------------------------------------------------------------------------
  // Error surfaces
  // --------------------------------------------------------------------------------------------

  @Test
  public void missingTableCacheProducesActionableError() {
    DdlCompilationException ex = expectThrows(DdlCompilationException.class,
        () -> compileMv(
            "CREATE MATERIALIZED VIEW mv"
                + " REFRESH EVERY 1 DAY"
                + " PROPERTIES ('timeColumnName' = 'ts', 'bucketTimePeriod' = '1d')"
                + " AS SELECT ts FROM src", DdlCompileContext.STATELESS));
    // The inferer's "no TableCache" path produces a guidance message that points users at
    // either the TableCache wiring (controller bug) or the explicit column list (user bug).
    assertTrue(ex.getMessage().contains("TableCache")
        || ex.getMessage().contains("explicit column list"), ex.getMessage());
  }

  // --------------------------------------------------------------------------------------------
  // Dispatch: explicit-column path still works under the new entry point
  // --------------------------------------------------------------------------------------------

  /// Sanity check: an MV with an explicit column list must NOT touch the TableCache. The
  /// DDL must still compile under the STATELESS context (which has no TableCache), proving
  /// the inferer branch is gated on column-list emptiness.
  @Test
  public void explicitColumnListBypassesInferer() {
    CompiledCreateMaterializedView c = compileMv(
        "CREATE MATERIALIZED VIEW mv ("
            + "  ts TIMESTAMP DATETIME FORMAT '1:MILLISECONDS:TIMESTAMP' GRANULARITY '1:DAYS',"
            + "  carrier STRING"
            + ")"
            + " REFRESH EVERY 1 DAY"
            + " PROPERTIES ('timeColumnName' = 'ts', 'bucketTimePeriod' = '1d')"
            + " AS SELECT ts, carrier FROM src", DdlCompileContext.STATELESS);
    assertEquals(c.getSchema().size(), 2);
  }

  // --------------------------------------------------------------------------------------------
  // Helpers
  // --------------------------------------------------------------------------------------------

  private static CompiledCreateMaterializedView compileMv(String sql, DdlCompileContext ctx) {
    CompiledDdl c = DdlCompiler.compile(sql, ctx);
    assertEquals(c.getOperation(), DdlOperation.CREATE_MATERIALIZED_VIEW);
    return (CompiledCreateMaterializedView) c;
  }
}
