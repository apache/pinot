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

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.sql.SqlNode;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.CommonConstants.MaterializedViewTask;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Tests the [MaterializedViewDdlHandler] extension point: a registered handler can accept a JOIN in
/// the `AS` clause and route the MV task config under an alternative task type, while the default
/// handler preserves single-source / single-stage behavior.
public class MaterializedViewDdlHandlerTest {
  private static final String JOIN_DDL =
      "CREATE MATERIALIZED VIEW mv ("
          + "  ts TIMESTAMP DATETIME FORMAT '1:MILLISECONDS:TIMESTAMP' GRANULARITY '1:DAYS',"
          + "  country STRING,"
          + "  amount_sum DOUBLE METRIC"
          + ")"
          + " REFRESH EVERY 1 DAY"
          + " PROPERTIES ('timeColumnName' = 'ts', 'bucketTimePeriod' = '1d')"
          + " AS SELECT f.ts, d.country, SUM(f.amount) AS amount_sum"
          + " FROM fact f JOIN dim d ON f.id = d.id GROUP BY f.ts, d.country";

  @AfterMethod
  public void restoreDefaultHandler() {
    /// The handler is process-wide; restore the default so sibling tests see single-source behavior.
    DdlCompiler.setMaterializedViewDdlHandler(new DefaultMaterializedViewDdlHandler());
  }

  @Test
  public void defaultsToSingleSourceHandler() {
    assertTrue(DdlCompiler.getMaterializedViewDdlHandler() instanceof DefaultMaterializedViewDdlHandler);
  }

  @Test
  public void defaultHandlerRejectsJoin() {
    DdlCompilationException e =
        expectThrows(DdlCompilationException.class, () -> DdlCompiler.compile(JOIN_DDL));
    assertTrue(e.getMessage().contains("JOIN"), e.getMessage());
  }

  @Test
  public void registeredHandlerAcceptsJoinAndRoutesToCustomTaskType() {
    DdlCompiler.setMaterializedViewDdlHandler(new MultiSourceTestHandler());

    CompiledDdl compiled = DdlCompiler.compile(JOIN_DDL);
    assertEquals(compiled.getOperation(), DdlOperation.CREATE_MATERIALIZED_VIEW);
    TableConfig tableConfig = ((CompiledCreateMaterializedView) compiled).getTableConfig();
    assertTrue(tableConfig.isMaterializedView());

    Map<String, Map<String, String>> taskTypes = tableConfig.getTaskConfig().getTaskTypeConfigsMap();
    /// Stamped under the handler's task type; the built-in single-stage type is absent.
    assertTrue(taskTypes.containsKey(MultiSourceTestHandler.TASK_TYPE));
    assertFalse(taskTypes.containsKey(MaterializedViewTask.TASK_TYPE));

    Map<String, String> mvTaskConfig = taskTypes.get(MultiSourceTestHandler.TASK_TYPE);
    assertEquals(mvTaskConfig.get(MaterializedViewTask.DEFINED_SQL_KEY),
        "SELECT f.ts, d.country, SUM(f.amount) AS amount_sum"
            + " FROM fact f JOIN dim d ON f.id = d.id GROUP BY f.ts, d.country");
    /// bucketTimePeriod routed under the custom task type, and the consistency check passed against it.
    assertEquals(mvTaskConfig.get(MaterializedViewTask.BUCKET_TIME_PERIOD_KEY), "1d");
  }

  @Test
  public void customTaskTypePrefixedKnobsArePreserved() {
    DdlCompiler.setMaterializedViewDdlHandler(new MultiSourceTestHandler());

    /// A `task.<customTaskType>.<knob>` property must survive into the stamped task config (it must
    /// not be overwritten by the synthetic task-config map under the same task type).
    CompiledDdl compiled = DdlCompiler.compile(
        "CREATE MATERIALIZED VIEW mv ("
            + "  ts TIMESTAMP DATETIME FORMAT '1:MILLISECONDS:TIMESTAMP' GRANULARITY '1:DAYS',"
            + "  amount_sum DOUBLE METRIC"
            + ")"
            + " REFRESH EVERY 1 DAY"
            + " PROPERTIES ('timeColumnName' = 'ts', 'bucketTimePeriod' = '1d',"
            + "   'task." + MultiSourceTestHandler.TASK_TYPE + ".customKnob' = 'v')"
            + " AS SELECT f.ts, SUM(f.amount) AS amount_sum"
            + " FROM fact f JOIN dim d ON f.id = d.id GROUP BY f.ts");
    Map<String, String> mvTaskConfig = ((CompiledCreateMaterializedView) compiled).getTableConfig()
        .getTaskConfig().getTaskTypeConfigsMap().get(MultiSourceTestHandler.TASK_TYPE);
    assertEquals(mvTaskConfig.get("customKnob"), "v");
    /// Synthetic / bare-form keys remain present alongside the prefixed knob.
    assertEquals(mvTaskConfig.get(MaterializedViewTask.BUCKET_TIME_PERIOD_KEY), "1d");
    assertTrue(mvTaskConfig.containsKey(MaterializedViewTask.DEFINED_SQL_KEY));
  }

  @Test
  public void registeredHandlerJoinWithoutExplicitColumnsRejected() {
    DdlCompiler.setMaterializedViewDdlHandler(new MultiSourceTestHandler());

    /// Even when the handler permits a JOIN, schema inference (no column list) is single-source-only.
    DdlCompilationException e = expectThrows(DdlCompilationException.class, () -> DdlCompiler.compile(
        "CREATE MATERIALIZED VIEW mv"
            + " REFRESH EVERY 1 DAY"
            + " PROPERTIES ('timeColumnName' = 'ts', 'bucketTimePeriod' = '1d')"
            + " AS SELECT f.ts, d.country FROM fact f JOIN dim d ON f.id = d.id"));
    assertTrue(e.getMessage().contains("explicit column list"), e.getMessage());
  }

  @Test
  public void handlerReturningNullTaskTypeFailsClearly() {
    DdlCompiler.setMaterializedViewDdlHandler(new MaterializedViewDdlHandler() {
      @Override
      public void validateDefinedQuery(SqlNode queryNode, String definedSql, Map<String, String> properties) {
      }

      @Override
      public String applyTaskConfig(Map<String, String> properties, String definedSql,
          @Nullable String schedule, TableConfigBuilder builder) {
        return null;
      }
    });
    DdlCompilationException e =
        expectThrows(DdlCompilationException.class, () -> DdlCompiler.compile(JOIN_DDL));
    assertTrue(e.getMessage().contains("null task type"), e.getMessage());
  }

  @Test
  public void handlerReturningUnstampedTaskTypeFailsClearly() {
    DdlCompiler.setMaterializedViewDdlHandler(new MaterializedViewDdlHandler() {
      @Override
      public void validateDefinedQuery(SqlNode queryNode, String definedSql, Map<String, String> properties) {
      }

      @Override
      public String applyTaskConfig(Map<String, String> properties, String definedSql,
          @Nullable String schedule, TableConfigBuilder builder) {
        /// Stamp under one task type but return a different one — a handler-contract violation.
        MaterializedViewPropertyRouter.apply(properties, definedSql, schedule, builder, "StampedTask");
        return "ReturnedButNotStampedTask";
      }
    });
    DdlCompilationException e =
        expectThrows(DdlCompilationException.class, () -> DdlCompiler.compile(JOIN_DDL));
    assertTrue(e.getMessage().contains("did not stamp a task config"), e.getMessage());
  }

  /// Test handler that permits multi-source (JOIN) definitions and routes the MV task config under a
  /// distinct task type via the task-type-parameterized [MaterializedViewPropertyRouter#apply].
  private static final class MultiSourceTestHandler implements MaterializedViewDdlHandler {
    static final String TASK_TYPE = "CustomMaterializedViewTask";

    @Override
    public void validateDefinedQuery(SqlNode queryNode, String definedSql, Map<String, String> properties) {
      /// Multi-source allowed: no JOIN rejection, no single-stage re-compilation.
    }

    @Override
    public boolean supportsSchemaInference(Map<String, String> properties) {
      /// Multi-source projection inference is not supported; an explicit column list is required.
      return false;
    }

    @Override
    public String applyTaskConfig(Map<String, String> properties, String definedSql,
        @Nullable String schedule, TableConfigBuilder builder) {
      MaterializedViewPropertyRouter.apply(properties, definedSql, schedule, builder, TASK_TYPE);
      return TASK_TYPE;
    }
  }
}
