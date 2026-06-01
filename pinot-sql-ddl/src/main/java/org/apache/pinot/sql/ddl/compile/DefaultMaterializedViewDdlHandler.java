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
import org.apache.pinot.spi.utils.CommonConstants.MaterializedViewTask;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlParser;


/// Default, single-stage-engine (SSE) materialized-view DDL handler.
///
/// A materialized view reads from exactly one source table: a JOIN in the `AS <query>` clause is
/// rejected, and the MV task configuration is routed under the built-in `MaterializedViewTask` task
/// type. This is the behavior of OSS Pinot when no alternative handler is registered.
public class DefaultMaterializedViewDdlHandler implements MaterializedViewDdlHandler {

  @Override
  public void validateDefinedQuery(SqlNode queryNode, String definedSql, Map<String, String> properties) {
    if (MaterializedViewDdlHandler.containsJoin(queryNode)) {
      throw new DdlCompilationException(
          "CREATE MATERIALIZED VIEW does not support JOIN in the AS clause. "
              + "Materialized views currently read from a single source table; "
              + "pre-join the inputs into a base table and reference that single table "
              + "in the MV definition.");
    }
    /// Re-compile the extracted definedSQL as a single-stage Pinot query. This both guards against
    /// DDL-layer slicing bugs (off-by-one in the parser-position extraction) and enforces SSE
    /// compatibility, so the error surfaces here rather than at the first scheduler tick / analysis.
    try {
      CalciteSqlParser.compileToPinotQuery(definedSql);
    } catch (Exception e) {
      throw new DdlCompilationException(
          "AS <query> did not re-parse as a single-stage Pinot query: " + e.getMessage()
              + " (extracted text: " + definedSql + ")", e);
    }
  }

  @Override
  public String applyTaskConfig(Map<String, String> properties, String definedSql,
      @Nullable String schedule, TableConfigBuilder builder) {
    MaterializedViewPropertyRouter.apply(properties, definedSql, schedule, builder);
    return MaterializedViewTask.TASK_TYPE;
  }
}
