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
package org.apache.pinot.materializedview.context;

import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.materializedview.rewrite.ExecutionMode;
import org.apache.pinot.materializedview.rewrite.MatchType;
import org.apache.pinot.materializedview.rewrite.MaterializedViewRewritePlan;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.Assert;
import org.testng.annotations.Test;


/// Pins the factory + accessor contracts on `MaterializedViewContext`.  The broker keys on
/// `isFullRewrite()` / `isSplitRewrite()` to decide which post-compile path to take and on
/// `getMaterializedViewQueriedName()` for the response field, so changes to those contracts
/// must surface as a test failure here.
public class MaterializedViewContextTest {

  @Test
  public void testEmptyContextReportsNoRewrite() {
    MaterializedViewContext ctx = MaterializedViewContext.empty();
    Assert.assertFalse(ctx.isFullRewrite());
    Assert.assertFalse(ctx.isSplitRewrite());
    Assert.assertNull(ctx.getPlan());
    Assert.assertNull(ctx.getMaterializedViewQueriedName());
    Assert.assertNull(ctx.getSplitRewriteContext());
  }

  @Test
  public void testFullRewriteContextCarriesPlanAndFullRewriteFlag() {
    PinotQuery materializedViewQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT ts, SUM(revenue) FROM mv_baseTable_OFFLINE GROUP BY ts");
    MaterializedViewRewritePlan plan = new MaterializedViewRewritePlan(
        "mv_baseTable_OFFLINE", MatchType.EXACT, ExecutionMode.FULL_REWRITE, materializedViewQuery, 1.0);

    MaterializedViewContext ctx = MaterializedViewContext.forFullRewrite(plan);

    Assert.assertTrue(ctx.isFullRewrite());
    Assert.assertFalse(ctx.isSplitRewrite());
    Assert.assertSame(ctx.getPlan(), plan);
    Assert.assertEquals(ctx.getMaterializedViewQueriedName(), "mv_baseTable_OFFLINE");
    Assert.assertNull(ctx.getSplitRewriteContext(),
        "FULL_REWRITE context must not expose a split-rewrite context");
  }

  @Test
  public void testSplitRewriteContextCarriesMvBranchState() {
    PinotQuery materializedViewQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT ts, SUM(revenue) FROM mv_baseTable_OFFLINE GROUP BY ts");
    MaterializedViewRewritePlan plan = new MaterializedViewRewritePlan(
        "mv_baseTable_OFFLINE", MatchType.EXACT, ExecutionMode.SPLIT_REWRITE, materializedViewQuery, 1.0);
    Schema mvSchema = new Schema.SchemaBuilder()
        .setSchemaName("mv_baseTable")
        .addSingleValueDimension("ts", DataType.STRING)
        .addMetric("revenue", DataType.DOUBLE)
        .build();

    MaterializedViewContext ctx = MaterializedViewContext.forSplitRewrite(
        plan, materializedViewQuery, "mv_baseTable_OFFLINE", mvSchema);

    Assert.assertTrue(ctx.isSplitRewrite());
    Assert.assertFalse(ctx.isFullRewrite());
    Assert.assertSame(ctx.getPlan(), plan);
    Assert.assertEquals(ctx.getMaterializedViewQueriedName(), "mv_baseTable_OFFLINE");

    MaterializedViewContext.SplitRewriteContext splitCtx = ctx.getSplitRewriteContext();
    Assert.assertNotNull(splitCtx, "SPLIT_REWRITE context must expose the MV-branch state");
    Assert.assertSame(splitCtx.getMaterializedViewServerPinotQuery(), materializedViewQuery);
    Assert.assertEquals(splitCtx.getMaterializedViewTableNameWithType(), "mv_baseTable_OFFLINE");
    Assert.assertSame(splitCtx.getMaterializedViewSchema(), mvSchema);
  }
}
