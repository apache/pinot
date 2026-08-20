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
package org.apache.pinot.core.operator;

import java.util.Collections;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


/**
 * Unit tests for leaf-cap truncation detection in {@link InstanceResponseOperator#buildInstanceResponseBlock}.
 * Tests cover ORDER BY detection using {@code numDocsScanned > leafCap} under {@code LITE_CAP} provenance.
 */
public class InstanceResponseOperatorLiteCapTest {

  private static final DataSchema SCHEMA =
      new DataSchema(new String[]{"col"}, new ColumnDataType[]{ColumnDataType.INT});

  // Exposes the protected buildInstanceResponseBlock for unit testing without a real combine operator.
  private static class TestableInstanceResponseOperator extends InstanceResponseOperator {
    TestableInstanceResponseOperator(QueryContext queryContext) {
      super(null, Collections.emptyList(), Collections.emptyList(), queryContext);
    }

    InstanceResponseBlock buildBlock(SelectionResultsBlock block) {
      return buildInstanceResponseBlock(block);
    }
  }

  // Use a SET clause so the options map is a mutable HashMap, then inject the provenance key.
  private static QueryContext orderByContextWithLiteCap(int limit) {
    QueryContext ctx = QueryContextConverterUtils.getQueryContext(
        "SET \"numReplicaGroupsToQuery\"=1; SELECT col FROM myTable ORDER BY col LIMIT " + limit);
    ctx.getQueryOptions().put("leafLimitTruncationRisk", "LITE_CAP");
    return ctx;
  }

  private static QueryContext orderByContextNoCap(int limit) {
    return QueryContextConverterUtils.getQueryContext(
        "SET \"numReplicaGroupsToQuery\"=1; SELECT col FROM myTable ORDER BY col LIMIT " + limit);
  }

  private static QueryContext selectionOnlyContextWithLiteCap(int limit) {
    QueryContext ctx = QueryContextConverterUtils.getQueryContext(
        "SET \"numReplicaGroupsToQuery\"=1; SELECT col FROM myTable LIMIT " + limit);
    ctx.getQueryOptions().put("leafLimitTruncationRisk", "LITE_CAP");
    return ctx;
  }

  @Test
  public void orderByWithLiteCapAndExcessScansEmitsFlag() {
    // numDocsScanned(10) > leafCap(5): cap was binding → truncated
    QueryContext ctx = orderByContextWithLiteCap(5);
    TestableInstanceResponseOperator op = new TestableInstanceResponseOperator(ctx);

    SelectionResultsBlock block = new SelectionResultsBlock(SCHEMA, Collections.emptyList(),
        (a, b) -> 0, ctx);
    block.setNumDocsScanned(10);

    InstanceResponseBlock response = op.buildBlock(block);

    assertEquals(response.getResponseMetadata().get(DataTable.MetadataKey.LITE_MODE_LEAF_STAGE_LIMIT_REACHED.getName()), "true");
  }

  @Test
  public void orderByWithLiteCapButScansWithinCapNoFlag() {
    // numDocsScanned(3) <= leafCap(5): data exhausted before cap → not truncated
    QueryContext ctx = orderByContextWithLiteCap(5);
    TestableInstanceResponseOperator op = new TestableInstanceResponseOperator(ctx);

    SelectionResultsBlock block = new SelectionResultsBlock(SCHEMA, Collections.emptyList(),
        (a, b) -> 0, ctx);
    block.setNumDocsScanned(3);

    InstanceResponseBlock response = op.buildBlock(block);

    assertNull(response.getResponseMetadata().get(DataTable.MetadataKey.LITE_MODE_LEAF_STAGE_LIMIT_REACHED.getName()));
  }

  @Test
  public void orderByWithoutLiteCapProvenanceNoFlag() {
    // Excess scans present but no LITE_CAP provenance → cap was not the cause → no warning
    QueryContext ctx = orderByContextNoCap(5);
    TestableInstanceResponseOperator op = new TestableInstanceResponseOperator(ctx);

    SelectionResultsBlock block = new SelectionResultsBlock(SCHEMA, Collections.emptyList(),
        (a, b) -> 0, ctx);
    block.setNumDocsScanned(10);

    InstanceResponseBlock response = op.buildBlock(block);

    assertNull(response.getResponseMetadata().get(DataTable.MetadataKey.LITE_MODE_LEAF_STAGE_LIMIT_REACHED.getName()));
  }

  @Test
  public void selectionOnlyNullComparatorNoFlagEvenWithExcessScansAndLiteCap() {
    // SelectionOnly block has null comparator → instanceof check passes but comparator check fails → no warning.
    // SelectionOnly never needs a warning (any N rows is a valid answer with no ordering guarantee).
    QueryContext ctx = selectionOnlyContextWithLiteCap(5);
    TestableInstanceResponseOperator op = new TestableInstanceResponseOperator(ctx);

    // null comparator constructor → SelectionOnly, not SelectionOrderBy
    SelectionResultsBlock block = new SelectionResultsBlock(SCHEMA, Collections.emptyList(), ctx);
    block.setNumDocsScanned(10);

    InstanceResponseBlock response = op.buildBlock(block);

    assertNull(response.getResponseMetadata().get(DataTable.MetadataKey.LITE_MODE_LEAF_STAGE_LIMIT_REACHED.getName()));
  }

  @Test
  public void selectionOnlyWithHasMoreFilteredDocsEmitsFlag() {
    // SelectionOnly block with hasMoreFilteredDocs=true and LITE_CAP reason → truncation detected via hasMoreDocs
    QueryContext ctx = selectionOnlyContextWithLiteCap(5);
    TestableInstanceResponseOperator op = new TestableInstanceResponseOperator(ctx);

    SelectionResultsBlock block = new SelectionResultsBlock(SCHEMA, Collections.emptyList(), ctx);
    block.setNumDocsScanned(5);
    block.setHasMoreFilteredDocs(true);

    InstanceResponseBlock response = op.buildBlock(block);

    assertEquals(response.getResponseMetadata().get(DataTable.MetadataKey.LITE_MODE_LEAF_STAGE_LIMIT_REACHED.getName()), "true");
  }

  @Test
  public void selectionOnlyWithHasMoreFilteredDocsButNoLiteCapNoFlag() {
    // hasMoreFilteredDocs=true but no LITE_CAP reason → not a lite-cap truncation
    QueryContext ctx = QueryContextConverterUtils.getQueryContext(
        "SET \"numReplicaGroupsToQuery\"=1; SELECT col FROM myTable LIMIT 5");
    TestableInstanceResponseOperator op = new TestableInstanceResponseOperator(ctx);

    SelectionResultsBlock block = new SelectionResultsBlock(SCHEMA, Collections.emptyList(), ctx);
    block.setNumDocsScanned(5);
    block.setHasMoreFilteredDocs(true);

    InstanceResponseBlock response = op.buildBlock(block);

    assertNull(response.getResponseMetadata().get(DataTable.MetadataKey.LITE_MODE_LEAF_STAGE_LIMIT_REACHED.getName()));
  }
}
