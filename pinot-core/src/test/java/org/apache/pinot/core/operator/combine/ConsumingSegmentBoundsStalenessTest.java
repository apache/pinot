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
package org.apache.pinot.core.operator.combine;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.AcquireReleaseColumnsSegmentOperator;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.plan.maker.PlanMaker;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImplTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.spi.utils.CommonConstants.Server.SortedSelectionMergeMode;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/// Regression test for consuming-segment min/max staleness in the sorted selection combine operators. A combine reads a
/// consuming segment's first order-by column min/max in its constructor, but with the prefetch wrapper
/// ([AcquireReleaseColumnsSegmentOperator]) the segment's plan (and so its doc-count snapshot) is only built at
/// activation. Rows ingested in between can fall outside the bounds used to order and activate cursors, so the
/// streaming combine must not trust a consuming segment's bounds. The [MinMaxValueBasedSelectionOrderByCombineOperator]
/// cases are the baseline: they must stay correct for the same scenario.
///
/// Scenario (ASC): immutable sorted segment B = 50..59 and 120..129, consuming segment M = 100..199 (min 100). Row 5 is
/// ingested into M after the combine is constructed. The result must still start with 5, not emit B's 50..59 first.
/// DESC mirrors it with 999.
public class ConsumingSegmentBoundsStalenessTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "ConsumingSegmentBoundsStaleness");
  private static final String TABLE = "testTable";
  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .addSingleValueDimension("x", FieldSpec.DataType.INT)
      .addSingleValueDimension("id", FieldSpec.DataType.INT)
      .build();
  private static final PlanMaker PLAN_MAKER = new InstancePlanMakerImplV2();
  private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool();
  private static final int LIMIT = 25;

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(TEMP_DIR);
    EXECUTOR.shutdownNow();
  }

  @DataProvider(name = "cases")
  public Object[][] cases() {
    List<Object[]> cases = new ArrayList<>();
    for (boolean streaming : new boolean[]{true, false}) {
      for (boolean asc : new boolean[]{true, false}) {
        for (boolean raw : new boolean[]{false, true}) {
          cases.add(new Object[]{streaming, asc, raw});
        }
      }
    }
    return cases.toArray(new Object[0][]);
  }

  private static GenericRow row(int x, int id) {
    GenericRow row = new GenericRow();
    row.putValue("x", x);
    row.putValue("id", id);
    return row;
  }

  private static IndexSegment buildImmutable(String name, int[] xs)
      throws Exception {
    List<GenericRow> rows = new ArrayList<>();
    for (int x : xs) {
      rows.add(row(x, x));
    }
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE).setSortedColumn("x").build();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, SCHEMA);
    config.setTableName(TABLE);
    config.setSegmentName(name);
    config.setOutDir(TEMP_DIR.getPath());
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
    return ImmutableSegmentLoader.load(new File(TEMP_DIR, name), ReadMode.mmap);
  }

  @Test(dataProvider = "cases")
  public void testIngestBetweenCombineCtorAndActivation(boolean streaming, boolean asc, boolean raw)
      throws Exception {
    // Immutable segment: ASC has values below and above M's [100,199]; DESC mirrors it.
    int[] immutableValues = new int[20];
    for (int i = 0; i < 10; i++) {
      immutableValues[i] = asc ? 50 + i : 240 + i;
      immutableValues[10 + i] = asc ? 120 + i : 170 + i;
    }
    Arrays.sort(immutableValues);
    IndexSegment immutable = buildImmutable("imm_" + streaming + "_" + asc + "_" + raw, immutableValues);

    MutableSegmentImpl consuming = MutableSegmentImplTestUtils.createMutableSegmentImpl(SCHEMA,
        raw ? Set.of("x") : Set.of(), Set.of(), Set.of(), false);
    try {
      List<Integer> expected = new ArrayList<>();
      for (int x = 100; x < 200; x++) {
        consuming.index(row(x, x), null);
        expected.add(x);
      }
      for (int x : immutableValues) {
        expected.add(x);
      }

      String query = "SELECT x, id FROM testTable ORDER BY x " + (asc ? "ASC" : "DESC") + " LIMIT " + LIMIT;
      QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
      queryContext.setEndTimeMs(System.currentTimeMillis() + Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);
      if (streaming) {
        queryContext.setSortedSelectionMergeMode(SortedSelectionMergeMode.ON);
      }

      // Prefetch-style children: the plan is built lazily inside nextBlock(), after acquire().
      List<Operator> children = new ArrayList<>();
      for (IndexSegment segment : List.of(immutable, consuming)) {
        SegmentContext segmentContext = new SegmentContext(segment);
        children.add(new AcquireReleaseColumnsSegmentOperator(
            streaming ? PLAN_MAKER.makeStreamingSegmentPlanNode(segmentContext, queryContext)
                : PLAN_MAKER.makeSegmentPlanNode(segmentContext, queryContext), segment, null));
      }

      // Combine ctor: this is where min/max are read (a snapshot for a consuming segment).
      Operator<?> combine = streaming
          ? new StreamingSelectionOrderByCombineOperator(children, queryContext, EXECUTOR)
          : new MinMaxValueBasedSelectionOrderByCombineOperator(children, queryContext, EXECUTOR);

      DataSourceMetadata before = consuming.getDataSource("x", null).getDataSourceMetadata();
      assertNotNull(before.getMinValue(), "consuming min is null; bounds are not exercised");

      // Ingest after the ctor, before activation (nothing has called nextBlock yet).
      int newValue = asc ? 5 : 999;
      consuming.index(row(newValue, newValue), null);
      expected.add(newValue);

      List<Integer> actual = new ArrayList<>();
      if (streaming) {
        while (true) {
          BaseResultsBlock block = (BaseResultsBlock) combine.nextBlock();
          if (!(block instanceof SelectionResultsBlock)) {
            break;
          }
          for (Object[] r : ((SelectionResultsBlock) block).getRows()) {
            actual.add((Integer) r[0]);
          }
        }
      } else {
        SelectionResultsBlock block = (SelectionResultsBlock) combine.nextBlock();
        for (Object[] r : block.getRows()) {
          actual.add((Integer) r[0]);
        }
      }

      Comparator<Integer> cmp = asc ? Comparator.naturalOrder() : Comparator.reverseOrder();
      expected.sort(cmp);
      List<Integer> expectedTopN = expected.subList(0, LIMIT);
      List<Integer> sortedActual = new ArrayList<>(actual);
      sortedActual.sort(cmp);
      assertEquals(actual, sortedActual, "output not sorted");
      assertEquals(actual, expectedTopN, "output differs from true top-N over all rows visible at execution");
      assertTrue(actual.contains(newValue));
    } finally {
      consuming.destroy();
    }
  }
}
