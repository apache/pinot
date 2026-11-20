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
package org.apache.pinot.queries;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntPredicate;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// End-to-end test of the AND restriction push-down with null handling, against a segment whose scanned columns hold
/// both null and non-null values.
///
/// `idx` has an inverted index and seeds the push-down; `a` and `b` have none, so predicates on them are scans. Under
/// null handling a predicate on a null row is UNKNOWN, and `NOT` keeps UNKNOWN rows out, so the push-down has to carry
/// the null documents correctly through `excludeNulls()` and `getNotFalses()`. Each query is checked against a
/// row-by-row evaluation of three-valued logic, not only against the push-down turned off.
public class AndRestrictionPushdownNullQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "AndRestrictionPushdownNullQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final int NUM_ROWS = 5000;
  // BaseQueriesTest queries two instances of two copies of the segment
  private static final int NUM_SEGMENT_COPIES = 4;
  private static final String[] B_VALUES = {"x", "y", "z"};

  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  @Override
  protected String getFilter() {
    return "";
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return _indexSegments;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);
    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension("id", DataType.INT)
        .addSingleValueDimension("idx", DataType.INT)
        .addSingleValueDimension("a", DataType.INT)
        .addSingleValueDimension("b", DataType.STRING)
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setInvertedIndexColumns(List.of("idx"))
        .setNullHandlingEnabled(true)
        .build();

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow row = new GenericRow();
      row.putValue("id", i);
      row.putValue("idx", i % 10);
      row.putValue("a", a(i));
      row.putValue("b", b(i));
      rows.add(row);
    }

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setTableName(RAW_TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME);
    config.setOutDir(INDEX_DIR.getPath());
    config.setDefaultNullHandlingEnabled(true);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();

    ImmutableSegment segment =
        ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), new IndexLoadingConfig(tableConfig, schema));
    _indexSegment = segment;
    _indexSegments = List.of(segment, segment);
  }

  @AfterClass
  public void tearDown() {
    _indexSegment.destroy();
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  private static Integer a(int i) {
    return i % 7 == 0 ? null : i % 100;
  }

  private static String b(int i) {
    return i % 11 == 0 ? null : B_VALUES[i % 3];
  }

  /// Each filter has `idx = 1` to seed the push-down and an OR/AND over nullable scans, paired with the rows it must
  /// match under three-valued logic.
  @DataProvider
  public static Object[][] filters() {
    IntPredicate aTrue = i -> a(i) != null && a(i) > 50;
    IntPredicate aFalse = i -> a(i) != null && a(i) <= 50;
    IntPredicate bTrue = i -> b(i) != null && b(i).equals("x");
    IntPredicate bFalse = i -> b(i) != null && !b(i).equals("x");
    return new Object[][]{
        // excludeNulls() on each leaf under the pushed-into OR
        {"(a > 50 OR b = 'x')", aTrue.or(bTrue)},
        // NOT(OR) is true where the OR is false: every branch false, none UNKNOWN
        {"NOT (a > 50 OR b = 'x')", aFalse.and(bFalse)},
        // NOT(AND) is true where the AND is false: some branch false; getNotFalses() builds the AND with the flag
        {"NOT (a > 50 AND b = 'x')", aFalse.or(bFalse)}
    };
  }

  @Test(dataProvider = "filters")
  public void testPushdownMatchesThreeValuedLogic(String predicate, IntPredicate expected) {
    String query = "SELECT COUNT(*), SUM(id) FROM testTable WHERE idx = 1 AND " + predicate;
    long expectedCount = 0;
    long expectedSum = 0;
    for (int i = 0; i < NUM_ROWS; i++) {
      if (i % 10 == 1 && expected.test(i)) {
        expectedCount++;
        expectedSum += i;
      }
    }
    assertTrue(expectedCount > 0, "The filter must match some rows to be meaningful");

    BrokerResponseNative disabled = getBrokerResponse(withNullHandling("never", query));
    BrokerResponseNative enabled = getBrokerResponse(withNullHandling("always", query));

    for (BrokerResponseNative response : List.of(disabled, enabled)) {
      Object[] row = response.getResultTable().getRows().get(0);
      assertEquals(((Number) row[0]).longValue(), expectedCount * NUM_SEGMENT_COPIES, predicate);
      assertEquals(((Number) row[1]).longValue(), expectedSum * NUM_SEGMENT_COPIES, predicate);
    }
    assertTrue(enabled.getNumEntriesScannedInFilter() < disabled.getNumEntriesScannedInFilter(),
        "The push-down must reduce the entries scanned in the filter for " + predicate + ", but scanned "
            + enabled.getNumEntriesScannedInFilter() + " with it and " + disabled.getNumEntriesScannedInFilter()
            + " without");
  }

  @Test(dataProvider = "filters")
  public void testPushdownKeepsTheSameRowsForASelectionQuery(String predicate, IntPredicate expected) {
    String query = "SELECT id, a, b FROM testTable WHERE idx = 1 AND " + predicate + " ORDER BY id LIMIT 100000";

    List<Object[]> enabledRows = getBrokerResponse(withNullHandling("always", query)).getResultTable().getRows();
    List<Object[]> disabledRows = getBrokerResponse(withNullHandling("never", query)).getResultTable().getRows();

    assertEquals(enabledRows.size(), disabledRows.size(), predicate);
    for (int i = 0; i < enabledRows.size(); i++) {
      assertEquals(enabledRows.get(i), disabledRows.get(i), predicate + ", row " + i);
      assertTrue(expected.test((Integer) enabledRows.get(i)[0]), predicate + ", row " + i);
    }
  }

  private static String withNullHandling(String mode, String query) {
    return "SET enableNullHandling = true; SET " + QueryOptionKey.AND_RESTRICTION_PUSHDOWN_MODE + " = '" + mode
        + "'; " + query;
  }
}
