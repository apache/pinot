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
import java.io.IOException;
import java.nio.file.Files;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.sql.parsers.SqlCompilationException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// End-to-end tests for the HAVING clause in the single-stage engine.
///
/// The engine used to evaluate a HAVING filter only when reducing a GROUP BY aggregation. Every other shape answered
/// as if the clause were absent, so these queries returned rows that the predicate excludes. Each expectation here
/// matches what the multi-stage engine produces for the same query.
public class HavingQueriesTest {
  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
  // Columns are read positionally in schema order, which sorts "amount" before "city".
  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName("testTable")
      .addMetric("amount", FieldSpec.DataType.INT, 0)
      .addSingleValueDimension("city", FieldSpec.DataType.STRING)
      .build();

  /// Same shape but with a nullable metric, for the null-aware predicate path.
  private static final Schema NULLABLE_SCHEMA = new Schema.SchemaBuilder().setSchemaName("testTable")
      .setEnableColumnBasedNullHandling(true)
      .addMetric("amount", FieldSpec.DataType.INT, 0)
      .addSingleValueDimension("city", FieldSpec.DataType.STRING)
      .build();

  private File _baseDir;

  @BeforeClass
  void createBaseDir()
      throws IOException {
    _baseDir = Files.createTempDirectory(getClass().getSimpleName()).toFile();
  }

  @AfterClass
  void destroyBaseDir()
      throws IOException {
    if (_baseDir != null) {
      FileUtils.deleteDirectory(_baseDir);
    }
  }

  /// Each instance holds the same three rows, so the queried table has Athens twice over and Madrid twice over:
  /// Athens has 4 rows summing to 6, Madrid has 2 rows summing to 20.
  private FluentQueryTest.OnSecondInstance givenTable() {
    return FluentQueryTest.withBaseDir(_baseDir).givenTable(SCHEMA, TABLE_CONFIG)
        .onFirstInstance(new Object[]{1, "Athens"}, new Object[]{2, "Athens"}, new Object[]{10, "Madrid"})
        .andOnSecondInstance(new Object[]{1, "Athens"}, new Object[]{2, "Athens"}, new Object[]{10, "Madrid"});
  }

  /// An aggregation without GROUP BY is a single group over the whole table. When the predicate rejects that group the
  /// result is empty; the engine used to return the unfiltered aggregate.
  @Test
  public void testHavingOnAggregationWithoutGroupBy() {
    givenTable().whenQuery("SELECT COUNT(*) FROM testTable HAVING COUNT(*) > 100").thenResultIs(new Object[0][]);
    givenTable().whenQuery("SELECT COUNT(*) FROM testTable HAVING COUNT(*) > 1")
        .thenResultIs(new Object[]{6L});
    givenTable().whenQuery("SELECT SUM(amount) FROM testTable HAVING SUM(amount) > 100").thenResultIs(new Object[0][]);
    givenTable().whenQuery("SELECT SUM(amount) FROM testTable HAVING SUM(amount) > 1")
        .thenResultIs(new Object[]{26.0});
    // The predicate may reference an aggregate that is not in the SELECT list.
    givenTable().whenQuery("SELECT COUNT(*) FROM testTable HAVING SUM(amount) > 100").thenResultIs(new Object[0][]);
  }

  /// A GROUP BY whose SELECT list holds no aggregate is rewritten to a DISTINCT, which has no reduce step that can
  /// evaluate a HAVING filter, so the predicate used to be dropped. An aggregate predicate keeps the query on the
  /// GROUP BY reduce path instead.
  @Test
  public void testHavingOnGroupByWithoutAggregateInSelectList() {
    givenTable().whenQuery("SELECT city FROM testTable GROUP BY city HAVING COUNT(*) > 2")
        .thenResultIs(new Object[]{"Athens"});
    givenTable().whenQuery("SELECT city FROM testTable GROUP BY city HAVING SUM(amount) > 10")
        .thenResultIs(new Object[]{"Madrid"});
  }

  /// The shape that always worked must keep working.
  @Test
  public void testHavingOnGroupByWithAggregateInSelectList() {
    givenTable().whenQuery("SELECT city, COUNT(*) FROM testTable GROUP BY city HAVING COUNT(*) > 2")
        .thenResultIs(new Object[]{"Athens", 4L});
  }

  /// A HAVING clause imposes grouping semantics, so a predicate on a non-grouped column has no single value to test
  /// and a bare SELECT column has no single value to report. The multi-stage engine rejects all of these too.
  @Test
  public void testInvalidHavingIsRejected() {
    assertRejected("SELECT city FROM testTable HAVING city > 'B'");
    assertRejected("SELECT DISTINCT city FROM testTable HAVING city > 'B'");
    assertRejected("SELECT city FROM testTable HAVING COUNT(*) > 1");
    assertRejected("SELECT DISTINCT city FROM testTable HAVING COUNT(*) > 1");
    assertRejected("SELECT COUNT(*) FROM testTable HAVING amount > 1");
    assertRejected("SELECT city FROM testTable GROUP BY city HAVING amount > 1");
    // An aggregate elsewhere in the predicate does not make a bare column resolvable: the reducer has no single value
    // per group for it, and used to fail at run time naming a GROUP BY clause the user never wrote.
    assertRejected("SELECT COUNT(*) FROM testTable HAVING COUNT(*) > amount");
    assertRejected("SELECT city FROM testTable GROUP BY city HAVING COUNT(*) > amount");
    assertRejected("SELECT city, COUNT(*) FROM testTable GROUP BY city HAVING COUNT(*) > amount");
    // A column inside an aggregate stays legal, and so does a grouping column next to one.
    givenTable().whenQuery("SELECT city, COUNT(*) FROM testTable GROUP BY city HAVING SUM(amount) > MIN(amount)")
        .thenResultIs(new Object[]{"Athens", 4L}, new Object[]{"Madrid", 2L});
    givenTable().whenQuery("SELECT city, COUNT(*) FROM testTable GROUP BY city HAVING COUNT(*) > 2 AND city > 'B'")
        .thenResultIs(new Object[0][]);
  }

  /// A GROUP BY with no aggregation anywhere has no grouping operator to filter: the query becomes a DISTINCT, whose
  /// reduce step cannot evaluate HAVING. Folding the predicate into WHERE would be equivalent for a single-valued
  /// grouping column but not for a multi-valued one, and the rewrite happens before any schema is available, so the
  /// shape is rejected rather than answered wrongly.
  @Test
  public void testHavingOnGroupByWithoutAnyAggregationIsRejected() {
    assertRejected("SELECT city FROM testTable GROUP BY city HAVING city > 'B'");
    // The SELECT list being a strict subset of the GROUP BY list reaches the same reduce path.
    assertRejected("SELECT city FROM testTable GROUP BY city, amount HAVING city > 'B'");
    // An aggregation anywhere -- SELECT list, HAVING or ORDER BY -- keeps the GROUP BY reduce path and is accepted.
    givenTable().whenQuery("SELECT city, COUNT(*) FROM testTable GROUP BY city HAVING city > 'B'")
        .thenResultIs(new Object[]{"Madrid", 2L});
  }

  /// The HAVING predicate is matched against the row before post-aggregation, so its operands are resolved through
  /// PostAggregationHandler rather than by position in the SELECT list. These shapes break if that mapping is wrong.
  @Test
  public void testHavingOperandsAreMappedThroughPostAggregation() {
    // Post-aggregation expression in the SELECT list shifts the result columns away from the aggregate positions.
    givenTable().whenQuery("SELECT SUM(amount) - COUNT(*) FROM testTable HAVING SUM(amount) > 1")
        .thenResultIs(new Object[]{20.0});
    givenTable().whenQuery("SELECT SUM(amount) - COUNT(*) FROM testTable HAVING SUM(amount) > 100")
        .thenResultIs(new Object[0][]);
    // An aggregate used only by the predicate is not in the SELECT list at all.
    givenTable().whenQuery("SELECT COUNT(*) FROM testTable HAVING SUM(amount) > 1")
        .thenResultIs(new Object[]{6L});
    givenTable().whenQuery("SELECT MIN(amount), MAX(amount) FROM testTable HAVING MIN(amount) < MAX(amount)")
        .thenResultIs(new Object[]{1.0, 10.0});
  }

  /// With null handling enabled an aggregation over an all-null column returns null, and the predicate must treat it
  /// as non-matching instead of unboxing it. The reduce path materializes such a null whichever way the
  /// null-handling option is set, so both modes are covered.
  @Test
  public void testHavingOnNullAggregateResult() {
    for (boolean nullHandling : new boolean[]{false, true}) {
      FluentQueryTest.withBaseDir(_baseDir).withNullHandling(nullHandling).givenTable(NULLABLE_SCHEMA, TABLE_CONFIG)
          .onFirstInstance(new Object[]{null, "Athens"}, new Object[]{null, "Madrid"})
          .whenQuery("SELECT MAX(amount) FROM testTable WHERE city = 'Nowhere' HAVING MAX(amount) > 1")
          .thenResultIs(new Object[0][]);
    }
  }

  private void assertRejected(String query) {
    SqlCompilationException e =
        expectThrows(SqlCompilationException.class, () -> givenTable().whenQuery(query));
    assertTrue(e.getMessage().contains("HAVING"), "Unexpected message for '" + query + "': " + e.getMessage());
  }
}
