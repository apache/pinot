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
package org.apache.pinot.integration.tests.custom;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.util.List;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.pinot.integration.tests.QueryAssert;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * End-to-end coverage of SQL:2016 {@code MATCH_RECOGNIZE} against a real Pinot cluster (ZooKeeper + controller +
 * broker + two servers), as opposed to the mock server enclosures used by
 * {@code pinot-query-runtime/src/test/resources/queries/MatchRecognize.json}.
 *
 * <p>The fixture is a small stock ticker with five symbols of different lengths, written round-robin into
 * {@link #getNumAvroFiles()} Avro files, so every partition is spread over several segments and both servers. That
 * makes every assertion here also an assertion that the sort exchange inserted by
 * {@code PinotMatchExchangeNodeInsertRule} really does deliver each partition contiguously and in ORDER BY order.
 *
 * <p>Every expected result set below is hand-computed from {@link #PRICES} rather than captured from a run.
 */
@Test(suiteName = "CustomClusterIntegrationTest")
public class MatchRecognizeIntegrationTest extends CustomDataQueryClusterIntegrationTest {

  private static final String DEFAULT_TABLE_NAME = "MatchRecognizeIntegrationTest";
  private static final String SYMBOL_COLUMN = "symbolCol";
  private static final String SEQ_COLUMN = "seqCol";
  private static final String PRICE_COLUMN = "priceCol";

  /** Symbols in ascending order, which is also the order every query below sorts its output by. */
  private static final String[] SYMBOLS = {"AAPL", "AMZN", "GOOG", "MSFT", "NFLX"};

  /**
   * Prices per symbol, indexed like {@link #SYMBOLS}. {@code seqCol} is the 1-based index within the symbol.
   * <ul>
   *   <li>AAPL: two disjoint V shapes, the second one overlapping the first under SKIP TO NEXT ROW.</li>
   *   <li>AMZN: one long descent, so a single match covers most of the partition.</li>
   *   <li>GOOG: flat, so no strict rise or fall matches anywhere - a partition that contributes nothing.</li>
   *   <li>MSFT: the minimal V shape.</li>
   *   <li>NFLX: strictly increasing, which is what separates greedy from reluctant quantifiers.</li>
   * </ul>
   */
  private static final int[][] PRICES = {
      {10, 8, 5, 9, 12, 7, 11},
      {20, 15, 10, 5, 25},
      {4, 4, 4},
      {5, 3, 8},
      {1, 2, 3, 4, 5, 6, 7, 8}
  };

  private static final String V_SHAPE_DEFINE =
      " DEFINE DOWN AS DOWN.priceCol < PREV(DOWN.priceCol), UP AS UP.priceCol > PREV(UP.priceCol)";

  /**
   * The canonical vendor-documentation V-shape query: a start row, a strictly falling run, then a strictly rising run.
   */
  @Test(dataProvider = "useV2QueryEngine")
  public void testCanonicalVShape(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = "SELECT * FROM " + getTableName() + " MATCH_RECOGNIZE ("
        + " PARTITION BY symbolCol"
        + " ORDER BY seqCol"
        + " MEASURES MATCH_NUMBER() AS mno, STRT.seqCol AS start_seq, LAST(DOWN.seqCol) AS bottom_seq,"
        + " LAST(UP.seqCol) AS end_seq, LAST(DOWN.priceCol) AS bottom_price"
        + " ONE ROW PER MATCH"
        + " AFTER MATCH SKIP PAST LAST ROW"
        + " PATTERN (STRT DOWN+ UP+)"
        + V_SHAPE_DEFINE
        + ") AS mr ORDER BY symbolCol, start_seq";
    // STRT consumes one row, so the second AAPL V (seq 5..7) is unreachable: the first match ends at seq 5 and
    // SKIP PAST LAST ROW resumes at seq 6, leaving no row for STRT before the fall at seq 6.
    assertMatchRows(query, new Object[][]{
        {"AAPL", 1, 1, 3, 5, 5},
        {"AMZN", 1, 1, 4, 5, 5},
        {"MSFT", 1, 1, 2, 3, 3}
    });
  }

  /**
   * THE critical default. SQL:2016, Trino, Snowflake and Oracle all default an omitted AFTER MATCH clause to
   * SKIP PAST LAST ROW, while Calcite's {@code SqlToRelConverter} silently substitutes SKIP TO NEXT ROW. The two
   * differ in whether matches may overlap, so a regression here changes results without changing anything visible.
   */
  @Test(dataProvider = "useV2QueryEngine")
  public void testOmittedAfterMatchIsSkipPastLastRow(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    Object[][] nonOverlapping = new Object[][]{
        {"AAPL", 2, 8, 12},
        {"AAPL", 6, 7, 11},
        {"AMZN", 2, 15, 25},
        {"MSFT", 2, 3, 8}
    };
    assertMatchRows(vShapeQuery("AFTER MATCH SKIP PAST LAST ROW"), nonOverlapping);
    assertMatchRows(vShapeQuery(""), nonOverlapping);

    // Same rows plus the overlapping ones. An engine that kept Calcite's default would return exactly this for the
    // two queries above.
    assertMatchRows(vShapeQuery("AFTER MATCH SKIP TO NEXT ROW"), new Object[][]{
        {"AAPL", 2, 8, 12},
        {"AAPL", 3, 5, 12},
        {"AAPL", 6, 7, 11},
        {"AMZN", 2, 15, 25},
        {"AMZN", 3, 10, 25},
        {"AMZN", 4, 5, 25},
        {"MSFT", 2, 3, 8}
    });
  }

  /**
   * All four skip modes over the same pattern, each producing a different row set. {@code S{2}} pushes the first row
   * mapped to {@code U} two rows past the start of the match, which is what makes SKIP TO FIRST U differ from
   * SKIP TO NEXT ROW.
   */
  @Test(dataProvider = "useV2QueryEngine")
  public void testSkipToFirstAndLastOfPatternVariable(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    // NFLX (8 strictly increasing rows) admits a match at every start row 1..5; each mode consumes them differently.
    assertMatchRows(skipTargetQuery("AFTER MATCH SKIP PAST LAST ROW"), new Object[][]{
        {"AAPL", 2, 5},
        {"NFLX", 1, 4},
        {"NFLX", 5, 8}
    });
    assertMatchRows(skipTargetQuery("AFTER MATCH SKIP TO NEXT ROW"), new Object[][]{
        {"AAPL", 2, 5},
        {"NFLX", 1, 4},
        {"NFLX", 2, 5},
        {"NFLX", 3, 6},
        {"NFLX", 4, 7},
        {"NFLX", 5, 8}
    });
    // FIRST(U) is the third row of the match, so matching resumes two rows in.
    assertMatchRows(skipTargetQuery("AFTER MATCH SKIP TO FIRST U"), new Object[][]{
        {"AAPL", 2, 5},
        {"NFLX", 1, 4},
        {"NFLX", 3, 6},
        {"NFLX", 5, 8}
    });
    // LAST(U) is the fourth and last row of the match, so matching resumes three rows in.
    assertMatchRows(skipTargetQuery("AFTER MATCH SKIP TO LAST U"), new Object[][]{
        {"AAPL", 2, 5},
        {"NFLX", 1, 4},
        {"NFLX", 4, 7}
    });
  }

  /**
   * A greedy quantifier takes the longest run it can, a reluctant one the shortest. On NFLX the difference is one
   * seven-row match versus four two-row matches.
   */
  @Test(dataProvider = "useV2QueryEngine")
  public void testGreedyVersusReluctantQuantifier(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    assertMatchRows(quantifierQuery("+"), new Object[][]{
        {"AAPL", 3, 2, 5},
        {"AAPL", 6, 1, 7},
        {"AMZN", 4, 1, 5},
        {"MSFT", 2, 1, 3},
        {"NFLX", 1, 7, 8}
    });
    assertMatchRows(quantifierQuery("+?"), new Object[][]{
        {"AAPL", 3, 1, 4},
        {"AAPL", 6, 1, 7},
        {"AMZN", 4, 1, 5},
        {"MSFT", 2, 1, 3},
        {"NFLX", 1, 1, 2},
        {"NFLX", 3, 1, 4},
        {"NFLX", 5, 1, 6},
        {"NFLX", 7, 1, 8}
    });
  }

  /**
   * Alternation prefers the leftmost branch that lets the whole pattern complete, which CLASSIFIER() reports.
   */
  @Test(dataProvider = "useV2QueryEngine")
  public void testAlternation(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = "SELECT * FROM " + getTableName() + " MATCH_RECOGNIZE ("
        + " PARTITION BY symbolCol ORDER BY seqCol"
        + " MEASURES FIRST(S.seqCol) AS start_seq, CLASSIFIER() AS cls"
        + " ONE ROW PER MATCH AFTER MATCH SKIP PAST LAST ROW"
        + " PATTERN (S (UP | DN))"
        + " DEFINE UP AS UP.priceCol > PREV(UP.priceCol), DN AS DN.priceCol < PREV(DN.priceCol)"
        + ") AS mr ORDER BY symbolCol, start_seq";
    // GOOG is flat, so neither branch can ever match and the partition contributes nothing.
    assertMatchRows(query, new Object[][]{
        {"AAPL", 1, "DN"},
        {"AAPL", 3, "UP"},
        {"AAPL", 5, "DN"},
        {"AMZN", 1, "DN"},
        {"AMZN", 3, "DN"},
        {"MSFT", 1, "DN"},
        {"NFLX", 1, "UP"},
        {"NFLX", 3, "UP"},
        {"NFLX", 5, "UP"},
        {"NFLX", 7, "UP"}
    });
  }

  /**
   * A bounded quantifier honours both bounds: AAPL can only supply the minimum of two rows, NFLX could supply seven
   * but is capped at the maximum of three.
   */
  @Test(dataProvider = "useV2QueryEngine")
  public void testBoundedQuantifier(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = "SELECT * FROM " + getTableName() + " MATCH_RECOGNIZE ("
        + " PARTITION BY symbolCol ORDER BY seqCol"
        + " MEASURES FIRST(S.seqCol) AS start_seq, COUNT(UP.priceCol) AS up_count, LAST(UP.seqCol) AS end_seq"
        + " ONE ROW PER MATCH AFTER MATCH SKIP PAST LAST ROW"
        + " PATTERN (S UP{2,3})"
        + " DEFINE UP AS UP.priceCol > PREV(UP.priceCol)"
        + ") AS mr ORDER BY symbolCol, start_seq";
    assertMatchRows(query, new Object[][]{
        {"AAPL", 3, 2, 5},
        {"NFLX", 1, 3, 4},
        {"NFLX", 5, 3, 8}
    });
  }

  /**
   * MATCH_NUMBER() restarts at 1 in every partition, CLASSIFIER() reports the label of the final row of the match,
   * and each single-variable aggregate sees only the rows bound to its own pattern variable.
   *
   * <p>COUNT is pinned here because {@code MatchTerm.Aggregate} used to route {@code COUNT(<expr>)} through the
   * accumulator, which has no COUNT branch. AVG is pinned because an integral result type would truncate 6.5 to 6.
   */
  @Test(dataProvider = "useV2QueryEngine")
  public void testMatchNumberClassifierAndAggregates(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = "SELECT * FROM " + getTableName() + " MATCH_RECOGNIZE ("
        + " PARTITION BY symbolCol ORDER BY seqCol"
        + " MEASURES MATCH_NUMBER() AS mno, CLASSIFIER() AS cls, COUNT(DOWN.priceCol) AS down_count,"
        + " COUNT(*) AS all_count, SUM(DOWN.priceCol) AS down_sum, MIN(DOWN.priceCol) AS down_min,"
        + " MAX(DOWN.priceCol) AS down_max, AVG(DOWN.priceCol) AS down_avg"
        + " ONE ROW PER MATCH"
        + " PATTERN (DOWN+ UP+)"
        + V_SHAPE_DEFINE
        + ") AS mr ORDER BY symbolCol, mno";
    assertMatchRows(query, new Object[][]{
        // AAPL match 1 maps prices 8 and 5 to DOWN, prices 9 and 12 to UP.
        {"AAPL", 1, "UP", 2, 4, 13, 5, 8, 6.5d},
        {"AAPL", 2, "UP", 1, 2, 7, 7, 7, 7.0d},
        {"AMZN", 1, "UP", 3, 4, 30, 5, 15, 10.0d},
        {"MSFT", 1, "UP", 1, 2, 3, 3, 3, 3.0d}
    });
  }

  /**
   * PREV and NEXT are bounded by the partition, not by the input: at a partition boundary they must yield NULL rather
   * than the neighbouring partition's row.
   *
   * <p>The navigations live in DEFINE rather than in MEASURES because Calcite's {@code SqlValidatorImpl
   * .PatternValidator} unconditionally rejects PREV/NEXT inside a MEASURES item (see
   * {@link #testDeferredConstructsAreRejected}), so DEFINE is the only place a query can reach them.
   *
   * <p>{@code PREV(...)} being NULL makes the whole predicate NULL, which SQL:2016 treats as "not matched". So the
   * proof that PREV stops at the partition start is that <b>no</b> row with {@code seqCol = 1} appears in the first
   * result, and the proof that NEXT stops at the partition end is that no partition's last row appears in the second
   * one - even though every one of those rows would satisfy the predicate against a neighbouring partition's price.
   */
  @Test(dataProvider = "useV2QueryEngine")
  public void testPrevAndNextAreBoundedByPartition(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    // Rows whose predecessor within the partition is cheaper. seqCol = 1 is absent for every symbol.
    assertMatchRows(neighbourQuery("PREV(A.priceCol) < A.priceCol"), new Object[][]{
        {"AAPL", 4, 9}, {"AAPL", 5, 12}, {"AAPL", 7, 11},
        {"AMZN", 5, 25},
        {"MSFT", 3, 8},
        {"NFLX", 2, 2}, {"NFLX", 3, 3}, {"NFLX", 4, 4}, {"NFLX", 5, 5}, {"NFLX", 6, 6}, {"NFLX", 7, 7},
        {"NFLX", 8, 8}
    });
    // Rows whose successor within the partition is dearer. The last row of AAPL (7), AMZN (5), GOOG (3), MSFT (3)
    // and NFLX (8) is absent from every one of them.
    assertMatchRows(neighbourQuery("NEXT(A.priceCol) > A.priceCol"), new Object[][]{
        {"AAPL", 3, 5}, {"AAPL", 4, 9}, {"AAPL", 6, 7},
        {"AMZN", 4, 5},
        {"MSFT", 2, 3},
        {"NFLX", 1, 1}, {"NFLX", 2, 2}, {"NFLX", 3, 3}, {"NFLX", 4, 4}, {"NFLX", 5, 5}, {"NFLX", 6, 6},
        {"NFLX", 7, 7}
    });

    // And directly: the NULL is observable, and it happens exactly once per partition on each side.
    assertMatchRows(neighbourQuery("PREV(A.priceCol) IS NULL"), new Object[][]{
        {"AAPL", 1, 10}, {"AMZN", 1, 20}, {"GOOG", 1, 4}, {"MSFT", 1, 5}, {"NFLX", 1, 1}
    });
    assertMatchRows(neighbourQuery("NEXT(A.priceCol) IS NULL"), new Object[][]{
        {"AAPL", 7, 11}, {"AMZN", 5, 25}, {"GOOG", 3, 4}, {"MSFT", 3, 8}, {"NFLX", 8, 8}
    });
  }

  /**
   * PARTITION BY genuinely isolates matches. The {@code ^} and {@code $} anchors are relative to the partition, so
   * each must match exactly once per symbol; if partitioning leaked they would match once for the whole table.
   */
  @Test(dataProvider = "useV2QueryEngine")
  public void testPartitionIsolationAcrossSegments(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    // The fixture really is spread over several segments, so the exchange below the match operator is exercised.
    int numSegments = getSharedHelixResourceManager()
        .getSegmentsFor(TableNameBuilder.OFFLINE.tableNameWithType(getTableName()), false).size();
    assertTrue(numSegments > 1, "Expected the fixture to span more than one segment, but found " + numSegments);

    assertMatchRows(anchoredQuery("^ A"), new Object[][]{
        {"AAPL", 1, 10},
        {"AMZN", 1, 20},
        {"GOOG", 1, 4},
        {"MSFT", 1, 5},
        {"NFLX", 1, 1}
    });
    assertMatchRows(anchoredQuery("A $"), new Object[][]{
        {"AAPL", 7, 11},
        {"AMZN", 5, 25},
        {"GOOG", 3, 4},
        {"MSFT", 3, 8},
        {"NFLX", 8, 8}
    });

    // A pattern that pairs up adjacent rows. Partition sizes are 7, 5, 3, 3 and 8, so the odd row at the end of AAPL,
    // GOOG, MSFT and AMZN is dropped rather than paired with the next partition's first row: 11 matches, not the 13
    // that a single 26-row stream would produce.
    String pairs = "SELECT * FROM " + getTableName() + " MATCH_RECOGNIZE ("
        + " PARTITION BY symbolCol ORDER BY seqCol"
        + " MEASURES FIRST(A.seqCol) AS start_seq, LAST(B.seqCol) AS end_seq"
        + " ONE ROW PER MATCH AFTER MATCH SKIP PAST LAST ROW"
        + " PATTERN (A B)"
        + " DEFINE A AS A.priceCol > 0, B AS B.priceCol > 0"
        + ") AS mr ORDER BY symbolCol, start_seq";
    assertMatchRows(pairs, new Object[][]{
        {"AAPL", 1, 2}, {"AAPL", 3, 4}, {"AAPL", 5, 6},
        {"AMZN", 1, 2}, {"AMZN", 3, 4},
        {"GOOG", 1, 2},
        {"MSFT", 1, 2},
        {"NFLX", 1, 2}, {"NFLX", 3, 4}, {"NFLX", 5, 6}, {"NFLX", 7, 8}
    });
  }

  /**
   * A multi-column PARTITION BY whose source order is not ascending input-column order.
   *
   * <p>The Pinot schema is a {@code TreeMap}, so the input columns are {@code priceCol(0), seqCol(1), symbolCol(2)}
   * and {@code PARTITION BY symbolCol, priceCol} is therefore in <b>descending</b> index order. Calcite's
   * {@code Match#getPartitionKeys()} is an {@code ImmutableBitSet}, which loses that order, while the output row type
   * keeps it - so an engine that read the partition keys off the bit set would write {@code priceCol}'s Integer into
   * the STRING slot and fail the whole query with a ClassCastException.
   *
   * <p>GOOG is the load-bearing row: its three rows all cost 4, so they form one three-row partition and
   * {@code COUNT(*)} reports 3. Every other (symbol, price) pair is unique and yields a one-row partition.
   */
  @Test(dataProvider = "useV2QueryEngine")
  public void testPartitionByKeepsItsSourceOrder(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = "SELECT * FROM " + getTableName() + " MATCH_RECOGNIZE ("
        + " PARTITION BY symbolCol, priceCol"
        + " ORDER BY seqCol"
        + " MEASURES COUNT(*) AS cnt"
        + " ONE ROW PER MATCH AFTER MATCH SKIP PAST LAST ROW"
        + " PATTERN (A+)"
        + " DEFINE A AS A.priceCol > 0"
        + ") AS mr ORDER BY symbolCol, priceCol";
    assertMatchRows(query, new Object[][]{
        {"AAPL", 5, 1}, {"AAPL", 7, 1}, {"AAPL", 8, 1}, {"AAPL", 9, 1}, {"AAPL", 10, 1}, {"AAPL", 11, 1},
        {"AAPL", 12, 1},
        {"AMZN", 5, 1}, {"AMZN", 10, 1}, {"AMZN", 15, 1}, {"AMZN", 20, 1}, {"AMZN", 25, 1},
        {"GOOG", 4, 3},
        {"MSFT", 3, 1}, {"MSFT", 5, 1}, {"MSFT", 8, 1},
        {"NFLX", 1, 1}, {"NFLX", 2, 1}, {"NFLX", 3, 1}, {"NFLX", 4, 1}, {"NFLX", 5, 1}, {"NFLX", 6, 1},
        {"NFLX", 7, 1}, {"NFLX", 8, 1}
    });
  }

  /**
   * Every deferred construct is rejected during planning with a message that names it, rather than producing a wrong
   * result or an internal Calcite failure.
   */
  @Test(dataProvider = "useV2QueryEngine")
  public void testDeferredConstructsAreRejected(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    assertPlanningError("SELECT * FROM " + getTableName() + " MATCH_RECOGNIZE ("
        + " PARTITION BY symbolCol ORDER BY seqCol"
        + " MEASURES CLASSIFIER() AS cls"
        + " ALL ROWS PER MATCH"
        + " PATTERN (DOWN+ UP+)"
        + V_SHAPE_DEFINE
        + ") AS mr", "ALL ROWS PER MATCH is not supported yet");

    assertPlanningError("SELECT * FROM " + getTableName() + " MATCH_RECOGNIZE ("
        + " PARTITION BY symbolCol ORDER BY seqCol"
        + " MEASURES LAST(DOWN.priceCol) AS p"
        + " ONE ROW PER MATCH"
        + " PATTERN (DOWN+ UP+)"
        + " SUBSET BOTH = (DOWN, UP)"
        + V_SHAPE_DEFINE
        + ") AS mr", "SUBSET is not supported yet");

    assertPlanningError("SELECT * FROM " + getTableName() + " MATCH_RECOGNIZE ("
        + " PARTITION BY symbolCol"
        + " MEASURES LAST(DOWN.priceCol) AS p"
        + " ONE ROW PER MATCH"
        + " PATTERN (DOWN+ UP+)"
        + V_SHAPE_DEFINE
        + ") AS mr", "MATCH_RECOGNIZE requires an ORDER BY clause");

    assertPlanningError("SELECT * FROM " + getTableName() + " MATCH_RECOGNIZE ("
        + " PARTITION BY symbolCol ORDER BY seqCol"
        + " MEASURES LAST(DOWN.priceCol) AS p"
        + " ONE ROW PER MATCH"
        + " PATTERN (DOWN+ UP+)"
        + " DEFINE DOWN AS COUNT(DOWN.priceCol) < 3, UP AS UP.priceCol > PREV(UP.priceCol)"
        + ") AS mr", "is not supported yet in the MATCH_RECOGNIZE DEFINE clause");

    // PERMUTE is a non-reserved keyword, so the single argument form parses as a concatenation of an undefined
    // pattern variable named PERMUTE and a group. MatchRecognizeValidator turns that silently wrong plan into an
    // explicit error.
    assertPlanningError("SELECT * FROM " + getTableName() + " MATCH_RECOGNIZE ("
        + " PARTITION BY symbolCol ORDER BY seqCol"
        + " MEASURES LAST(DOWN.priceCol) AS p"
        + " ONE ROW PER MATCH"
        + " PATTERN (PERMUTE(DOWN))"
        + " DEFINE DOWN AS DOWN.priceCol < PREV(DOWN.priceCol)"
        + ") AS mr", "PERMUTE is not supported yet");

    // Calcite's SqlValidatorImpl.PatternValidator rejects PREV/NEXT anywhere inside a MEASURES item, however they are
    // nested. SQL:2016, Oracle and Trino all allow them there, and MatchTerm.Navigation implements them, but no query
    // can reach that path: physical navigation is only usable from DEFINE. Pinned here so the day the restriction is
    // lifted or wrapped in a Pinot specific message, this test says so.
    assertPlanningError("SELECT * FROM " + getTableName() + " MATCH_RECOGNIZE ("
        + " PARTITION BY symbolCol ORDER BY seqCol"
        + " MEASURES PREV(A.priceCol) AS prev_price"
        + " ONE ROW PER MATCH"
        + " PATTERN (A)"
        + " DEFINE A AS A.priceCol > 0"
        + ") AS mr", "Cannot use PREV/NEXT in MEASURE");

    // MEASURES is optional in SQL:2016, but Calcite then makes the MATCH_RECOGNIZE row type the whole *input* row
    // type instead of the ONE ROW PER MATCH shape, so the query used to plan and then die on the server.
    assertPlanningError("SELECT * FROM " + getTableName() + " MATCH_RECOGNIZE ("
        + " PARTITION BY symbolCol ORDER BY seqCol"
        + " ONE ROW PER MATCH"
        + " PATTERN (DOWN+ UP+)"
        + V_SHAPE_DEFINE
        + ") AS mr", "MATCH_RECOGNIZE requires a MEASURES clause");

    // Calcite adds a measure to the row type only when its alias is still free, so a measure aliased to a PARTITION
    // BY column silently disappears from the output.
    assertPlanningError("SELECT * FROM " + getTableName() + " MATCH_RECOGNIZE ("
        + " PARTITION BY symbolCol ORDER BY seqCol"
        + " MEASURES LAST(DOWN.priceCol) AS symbolCol"
        + " ONE ROW PER MATCH"
        + " PATTERN (DOWN+ UP+)"
        + V_SHAPE_DEFINE
        + ") AS mr", "collides with a PARTITION BY column or an earlier measure alias");

    // Calcite qualifies an unqualified column reference with the row source alias, so a pattern variable of the same
    // name steals every unqualified reference from the SQL:2016 universal row pattern variable - silently changing
    // both measure values and which rows match.
    assertPlanningError("SELECT * FROM " + getTableName() + " AS DOWN MATCH_RECOGNIZE ("
        + " PARTITION BY symbolCol ORDER BY seqCol"
        + " MEASURES LAST(priceCol) AS p"
        + " ONE ROW PER MATCH"
        + " PATTERN (DOWN+ UP+)"
        + V_SHAPE_DEFINE
        + ")", "collides with the row source alias");
  }

  private String neighbourQuery(String definition) {
    return "SELECT * FROM " + getTableName() + " MATCH_RECOGNIZE ("
        + " PARTITION BY symbolCol ORDER BY seqCol"
        + " MEASURES LAST(A.seqCol) AS seq, LAST(A.priceCol) AS cur_price"
        + " ONE ROW PER MATCH AFTER MATCH SKIP PAST LAST ROW"
        + " PATTERN (A)"
        + " DEFINE A AS " + definition
        + ") AS mr ORDER BY symbolCol, seq";
  }

  private String anchoredQuery(String pattern) {
    return "SELECT * FROM " + getTableName() + " MATCH_RECOGNIZE ("
        + " PARTITION BY symbolCol ORDER BY seqCol"
        + " MEASURES LAST(A.seqCol) AS seq, LAST(A.priceCol) AS cur_price"
        + " ONE ROW PER MATCH AFTER MATCH SKIP PAST LAST ROW"
        + " PATTERN (" + pattern + ")"
        + " DEFINE A AS A.priceCol > 0"
        + ") AS mr ORDER BY symbolCol";
  }

  private String vShapeQuery(String afterMatch) {
    return "SELECT * FROM " + getTableName() + " MATCH_RECOGNIZE ("
        + " PARTITION BY symbolCol ORDER BY seqCol"
        + " MEASURES FIRST(DOWN.seqCol) AS start_seq, FIRST(DOWN.priceCol) AS start_price,"
        + " LAST(UP.priceCol) AS end_price"
        + " ONE ROW PER MATCH " + afterMatch
        + " PATTERN (DOWN+ UP+)"
        + V_SHAPE_DEFINE
        + ") AS mr ORDER BY symbolCol, start_seq";
  }

  private String skipTargetQuery(String afterMatch) {
    return "SELECT * FROM " + getTableName() + " MATCH_RECOGNIZE ("
        + " PARTITION BY symbolCol ORDER BY seqCol"
        + " MEASURES FIRST(S.seqCol) AS start_seq, LAST(U.seqCol) AS end_seq"
        + " ONE ROW PER MATCH " + afterMatch
        + " PATTERN (S{2} U{2})"
        + " DEFINE U AS U.priceCol > PREV(U.priceCol)"
        + ") AS mr ORDER BY symbolCol, start_seq";
  }

  private String quantifierQuery(String quantifier) {
    return "SELECT * FROM " + getTableName() + " MATCH_RECOGNIZE ("
        + " PARTITION BY symbolCol ORDER BY seqCol"
        + " MEASURES FIRST(S.seqCol) AS start_seq, COUNT(U.priceCol) AS u_count, LAST(U.seqCol) AS end_seq"
        + " ONE ROW PER MATCH AFTER MATCH SKIP PAST LAST ROW"
        + " PATTERN (S U" + quantifier + ")"
        + " DEFINE U AS U.priceCol > PREV(U.priceCol)"
        + ") AS mr ORDER BY symbolCol, start_seq";
  }

  /**
   * Asserts the full result set of {@code query}, cell by cell. {@code null} expects a SQL NULL, a {@link String}
   * expects a string, a {@link Double} expects an approximate double and any other {@link Number} an exact integer.
   */
  private void assertMatchRows(String query, Object[][] expected)
      throws Exception {
    JsonNode response = postQuery(query);
    QueryAssert.assertThat(response).hasNoExceptions();
    JsonNode rows = response.get("resultTable").get("rows");
    assertNotNull(rows, "No rows in response for query: " + query);
    String context = "\nQuery: " + query + "\nRows: " + rows;
    assertEquals(rows.size(), expected.length, "Unexpected number of matches." + context);
    for (int i = 0; i < expected.length; i++) {
      JsonNode row = rows.get(i);
      Object[] expectedRow = expected[i];
      assertEquals(row.size(), expectedRow.length, "Unexpected number of columns in row " + i + "." + context);
      for (int j = 0; j < expectedRow.length; j++) {
        Object expectedValue = expectedRow[j];
        JsonNode actual = row.get(j);
        String where = "Mismatch at row " + i + ", column " + j + "." + context;
        if (expectedValue == null) {
          assertTrue(actual.isNull(), where + "\nExpected NULL but got: " + actual);
        } else if (expectedValue instanceof String) {
          assertEquals(actual.asText(), expectedValue, where);
        } else if (expectedValue instanceof Double) {
          assertEquals(actual.asDouble(), (double) (Double) expectedValue, 1e-9, where);
        } else {
          assertEquals(actual.asLong(), ((Number) expectedValue).longValue(), where);
        }
      }
    }
  }

  private void assertPlanningError(String query, String expectedMessage)
      throws Exception {
    QueryAssert.assertThat(postQuery(query)).firstException().containsMessage(expectedMessage);
  }

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension(SYMBOL_COLUMN, FieldSpec.DataType.STRING)
        .addSingleValueDimension(SEQ_COLUMN, FieldSpec.DataType.INT)
        .addSingleValueDimension(PRICE_COLUMN, FieldSpec.DataType.INT)
        .build();
  }

  @Override
  public int getNumAvroFiles() {
    return 3;
  }

  @Override
  protected long getCountStarResult() {
    int total = 0;
    for (int[] prices : PRICES) {
      total += prices.length;
    }
    return total;
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(List.of(
        new org.apache.avro.Schema.Field(SYMBOL_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), null, null),
        new org.apache.avro.Schema.Field(SEQ_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT), null, null),
        new org.apache.avro.Schema.Field(PRICE_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT), null, null)
    ));

    try (AvroFilesAndWriters avroFilesAndWriters = createAvroFilesAndWriters(avroSchema)) {
      List<DataFileWriter<GenericData.Record>> writers = avroFilesAndWriters.getWriters();
      // Round-robin so that consecutive rows of a partition land in different segments: the match operator must rely
      // on the sort exchange below it, never on the physical layout.
      int rowIndex = 0;
      for (int s = 0; s < SYMBOLS.length; s++) {
        int[] prices = PRICES[s];
        for (int i = 0; i < prices.length; i++) {
          GenericData.Record record = new GenericData.Record(avroSchema);
          record.put(SYMBOL_COLUMN, SYMBOLS[s]);
          record.put(SEQ_COLUMN, i + 1);
          record.put(PRICE_COLUMN, prices[i]);
          writers.get(rowIndex++ % getNumAvroFiles()).append(record);
        }
      }
      return avroFilesAndWriters.getAvroFiles();
    }
  }
}
