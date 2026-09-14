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
package org.apache.pinot.common.pagecache;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pinot.common.pagecache.WarmupQueryUtils.Candidate;
import org.apache.pinot.common.pagecache.WarmupQueryUtils.Config;
import org.apache.pinot.common.pagecache.WarmupQueryUtils.Policy;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class WarmupQueryUtilsTest {
  private static final long MILLIS_PER_HOUR = 3600_000L;

  private static Candidate candidate(String query, long requestTimeMs, long latencyMs, long numDocsScanned,
      int errorCode) {
    return new Candidate(query, requestTimeMs, latencyMs, numDocsScanned, errorCode, null);
  }

  private static Config config() {
    return Config.builder().build();
  }

  // ---------------------------------------------------------------------------------------------
  // Selection
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testLatencyPolicyKeepsAbovePercentileSortedDesc() {
    List<Candidate> candidates = new ArrayList<>();
    for (int i = 1; i <= 10; i++) {
      candidates.add(candidate("q" + i, 0L, i, 0L, 0));
    }
    Config config = Config.builder().setPercentile(50).build();
    List<String> selected = WarmupQueryUtils.select(candidates, Policy.LATENCY, config);
    // p50 cut-off is 5 (nearest-rank); keep latency > 5, ordered by latency descending.
    assertEquals(selected, List.of("q10", "q9", "q8", "q7", "q6"));
  }

  @Test
  public void testNumDocsScannedPolicyKeepsAbovePercentileSortedDesc() {
    List<Candidate> candidates = new ArrayList<>();
    for (int i = 1; i <= 10; i++) {
      candidates.add(candidate("q" + i, 0L, 0L, i * 100L, 0));
    }
    Config config = Config.builder().setPercentile(50).build();
    List<String> selected = WarmupQueryUtils.select(candidates, Policy.NUM_DOCS_SCANNED, config);
    assertEquals(selected, List.of("q10", "q9", "q8", "q7", "q6"));
  }

  @Test
  public void testUniformPolicySpreadsAcrossBucketsAndExcludesOutOfWindow() {
    long now = 100_000_000_000L;
    List<Candidate> candidates = new ArrayList<>();
    // Bucket [0,2)h
    candidates.add(candidate("recent1", now - 1 * MILLIS_PER_HOUR, 0L, 0L, 0));
    candidates.add(candidate("recent2", now - 1 * MILLIS_PER_HOUR, 0L, 0L, 0));
    candidates.add(candidate("recent3", now - 1 * MILLIS_PER_HOUR, 0L, 0L, 0));
    // Bucket [2,4)h
    candidates.add(candidate("older1", now - 3 * MILLIS_PER_HOUR, 0L, 0L, 0));
    candidates.add(candidate("older2", now - 3 * MILLIS_PER_HOUR, 0L, 0L, 0));
    candidates.add(candidate("older3", now - 3 * MILLIS_PER_HOUR, 0L, 0L, 0));
    // Outside the 4h look-back window
    candidates.add(candidate("ancient", now - 10 * MILLIS_PER_HOUR, 0L, 0L, 0));

    Config config = Config.builder()
        .setLookbackHours(4).setFrequencyHours(2).setMaxQueries(4).setNowMs(now).build();
    List<String> selected = WarmupQueryUtils.select(candidates, Policy.UNIFORM, config);
    // perBucket = max(1, 4/2) = 2, drawn from each of the two buckets; the ancient query is excluded.
    assertEquals(selected.size(), 4);
    assertFalse(selected.contains("ancient"));
    Set<String> selectedSet = new HashSet<>(selected);
    assertTrue(selectedSet.stream().anyMatch(q -> q.startsWith("recent")));
    assertTrue(selectedSet.stream().anyMatch(q -> q.startsWith("older")));
  }

  @Test
  public void testDedupByQueryText() {
    List<Candidate> candidates = List.of(
        candidate("SELECT 1", 0L, 5L, 0L, 0),
        candidate("SELECT 1", 0L, 7L, 0L, 0));
    assertEquals(WarmupQueryUtils.select(candidates, Policy.LATENCY, config()), List.of("SELECT 1"));
  }

  @Test
  public void testErrorResponsesFilteredOut() {
    List<Candidate> candidates = List.of(
        candidate("ok", 0L, 5L, 0L, 0),
        candidate("failed", 0L, 9L, 0L, 1));
    assertEquals(WarmupQueryUtils.select(candidates, Policy.UNIFORM, config()), List.of("ok"));
  }

  @Test
  public void testOverLongQueriesFilteredOut() {
    Config config = Config.builder().setMaxQueryLength(8).build();
    List<Candidate> candidates = List.of(
        candidate("short", 0L, 0L, 0L, 0),
        candidate("this-is-way-too-long", 0L, 0L, 0L, 0));
    assertEquals(WarmupQueryUtils.select(candidates, Policy.UNIFORM, config), List.of("short"));
  }

  @Test
  public void testMaxQueriesCap() {
    List<Candidate> candidates = List.of(
        candidate("q1", 0L, 0L, 0L, 0),
        candidate("q2", 0L, 0L, 0L, 0),
        candidate("q3", 0L, 0L, 0L, 0));
    Config config = Config.builder().setMaxQueries(2).build();
    assertEquals(WarmupQueryUtils.select(candidates, Policy.UNIFORM, config).size(), 2);
  }

  @Test
  public void testStatlessCandidatesFallBackToOrder() {
    // File-style candidates (no latency/time stats) must still yield a usable set under any policy.
    List<Candidate> candidates = List.of(
        candidate("a", 0L, 0L, 0L, 0),
        candidate("b", 0L, 0L, 0L, 0),
        candidate("c", 0L, 0L, 0L, 0));
    assertEquals(WarmupQueryUtils.select(candidates, Policy.LATENCY, config()), List.of("a", "b", "c"));
  }

  @Test
  public void testHybridBlendsAndDedups() {
    long now = 100_000_000_000L;
    List<Candidate> candidates = List.of(
        candidate("slow", now - MILLIS_PER_HOUR, 1000L, 10L, 0),
        candidate("heavy", now - MILLIS_PER_HOUR, 10L, 1000L, 0),
        candidate("plain", now - MILLIS_PER_HOUR, 10L, 10L, 0));
    Config config = Config.builder().setNowMs(now).build();
    List<String> selected = WarmupQueryUtils.select(candidates, Policy.HYBRID, config);
    assertEquals(new HashSet<>(selected), Set.of("slow", "heavy", "plain"));
    assertEquals(selected.size(), 3);
  }

  @Test
  public void testEmptyInput() {
    assertTrue(WarmupQueryUtils.select(List.of(), Policy.HYBRID, config()).isEmpty());
  }

  @Test
  public void testPercentileNearestRank() {
    long[] values = {1, 2, 3, 4, 5};
    assertEquals(WarmupQueryUtils.percentile(values, 50), 3);
    assertEquals(WarmupQueryUtils.percentile(values, 100), 5);
    assertEquals(WarmupQueryUtils.percentile(values, 0), 1);
    assertEquals(WarmupQueryUtils.percentile(new long[0], 90), 0);
  }

  // ---------------------------------------------------------------------------------------------
  // Table-name rewrite
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testRewriteSimpleFrom() {
    assertEquals(WarmupQueryUtils.rewriteTableName("SELECT * FROM myTable", "myTable", "myTable_OFFLINE"),
        "SELECT * FROM myTable_OFFLINE");
  }

  @Test
  public void testRewriteFromWithTrailingClause() {
    assertEquals(WarmupQueryUtils.rewriteTableName("SELECT a FROM myTable WHERE a > 1", "myTable", "myTable_REALTIME"),
        "SELECT a FROM myTable_REALTIME WHERE a > 1");
  }

  @Test
  public void testRewriteQuotedIdentifierKeepsQuotes() {
    assertEquals(WarmupQueryUtils.rewriteTableName("SELECT * FROM \"myTable\"", "myTable", "myTable_OFFLINE"),
        "SELECT * FROM \"myTable_OFFLINE\"");
  }

  @Test
  public void testRewriteCaseInsensitiveKeyword() {
    assertEquals(WarmupQueryUtils.rewriteTableName("select * from myTable", "myTable", "myTable_OFFLINE"),
        "select * from myTable_OFFLINE");
  }

  @Test
  public void testRewriteNewlineWhitespacePreserved() {
    assertEquals(WarmupQueryUtils.rewriteTableName("SELECT *\nFROM  myTable", "myTable", "myTable_OFFLINE"),
        "SELECT *\nFROM  myTable_OFFLINE");
  }

  @Test
  public void testRewriteColumnWithTableNameAsPrefixNotRewritten() {
    assertEquals(WarmupQueryUtils.rewriteTableName("SELECT myTable_id FROM myTable", "myTable", "myTable_OFFLINE"),
        "SELECT myTable_id FROM myTable_OFFLINE");
  }

  @Test
  public void testRewriteColumnEqualToTableNameNotRewritten() {
    assertEquals(WarmupQueryUtils.rewriteTableName("SELECT myTable FROM myTable", "myTable", "myTable_OFFLINE"),
        "SELECT myTable FROM myTable_OFFLINE");
  }

  @Test
  public void testRewriteStringLiteralNotRewritten() {
    assertEquals(
        WarmupQueryUtils.rewriteTableName("SELECT * FROM myTable WHERE c = 'myTable'", "myTable", "myTable_OFFLINE"),
        "SELECT * FROM myTable_OFFLINE WHERE c = 'myTable'");
  }

  @Test
  public void testRewriteAlreadyTypedIsIdempotent() {
    assertEquals(WarmupQueryUtils.rewriteTableName("SELECT * FROM myTable_OFFLINE", "myTable", "myTable_OFFLINE"),
        "SELECT * FROM myTable_OFFLINE");
  }

  @Test
  public void testRewriteJoinTargetRewritten() {
    assertEquals(
        WarmupQueryUtils.rewriteTableName("SELECT * FROM a JOIN myTable ON a.id = myTable.id", "myTable",
            "myTable_OFFLINE"),
        "SELECT * FROM a JOIN myTable_OFFLINE ON a.id = myTable.id");
  }

  @Test
  public void testRewriteTrailingSemicolon() {
    assertEquals(WarmupQueryUtils.rewriteTableName("SELECT * FROM myTable;", "myTable", "myTable_OFFLINE"),
        "SELECT * FROM myTable_OFFLINE;");
  }

  @Test
  public void testRewriteUnrelatedQueryUnchanged() {
    assertEquals(WarmupQueryUtils.rewriteTableName("SELECT * FROM otherTable", "myTable", "myTable_OFFLINE"),
        "SELECT * FROM otherTable");
  }

  // ---------------------------------------------------------------------------------------------
  // Query-log parsing
  // ---------------------------------------------------------------------------------------------

  private static final String SQL = "SELECT a, b, c FROM myTable WHERE x = 1";
  private static final String LINE =
      "2024/06/21 10:00:00.000 INFO [BrokerRequestHandler] [qp-1] requestId=5,table=myTable,queryHash=ab12,"
          + "timeMs=123,docs=1000/5000,entries=10/20,exceptions=0,workloadName=default,query=" + SQL;

  @Test
  public void testParseStructuredFieldsAndSqlWithCommas() {
    WarmupQueryUtils.ParsedLine parsed = WarmupQueryUtils.parseLogLine(LINE, 0);
    assertNotNull(parsed);
    assertEquals(parsed.getTableName(), "myTable");
    Candidate candidate = parsed.getCandidate();
    // The SQL, including its embedded commas, is preserved verbatim.
    assertEquals(candidate.getQuery(), SQL);
    assertEquals(candidate.getLatencyMs(), 123L);
    assertEquals(candidate.getNumDocsScanned(), 1000L);
    assertEquals(candidate.getErrorCode(), 0);
    assertEquals(candidate.getDedupKey(), "ab12");
    assertEquals(candidate.getRequestTimeMs(), 0L);
  }

  @Test
  public void testParseExceptionsRecordedAsErrorCode() {
    WarmupQueryUtils.ParsedLine parsed = WarmupQueryUtils.parseLogLine(LINE.replace("exceptions=0", "exceptions=2"), 0);
    assertNotNull(parsed);
    assertEquals(parsed.getCandidate().getErrorCode(), 2);
  }

  @Test
  public void testParseTruncatedQueryDropped() {
    // When the logged query length reaches the configured cap, it is assumed truncated and dropped.
    assertNull(WarmupQueryUtils.parseLogLine(LINE, SQL.length()));
    assertNotNull(WarmupQueryUtils.parseLogLine(LINE, SQL.length() + 1));
  }

  @Test
  public void testParseTypeSuffixNormalizedToRawTableName() {
    WarmupQueryUtils.ParsedLine parsed =
        WarmupQueryUtils.parseLogLine(LINE.replace("table=myTable,", "table=myTable_OFFLINE,"), 0);
    assertNotNull(parsed);
    assertEquals(parsed.getTableName(), "myTable");
  }

  @Test
  public void testParseMultiStageTableListSkipped() {
    assertNull(WarmupQueryUtils.parseLogLine(LINE.replace("table=myTable,", "table=[t1, t2],"), 0));
  }

  @Test
  public void testParseLineWithoutQueryMarkerReturnsNull() {
    assertNull(WarmupQueryUtils.parseLogLine("requestId=5,table=myTable,timeMs=1,exceptions=0", 0));
  }

  @Test
  public void testParseBlankLineReturnsNull() {
    assertNull(WarmupQueryUtils.parseLogLine("   ", 0));
  }

  @Test
  public void testParseMissingTableReturnsNull() {
    assertNull(WarmupQueryUtils.parseLogLine("requestId=5,timeMs=1,exceptions=0,query=" + SQL, 0));
  }
}
