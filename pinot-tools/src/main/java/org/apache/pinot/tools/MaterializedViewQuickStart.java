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
package org.apache.pinot.tools;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.minion.MinionClient;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.apache.pinot.tools.admin.command.QuickstartRunner;


/**
 * Quickstart that demonstrates Materialized View (MV) creation and ingestion.
 *
 * <p>This quickstart:
 * <ol>
 *   <li>Loads the {@code airlineStats} base table (31 days of flight data, Jan 2014).</li>
 *   <li>Creates an empty {@code airlineStatsMv} table configured with a
 *       {@code MaterializedViewTask} that pre-aggregates daily carrier metrics.</li>
 *   <li>Triggers the minion task to materialize the MV and waits for completion.</li>
 *   <li>Runs each aggregation against both the base table and the MV table, prints both
 *       result sets, and asserts they agree (the MV is correct iff the re-aggregation over
 *       the MV's per-day-per-carrier rows reproduces the base table's answer).</li>
 * </ol>
 *
 * <p>The MV definition is:
 * <pre>
 *   SELECT DaysSinceEpoch, Carrier,
 *          SUM(ArrDelay) AS sum_ArrDelay,
 *          COUNT(*) AS flight_count,
 *          MIN(ArrDelay) AS min_ArrDelay,
 *          MAX(ArrDelay) AS max_ArrDelay,
 *          DISTINCTCOUNTRAWHLL(FlightNum) AS raw_hll_FlightNum,
 *          DISTINCTCOUNTRAWHLLPLUS(FlightNum) AS raw_hllplus_FlightNum
 *   FROM airlineStats
 *   GROUP BY DaysSinceEpoch, Carrier
 * </pre>
 *
 * <p>Broker query rewrite (so callers don't have to know about the MV) lands in a follow-up
 * PR; until then, callers query the MV table directly. The comparison step in this quickstart
 * uses the same re-aggregation pattern the rewrite engine will use:
 * {@code SUM} over {@code sum_ArrDelay}, {@code SUM} over {@code flight_count},
 * {@code MIN}/{@code MAX} over their stored mins/maxes, and {@code DISTINCTCOUNTHLL} /
 * {@code DISTINCTCOUNTHLLPLUS} applied directly to the raw-sketch columns (the sketch is
 * deserialized and merged in-place).
 *
 * <p>The example table config sets {@code maxTasksPerBatch=31} to backfill all 31 days of
 * the airlineStats fixture in a single scheduling cycle. Production deployments typically
 * leave the default of 1; raise it only when intentionally back-filling and after sizing
 * the minion pool to absorb the resulting concurrent task load.
 *
 * <p>Run via: {@code bin/pinot-admin.sh QuickStart -type MATERIALIZED_VIEW}
 */
public class MaterializedViewQuickStart extends Quickstart {

  private static final String BASE_TABLE = "airlineStats";
  private static final String MATERIALIZED_VIEW_TABLE = "airlineStatsMv";
  private static final int FIXTURE_COVERAGE_UPPER_DAY = 16102;
  private static final long TASK_POLL_INTERVAL_MS = 5_000L;
  private static final long TASK_TIMEOUT_MS = 300_000L;

  @Override
  public List<String> types() {
    return Arrays.asList("MATERIALIZED_VIEW", "MATERIALIZED-VIEW", "BATCH_MV");
  }

  @Override
  public Map<String, Object> getConfigOverrides() {
    Map<String, Object> overrides = new HashMap<>(super.getConfigOverrides());
    overrides.putIfAbsent("controller.task.scheduler.enabled", true);
    return overrides;
  }

  @Override
  protected String[] getDefaultBatchTableDirectories() {
    return new String[]{
        "examples/batch/airlineStats",
        "examples/batch/airlineStatsMv"
    };
  }

  @Override
  protected String getValidationTypesToSkip() {
    return "TASK";
  }

  @Override
  public void runSampleQueries(QuickstartRunner runner)
      throws Exception {
    printStatus(Color.CYAN, "***** Step 1: Verify airlineStats base table is loaded *****");

    runQuery(runner, "Count all flights in airlineStats",
        "SELECT COUNT(*) FROM " + BASE_TABLE + " LIMIT 1");

    runQuery(runner, "Top 10 carriers by total arrival delay (direct base table query)",
        "SELECT Carrier, SUM(ArrDelay) AS total_delay, COUNT(*) AS flights "
            + "FROM " + BASE_TABLE + " GROUP BY Carrier ORDER BY total_delay DESC LIMIT 10");

    MinionClient minionClient = new MinionClient(
        "http://localhost:" + QuickstartRunner.DEFAULT_CONTROLLER_PORT, null);

    printStatus(Color.CYAN, "***** Step 2: Trigger MaterializedViewTask to generate MV segments *****");
    printStatus(Color.GREEN,
        "airlineStatsMv stores SUM, COUNT, MIN, MAX and raw HLL/HLLPlus sketches by day and carrier.");
    triggerMaterializedViewTask(minionClient);

    printStatus(Color.CYAN, "***** Step 3: Wait for MV segments to be generated and served *****");
    waitForMaterializedViewSegments(runner, minionClient);

    printStatus(Color.CYAN, "***** Step 4: Verify base-table vs MV-table results match *****");
    printStatus(Color.GREEN,
        "For each aggregation, the same logical answer is computed two ways: directly from the base, and by "
            + "re-aggregating the pre-computed values stored in the MV. Mismatches indicate an MV ingestion bug.");

    String windowFilter = " WHERE DaysSinceEpoch < " + FIXTURE_COVERAGE_UPPER_DAY + " ";

    runComparison(runner, "SUM and COUNT: top 10 carriers by total arrival delay",
        "SELECT Carrier, SUM(ArrDelay) AS total_delay, COUNT(*) AS flights "
            + "FROM " + BASE_TABLE + windowFilter
            + "GROUP BY Carrier ORDER BY Carrier LIMIT 100",
        "SELECT Carrier, SUM(sum_ArrDelay) AS total_delay, SUM(flight_count) AS flights "
            + "FROM " + MATERIALIZED_VIEW_TABLE + " GROUP BY Carrier ORDER BY Carrier LIMIT 100");

    // Time-filtered carrier comparison. Both sides apply a day-window predicate on their native
    // time column (DaysSinceEpoch on the base, tsMs=DaysSinceEpoch*86400000 on the MV) and
    // collapse to per-Carrier totals so the row shapes match without TIMESTAMP-vs-LONG wire
    // serialization differences. Exercises the time predicate on the MV side; the unfiltered
    // queries above already cover full-table re-aggregation correctness.
    long fromTsMs = 16071L * 86400000L;
    long toTsMs = 16080L * 86400000L;
    runComparison(runner, "SUM (time-filtered): carrier totals over first 10 days of Jan 2014",
        "SELECT Carrier, SUM(ArrDelay) AS total_delay, COUNT(*) AS flights "
            + "FROM " + BASE_TABLE
            + " WHERE DaysSinceEpoch BETWEEN 16071 AND 16080 "
            + "GROUP BY Carrier ORDER BY Carrier LIMIT 100",
        "SELECT Carrier, SUM(sum_ArrDelay) AS total_delay, SUM(flight_count) AS flights "
            + "FROM " + MATERIALIZED_VIEW_TABLE
            + " WHERE tsMs BETWEEN " + fromTsMs + " AND " + toTsMs + " "
            + "GROUP BY Carrier ORDER BY Carrier LIMIT 100");

    runComparison(runner, "MIN and MAX: arrival delay range by carrier",
        "SELECT Carrier, MIN(ArrDelay) AS min_delay, MAX(ArrDelay) AS max_delay "
            + "FROM " + BASE_TABLE + windowFilter
            + "GROUP BY Carrier ORDER BY Carrier LIMIT 100",
        "SELECT Carrier, MIN(min_ArrDelay) AS min_delay, MAX(max_ArrDelay) AS max_delay "
            + "FROM " + MATERIALIZED_VIEW_TABLE + " GROUP BY Carrier ORDER BY Carrier LIMIT 100");

    // HLL sketches: DISTINCTCOUNTHLL applied to the raw-sketch column merges sketches before
    // returning the cardinality estimate.  Identical sketch parameters in both queries are
    // required for byte-identical results — the MV stored DISTINCTCOUNTRAWHLL with defaults,
    // so we query DISTINCTCOUNTHLL with defaults on both sides.
    runComparison(runner, "DISTINCTCOUNTHLL: approximate distinct flight numbers by carrier",
        "SELECT Carrier, DISTINCTCOUNTHLL(FlightNum) AS approx_flight_nums "
            + "FROM " + BASE_TABLE + windowFilter
            + "GROUP BY Carrier ORDER BY Carrier LIMIT 100",
        "SELECT Carrier, DISTINCTCOUNTHLL(raw_hll_FlightNum) AS approx_flight_nums "
            + "FROM " + MATERIALIZED_VIEW_TABLE + " GROUP BY Carrier ORDER BY Carrier LIMIT 100");

    runComparison(runner, "DISTINCTCOUNTHLLPLUS: approximate distinct flight numbers by carrier",
        "SELECT Carrier, DISTINCTCOUNTHLLPLUS(FlightNum) AS approx_flight_nums_hllplus "
            + "FROM " + BASE_TABLE + windowFilter
            + "GROUP BY Carrier ORDER BY Carrier LIMIT 100",
        "SELECT Carrier, DISTINCTCOUNTHLLPLUS(raw_hllplus_FlightNum) AS approx_flight_nums_hllplus "
            + "FROM " + MATERIALIZED_VIEW_TABLE + " GROUP BY Carrier ORDER BY Carrier LIMIT 100");

    printStatus(Color.GREEN, String.format(
        "You can always go to http://localhost:%d to play around in the query console",
        QuickstartRunner.DEFAULT_CONTROLLER_PORT));
    printStatus(Color.GREEN,
        "Try: SELECT Carrier, MIN(min_ArrDelay), MAX(max_ArrDelay) "
            + "FROM airlineStatsMv GROUP BY Carrier");
  }

  private JsonNode runQuery(QuickstartRunner runner, String description, String query)
      throws Exception {
    printStatus(Color.YELLOW, description);
    printStatus(Color.CYAN, "Query : " + query);
    JsonNode response = runner.runQuery(query);
    printStatus(Color.YELLOW, prettyPrintResponse(response));
    printStatus(Color.GREEN, "***************************************************");
    return response;
  }

  /// Runs the same logical aggregation against the base table and the MV table, prints both
  /// result sets, and reports whether the row data matches. Both queries must produce results
  /// in the same column order (the comparison is positional within each row); the caller is
  /// responsible for crafting them to be apples-to-apples.
  private void runComparison(QuickstartRunner runner, String description, String baseQuery, String mvQuery)
      throws Exception {
    printStatus(Color.YELLOW, description + " — base table");
    printStatus(Color.CYAN, "Base  : " + baseQuery);
    JsonNode baseResponse = runner.runQuery(baseQuery);
    printStatus(Color.YELLOW, prettyPrintResponse(baseResponse));

    printStatus(Color.YELLOW, description + " — MV table");
    printStatus(Color.CYAN, "MV    : " + mvQuery);
    JsonNode mvResponse = runner.runQuery(mvQuery);
    printStatus(Color.YELLOW, prettyPrintResponse(mvResponse));

    String mismatch = compareResultRows(baseResponse, mvResponse);
    if (mismatch == null) {
      printStatus(Color.GREEN, "*** Base and MV results MATCH ***");
    } else {
      printStatus(Color.YELLOW, "WARNING: base and MV results DIFFER — " + mismatch);
    }
    printStatus(Color.GREEN, "***************************************************");
  }

  /// Compares the `rows` arrays of two Pinot query responses positionally with
  /// type-tolerant cell equality: numeric cells (including DOUBLE/LONG/INT and
  /// JSON-stringified numbers) are compared as doubles so a base-side INT `COUNT`
  /// matches an MV-side DOUBLE `SUM(flight_count)`; non-numeric cells fall back to
  /// string equality.
  ///
  /// Returns `null` when the rows agree, or a short diagnostic string when they do not.
  private static String compareResultRows(JsonNode baseResponse, JsonNode mvResponse) {
    if (baseResponse == null || mvResponse == null) {
      return "one of the responses was null";
    }
    if (responseHasException(baseResponse)) {
      return "base query produced exceptions";
    }
    if (responseHasException(mvResponse)) {
      return "MV query produced exceptions";
    }
    JsonNode baseRows = baseResponse.path("resultTable").path("rows");
    JsonNode mvRows = mvResponse.path("resultTable").path("rows");
    if (!baseRows.isArray() || !mvRows.isArray()) {
      return "missing resultTable.rows array on at least one side";
    }
    if (baseRows.size() != mvRows.size()) {
      return "row count differs: base=" + baseRows.size() + ", mv=" + mvRows.size();
    }
    for (int r = 0; r < baseRows.size(); r++) {
      JsonNode baseRow = baseRows.get(r);
      JsonNode mvRow = mvRows.get(r);
      if (baseRow.size() != mvRow.size()) {
        return "row " + r + " column count differs: base=" + baseRow.size() + ", mv=" + mvRow.size();
      }
      for (int c = 0; c < baseRow.size(); c++) {
        if (!cellEquals(baseRow.get(c), mvRow.get(c))) {
          return "row " + r + " column " + c + " differs: base=" + baseRow.get(c).asText()
              + ", mv=" + mvRow.get(c).asText();
        }
      }
    }
    return null;
  }

  /// Cell-level equality with type tolerance. Treats `COUNT(*)`-as-INT and
  /// `SUM(flight_count)`-as-DOUBLE as equal when their numeric values agree, and falls
  /// back to string compare otherwise.
  private static boolean cellEquals(JsonNode base, JsonNode mv) {
    String baseText = base.asText();
    String mvText = mv.asText();
    if (baseText.equals(mvText)) {
      return true;
    }
    Double baseNum = tryParseDouble(baseText);
    Double mvNum = tryParseDouble(mvText);
    if (baseNum != null && mvNum != null) {
      return baseNum.doubleValue() == mvNum.doubleValue();
    }
    return false;
  }

  private static Double tryParseDouble(String s) {
    if (s == null || s.isEmpty()) {
      return null;
    }
    try {
      return Double.parseDouble(s);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static boolean responseHasException(JsonNode response) {
    JsonNode exceptions = response.path("exceptions");
    return exceptions.isArray() && exceptions.size() > 0;
  }

  private void triggerMaterializedViewTask(MinionClient minionClient) {
    try {
      Map<String, String> scheduled = minionClient.scheduleMinionTasks(
          CommonConstants.MaterializedViewTask.TASK_TYPE,
          MATERIALIZED_VIEW_TABLE + "_OFFLINE");
      if (scheduled.isEmpty()) {
        printStatus(Color.YELLOW,
            "No tasks scheduled — MV may already be up-to-date or minion is still starting up");
      } else {
        printStatus(Color.GREEN, "Scheduled MV tasks: " + scheduled);
      }
    } catch (Exception e) {
      printStatus(Color.YELLOW, "Could not schedule MV task (will retry): " + e.getMessage());
    }
  }

  private void waitForMaterializedViewSegments(QuickstartRunner runner, MinionClient minionClient)
      throws Exception {
    long expectedRows = getExpectedMaterializedViewRowCount(runner);
    if (expectedRows > 0) {
      printStatus(Color.CYAN,
          "Waiting up to 5 minutes for all " + expectedRows + " MV pre-aggregated rows to be generated...");
    } else {
      printStatus(Color.CYAN, "Waiting up to 5 minutes for MV segments to be generated...");
    }
    long deadline = System.currentTimeMillis() + TASK_TIMEOUT_MS;
    while (System.currentTimeMillis() < deadline) {
      try {
        JsonNode result = runner.runQuery("SELECT COUNT(*) FROM " + MATERIALIZED_VIEW_TABLE + " LIMIT 1");
        JsonNode rows = result.path("resultTable").path("rows");
        if (rows.isArray() && rows.size() > 0) {
          long count = rows.get(0).get(0).asLong();
          if (expectedRows > 0) {
            if (count >= expectedRows) {
              printStatus(Color.GREEN,
                  "MV table " + MATERIALIZED_VIEW_TABLE + " is ready with " + count + " pre-aggregated rows.");
              return;
            }
            printStatus(Color.CYAN,
                "MV table " + MATERIALIZED_VIEW_TABLE + " has " + count + " of " + expectedRows
                    + " pre-aggregated rows, retrying...");
          } else if (count > 0) {
            printStatus(Color.GREEN,
                "MV table " + MATERIALIZED_VIEW_TABLE + " is ready with " + count + " pre-aggregated rows.");
            return;
          }
        }
      } catch (Exception e) {
        printStatus(Color.YELLOW, "MV not ready yet (" + e.getMessage() + "), retrying...");
      }
      printStatus(Color.CYAN,
          "MV not ready yet, retrying in " + (TASK_POLL_INTERVAL_MS / 1000) + "s...");

      // Re-trigger in case the scheduler hasn't picked it up yet
      triggerMaterializedViewTask(minionClient);

      Thread.sleep(TASK_POLL_INTERVAL_MS);
    }
    printStatus(Color.YELLOW,
        "Timed out waiting for MV segments. Comparison step will likely show mismatches.");
  }

  private long getExpectedMaterializedViewRowCount(QuickstartRunner runner) {
    try {
      JsonNode result = runner.runQuery("SELECT DaysSinceEpoch, Carrier, COUNT(*) "
          + "FROM " + BASE_TABLE + " GROUP BY DaysSinceEpoch, Carrier LIMIT 10000");
      JsonNode rows = result.path("resultTable").path("rows");
      if (rows.isArray()) {
        return rows.size();
      }
    } catch (Exception e) {
      printStatus(Color.YELLOW,
          "Could not compute expected MV row count from base table (" + e.getMessage()
              + "); falling back to first served MV segment.");
    }
    return -1;
  }

  public static void main(String[] args)
      throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", "MATERIALIZED_VIEW"));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[arguments.size()]));
  }
}
