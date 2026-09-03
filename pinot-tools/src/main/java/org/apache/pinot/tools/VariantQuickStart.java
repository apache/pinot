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
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.apache.pinot.tools.admin.command.QuickstartRunner;


/**
 * Batch quickstart for ingesting and querying an Apache Parquet VARIANT column.
 */
public class VariantQuickStart extends Quickstart {
  private static final String[] VARIANT_TABLE_DIRECTORIES = {"examples/batch/variantEvents"};
  private static final int EXPECTED_NUM_ROWS = 5;
  private static final long BOOTSTRAP_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(5);
  private static final long BOOTSTRAP_POLL_INTERVAL_MS = TimeUnit.SECONDS.toMillis(1);

  @Override
  public List<String> types() {
    return List.of("VARIANT", "OFFLINE_VARIANT", "OFFLINE-VARIANT", "BATCH_VARIANT", "BATCH-VARIANT");
  }

  @Override
  protected String[] getDefaultBatchTableDirectories() {
    return VARIANT_TABLE_DIRECTORIES;
  }

  @Override
  protected void waitForBootstrapToComplete(QuickstartRunner runner)
      throws Exception {
    printStatus(Color.CYAN,
        "***** Waiting up to 5 minutes for the Parquet VARIANT segment to become queryable *****");
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(BOOTSTRAP_TIMEOUT_MS);
    JsonNode lastResponse = null;
    Exception lastException = null;
    while (System.nanoTime() < deadline) {
      try {
        lastResponse = runner.runQuery("SELECT COUNT(*) FROM variantEvents");
        JsonNode rows = lastResponse.path("resultTable").path("rows");
        if (lastResponse.path("exceptions").isEmpty() && rows.isArray() && rows.size() == 1
            && rows.get(0).isArray() && rows.get(0).size() == 1
            && rows.get(0).get(0).asInt(-1) == EXPECTED_NUM_ROWS) {
          printStatus(Color.GREEN,
              "***** Parquet VARIANT segment is ready with " + EXPECTED_NUM_ROWS + " rows *****");
          return;
        }
      } catch (Exception e) {
        lastException = e;
      }
      Thread.sleep(BOOTSTRAP_POLL_INTERVAL_MS);
    }

    String detail = lastResponse != null ? lastResponse.toString()
        : lastException != null ? lastException.getMessage() : "no query response";
    throw new IllegalStateException(
        "Timed out waiting for variantEvents to contain exactly " + EXPECTED_NUM_ROWS + " rows; last result: "
            + detail, lastException);
  }

  @Override
  public void runSampleQueries(QuickstartRunner runner)
      throws Exception {
    printStatus(Color.YELLOW, "***** Parquet VARIANT quickstart setup complete *****");

    runQuery(runner, "Materialized hot field",
        "SELECT eventType, COUNT(*) FROM variantEvents GROUP BY eventType ORDER BY eventType");
    runQuery(runner, "Extract nested values directly from VARIANT",
        "SELECT eventId, variant_get(payload, '$.user.id', 'STRING') AS userId, "
            + "variant_get(payload, '$.amount', 'DOUBLE') AS amount "
            + "FROM variantEvents WHERE eventType = 'checkout' ORDER BY eventId");
    runQuery(runner, "Render the retained Parquet VARIANT value as JSON",
        "SELECT eventId, variantToJson(payload) FROM variantEvents ORDER BY eventId LIMIT 5");
  }

  private static void runQuery(QuickstartRunner runner, String description, String query)
      throws Exception {
    printStatus(Color.YELLOW, description);
    printStatus(Color.CYAN, "Query : " + query);
    JsonNode response = runner.runQuery("SET enableNullHandling=true; " + query);
    if (!response.path("exceptions").isEmpty()) {
      throw new IllegalStateException("VARIANT quickstart query failed: " + response.path("exceptions"));
    }
    printStatus(Color.YELLOW, prettyPrintResponse(response));
    printStatus(Color.GREEN, "***************************************************");
  }

  public static void main(String[] args)
      throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", "VARIANT"));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[0]));
  }
}
