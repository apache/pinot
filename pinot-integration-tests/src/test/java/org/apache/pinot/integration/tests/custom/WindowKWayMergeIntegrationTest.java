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
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.pinot.broker.broker.helix.BaseBrokerStarter;
import org.apache.pinot.broker.requesthandler.BrokerRequestHandlerDelegate;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Exercises sender sorting and merge receive through planning, serialization and real mailbox transport.
/// The hybrid fixture ensures each sender sorts one complete run across its physical leaf requests.
@Test(suiteName = "CustomClusterIntegrationTest")
public class WindowKWayMergeIntegrationTest extends CustomDataQueryClusterIntegrationTest {
  private static final int ROWS_PER_TABLE = 8;
  private static final long OFFLINE_TIME = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(3);

  @Override
  public String getTableName() {
    return "WindowKWayMerge";
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName()).setEnableColumnBasedNullHandling(true)
        .addSingleValueDimension("id", DataType.INT)
        .addSingleValueDimension("sortKey", DataType.INT)
        .addMetric("amount", DataType.INT)
        .addDateTime(TIMESTAMP_FIELD_NAME, DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS").build();
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(getTableName())
        .setTimeColumnName(TIMESTAMP_FIELD_NAME).setNullHandlingEnabled(true).build();
  }

  @Override
  protected boolean getNullHandlingEnabled() {
    return true;
  }

  @Override
  protected void setUpTable()
      throws Exception {
    super.setUpTable();
    createSharedKafkaTopic(getKafkaTopic(), getNumKafkaPartitions());
    List<File> realtimeFiles = createAvroFiles(true);
    addTableConfig(createRealtimeTableConfig(realtimeFiles.get(0)));
    TestUtils.waitForCondition(() -> getCurrentCountStarResult(getTableName() + "_OFFLINE") == ROWS_PER_TABLE,
        100L, 60_000, "Failed to load offline window rows", null);
    // These disjoint rows need a boundary at the offline end, rather than the default one-day overlap.
    getOrCreateAdminClient().getTableClient().setTimeBoundary(getTableName());
    pushAvroIntoKafka(realtimeFiles);
  }

  @Override
  public List<File> createAvroFiles()
      throws IOException {
    return createAvroFiles(false);
  }

  private List<File> createAvroFiles(boolean realtime)
      throws IOException {
    org.apache.avro.Schema avroSchema = SchemaBuilder.record("windowRow").fields()
        .requiredInt("id").optionalInt("sortKey").requiredInt("amount").requiredLong(TIMESTAMP_FIELD_NAME)
        .endRecord();
    try (AvroFilesAndWriters files = createAvroFilesAndWriters(avroSchema)) {
      List<DataFileWriter<GenericData.Record>> writers = files.getWriters();
      for (int i = 0; i < ROWS_PER_TABLE; i++) {
        int id = i + (realtime ? ROWS_PER_TABLE : 0);
        GenericData.Record row = new GenericData.Record(avroSchema);
        row.put("id", id);
        row.put("sortKey", id % 5 == 0 ? null : id % 3);
        row.put("amount", id + 1);
        row.put(TIMESTAMP_FIELD_NAME, OFFLINE_TIME + (realtime ? TimeUnit.DAYS.toMillis(2) : 0) + i);
        writers.get(i % writers.size()).append(row);
      }
      return files.getAvroFiles();
    }
  }

  @Override
  protected long getCountStarResult() {
    return 2 * ROWS_PER_TABLE;
  }

  @Test
  public void testMergeMatchesReceiverSortOnHybridTable()
      throws Exception {
    setUseMultiStageQueryEngine(true);
    // Snapshot clusters deliberately cannot pass the production version gate. Open only this test's capability hook.
    for (BaseBrokerStarter broker : getSharedBrokerStarters()) {
      ((BrokerRequestHandlerDelegate) broker.getBrokerRequestHandler()).getMultiStageBrokerRequestHandler()
          .setKWayMergeSupported(() -> true);
    }
    try {
      for (String frame : List.of(
          "ORDER BY sortKey DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING",
          "ORDER BY sortKey DESC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW")) {
        String query = "SELECT id, sortKey, SUM(amount) OVER (" + frame + ") FROM " + getTableName()
            + " ORDER BY id LIMIT 100";
        JsonNode receiver = postQuery(options(false) + query);
        JsonNode merge = postQuery(options(true) + query);
        assertNoError(receiver);
        assertNoError(merge);
        JsonNode rows = merge.get("resultTable").get("rows");
        assertEquals(rows.size(), (int) getCountStarResult());
        for (int id = 0; id < rows.size(); id++) {
          assertEquals(rows.get(id).get(0).intValue(), id);
        }
        assertEquals(rows, receiver.get("resultTable").get("rows"), frame);
        assertTrue(explain(true, query).contains("MAIL_MERGE_RECEIVE"));
        assertFalse(explain(false, query).contains("MAIL_MERGE_RECEIVE"));
      }
    } finally {
      for (BaseBrokerStarter broker : getSharedBrokerStarters()) {
        ((BrokerRequestHandlerDelegate) broker.getBrokerRequestHandler()).getMultiStageBrokerRequestHandler()
            .setKWayMergeSupported(() -> false);
      }
    }
  }

  private String explain(boolean merge, String query)
      throws Exception {
    JsonNode response = postQuery(options(merge) + "EXPLAIN IMPLEMENTATION PLAN FOR " + query);
    assertNoError(response);
    return response.get("resultTable").get("rows").toString();
  }

  private static String options(boolean merge) {
    return "SET windowKWayMerge=" + merge + "; SET nullHandlingEnabled=true; ";
  }

  @AfterClass(alwaysRun = true)
  @Override
  public void tearDown()
      throws IOException {
    dropRealtimeTable(getTableName());
    super.tearDown();
  }
}
