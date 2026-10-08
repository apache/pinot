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
import java.util.function.BooleanSupplier;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.broker.requesthandler.BrokerRequestHandlerDelegate;
import org.apache.pinot.broker.requesthandler.MultiStageBrokerRequestHandler;
import org.apache.pinot.integration.tests.ClusterIntegrationTestUtils;
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


/// Exercises the actual merge plan, wire serialization and local/gRPC execution against receiver sorting on a hybrid
/// table. The capability override is scoped to this test because development clusters report SNAPSHOT versions.
/// The shared broker override and table setup are used serially; this test is not thread-safe.
@Test(suiteName = "CustomClusterIntegrationTest")
public class WindowKWayMergeIntegrationTest extends CustomDataQueryClusterIntegrationTest {
  private boolean _offlineCreated;
  private boolean _realtimeCreated;

  @Override
  public String getTableName() {
    return "WindowKWayMergeIntegrationTest";
  }

  @Override
  protected long getCountStarResult() {
    return 32;
  }

  @Override
  public int getNumAvroFiles() {
    return 4;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension("id", DataType.INT)
        .addSingleValueDimension("orderKey", DataType.INT)
        .addMetric("value", DataType.INT)
        .addDateTime("ts", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS").build();
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse("""
        {"type":"record","name":"windowRow","fields":[
          {"name":"id","type":"int"},
          {"name":"orderKey","type":["null","int"],"default":null},
          {"name":"value","type":"int"},
          {"name":"ts","type":"long"}]}
        """);
    long oldTimestamp = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(2);
    try (AvroFilesAndWriters files = createAvroFilesAndWriters(avroSchema)) {
      for (int i = 0; i < 32; i++) {
        GenericData.Record row = new GenericData.Record(avroSchema);
        row.put("id", i);
        row.put("orderKey", i % 7 == 0 ? null : i % 4);
        row.put("value", i + 1);
        row.put("ts", oldTimestamp + (i < 16 ? 0 : TimeUnit.DAYS.toMillis(1)) + i * 1000L);
        DataFileWriter<GenericData.Record> writer = files.getWriters().get(i / 8);
        writer.append(row);
      }
      return files.getAvroFiles();
    }
  }

  @Override
  protected void setUpTable()
      throws Exception {
    Schema schema = createSchema();
    addSchema(schema);
    List<File> files = createAvroFiles();
    TableConfig offline = new TableConfigBuilder(TableType.OFFLINE).setTableName(getTableName())
        .setTimeColumnName(getTimeColumnName()).setNullHandlingEnabled(true).build();
    addTableConfig(offline);
    _offlineCreated = true;
    for (int i = 0; i < 2; i++) {
      ClusterIntegrationTestUtils.buildSegmentFromAvro(files.get(i), offline, schema, i, _segmentDir, _tarDir);
    }
    uploadSegments(getTableName(), _tarDir);
    createSharedKafkaTopic(getKafkaTopic(), getNumKafkaPartitions());
    waitForKafkaTopicMetadataReadyForConsumer(getKafkaTopic(), getNumKafkaPartitions());
    TableConfig realtime = createRealtimeTableConfig(files.get(2));
    realtime.getIndexingConfig().setNullHandlingEnabled(true);
    addRealtimeTableConfigWithRetry(realtime);
    _realtimeCreated = true;
    pushAvroIntoKafka(files.subList(2, 4));
  }

  @Override
  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws IOException {
    try {
      if (_realtimeCreated) {
        dropRealtimeTable(getTableName());
      }
    } finally {
      if (_offlineCreated) {
        super.tearDown();
      } else {
        FileUtils.deleteDirectory(_tempDir);
      }
    }
  }

  @Override
  protected void waitForAllDocsLoaded(long timeoutMs)
      throws Exception {
    TestUtils.waitForCondition(aVoid -> getCurrentCountStarResult(getTableName() + "_OFFLINE") == 16
        && getCurrentCountStarResult(getTableName() + "_REALTIME") == 16, 100L, timeoutMs,
        "Both physical tables must be loaded before setting the hybrid boundary");
    // The automatic boundary subtracts a day for overlap. This disjoint fixture uses the actual offline maximum.
    getOrCreateAdminClient().getTableClient().setTimeBoundary(getTableName());
    super.waitForAllDocsLoaded(timeoutMs);
  }

  @Test
  public void testMergeMatchesReceiverSort()
      throws Exception {
    setUseMultiStageQueryEngine(true);
    MultiStageBrokerRequestHandler handler =
        ((BrokerRequestHandlerDelegate) getSharedBrokerStarter().getBrokerRequestHandler())
            .getMultiStageBrokerRequestHandler();
    // This is a test-only dependency override; no production query option bypasses the version gate.
    BooleanSupplier previous = handler.setKWayMergeSupported(() -> true);
    try {
      for (String frame : List.of("ORDER BY orderKey DESC NULLS FIRST, id ROWS BETWEEN CURRENT ROW AND "
          + "UNBOUNDED FOLLOWING", "ORDER BY orderKey DESC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND "
          + "CURRENT ROW")) {
        String query = "SELECT id, orderKey, SUM(value) OVER (" + frame + ") FROM " + getTableName()
            + " ORDER BY id LIMIT 100";
        JsonNode receiver = postQuery("SET windowKWayMerge=false; SET enableNullHandling=true; " + query);
        JsonNode merged = postQuery("SET windowKWayMerge=true; SET enableNullHandling=true; " + query);
        assertTrue(receiver.path("exceptions").isEmpty(), receiver.toString());
        assertTrue(merged.path("exceptions").isEmpty(), merged.toString());
        assertEquals(merged.path("resultTable").path("rows"), receiver.path("resultTable").path("rows"));
        assertEquals(merged.path("resultTable").path("rows").size(), 32);
        for (int i = 0; i < 32; i++) {
          assertEquals(merged.path("resultTable").path("rows").get(i).get(0).asInt(), i,
              "The window must retain rows from both physical tables");
        }
        assertFalse(merged.path("partialResult").asBoolean());
        JsonNode plan = postQuery("SET windowKWayMerge=true; SET enableNullHandling=true; "
            + "EXPLAIN IMPLEMENTATION PLAN FOR " + query);
        assertTrue(plan.toString().contains("MAIL_MERGE_RECEIVE"), plan.toString());
        assertTrue(containsMergeStats(merged.path("stageStats")), merged.toString());
      }
    } finally {
      handler.setKWayMergeSupported(previous);
    }
  }

  private static boolean containsMergeStats(JsonNode node) {
    if (node.path("type").asText().equals("WINDOW")) {
      for (JsonNode receiver : node.path("children")) {
        if (receiver.path("type").asText().equals("MAILBOX_RECEIVE")
            && receiver.path("rawMessages").asInt() > 0
            && receiver.path("inMemoryMessages").asInt() > 0) {
          for (JsonNode sender : receiver.path("children")) {
            for (JsonNode input : sender.path("children")) {
              if (input.path("type").asText().equals("SORT_OR_LIMIT")) {
                return true;
              }
            }
          }
        }
      }
    }
    for (JsonNode child : node.path("children")) {
      if (containsMergeStats(child)) {
        return true;
      }
    }
    return false;
  }
}
