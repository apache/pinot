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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.MetricValueUtils;
import org.apache.pinot.common.metrics.ValidationMetrics;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.controller.helix.core.periodictask.ControllerPeriodicTask;
import org.apache.pinot.controller.validation.RealtimeSegmentValidationManager;
import org.apache.pinot.core.periodictask.PeriodicTask;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.metrics.PinotMeter;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Integration test that enables aggregate metrics for the LLC real-time table.
@Test(suiteName = "CustomClusterIntegrationTest")
public class AggregateMetricsTest extends CustomDataQueryClusterIntegrationTest {

  private static final long EXPECTED_SUM_AIR_TIME = -165429728L;
  private static final long EXPECTED_SUM_ARR_DELAY = -175625957L;

  @Override
  public String getTableName() {
    return "AggregateMetricsTest";
  }

  @Override
  public boolean isRealtimeTable() {
    return true;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension("Carrier", DataType.STRING)
        .addSingleValueDimension("Origin", DataType.STRING)
        .addMetric("AirTime", DataType.LONG)
        .addMetric("ArrDelay", DataType.DOUBLE)
        .addDateTime("DaysSinceEpoch", DataType.INT, "1:DAYS:EPOCH", "1:DAYS")
        .build();
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    return unpackAvroData(_tempDir);
  }

  @Override
  protected TableConfig createRealtimeTableConfig(File sampleAvroFile) {
    TableConfig tableConfig = super.createRealtimeTableConfig(sampleAvroFile);
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    indexingConfig.setSortedColumn(List.of("Carrier"));
    indexingConfig.setInvertedIndexColumns(List.of("Origin"));
    indexingConfig.setNoDictionaryColumns(Arrays.asList("AirTime", "ArrDelay"));
    indexingConfig.setRangeIndexColumns(List.of("DaysSinceEpoch"));
    indexingConfig.setBloomFilterColumns(List.of("Origin"));
    indexingConfig.setAggregateMetrics(true);
    return tableConfig;
  }

  @Nullable
  @Override
  protected String getSortedColumn() {
    return null;
  }

  @Override
  public String getTimeColumnName() {
    return "DaysSinceEpoch";
  }

  @Override
  protected void waitForAllDocsLoaded(long timeoutMs) {
    // For aggregate metrics, documents can be merged during ingestion, so we check aggregation results
    // instead of document count.
    String sql = "SELECT SUM(AirTime), SUM(ArrDelay) FROM " + getTableName();
    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode queryResult = postQuery(sql);
        JsonNode aggregationResults = queryResult.get("resultTable").get("rows").get(0);
        return aggregationResults.get(0).asLong() == EXPECTED_SUM_AIR_TIME
            && aggregationResults.get(1).asLong() == EXPECTED_SUM_ARR_DELAY;
      } catch (Exception e) {
        return null;
      }
    }, 100L, timeoutMs, "Failed to load all documents");
  }

  @Test
  public void testAggregateMetricsQueries()
      throws Exception {
    // Test total aggregation
    JsonNode result = postQuery("SELECT SUM(AirTime), SUM(ArrDelay) FROM " + getTableName());
    JsonNode rows = result.get("resultTable").get("rows").get(0);
    assertEquals(rows.get(0).asLong(), EXPECTED_SUM_AIR_TIME);
    assertEquals(rows.get(1).asLong(), EXPECTED_SUM_ARR_DELAY);

    // Test group by with order
    result = postQuery("SELECT SUM(AirTime), DaysSinceEpoch FROM " + getTableName()
        + " GROUP BY DaysSinceEpoch ORDER BY SUM(AirTime) DESC LIMIT 1");
    assertEquals(result.get("exceptions").size(), 0);

    // Test filter with group by
    result = postQuery("SELECT Origin, SUM(ArrDelay) FROM " + getTableName()
        + " WHERE Carrier = 'AA' GROUP BY Origin ORDER BY Origin LIMIT 10");
    assertEquals(result.get("exceptions").size(), 0);
  }

  @Test(dependsOnMethods = "testAggregateMetricsQueries")
  public void testRealtimeValidationPreservesAggregateMetrics()
      throws Exception {
    String tableNameWithType = TableNameBuilder.REALTIME.tableNameWithType(getTableName());
    int numReplicas = getSharedHelixResourceManager().getTableConfig(tableNameWithType).getReplication();
    waitForAllDocsLoaded(60_000L);
    TestUtils.waitForCondition(aVoid -> getSharedControllerStarter().getLeadControllerManager()
            .isLeaderForTable(tableNameWithType) && hasHealthyConsumingPartitions(tableNameWithType, numReplicas),
        100L, 60_000L, "Realtime table did not become ready for validation");

    RealtimeSegmentValidationManager validationManager =
        getSharedControllerStarter().getRealtimeSegmentValidationManager();
    assertTrue(validationManager.isStarted());
    String taskName = validationManager.getTaskName();
    ControllerMetrics controllerMetrics = validationManager.getControllerMetrics();
    PinotMeter runMeter =
        controllerMetrics.getMeteredTableValue(taskName, ControllerMeter.CONTROLLER_PERIODIC_TASK_RUN);
    PinotMeter taskErrorMeter =
        controllerMetrics.getMeteredTableValue(taskName, ControllerMeter.CONTROLLER_PERIODIC_TASK_ERROR);
    PinotMeter tableErrorMeter = controllerMetrics.getMeteredTableValue(tableNameWithType + "." + taskName,
        ControllerMeter.PERIODIC_TASK_ERROR);
    long taskErrorsBefore = taskErrorMeter.count();
    long tableErrorsBefore = tableErrorMeter.count();

    Properties properties = new Properties();
    properties.setProperty(PeriodicTask.PROPERTY_KEY_TABLE_NAME, tableNameWithType);
    properties.setProperty(ControllerPeriodicTask.RUN_SEGMENT_LEVEL_VALIDATION, Boolean.TRUE.toString());
    Map<String, Long> metadataDocsBefore = Map.of();
    Map<String, Long> metadataDocsAfter = Map.of();
    boolean validatedStableMetadata = false;
    for (int attempt = 0; attempt < 3; attempt++) {
      metadataDocsBefore = getMetadataDocumentCounts(tableNameWithType);
      long runsBefore = runMeter.count();
      // run() waits for any current invocation and completes this validation synchronously.
      validationManager.run(properties);

      // Periodic tasks catch their own exceptions. Never retry an unsuccessful validation.
      assertEquals(taskErrorMeter.count(), taskErrorsBefore, "Controller validation failed");
      assertEquals(tableErrorMeter.count(), tableErrorsBefore, "Table validation failed");
      assertTrue(runMeter.count() > runsBefore, "Validation was not invoked");
      metadataDocsAfter = getMetadataDocumentCounts(tableNameWithType);
      if (metadataDocsBefore.equals(metadataDocsAfter)) {
        assertEquals(MetricValueUtils.getGlobalGaugeValue(controllerMetrics, taskName,
            ControllerGauge.PERIODIC_TASK_NUM_TABLES_PROCESSED), 1L);
        long validatedDocs = validationManager.getValidationMetrics()
            .getValueOfGauge(ValidationMetrics.makeGaugeName(tableNameWithType, "TotalDocumentCount"));
        // This gauge counts persisted segment metadata, not rows merged by aggregate-metrics ingestion.
        assertEquals(validatedDocs, metadataDocsAfter.values().stream().mapToLong(Long::longValue).sum());
        validatedStableMetadata = true;
        break;
      }
      // A commit can update a segment's document count and add a new consuming segment with count -1.
      // Only an actual metadata transition warrants another validation against a fresh snapshot.
    }
    assertTrue(validatedStableMetadata,
        "Segment metadata changed during all validation attempts: before=" + metadataDocsBefore
            + ", after=" + metadataDocsAfter);

    TestUtils.waitForCondition(aVoid -> hasHealthyConsumingPartitions(tableNameWithType, numReplicas),
        100L, 60_000L, "Validation left incomplete or unhealthy consuming replicas");
    JsonNode result = postQuery("SELECT SUM(AirTime), SUM(ArrDelay) FROM " + getTableName());
    assertEquals(result.get("exceptions").size(), 0);
    JsonNode rows = result.get("resultTable").get("rows").get(0);
    assertEquals(rows.get(0).asLong(), EXPECTED_SUM_AIR_TIME);
    assertEquals(rows.get(1).asLong(), EXPECTED_SUM_ARR_DELAY);
  }

  private Map<String, Long> getMetadataDocumentCounts(String tableNameWithType) {
    Map<String, Long> documentCounts = new HashMap<>();
    for (SegmentZKMetadata metadata : getSharedHelixResourceManager().getSegmentsZKMetadata(tableNameWithType)) {
      documentCounts.put(metadata.getSegmentName(), metadata.getTotalDocs());
    }
    return documentCounts;
  }

  private boolean hasHealthyConsumingPartitions(String tableNameWithType, int numReplicas) {
    IdealState idealState = getSharedHelixResourceManager().getTableIdealState(tableNameWithType);
    ExternalView externalView = getSharedHelixResourceManager().getTableExternalView(tableNameWithType);
    if (idealState == null || externalView == null) {
      return false;
    }
    Map<String, Map<String, String>> assignments = idealState.getRecord().getMapFields();
    if (assignments.isEmpty() || !assignments.equals(externalView.getRecord().getMapFields())) {
      return false;
    }
    Set<Integer> consumingPartitions = new HashSet<>();
    for (Map.Entry<String, Map<String, String>> entry : assignments.entrySet()) {
      LLCSegmentName segmentName = LLCSegmentName.of(entry.getKey());
      Map<String, String> replicaStates = entry.getValue();
      if (segmentName == null || replicaStates.size() != numReplicas) {
        return false;
      }
      if (replicaStates.values().stream().allMatch(SegmentStateModel.CONSUMING::equals)) {
        if (!consumingPartitions.add(segmentName.getPartitionGroupId())) {
          return false;
        }
      } else if (!replicaStates.values().stream().allMatch(SegmentStateModel.ONLINE::equals)) {
        return false;
      }
    }
    Set<Integer> expectedPartitions = new HashSet<>();
    for (int partition = 0; partition < getNumKafkaPartitions(); partition++) {
      expectedPartitions.add(partition);
    }
    return consumingPartitions.equals(expectedPartitions);
  }
}
