package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.helix.task.TaskState;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.minion.MergeRollupTaskMetadata;
import org.apache.pinot.common.utils.SqlResultComparator;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * Integration test for minion task of type "MergeRollupTask" configured on realtime tables.
 */
public class MergeRollupMinionRealtimeClusterIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final long TIMEOUT_IN_MS = 10_000L;

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();
    startMinion();

    // Start Kafka
    startKafka();

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createRealtimeTableConfig(avroFiles.get(0));
    addTableConfig(tableConfig);

    // Push data into Kafka
    pushAvroIntoKafka(avroFiles);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }


  @Override
  protected TableTaskConfig getTaskConfig() {
    Map<String, String> tableTaskConfigs = new HashMap<>();
    tableTaskConfigs.put("100days.mergeType", "concat");
    tableTaskConfigs.put("100days.bufferTimePeriod", "1d");
    tableTaskConfigs.put("100days.bucketTimePeriod", "100d");
    tableTaskConfigs.put("100days.maxNumRecordsPerSegment", "15000");
    tableTaskConfigs.put("100days.maxNumRecordsPerTask", "15000");
    tableTaskConfigs.put("ActualElapsedTime.aggregationType", "min");
    tableTaskConfigs.put("WeatherDelay.aggregationType", "sum");
    return new TableTaskConfig(Collections.singletonMap(MinionConstants.MergeRollupTask.TASK_TYPE, tableTaskConfigs));
  }


  // The use case is similar as the one defined in offline table
  @Test
  public void testSingleLevelConcat()
      throws Exception {
    // The original segments:
    // mytable__0__0__{ts00} ... mytable__0__23__{ts023}
    // mytable__1__0__{ts10} ... mytable__1__22__{ts122}
    //
    // Expected result segments:
    // merged_100days_{ts1}_0_mytable_16071_16099_0
    // merged_100days_{ts2}_0_mytable_16100_16154_0
    // merged_100days_{ts2}_0_mytable_16101_16146_1
    // merged_100days_{ts2}_1_mytable_16147_16199_0
    // merged_100days_{ts2}_2_mytable_16196_16199_0
    // merged_100days_{ts3}_0_mytable_16200_16252_1
    // merged_100days_{ts3}_0_mytable_16200_16252_0
    // merged_100days_{ts3}_1_mytable_16245_16295_0
    // merged_100days_{ts3}_2_mytable_16290_16299_0
    // merged_100days_{ts4}_0_mytable_16300_16359_0
    // merged_100days_{ts4}_0_mytable_16323_16345_1
    // merged_100days_{ts4}_1_mytable_16358_16399_0
    // merged_100days_{ts5}_0_mytable_16400_16435_0
    // mytable__0__23__{ts023} (in progress)
    // mytable__1__22__{ts122} (in progress)
    PinotHelixTaskResourceManager helixTaskResourceManager = _controllerStarter.getHelixTaskResourceManager();
    PinotTaskManager taskManager = _controllerStarter.getTaskManager();
    PinotHelixResourceManager pinotHelixResourceManager = _controllerStarter.getHelixResourceManager();
    String tableName = getTableName();

    String sqlQuery = "SELECT count(*) FROM " + tableName; // 115545 rows for the test table
    JsonNode expectedJson = postQuery(sqlQuery, _brokerBaseApiUrl);
    int[] expectedNumSubTasks = {1, 3, 3, 2, 1};
    int[] expectedNumSegmentsQueried = {44, 37, 26, 18, 15};
    long expectedWatermark = 16000 * 86_400_000L;
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    int numTasks = 0;
    for (String tasks = taskManager.scheduleTasks(realtimeTableName).get(MinionConstants.MergeRollupTask.TASK_TYPE);
        tasks != null; tasks =
        taskManager.scheduleTasks(realtimeTableName).get(MinionConstants.MergeRollupTask.TASK_TYPE), numTasks++) {
      assertEquals(helixTaskResourceManager.getSubtaskConfigs(tasks).size(), expectedNumSubTasks[numTasks]);
      assertTrue(helixTaskResourceManager.getTaskQueues()
          .contains(PinotHelixTaskResourceManager.getHelixJobQueueName(MinionConstants.MergeRollupTask.TASK_TYPE)));

      // Will not schedule task if there's incomplete task
      assertNull(
          taskManager.scheduleTasks(realtimeTableName).get(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE));
      waitForTaskToComplete();

      // Check watermark
      MergeRollupTaskMetadata minionTaskMetadata = MergeRollupTaskMetadata.fromZNRecord(
          taskManager.getClusterInfoAccessor()
              .getMinionTaskMetadataZNRecord(MinionConstants.MergeRollupTask.TASK_TYPE, realtimeTableName));
      assertNotNull(minionTaskMetadata);
      assertEquals((long) minionTaskMetadata.getWatermarkMap().get("100days"), expectedWatermark);
      expectedWatermark += 100 * 86_400_000L;

      // Check metadata of merged segments
      for (SegmentZKMetadata metadata : pinotHelixResourceManager.getSegmentsZKMetadata(realtimeTableName)) {
        if (metadata.getSegmentName().startsWith("merged")) {
          // Check merged segment zk metadata
          assertNotNull(metadata.getCustomMap());
          assertEquals("100days",
              metadata.getCustomMap().get(MinionConstants.MergeRollupTask.SEGMENT_ZK_METADATA_MERGE_LEVEL_KEY));
          // Check merged segments are time partitioned
          assertEquals(metadata.getEndTimeMs() / (86_400_000L * 100), metadata.getStartTimeMs() / (86_400_000L * 100));
        }
      }

      final int finalNumTasks = numTasks;
      TestUtils.waitForCondition(aVoid -> {
        try {
          // Check num total doc of merged segments are the same as the original segments
          JsonNode actualJson = postQuery(sqlQuery, _brokerBaseApiUrl);
          if (!SqlResultComparator.areEqual(actualJson, expectedJson, sqlQuery)) {
            return false;
          }
          // Check query routing
          int numSegmentsQueried = actualJson.get("numSegmentsQueried").asInt();
          return numSegmentsQueried == expectedNumSegmentsQueried[finalNumTasks];
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }, TIMEOUT_IN_MS, "Timeout while validating segments");
    }
    // Check total tasks
    assertEquals(numTasks, 5);

    assertTrue(_controllerStarter.getControllerMetrics()
        .containsGauge("mergeRollupTaskDelayInNumBuckets.mytable_REALTIME.100days"));
  }

  private void waitForTaskToComplete() {
    PinotHelixTaskResourceManager helixTaskResourceManager = _controllerStarter.getHelixTaskResourceManager();
    TestUtils.waitForCondition(input -> {
      // Check task state
      for (TaskState taskState : helixTaskResourceManager.getTaskStates(MinionConstants.MergeRollupTask.TASK_TYPE)
          .values()) {
        if (taskState != TaskState.COMPLETED) {
          return false;
        }
      }
      return true;
    }, 600_000L, "Failed to complete task");
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    dropRealtimeTable(getTableName());
    cleanupTestTableDataManager(TableNameBuilder.REALTIME.tableNameWithType(getTableName()));
    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }
}
