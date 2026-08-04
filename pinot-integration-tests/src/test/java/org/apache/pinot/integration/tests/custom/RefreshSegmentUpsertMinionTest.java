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
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.task.TaskState;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.controller.helix.core.minion.TaskSchedulingContext;
import org.apache.pinot.controller.helix.core.minion.TaskSchedulingInfo;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.ReplicaGroupStrategyConfig;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.CommonConstants.Segment.BuiltInVirtualColumn;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


/// Verifies that a refreshed old segment cannot replace newer OFFLINE upsert data.
@Test(suiteName = "CustomClusterIntegrationTest")
public class RefreshSegmentUpsertMinionTest extends CustomDataQueryClusterIntegrationTest {
  private static final String TABLE_NAME = "RefreshSegmentUpsertMinionTest";
  private static final String PRIMARY_KEY_COLUMN = "pk";
  private static final String VALUE_COLUMN = "value";
  private static final String REFRESH_COLUMN = "refreshed";
  private static final String OLD_SEGMENT = "oldSegment";
  private static final String NEW_SEGMENT = "newSegment";
  private static final String REBUILT_OLD_SEGMENT = "rebuiltOldSegment";
  private static final String OLD_VALUE = "old";
  private static final String NEW_VALUE = "new";
  private static final long OLD_CREATION_TIME = 1_000L;
  private static final long NEW_CREATION_TIME = 2_000L;
  private static final long REBUILT_CREATION_TIME = 3_000L;

  @Override
  public String getTableName() {
    return TABLE_NAME;
  }

  @Override
  protected long getCountStarResult() {
    return 1;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder()
        .setSchemaName(TABLE_NAME)
        .addSingleValueDimension(PRIMARY_KEY_COLUMN, FieldSpec.DataType.INT)
        .addSingleValueDimension(VALUE_COLUMN, FieldSpec.DataType.STRING)
        .setPrimaryKeyColumns(List.of(PRIMARY_KEY_COLUMN))
        .build();
  }

  @Override
  public List<File> createAvroFiles() {
    return List.of();
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    Map<String, ColumnPartitionConfig> partitionConfig =
        Map.of(PRIMARY_KEY_COLUMN, new ColumnPartitionConfig("Murmur", 1));
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(TABLE_NAME)
        .setNumReplicas(2)
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL))
        .setRoutingConfig(new RoutingConfig(null, null,
            RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .setSegmentPartitionConfig(new SegmentPartitionConfig(partitionConfig))
        .setReplicaGroupStrategyConfig(new ReplicaGroupStrategyConfig(PRIMARY_KEY_COLUMN, 1))
        .build();
    tableConfig.setTaskConfig(new TableTaskConfig(
        Map.of(MinionConstants.RefreshSegmentTask.TASK_TYPE, new HashMap<>())));
    return tableConfig;
  }

  @Override
  protected void setUpTable()
      throws Exception {
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);
    buildAndUploadSegment(tableConfig, schema, OLD_SEGMENT, OLD_VALUE, OLD_CREATION_TIME);
  }

  @Test
  public void testRefreshCarriesCreationTimeAcrossReplacement()
      throws Exception {
    String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME);
    Schema refreshedSchema = createSchema();
    refreshedSchema.addField(new DimensionFieldSpec(REFRESH_COLUMN, FieldSpec.DataType.STRING, true, "default"));
    addSchema(refreshedSchema);
    waitForTasksToComplete(scheduleRefresh(tableNameWithType));

    TableConfig tableConfig = getSharedHelixResourceManager().getOfflineTableConfig(TABLE_NAME);
    File rebuiltSegmentTarDir = rebuildRefreshedSegment(tableNameWithType, tableConfig);
    buildAndUploadSegment(tableConfig, createSchema(), NEW_SEGMENT, NEW_VALUE, NEW_CREATION_TIME);

    String lineageEntryId = getSharedHelixResourceManager().startReplaceSegments(tableNameWithType,
        List.of(OLD_SEGMENT), List.of(REBUILT_OLD_SEGMENT), false, null);
    uploadSegments(TABLE_NAME, rebuiltSegmentTarDir);
    getSharedHelixResourceManager().endReplaceSegments(tableNameWithType, lineageEntryId, null);

    // Wait for replacement routing before checking which upsert row wins.
    waitForRawSegment(REBUILT_OLD_SEGMENT);
    waitForValue(NEW_VALUE);
  }

  private void buildAndUploadSegment(TableConfig tableConfig, Schema schema, String segmentName, String value,
      long creationTime)
      throws Exception {
    File segmentDir = new File(_tempDir, segmentName + "Build");
    File tarDir = new File(_tempDir, segmentName + "Tar");
    TestUtils.ensureDirectoriesExistAndEmpty(segmentDir, tarDir);
    GenericRow row = new GenericRow();
    row.putValue(PRIMARY_KEY_COLUMN, 1);
    row.putValue(VALUE_COLUMN, value);

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(segmentDir.getPath());
    config.setSegmentName(segmentName);
    config.setCreationTime(String.valueOf(creationTime));
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(List.of(row)));
    driver.build();

    File indexDir = new File(segmentDir, segmentName);
    File tarFile = new File(tarDir, segmentName + TarCompressionUtils.TAR_GZ_FILE_EXTENSION);
    TarCompressionUtils.createCompressedTarFile(indexDir, tarFile);
    uploadSegments(TABLE_NAME, tarDir);
  }

  private File rebuildRefreshedSegment(String tableNameWithType, TableConfig tableConfig)
      throws Exception {
    File downloadDir = new File(_tempDir, "refreshedDownload");
    File untarDir = new File(_tempDir, "refreshedUntar");
    File outputDir = new File(_tempDir, "rebuiltOutput");
    File tarDir = new File(_tempDir, "rebuiltTar");
    TestUtils.ensureDirectoriesExistAndEmpty(downloadDir, untarDir, outputDir, tarDir);

    File downloadedTar = new File(downloadDir, OLD_SEGMENT + TarCompressionUtils.TAR_GZ_FILE_EXTENSION);
    Files.write(downloadedTar.toPath(),
        getOrCreateAdminClient().getSegmentClient().downloadSegment(tableNameWithType, OLD_SEGMENT));
    File sourceIndexDir = TarCompressionUtils.untar(downloadedTar, untarDir).get(0);
    SegmentMetadataImpl sourceMetadata = new SegmentMetadataImpl(sourceIndexDir);
    assertEquals(sourceMetadata.getIndexCreationTime(), OLD_CREATION_TIME);
    assertNotNull(sourceMetadata.getColumnMetadataFor(REFRESH_COLUMN));
    assertPhysicalCreationTime(sourceMetadata, OLD_CREATION_TIME);

    Schema schema = getSharedHelixResourceManager().getSchema(TABLE_NAME);
    assertNotNull(schema);
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(outputDir.getPath());
    config.setSegmentName(REBUILT_OLD_SEGMENT);
    config.setCreationTime(String.valueOf(REBUILT_CREATION_TIME));
    try (PinotSegmentRecordReader recordReader = new PinotSegmentRecordReader()) {
      recordReader.init(sourceIndexDir, null, null);
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(config, recordReader);
      driver.build();
    }

    File rebuiltIndexDir = new File(outputDir, REBUILT_OLD_SEGMENT);
    SegmentMetadataImpl rebuiltMetadata = new SegmentMetadataImpl(rebuiltIndexDir);
    assertEquals(rebuiltMetadata.getIndexCreationTime(), REBUILT_CREATION_TIME);
    assertPhysicalCreationTime(rebuiltMetadata, OLD_CREATION_TIME);
    File tarFile = new File(tarDir, REBUILT_OLD_SEGMENT + TarCompressionUtils.TAR_GZ_FILE_EXTENSION);
    TarCompressionUtils.createCompressedTarFile(rebuiltIndexDir, tarFile);
    return tarDir;
  }

  private static void assertPhysicalCreationTime(SegmentMetadataImpl segmentMetadata, long expectedCreationTime) {
    ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(BuiltInVirtualColumn.CREATIONTIME);
    assertNotNull(columnMetadata);
    assertEquals(columnMetadata.getDataType(), FieldSpec.DataType.LONG);
    assertEquals(columnMetadata.getMinValue(), expectedCreationTime);
    assertEquals(columnMetadata.getMaxValue(), expectedCreationTime);
  }

  private List<String> scheduleRefresh(String tableNameWithType) {
    TaskSchedulingInfo schedulingInfo = getTaskManager().scheduleTasks(new TaskSchedulingContext()
            .setTablesToSchedule(Set.of(tableNameWithType)))
        .get(MinionConstants.RefreshSegmentTask.TASK_TYPE);
    assertNotNull(schedulingInfo);
    List<String> taskNames = schedulingInfo.getScheduledTaskNames();
    assertEquals(taskNames.size(), 1);
    return taskNames;
  }

  private void waitForTasksToComplete(List<String> taskNames) {
    TestUtils.waitForCondition(input -> {
      Map<String, TaskState> taskStates =
          getHelixTaskResourceManager().getTaskStates(MinionConstants.RefreshSegmentTask.TASK_TYPE);
      return taskNames.stream().allMatch(taskName -> taskStates.get(taskName) == TaskState.COMPLETED);
    }, 600_000L, "Failed to complete RefreshSegmentTask");
  }

  private void waitForRawSegment(String segmentName) {
    TestUtils.waitForCondition(input -> {
      try {
        JsonNode response = postQuery("SELECT " + VALUE_COLUMN + " FROM " + TABLE_NAME + " WHERE $segmentName = '"
            + segmentName + "' OPTION(skipUpsert=true)");
        JsonNode rows = response.get("resultTable").get("rows");
        return rows.size() == 1 && OLD_VALUE.equals(rows.get(0).get(0).asText());
      } catch (Exception e) {
        return false;
      }
    }, 60_000L, "Failed to observe replacement segment: " + segmentName);
  }

  private void waitForValue(String expectedValue) {
    TestUtils.waitForCondition(input -> {
      try {
        JsonNode response = postQuery(
            "SELECT " + VALUE_COLUMN + " FROM " + TABLE_NAME + " WHERE " + PRIMARY_KEY_COLUMN + " = 1");
        JsonNode rows = response.get("resultTable").get("rows");
        return rows.size() == 1 && expectedValue.equals(rows.get(0).get(0).asText());
      } catch (Exception e) {
        return false;
      }
    }, 60_000L, "Failed to observe value: " + expectedValue);
  }
}
