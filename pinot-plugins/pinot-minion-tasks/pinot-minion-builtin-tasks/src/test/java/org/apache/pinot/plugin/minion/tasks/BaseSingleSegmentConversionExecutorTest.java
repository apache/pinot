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
package org.apache.pinot.plugin.minion.tasks;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.common.metrics.MinionMetrics;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.minion.MinionContext;
import org.apache.pinot.minion.event.MinionEventObservers;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Tests the {@link BaseSingleSegmentConversionExecutor#executeTask} upload-failure handling: a segment-upload failure
 * must propagate so the task is marked failed (and retried) rather than being silently reported as successful.
 */
public class BaseSingleSegmentConversionExecutorTest {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), "BaseSingleSegmentConversionExecutorTest");
  private static final File SEGMENT_DIR = new File(TEMP_DIR, "segment");
  private static final File DATA_DIR = new File(TEMP_DIR, "minionData");

  private static final int NUM_ROWS = 5;
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String TABLE_NAME_WITH_TYPE = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME);
  private static final String SEGMENT_NAME = "testSegment";
  private static final String TASK_TYPE = "TestSingleSegmentConversionTask";
  private static final String TASK_ID = "Task_" + TASK_TYPE + "_0";
  private static final long SEGMENT_CRC = 100L;
  private static final String D1 = "d1";

  private File _segmentIndexDir;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
    MinionMetrics.register(Mockito.mock(MinionMetrics.class));

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension(D1, FieldSpec.DataType.INT).build();
    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow row = new GenericRow();
      row.putValue(D1, i);
      rows.add(row);
    }

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setInstanceType(InstanceType.MINION);
    config.setOutDir(SEGMENT_DIR.getPath());
    config.setSegmentName(SEGMENT_NAME);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
    _segmentIndexDir = new File(SEGMENT_DIR, SEGMENT_NAME);

    Assert.assertTrue(DATA_DIR.mkdirs());
    MinionContext.getInstance().setDataDir(DATA_DIR);
    // executeTask resolves the event observer from the registry by task id; register one so it is non-null.
    MinionEventObservers.getInstance().addMinionEventObserver(TASK_ID, MinionTaskTestUtils.getMinionProgressObserver());
  }

  @Test
  public void testExecuteTaskRethrowsWhenUploadFails()
      throws Exception {
    try (MockedStatic<SegmentConversionUtils> mocked = Mockito.mockStatic(SegmentConversionUtils.class)) {
      mocked.when(() -> SegmentConversionUtils.uploadSegment(Mockito.any(), Mockito.any(), Mockito.any(),
              Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.any(File.class)))
          .thenThrow(new RuntimeException("simulated upload failure"));

      TestSingleSegmentConversionExecutor executor = new TestSingleSegmentConversionExecutor();
      try {
        executor.executeTask(createTaskConfig());
        Assert.fail("executeTask must rethrow when segment upload fails, not report success");
      } catch (RuntimeException e) {
        Assert.assertEquals(e.getMessage(), "simulated upload failure");
      }
    }
  }

  @Test
  public void testExecuteTaskSucceedsWhenUploadSucceeds()
      throws Exception {
    try (MockedStatic<SegmentConversionUtils> mocked = Mockito.mockStatic(SegmentConversionUtils.class)) {
      // uploadSegment is a no-op by default for the mocked static, simulating a successful upload.
      TestSingleSegmentConversionExecutor executor = new TestSingleSegmentConversionExecutor();
      SegmentConversionResult result = executor.executeTask(createTaskConfig());
      Assert.assertEquals(result.getSegmentName(), SEGMENT_NAME);
      Assert.assertEquals(result.getTableNameWithType(), TABLE_NAME_WITH_TYPE);
      mocked.verify(() -> SegmentConversionUtils.uploadSegment(Mockito.any(), Mockito.any(), Mockito.any(),
          Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.any(File.class)));
    }
  }

  private PinotTaskConfig createTaskConfig() {
    Map<String, String> configs = new HashMap<>();
    configs.put(MinionConstants.TABLE_NAME_KEY, TABLE_NAME_WITH_TYPE);
    configs.put(MinionConstants.SEGMENT_NAME_KEY, SEGMENT_NAME);
    configs.put(MinionConstants.DOWNLOAD_URL_KEY, "http://unused/download");
    configs.put(MinionConstants.UPLOAD_URL_KEY, "http://unused/upload");
    configs.put(MinionConstants.ORIGINAL_SEGMENT_CRC_KEY, Long.toString(SEGMENT_CRC));
    configs.put("TASK_ID", TASK_ID);
    return new PinotTaskConfig(TASK_TYPE, configs);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    // Restore the process-global state mutated in setUp so it does not leak into other test classes.
    MinionEventObservers.getInstance().removeMinionEventObserver(TASK_ID);
    MinionContext.getInstance().setDataDir(null);
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  /**
   * Minimal concrete executor that stubs out the infrastructure-dependent hooks (download, CRC check, conversion, ZK
   * metadata modifier) so {@code executeTask} runs to the upload step without a server, controller, or deep store.
   */
  private class TestSingleSegmentConversionExecutor extends BaseSingleSegmentConversionExecutor {
    @Override
    protected File downloadSegmentToLocalAndUntar(String tableNameWithType, String segmentName, String deepstoreURL,
        String taskType, File tempDataDir, String suffix)
        throws Exception {
      File indexDir = new File(tempDataDir, "inputSegment");
      FileUtils.copyDirectory(_segmentIndexDir, indexDir);
      return indexDir;
    }

    @Override
    protected long getSegmentCrc(String tableNameWithType, String segmentName) {
      return SEGMENT_CRC;
    }

    @Override
    protected SegmentConversionResult convert(PinotTaskConfig pinotTaskConfig, File indexDir, File workingDir)
        throws Exception {
      File convertedDir = new File(workingDir, SEGMENT_NAME);
      FileUtils.copyDirectory(indexDir, convertedDir);
      return new SegmentConversionResult.Builder().setFile(convertedDir)
          .setTableNameWithType(pinotTaskConfig.getConfigs().get(MinionConstants.TABLE_NAME_KEY))
          .setSegmentName(SEGMENT_NAME).build();
    }

    @Override
    protected SegmentZKMetadataCustomMapModifier getSegmentZKMetadataCustomMapModifier(PinotTaskConfig pinotTaskConfig,
        SegmentConversionResult segmentConversionResult) {
      return new SegmentZKMetadataCustomMapModifier(SegmentZKMetadataCustomMapModifier.ModifyMode.UPDATE,
          Collections.emptyMap());
    }
  }
}
