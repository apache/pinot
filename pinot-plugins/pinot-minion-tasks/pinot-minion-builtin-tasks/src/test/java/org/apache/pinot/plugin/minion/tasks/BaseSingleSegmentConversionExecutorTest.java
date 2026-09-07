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
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.common.metrics.MinionMetrics;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.minion.MinionContext;
import org.apache.pinot.minion.event.MinionEventObservers;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.utils.SegmentPushUtils;
import org.apache.pinot.segment.local.utils.SegmentReplacementUtils;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.filesystem.LocalPinotFS;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


/// Tests the [BaseSingleSegmentConversionExecutor#executeTask] upload-failure handling: a segment-upload failure
/// must propagate so the task is marked failed (and retried) rather than being silently reported as successful.
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
    MinionMetrics.register(mock(MinionMetrics.class));

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

    assertTrue(DATA_DIR.mkdirs());
    MinionContext.getInstance().setDataDir(DATA_DIR);
    // executeTask resolves the event observer from the registry by task id; register one so it is non-null.
    MinionEventObservers.getInstance().addMinionEventObserver(TASK_ID, MinionTaskTestUtils.getMinionProgressObserver());
  }

  @Test
  public void testExecuteTaskRethrowsWhenUploadFails()
      throws Exception {
    try (MockedStatic<SegmentConversionUtils> mocked = mockStatic(SegmentConversionUtils.class)) {
      mocked.when(() -> SegmentConversionUtils.uploadSegment(any(), any(), any(),
              anyString(), anyString(), anyString(), any(File.class)))
          .thenThrow(new RuntimeException("simulated upload failure"));

      TestSingleSegmentConversionExecutor executor = new TestSingleSegmentConversionExecutor();
      try {
        executor.executeTask(createTaskConfig());
        fail("executeTask must rethrow when segment upload fails, not report success");
      } catch (RuntimeException e) {
        assertEquals(e.getMessage(), "simulated upload failure");
      }
    }
  }

  @Test
  public void testExecuteTaskSucceedsWhenUploadSucceeds()
      throws Exception {
    try (MockedStatic<SegmentConversionUtils> mocked = mockStatic(SegmentConversionUtils.class)) {
      // uploadSegment is a no-op by default for the mocked static, simulating a successful upload.
      TestSingleSegmentConversionExecutor executor = new TestSingleSegmentConversionExecutor();
      SegmentConversionResult result = executor.executeTask(createTaskConfig());
      assertEquals(result.getSegmentName(), SEGMENT_NAME);
      assertEquals(result.getTableNameWithType(), TABLE_NAME_WITH_TYPE);
      mocked.verify(() -> SegmentConversionUtils.uploadSegment(any(), any(), any(),
          anyString(), anyString(), anyString(), any(File.class)));
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

  /// A lost response can follow successful registration. Retain the output and propagate the failure.
  @Test
  public void testExecuteTaskKeepsStagedTarWhenMetadataPushFails()
      throws Exception {
    File outputDir = new File(TEMP_DIR, "output");
    FileUtils.forceMkdir(outputDir);
    PinotFS mockOutputFS = mock(PinotFS.class);
    when(mockOutputFS.exists(any())).thenReturn(false);

    try (MockedStatic<MinionTaskUtils> minionTaskUtils =
            mockStatic(MinionTaskUtils.class, CALLS_REAL_METHODS);
        MockedStatic<SegmentPushUtils> segmentPushUtils =
            mockStatic(SegmentPushUtils.class, CALLS_REAL_METHODS);
        MockedStatic<PinotFSFactory> factory = mockStatic(PinotFSFactory.class)) {
      factory.when(() -> PinotFSFactory.create("file")).thenAnswer(i -> new LocalPinotFS());
      minionTaskUtils.when(() -> MinionTaskUtils.getOutputPinotFS(any(), any()))
          .thenReturn(mockOutputFS);
      segmentPushUtils.when(() -> SegmentPushUtils.sendSegmentUriAndMetadata(any(), any(),
              any(), anyList(), anyList()))
          .thenThrow(new RuntimeException("simulated metadata push failure"));

      TestSingleSegmentConversionExecutor executor = new TestSingleSegmentConversionExecutor();
      try {
        executor.executeTask(createMetadataPushTaskConfig(outputDir));
        fail("executeTask must rethrow when metadata push fails");
      } catch (RuntimeException e) {
        assertEquals(e.getMessage(), "simulated metadata push failure");
      }
      verify(mockOutputFS, never()).delete(any(URI.class), anyBoolean());
      assertEquals(new File(DATA_DIR, TASK_TYPE).list().length, 0, "Local task files are still cleaned in finally");
    }
  }

  @Test
  public void testMetadataPushKeepsReplacementHeaders() throws Exception {
    PinotFS outputFS = mock(PinotFS.class);
    try (MockedStatic<MinionTaskUtils> utils = mockStatic(MinionTaskUtils.class, CALLS_REAL_METHODS);
        MockedStatic<SegmentPushUtils> push = mockStatic(SegmentPushUtils.class, CALLS_REAL_METHODS);
        MockedStatic<PinotFSFactory> factory = mockStatic(PinotFSFactory.class)) {
      factory.when(() -> PinotFSFactory.create("file")).thenAnswer(i -> new LocalPinotFS());
      utils.when(() -> MinionTaskUtils.getOutputPinotFS(anyMap(), any())).thenReturn(outputFS);
      push.when(() -> SegmentPushUtils.sendSegmentUriAndMetadata(any(), any(), any(), anyList(), anyList()))
          .thenAnswer(i -> null);
      TestSingleSegmentConversionExecutor executor = new TestSingleSegmentConversionExecutor();
      executor.executeTask(createMetadataPushTaskConfig(new File(TEMP_DIR, "headers")));
      ArgumentCaptor<List<Header>> headers = ArgumentCaptor.forClass(List.class);
      push.verify(() -> SegmentPushUtils.sendSegmentUriAndMetadata(any(), any(), any(),
          headers.capture(), anyList()));
      assertTrue(headers.getValue().stream().anyMatch(h -> HttpHeaders.IF_MATCH.equals(h.getName())
          && Long.toString(SEGMENT_CRC).equals(h.getValue())));
      assertTrue(headers.getValue().stream().anyMatch(h ->
          FileUploadDownloadClient.CustomHeaders.REFRESH_ONLY.equals(h.getName()) && "true".equals(h.getValue())));
      assertTrue(headers.getValue().stream().anyMatch(h ->
          FileUploadDownloadClient.CustomHeaders.SEGMENT_ZK_METADATA_CUSTOM_MAP_MODIFIER.equals(h.getName())));
    }
  }

  @Test
  public void testRetriesNeverOverwriteExistingSegment() throws Exception {
    File original = new File(TEMP_DIR, "immutable/" + SEGMENT_NAME + ".tar.gz");
    File tar = new File(TEMP_DIR, "local/" + SEGMENT_NAME + ".tar.gz");
    FileUtils.writeStringToFile(original, "original", StandardCharsets.UTF_8);
    FileUtils.writeStringToFile(tar, "replacement", StandardCharsets.UTF_8);
    Map<String, String> configs = createMetadataPushTaskConfig(original.getParentFile()).getConfigs();
    configs.put(BatchConfigProperties.OVERWRITE_OUTPUT, "true");
    try (MockedStatic<PinotFSFactory> factory = mockStatic(PinotFSFactory.class)) {
      factory.when(() -> PinotFSFactory.create("file")).thenAnswer(i -> new LocalPinotFS());
      TestSingleSegmentConversionExecutor executor = new TestSingleSegmentConversionExecutor();
      URI first = executor.moveSegmentToOutputPinotFS(configs, tar);
      URI retry = executor.moveSegmentToOutputPinotFS(configs, tar);
      assertNotEquals(first, retry);
      assertEquals(Files.readString(original.toPath()), "original");
      assertEquals(Files.readString(new File(first).toPath()), "replacement");
      assertEquals(Files.readString(new File(retry).toPath()), "replacement");
    }
  }

  @Test
  public void testPartialCopyIsRemovedBeforeSubmission() throws Exception {
    PinotFS outputFS = mock(PinotFS.class);
    doThrow(new RuntimeException("partial copy")).when(outputFS).copyFromLocalFile(any(), any());
    try (MockedStatic<PinotFSFactory> factory = mockStatic(PinotFSFactory.class);
        MockedStatic<MinionTaskUtils> utils = mockStatic(MinionTaskUtils.class, CALLS_REAL_METHODS)) {
      factory.when(() -> PinotFSFactory.create("file")).thenAnswer(i -> new LocalPinotFS());
      utils.when(() -> MinionTaskUtils.getOutputPinotFS(anyMap(), any())).thenReturn(outputFS);
      TestSingleSegmentConversionExecutor executor = new TestSingleSegmentConversionExecutor();
      expectThrows(RuntimeException.class, () -> executor.moveSegmentToOutputPinotFS(
          createMetadataPushTaskConfig(new File(TEMP_DIR, "partial")).getConfigs(), new File("segment.tar.gz")));
      ArgumentCaptor<URI> output = ArgumentCaptor.forClass(URI.class);
      verify(outputFS).copyFromLocalFile(any(), output.capture());
      verify(outputFS).delete(output.getValue(), false);
    }
  }

  @Test
  public void testOldMetadataTaskWithoutRegistryFailsBeforeWriting() throws Exception {
    Map<String, String> configs = createMetadataPushTaskConfig(new File(TEMP_DIR, "old-task")).getConfigs();
    configs.remove(SegmentReplacementUtils.ROOT_REFERENCES_CONFIG_KEY);
    TestSingleSegmentConversionExecutor executor = new TestSingleSegmentConversionExecutor();
    try (MockedStatic<PinotFSFactory> factory = mockStatic(PinotFSFactory.class)) {
      IllegalStateException failure = expectThrows(IllegalStateException.class,
          () -> executor.moveSegmentToOutputPinotFS(configs, new File("segment.tar.gz")));
      assertTrue(failure.getMessage().contains("regenerate"));
      factory.verifyNoInteractions();
    }
  }

  @Test
  public void testUriAliasesCannotHideCleanupOwnership() throws Exception {
    Map<String, String> configs = createMetadataPushTaskConfig(new File(TEMP_DIR, "alias")).getConfigs();
    configs.put(BatchConfigProperties.PUSH_SEGMENT_URI_PREFIX, "https://alias/");
    TestSingleSegmentConversionExecutor executor = new TestSingleSegmentConversionExecutor();
    try (MockedStatic<PinotFSFactory> factory = mockStatic(PinotFSFactory.class)) {
      expectThrows(IllegalStateException.class,
          () -> executor.moveSegmentToOutputPinotFS(configs, new File("segment.tar.gz")));
      factory.verifyNoInteractions();
    }
  }

  private PinotTaskConfig createMetadataPushTaskConfig(File outputDir) {
    Map<String, String> configs = new HashMap<>();
    configs.put(MinionConstants.TABLE_NAME_KEY, TABLE_NAME_WITH_TYPE);
    configs.put(MinionConstants.SEGMENT_NAME_KEY, SEGMENT_NAME);
    configs.put(MinionConstants.DOWNLOAD_URL_KEY, "http://unused/download");
    configs.put(MinionConstants.UPLOAD_URL_KEY, "http://unused/upload");
    configs.put(MinionConstants.ORIGINAL_SEGMENT_CRC_KEY, Long.toString(SEGMENT_CRC));
    configs.put("TASK_ID", TASK_ID);
    configs.put(BatchConfigProperties.PUSH_MODE, BatchConfigProperties.SegmentPushType.METADATA.name());
    configs.put(BatchConfigProperties.OUTPUT_SEGMENT_DIR_URI, outputDir.toURI().toString());
    configs.put(SegmentReplacementUtils.ROOT_REFERENCES_CONFIG_KEY,
        new File(TEMP_DIR, "references").toURI().toString());
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

  /// Minimal concrete executor that stubs out the infrastructure-dependent hooks (download, CRC check, conversion, ZK
  /// metadata modifier) so `executeTask` runs to the upload step without a server, controller, or deep store.
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
      return new SegmentZKMetadataCustomMapModifier(SegmentZKMetadataCustomMapModifier.ModifyMode.UPDATE, Map.of());
    }
  }
}
