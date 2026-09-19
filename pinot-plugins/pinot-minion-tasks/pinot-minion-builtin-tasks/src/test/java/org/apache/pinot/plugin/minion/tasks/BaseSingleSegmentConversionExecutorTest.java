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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.common.metrics.MinionMetrics;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.minion.MinionContext;
import org.apache.pinot.minion.event.MinionEventObservers;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.utils.SegmentPushUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/// Tests executeTask: upload failures propagate, the default METADATA push is unchanged, and the opt-in controller-copy
/// METADATA push is safe for a same-name segment refresh.
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
  private static final String DOWNLOAD_URL = "http://unused/download";
  // A CRC that never matches the built segment, so the converted segment always counts as changed.
  private static final long STALE_SEGMENT_CRC = 100L;
  private static final String D1 = "d1";

  private File _segmentIndexDir;
  private long _segmentCrc;

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
    _segmentCrc = Long.parseLong(new SegmentMetadataImpl(_segmentIndexDir).getCrc());

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

      TestSingleSegmentConversionExecutor executor = new TestSingleSegmentConversionExecutor(STALE_SEGMENT_CRC);
      try {
        executor.executeTask(createTaskConfig(STALE_SEGMENT_CRC));
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
      TestSingleSegmentConversionExecutor executor = new TestSingleSegmentConversionExecutor(STALE_SEGMENT_CRC);
      SegmentConversionResult result = executor.executeTask(createTaskConfig(STALE_SEGMENT_CRC));
      Assert.assertEquals(result.getSegmentName(), SEGMENT_NAME);
      Assert.assertEquals(result.getTableNameWithType(), TABLE_NAME_WITH_TYPE);
      mocked.verify(() -> SegmentConversionUtils.uploadSegment(Mockito.any(), Mockito.any(), Mockito.any(),
          Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.any(File.class)));
    }
  }

  /// Verifies that when a METADATA-mode push fails after the converted tar was already staged to the output PinotFS,
  /// the staged tar is deleted before the exception propagates. Without this cleanup the rethrow would make the retry
  /// fail in moveSegmentToOutputPinotFS with "Output file already exists" (overwriteOutput defaults to false), so
  /// transient push failures would never self-heal.
  @Test
  public void testExecuteTaskCleansUpStagedTarWhenMetadataPushFails()
      throws Exception {
    File outputDir = new File(TEMP_DIR, "output");
    FileUtils.forceMkdir(outputDir);
    PinotFS mockOutputFS = Mockito.mock(PinotFS.class);
    Mockito.when(mockOutputFS.exists(Mockito.any())).thenReturn(false);

    try (MockedStatic<MinionTaskUtils> minionTaskUtils =
            Mockito.mockStatic(MinionTaskUtils.class, Mockito.CALLS_REAL_METHODS);
        MockedStatic<SegmentPushUtils> segmentPushUtils =
            Mockito.mockStatic(SegmentPushUtils.class, Mockito.CALLS_REAL_METHODS)) {
      minionTaskUtils.when(() -> MinionTaskUtils.getOutputPinotFS(Mockito.any(), Mockito.any()))
          .thenReturn(mockOutputFS);
      segmentPushUtils.when(() -> SegmentPushUtils.sendSegmentUriAndMetadata(Mockito.any(), Mockito.any(),
              Mockito.any(), Mockito.anyList(), Mockito.anyList()))
          .thenThrow(new RuntimeException("simulated metadata push failure"));

      TestSingleSegmentConversionExecutor executor = new TestSingleSegmentConversionExecutor(STALE_SEGMENT_CRC);
      try {
        executor.executeTask(createMetadataPushTaskConfig(STALE_SEGMENT_CRC, outputDir));
        Assert.fail("executeTask must rethrow when metadata push fails");
      } catch (RuntimeException e) {
        Assert.assertEquals(e.getMessage(), "simulated metadata push failure");
      }
      // The staged tar must be deleted so a retry can re-stage it and self-heal.
      Mockito.verify(mockOutputFS).delete(Mockito.any(URI.class), Mockito.eq(true));
    }
  }


  /// The default METADATA push still registers the staged tar's URI and never takes the controller-copy path.
  @Test
  public void testDefaultMetadataPushRegistersStagedUri()
      throws Exception {
    File outputDir = new File(TEMP_DIR, "output-default");
    FileUtils.forceMkdir(outputDir);
    try (MockedStatic<SegmentPushUtils> segmentPushUtils = Mockito.mockStatic(SegmentPushUtils.class,
            Mockito.CALLS_REAL_METHODS);
        MockedStatic<SegmentConversionUtils> conversionUtils = Mockito.mockStatic(SegmentConversionUtils.class)) {
      segmentPushUtils.when(() -> SegmentPushUtils.sendSegmentUriAndMetadata(Mockito.any(), Mockito.any(),
          Mockito.any(), Mockito.anyList(), Mockito.anyList())).thenAnswer(invocation -> null);

      new TestSingleSegmentConversionExecutor(STALE_SEGMENT_CRC).executeTask(
          createMetadataPushTaskConfig(STALE_SEGMENT_CRC, outputDir));

      String stagedTarName = SEGMENT_NAME + TarCompressionUtils.TAR_GZ_FILE_EXTENSION;
      segmentPushUtils.verify(() -> SegmentPushUtils.sendSegmentUriAndMetadata(Mockito.any(), Mockito.any(),
          Mockito.argThat((Map<String, String> uriToTar) -> uriToTar.size() == 1
              && uriToTar.values().iterator().next().endsWith(stagedTarName)),
          Mockito.anyList(), Mockito.anyList()));
      conversionUtils.verifyNoInteractions();
    }
    // The staged tar is the segment's download URL in this mode, so it stays.
    Assert.assertTrue(new File(outputDir, SEGMENT_NAME + TarCompressionUtils.TAR_GZ_FILE_EXTENSION).isFile());
  }

  /// Controller-copy push of a changed segment: task-unique staging name, TAR guards plus copy flag, a metadata-only
  /// tar built locally, and the staged tar deleted once the push is done.
  @Test
  public void testMetadataPushStagesTarAndRegistersMetadata()
      throws Exception {
    File outputDir = new File(TEMP_DIR, "output-changed");
    FileUtils.forceMkdir(outputDir);
    File stagedTar = new File(outputDir, SEGMENT_NAME + "." + TASK_ID + TarCompressionUtils.TAR_GZ_FILE_EXTENSION);
    File capturedMetadataTar = new File(TEMP_DIR, "captured-metadata.tar.gz");
    List<Header> capturedHeaders = new ArrayList<>();

    try (MockedStatic<SegmentConversionUtils> mocked = Mockito.mockStatic(SegmentConversionUtils.class)) {
      stubUploadSegmentMetadata(mocked, invocation -> {
        // The controller copies from the staged tar while handling the request, so it must exist at this point.
        Assert.assertTrue(stagedTar.isFile(), "staged tar must exist while the metadata push is in flight");
        assertTarHoldsSegment(stagedTar, new File(TEMP_DIR, "untar-staged"));
        capturedHeaders.addAll(invocation.getArgument(1));
        FileUtils.copyFile(invocation.getArgument(6), capturedMetadataTar);
      });

      TestSingleSegmentConversionExecutor executor = new TestSingleSegmentConversionExecutor(STALE_SEGMENT_CRC, true);
      SegmentConversionResult result =
          executor.executeTask(createMetadataPushTaskConfig(STALE_SEGMENT_CRC, outputDir));
      Assert.assertEquals(result.getSegmentName(), SEGMENT_NAME);
      mocked.verify(() -> SegmentConversionUtils.uploadSegment(Mockito.any(), Mockito.any(), Mockito.any(),
          Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.any(File.class)), Mockito.never());
    }

    // Nothing references the staged tar after the controller copied it, so it must be gone.
    Assert.assertEquals(outputDir.list().length, 0, "staged tar must be deleted after the push");

    Assert.assertEquals(headerValue(capturedHeaders, HttpHeaders.IF_MATCH), String.valueOf(STALE_SEGMENT_CRC));
    Assert.assertEquals(headerValue(capturedHeaders, FileUploadDownloadClient.CustomHeaders.REFRESH_ONLY), "true");
    Assert.assertNotNull(
        headerValue(capturedHeaders, FileUploadDownloadClient.CustomHeaders.SEGMENT_ZK_METADATA_CUSTOM_MAP_MODIFIER));
    Assert.assertEquals(headerValue(capturedHeaders, FileUploadDownloadClient.CustomHeaders.UPLOAD_TYPE),
        FileUploadDownloadClient.FileUploadType.METADATA.toString());
    Assert.assertEquals(
        headerValue(capturedHeaders, FileUploadDownloadClient.CustomHeaders.COPY_SEGMENT_TO_DEEP_STORE), "true");
    String downloadUri = headerValue(capturedHeaders, FileUploadDownloadClient.CustomHeaders.DOWNLOAD_URI);
    Assert.assertEquals(new File(URI.create(downloadUri)), stagedTar);

    // The metadata tar carries only the two files the controller reads, and they describe the converted segment.
    File untarredMetadataDir = TarCompressionUtils.untar(capturedMetadataTar, new File(TEMP_DIR, "untar-metadata"))
        .get(0);
    Set<String> fileNames =
        java.util.Arrays.stream(untarredMetadataDir.listFiles()).map(File::getName).collect(Collectors.toSet());
    Assert.assertEquals(fileNames,
        Set.of(V1Constants.MetadataKeys.METADATA_FILE_NAME, V1Constants.SEGMENT_CREATION_META));
    SegmentMetadataImpl pushedMetadata = new SegmentMetadataImpl(untarredMetadataDir);
    Assert.assertEquals(pushedMetadata.getName(), SEGMENT_NAME);
    Assert.assertEquals(Long.parseLong(pushedMetadata.getCrc()), _segmentCrc);
  }

  /// A plain-path output dir (local deep store) must reach the controller as a file URI.
  @Test
  public void testMetadataPushQualifiesSchemelessOutputDir()
      throws Exception {
    File outputDir = new File(TEMP_DIR, "output-schemeless");
    FileUtils.forceMkdir(outputDir);
    File stagedTar = new File(outputDir, SEGMENT_NAME + "." + TASK_ID + TarCompressionUtils.TAR_GZ_FILE_EXTENSION);
    List<Header> capturedHeaders = new ArrayList<>();

    try (MockedStatic<SegmentConversionUtils> mocked = Mockito.mockStatic(SegmentConversionUtils.class)) {
      stubUploadSegmentMetadata(mocked, invocation -> {
        Assert.assertTrue(stagedTar.isFile());
        capturedHeaders.addAll(invocation.getArgument(1));
      });
      PinotTaskConfig taskConfig = createTaskConfig(STALE_SEGMENT_CRC);
      Map<String, String> configs = taskConfig.getConfigs();
      configs.put(BatchConfigProperties.PUSH_MODE, BatchConfigProperties.SegmentPushType.METADATA.name());
      configs.put(BatchConfigProperties.OUTPUT_SEGMENT_DIR_URI, outputDir.getAbsolutePath());
      new TestSingleSegmentConversionExecutor(STALE_SEGMENT_CRC, true).executeTask(taskConfig);
    }

    URI downloadUri = URI.create(headerValue(capturedHeaders, FileUploadDownloadClient.CustomHeaders.DOWNLOAD_URI));
    Assert.assertEquals(downloadUri.getScheme(), "file");
    Assert.assertEquals(new File(downloadUri), stagedTar);
    Assert.assertEquals(outputDir.list().length, 0, "staged tar must be deleted after the push");
  }

  /// A failed push propagates and leaves no staged tar behind.
  @Test
  public void testMetadataPushDeletesStagedTarWhenPushFails()
      throws Exception {
    File outputDir = new File(TEMP_DIR, "output-failed");
    FileUtils.forceMkdir(outputDir);

    try (MockedStatic<SegmentConversionUtils> mocked = Mockito.mockStatic(SegmentConversionUtils.class)) {
      mocked.when(() -> SegmentConversionUtils.uploadSegmentMetadata(Mockito.any(), Mockito.anyList(),
              Mockito.anyList(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
              Mockito.any(File.class)))
          .thenThrow(new RuntimeException("simulated metadata push failure"));

      TestSingleSegmentConversionExecutor executor = new TestSingleSegmentConversionExecutor(STALE_SEGMENT_CRC, true);
      try {
        executor.executeTask(createMetadataPushTaskConfig(STALE_SEGMENT_CRC, outputDir));
        Assert.fail("executeTask must rethrow when metadata push fails");
      } catch (RuntimeException e) {
        Assert.assertEquals(e.getMessage(), "simulated metadata push failure");
      }
    }
    Assert.assertEquals(outputDir.list().length, 0, "staged tar must be deleted after a failed push");
  }

  /// A retry reuses the staging name and overwrites an interrupted attempt's leftover instead of failing.
  @Test
  public void testMetadataPushOverwritesStagedTarLeftByPreviousAttempt()
      throws Exception {
    File outputDir = new File(TEMP_DIR, "output-leftover");
    FileUtils.forceMkdir(outputDir);
    File stagedTar = new File(outputDir, SEGMENT_NAME + "." + TASK_ID + TarCompressionUtils.TAR_GZ_FILE_EXTENSION);
    FileUtils.writeStringToFile(stagedTar, "leftover from an interrupted attempt", StandardCharsets.UTF_8);

    try (MockedStatic<SegmentConversionUtils> mocked = Mockito.mockStatic(SegmentConversionUtils.class)) {
      stubUploadSegmentMetadata(mocked,
          invocation -> assertTarHoldsSegment(stagedTar, new File(TEMP_DIR, "untar-leftover")));
      new TestSingleSegmentConversionExecutor(STALE_SEGMENT_CRC, true).executeTask(
          createMetadataPushTaskConfig(STALE_SEGMENT_CRC, outputDir));
    }
    Assert.assertEquals(outputDir.list().length, 0, "staged tar must be deleted after the push");
  }

  /// An unchanged segment (same CRC) is re-registered against its download URL without staging or copying.
  @Test
  public void testMetadataPushRegistersMetadataOnlyWhenSegmentUnchanged()
      throws Exception {
    File outputDir = new File(TEMP_DIR, "output-unchanged");
    FileUtils.forceMkdir(outputDir);
    List<Header> capturedHeaders = new ArrayList<>();

    try (MockedStatic<SegmentConversionUtils> mocked = Mockito.mockStatic(SegmentConversionUtils.class)) {
      stubUploadSegmentMetadata(mocked, invocation -> {
        Assert.assertEquals(outputDir.list().length, 0, "an unchanged segment must not be staged");
        capturedHeaders.addAll(invocation.getArgument(1));
      });
      new TestSingleSegmentConversionExecutor(_segmentCrc, true).executeTask(
          createMetadataPushTaskConfig(_segmentCrc, outputDir));
    }

    Assert.assertEquals(headerValue(capturedHeaders, HttpHeaders.IF_MATCH), String.valueOf(_segmentCrc));
    Assert.assertEquals(headerValue(capturedHeaders, FileUploadDownloadClient.CustomHeaders.REFRESH_ONLY), "true");
    Assert.assertEquals(headerValue(capturedHeaders, FileUploadDownloadClient.CustomHeaders.DOWNLOAD_URI),
        DOWNLOAD_URL);
    Assert.assertEquals(
        headerValue(capturedHeaders, FileUploadDownloadClient.CustomHeaders.COPY_SEGMENT_TO_DEEP_STORE), "false");
    Assert.assertEquals(outputDir.list().length, 0);
  }

  private interface MetadataPushCheck {
    void check(org.mockito.invocation.InvocationOnMock invocation)
        throws Exception;
  }

  private static void stubUploadSegmentMetadata(MockedStatic<SegmentConversionUtils> mocked, MetadataPushCheck check) {
    mocked.when(() -> SegmentConversionUtils.uploadSegmentMetadata(Mockito.any(), Mockito.anyList(), Mockito.anyList(),
            Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.any(File.class)))
        .thenAnswer(invocation -> {
          check.check(invocation);
          return null;
        });
  }

  private void assertTarHoldsSegment(File tarFile, File untarDir)
      throws Exception {
    FileUtils.deleteDirectory(untarDir);
    File untarredSegmentDir = TarCompressionUtils.untar(tarFile, untarDir).get(0);
    Assert.assertEquals(new SegmentMetadataImpl(untarredSegmentDir).getName(), SEGMENT_NAME);
  }

  private static String headerValue(List<Header> headers, String name) {
    return headers.stream().filter(header -> header.getName().equals(name)).map(Header::getValue).findFirst()
        .orElse(null);
  }

  private PinotTaskConfig createTaskConfig(long originalSegmentCrc) {
    Map<String, String> configs = new HashMap<>();
    configs.put(MinionConstants.TABLE_NAME_KEY, TABLE_NAME_WITH_TYPE);
    configs.put(MinionConstants.SEGMENT_NAME_KEY, SEGMENT_NAME);
    configs.put(MinionConstants.DOWNLOAD_URL_KEY, DOWNLOAD_URL);
    configs.put(MinionConstants.UPLOAD_URL_KEY, "http://unused/upload");
    configs.put(MinionConstants.ORIGINAL_SEGMENT_CRC_KEY, Long.toString(originalSegmentCrc));
    configs.put("TASK_ID", TASK_ID);
    return new PinotTaskConfig(TASK_TYPE, configs);
  }

  private PinotTaskConfig createMetadataPushTaskConfig(long originalSegmentCrc, File outputDir) {
    PinotTaskConfig taskConfig = createTaskConfig(originalSegmentCrc);
    taskConfig.getConfigs().put(BatchConfigProperties.PUSH_MODE, BatchConfigProperties.SegmentPushType.METADATA.name());
    taskConfig.getConfigs().put(BatchConfigProperties.OUTPUT_SEGMENT_DIR_URI, outputDir.toURI().toString());
    return taskConfig;
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    // Restore the process-global state mutated in setUp so it does not leak into other test classes.
    MinionEventObservers.getInstance().removeMinionEventObserver(TASK_ID);
    MinionContext.getInstance().setDataDir(null);
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  /// Stubs download, CRC check, conversion (a copy, so the CRC is unchanged) and the ZK modifier.
  private class TestSingleSegmentConversionExecutor extends BaseSingleSegmentConversionExecutor {
    private final long _zkSegmentCrc;
    private final boolean _copyToDeepStore;

    TestSingleSegmentConversionExecutor(long zkSegmentCrc) {
      this(zkSegmentCrc, false);
    }

    TestSingleSegmentConversionExecutor(long zkSegmentCrc, boolean copyToDeepStore) {
      _zkSegmentCrc = zkSegmentCrc;
      _copyToDeepStore = copyToDeepStore;
    }

    @Override
    protected boolean isCopyToDeepStoreForMetadataPush() {
      return _copyToDeepStore;
    }

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
      return _zkSegmentCrc;
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
