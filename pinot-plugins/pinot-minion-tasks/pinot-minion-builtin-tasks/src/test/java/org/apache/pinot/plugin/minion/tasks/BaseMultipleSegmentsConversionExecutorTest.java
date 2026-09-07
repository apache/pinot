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
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.message.BasicNameValuePair;
import org.apache.pinot.common.auth.NullAuthProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.common.metrics.MinionMetrics;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.minion.MinionConf;
import org.apache.pinot.minion.MinionContext;
import org.apache.pinot.minion.event.MinionEventObservers;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.utils.SegmentPushUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.ingestion.batch.spec.Constants;
import org.apache.pinot.spi.ingestion.batch.spec.PushJobSpec;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class BaseMultipleSegmentsConversionExecutorTest {

  private AuthProvider _mockAuthProvider;
  private BaseMultipleSegmentsConversionExecutor _executor;
  private SegmentZKMetadataCustomMapModifier _zkMetadataCustomMapModifier;
  private File _tempDir;

  @AfterMethod
  public void tearDown() throws IOException {
    MinionContext.getInstance().setTaskAuthProvider(null);
    // Clean up the temporary directory
    FileUtils.deleteDirectory(_tempDir);
  }

  @BeforeMethod
  private void setup()
      throws IOException {
    _tempDir = new File(FileUtils.getTempDirectory(), "test-" + UUID.randomUUID());
    FileUtils.forceMkdir(_tempDir);

    _mockAuthProvider = Mockito.mock(AuthProvider.class);
    _zkMetadataCustomMapModifier = new SegmentZKMetadataCustomMapModifier(
        SegmentZKMetadataCustomMapModifier.ModifyMode.UPDATE, new HashMap<>());
    _executor = new BaseMultipleSegmentsConversionExecutor(null) {
      @Override
      protected List<SegmentConversionResult> convert(PinotTaskConfig pinotTaskConfig, List<File> segmentDirs,
          File workingDir) {
        return null;
      }

      @Override
      protected SegmentZKMetadataCustomMapModifier getSegmentZKMetadataCustomMapModifier(
          PinotTaskConfig pinotTaskConfig, SegmentConversionResult segmentConversionResult) {
        return _zkMetadataCustomMapModifier;
      }
    };
  }

  // Helper method to create task configs with optional AUTH_TOKEN
  private Map<String, String> createTaskConfigs(String authToken) {
    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put("tableName", "myTable_OFFLINE");
    taskConfigs.put("uploadURL", "http://controller:9000/upload");
    if (authToken != null) {
      taskConfigs.put(MinionConstants.AUTH_TOKEN, authToken);
    }
    return taskConfigs;
  }

  // Helper method to create SegmentUploadContext and get auth provider headers
  private List<Header> getAuthHeaders(Map<String, String> taskConfigs) {
    PinotTaskConfig pinotTaskConfig = new PinotTaskConfig("customMinionTask", taskConfigs);
    List<SegmentConversionResult> results = new ArrayList<>();
    BaseMultipleSegmentsConversionExecutor.SegmentUploadContext ctx =
        new BaseMultipleSegmentsConversionExecutor.SegmentUploadContext(pinotTaskConfig, results);
    return _executor.getSegmentPushCommonHeaders(pinotTaskConfig, ctx.getAuthProvider(), results);
  }

  // Helper method to get AuthProvider from task configs
  private AuthProvider getAuthProviderFromTaskConfigs(Map<String, String> taskConfigs) {
    PinotTaskConfig pinotTaskConfig = new PinotTaskConfig("customMinionTask", taskConfigs);
    List<SegmentConversionResult> results = new ArrayList<>();
    BaseMultipleSegmentsConversionExecutor.SegmentUploadContext ctx =
        new BaseMultipleSegmentsConversionExecutor.SegmentUploadContext(pinotTaskConfig, results);
    return ctx.getAuthProvider();
  }

  @Test
  public void testGetPushJobSpec() {
    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put(BatchConfigProperties.BATCH_SEGMENT_UPLOAD, "true");

    PushJobSpec batchPushJobSpec = _executor.getPushJobSpec(taskConfigs);
    Assert.assertTrue(batchPushJobSpec.isBatchSegmentUpload());
  }

  @Test
  public void testGetSegmentPushCommonHeaders() {
    PinotTaskConfig pinotTaskConfig = new PinotTaskConfig("customMinionTask", new HashMap<>());
    List<SegmentConversionResult> segmentConversionResults = new ArrayList<>();
    List<Header> headers =
        _executor.getSegmentPushCommonHeaders(pinotTaskConfig, _mockAuthProvider, segmentConversionResults);
    Assert.assertEquals(headers.size(), 1);
  }

  @Test
  public void testRuntimeAuthProviderUsedWhenNoExplicitToken() {
    // Set up a runtime provider in MinionContext
    AuthProvider runtimeProvider = new AuthProvider() {
      @Override
      public Map<String, Object> getRequestHeaders() {
        Map<String, Object> m = new HashMap<>();
        m.put("X-Runtime-Auth", "runtime-value");
        return m;
      }
      @Override
      public String getTaskToken() {
        return "IGNORED";
      }
    };
    MinionContext.getInstance().setTaskAuthProvider(runtimeProvider);

    // Create task WITHOUT explicit AUTH_TOKEN
    Map<String, String> taskConfigs = createTaskConfigs(null);
    List<Header> headers = getAuthHeaders(taskConfigs);

    // Should use runtime provider since no explicit token was provided
    boolean foundCustom = headers.stream().anyMatch(h -> h.getName().equals("X-Runtime-Auth")
        && h.getValue().equals("runtime-value"));
    Assert.assertTrue(foundCustom, "Expected custom header from runtime provider when no explicit token provided");
  }

  @Test
  public void testExplicitTaskTokenTakesPrecedence() {
    // Set up a runtime provider in MinionContext
    AuthProvider runtimeProvider = new AuthProvider() {
      @Override
      public Map<String, Object> getRequestHeaders() {
        Map<String, Object> m = new HashMap<>();
        m.put("X-Runtime-Auth", "should-not-be-used");
        return m;
      }
      @Override
      public String getTaskToken() {
        return "IGNORED";
      }
    };
    MinionContext.getInstance().setTaskAuthProvider(runtimeProvider);

    // Create task WITH explicit AUTH_TOKEN (should take precedence)
    Map<String, String> taskConfigs = createTaskConfigs("Bearer explicit-task-token");
    List<Header> headers = getAuthHeaders(taskConfigs);

    // Should use explicit AUTH_TOKEN, not runtime provider
    boolean foundExplicitToken = headers.stream().anyMatch(h -> h.getName().equals("Authorization")
        && h.getValue().equals("Bearer explicit-task-token"));
    Assert.assertTrue(foundExplicitToken, "Expected explicit AUTH_TOKEN to take precedence over runtime provider");

    // Should NOT have runtime provider's custom header
    boolean foundRuntimeHeader = headers.stream().anyMatch(h -> h.getName().equals("X-Runtime-Auth"));
    Assert.assertFalse(foundRuntimeHeader, "Runtime provider should not be used when explicit token is provided");
  }

  @Test
  public void testExplicitTokenUsedWhenRuntimeProviderIsNull() {
    // No runtime provider configured (or NullAuthProvider)
    MinionContext.getInstance().setTaskAuthProvider(new NullAuthProvider());

    // Create task WITH explicit AUTH_TOKEN
    Map<String, String> taskConfigs = createTaskConfigs("Bearer fallback-token");
    List<Header> headers = getAuthHeaders(taskConfigs);

    // Should use explicit AUTH_TOKEN
    boolean foundAuth = headers.stream().anyMatch(h -> h.getName().equals("Authorization")
        && h.getValue().equals("Bearer fallback-token"));
    Assert.assertTrue(foundAuth, "Expected explicit AUTH_TOKEN to be used"
        + " when runtime provider is null/NullAuthProvider");
  }

  @Test
  public void testReturnsNullAuthProviderWhenBothTokenAndProviderAbsent() {
    // No runtime provider configured
    MinionContext.getInstance().setTaskAuthProvider(null);

    // Create task WITHOUT explicit AUTH_TOKEN
    Map<String, String> taskConfigs = createTaskConfigs(null);
    List<Header> headers = getAuthHeaders(taskConfigs);

    // Should return NullAuthProvider (no auth headers)
    Assert.assertTrue(headers.isEmpty() || headers.stream().noneMatch(h -> h.getName().equals("Authorization")),
        "Expected no Authorization header when both explicit token and runtime provider are absent");

    // Verify the auth provider is indeed a NullAuthProvider
    AuthProvider resolvedProvider = getAuthProviderFromTaskConfigs(taskConfigs);
    Assert.assertTrue(resolvedProvider instanceof NullAuthProvider,
        "Expected NullAuthProvider when both explicit token and runtime provider are absent");
  }

  @Test
  public void testGetSegmentPushCommonParams() {
    String tableNameWithType = "myTable_OFFLINE";

    List<NameValuePair> params = _executor.getSegmentPushCommonParams(tableNameWithType);
    Assert.assertEquals(params.size(), 3);
    Assert.assertTrue(
        params.contains(new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.TABLE_NAME, "myTable")));
    Assert.assertTrue(
        params.contains(new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.TABLE_TYPE, "OFFLINE")));
  }

  @Test
  public void testGetSegmentUris()
      throws IOException {
    // setup
    File segmentDir = new File(_tempDir, "segments");
    FileUtils.forceMkdir(segmentDir);
    File segmentTarFile = new File(segmentDir, "segment.tar.gz");
    FileUtils.touch(segmentTarFile);

    URI outputSegmentTarURI = segmentTarFile.toURI();
    URI outputSegmentDirURI = segmentDir.toURI();

    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put(BatchConfigProperties.OUTPUT_SEGMENT_DIR_URI, outputSegmentDirURI.getPath());

    PushJobSpec pushJobSpec = new PushJobSpec();
    SegmentConversionResult conversionResult =
        new SegmentConversionResult.Builder().setSegmentName("mySegment").build();

    // test
    Set<String> segmentUris = _executor.getSegmentUris(taskConfigs, outputSegmentTarURI, conversionResult, pushJobSpec);

    // validate
    Assert.assertEquals(segmentUris, Set.of(_tempDir.toURI() + "segments/segment.tar.gz"));
  }

  @DataProvider(name = "batchSegmentUpload")
  public Object[][] batchSegmentUpload() {
    return new Object[][]{{false}, {true}};
  }

  /// On METADATA push the metadata tar of every output segment is built from the local converted segment and pushed
  /// with the local-metadata overload, so the staged tar is never downloaded back. Covers per-segment and batch pushes.
  @Test(dataProvider = "batchSegmentUpload")
  public void testMetadataPushUsesLocalMetadataTars(boolean batchSegmentUpload)
      throws Exception {
    File outputDir = new File(_tempDir, "output");
    FileUtils.forceMkdir(outputDir);
    File dataDir = new File(_tempDir, "minionData");
    FileUtils.forceMkdir(dataDir);
    File capturedDir = new File(_tempDir, "captured");
    FileUtils.forceMkdir(capturedDir);
    File inputSegmentDir = buildSegment("input_0");
    List<String> outputSegmentNames = List.of("merged_0", "merged_1");
    Map<String, File> outputSegmentDirs = new HashMap<>();
    for (String name : outputSegmentNames) {
      outputSegmentDirs.put(name, buildSegment(name));
    }
    String taskId = "Task_TestMultiSegmentTask_" + UUID.randomUUID();
    MinionContext.getInstance().setDataDir(dataDir);
    MinionMetrics.register(Mockito.mock(MinionMetrics.class));
    MinionEventObservers.getInstance().addMinionEventObserver(taskId, MinionTaskTestUtils.getMinionProgressObserver());

    Map<String, String> configs = new HashMap<>();
    configs.put(MinionConstants.TABLE_NAME_KEY, "myTable_OFFLINE");
    configs.put(MinionConstants.SEGMENT_NAME_KEY, "input_0");
    configs.put(MinionConstants.DOWNLOAD_URL_KEY, "http://unused/download");
    configs.put(MinionConstants.UPLOAD_URL_KEY, "http://unused/upload");
    configs.put(BatchConfigProperties.PUSH_MODE, BatchConfigProperties.SegmentPushType.METADATA.name());
    configs.put(BatchConfigProperties.OUTPUT_SEGMENT_DIR_URI, outputDir.toURI().toString());
    configs.put(BatchConfigProperties.BATCH_SEGMENT_UPLOAD, String.valueOf(batchSegmentUpload));
    configs.put("TASK_ID", taskId);

    Map<String, File> capturedUriToMetadataFile = new HashMap<>();
    try (MockedStatic<SegmentPushUtils> mocked = Mockito.mockStatic(SegmentPushUtils.class,
        Mockito.CALLS_REAL_METHODS)) {
      Answer<Void> capture = invocation -> {
        Map<String, File> uriToMetadataFile = invocation.getArgument(1);
        for (Map.Entry<String, File> entry : uriToMetadataFile.entrySet()) {
          File copy = new File(capturedDir, entry.getValue().getName());
          FileUtils.copyFile(entry.getValue(), copy);
          capturedUriToMetadataFile.put(entry.getKey(), copy);
        }
        return null;
      };
      mocked.when(() -> SegmentPushUtils.sendSegmentUriAndMetadata(Mockito.any(), Mockito.anyMap(), Mockito.anyList(),
          Mockito.anyList())).thenAnswer(capture);
      mocked.when(() -> SegmentPushUtils.sendSegmentsUriAndMetadata(Mockito.any(), Mockito.anyMap(), Mockito.anyList(),
          Mockito.anyList())).thenAnswer(capture);

      new TestMultipleSegmentsConversionExecutor(inputSegmentDir, outputSegmentDirs).executeTask(
          new PinotTaskConfig("TestMultiSegmentTask", configs));

      // Never the PinotFS variants, which download the staged tar back to extract the metadata
      mocked.verify(() -> SegmentPushUtils.sendSegmentUriAndMetadata(Mockito.any(), Mockito.any(PinotFS.class),
          Mockito.anyMap(), Mockito.anyList(), Mockito.anyList()), Mockito.never());
      mocked.verify(() -> SegmentPushUtils.sendSegmentsUriAndMetadata(Mockito.any(), Mockito.any(PinotFS.class),
          Mockito.anyMap(), Mockito.anyList(), Mockito.anyList()), Mockito.never());
    } finally {
      MinionEventObservers.getInstance().removeMinionEventObserver(taskId);
      MinionContext.getInstance().setDataDir(null);
    }

    Assert.assertEquals(capturedUriToMetadataFile.size(), 2);
    for (String name : outputSegmentNames) {
      File stagedTar = new File(outputDir, name + TarCompressionUtils.TAR_GZ_FILE_EXTENSION);
      Assert.assertTrue(stagedTar.isFile(), "staged tar is the download URL and must stay");
      File metadataTar = capturedUriToMetadataFile.get(stagedTar.toURI().toString());
      Assert.assertNotNull(metadataTar, "metadata pushed for " + stagedTar.toURI());
      Assert.assertEquals(metadataTar.getName(), name + Constants.METADATA_TAR_GZ_FILE_EXT);
      File untarred = TarCompressionUtils.untar(metadataTar, new File(_tempDir, "untar-" + name)).get(0);
      Assert.assertEquals(new HashSet<>(Arrays.asList(untarred.list())),
          Set.of(V1Constants.MetadataKeys.METADATA_FILE_NAME, V1Constants.SEGMENT_CREATION_META));
      Assert.assertEquals(new SegmentMetadataImpl(untarred).getName(), name);
    }
  }

  private File buildSegment(String segmentName)
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").build();
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension("d1", FieldSpec.DataType.INT).build();
    GenericRow row = new GenericRow();
    row.putValue("d1", 1);
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setInstanceType(InstanceType.MINION);
    config.setOutDir(new File(_tempDir, "built").getPath());
    config.setSegmentName(segmentName);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(List.of(row)));
    driver.build();
    return new File(new File(_tempDir, "built"), segmentName);
  }

  /// Stubs download, the segment-existence pre-check and conversion so executeTask runs to the push step offline.
  private static class TestMultipleSegmentsConversionExecutor extends BaseMultipleSegmentsConversionExecutor {
    private final File _inputSegmentDir;
    private final Map<String, File> _outputSegmentDirs;

    TestMultipleSegmentsConversionExecutor(File inputSegmentDir, Map<String, File> outputSegmentDirs) {
      super(new MinionConf());
      _inputSegmentDir = inputSegmentDir;
      _outputSegmentDirs = outputSegmentDirs;
    }

    @Override
    protected void preProcess(PinotTaskConfig pinotTaskConfig) {
    }

    @Override
    protected File downloadSegmentToLocalAndUntar(String tableNameWithType, String segmentName, String deepstoreURL,
        String taskType, File tempDataDir, String suffix)
        throws Exception {
      File indexDir = new File(tempDataDir, "input" + suffix);
      FileUtils.copyDirectory(_inputSegmentDir, indexDir);
      return indexDir;
    }

    @Override
    protected List<SegmentConversionResult> convert(PinotTaskConfig pinotTaskConfig, List<File> segmentDirs,
        File workingDir)
        throws Exception {
      List<SegmentConversionResult> results = new ArrayList<>();
      for (Map.Entry<String, File> entry : _outputSegmentDirs.entrySet()) {
        File convertedDir = new File(workingDir, entry.getKey());
        FileUtils.copyDirectory(entry.getValue(), convertedDir);
        results.add(new SegmentConversionResult.Builder().setFile(convertedDir)
            .setTableNameWithType(pinotTaskConfig.getConfigs().get(MinionConstants.TABLE_NAME_KEY))
            .setSegmentName(entry.getKey()).build());
      }
      return results;
    }

    @Override
    protected SegmentZKMetadataCustomMapModifier getSegmentZKMetadataCustomMapModifier(PinotTaskConfig pinotTaskConfig,
        SegmentConversionResult segmentConversionResult) {
      return new SegmentZKMetadataCustomMapModifier(SegmentZKMetadataCustomMapModifier.ModifyMode.UPDATE, Map.of());
    }
  }
}
