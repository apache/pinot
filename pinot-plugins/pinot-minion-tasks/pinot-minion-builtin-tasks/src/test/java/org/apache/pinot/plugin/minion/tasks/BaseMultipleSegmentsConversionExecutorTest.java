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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.message.BasicNameValuePair;
import org.apache.pinot.common.auth.NullAuthProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.minion.MinionContext;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.ingestion.batch.spec.PushJobSpec;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
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
  public void testRuntimeAuthProviderPreferredOverTaskToken() {
    AuthProvider runtimeProvider = new AuthProvider() {
      @Override
      public Map<String, Object> getRequestHeaders() {
        Map<String, Object> m = new HashMap<>();
        m.put("X-Test", "v1");
        return m;
      }
      @Override
      public String getTaskToken() {
        return "IGNORED";
      }
    };
    MinionContext.getInstance().setTaskAuthProvider(runtimeProvider);

    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put("tableName", "myTable_OFFLINE");
    taskConfigs.put("uploadURL", "http://controller:9000/upload");
    PinotTaskConfig pinotTaskConfig = new PinotTaskConfig("customMinionTask", taskConfigs);
    List<SegmentConversionResult> results = new ArrayList<>();
    BaseMultipleSegmentsConversionExecutor.SegmentUploadContext ctx =
        new BaseMultipleSegmentsConversionExecutor.SegmentUploadContext(pinotTaskConfig, results);

    List<Header> headers = _executor.getSegmentPushCommonHeaders(pinotTaskConfig, ctx.getAuthProvider(), results);

    boolean foundCustom = headers.stream().anyMatch(h -> h.getName().equals("X-Test") && h.getValue().equals("v1"));
    Assert.assertTrue(foundCustom, "Expected custom header from runtime provider");
  }

  @Test
  public void testFallbackToTaskTokenWhenNoRuntimeProvider() {
    MinionContext.getInstance().setTaskAuthProvider(new NullAuthProvider());
    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put("tableName", "myTable_OFFLINE");
    taskConfigs.put("uploadURL", "http://controller:9000/upload");
    taskConfigs.put(MinionConstants.AUTH_TOKEN, "Bearer foo");
    PinotTaskConfig pinotTaskConfig = new PinotTaskConfig("customMinionTask", taskConfigs);
    List<SegmentConversionResult> results = new ArrayList<>();
    BaseMultipleSegmentsConversionExecutor.SegmentUploadContext ctx =
        new BaseMultipleSegmentsConversionExecutor.SegmentUploadContext(pinotTaskConfig, results);

    List<Header> headers = _executor.getSegmentPushCommonHeaders(pinotTaskConfig, ctx.getAuthProvider(), results);

    boolean foundAuth = headers.stream().anyMatch(h -> h.getName().equals("Authorization")
        && h.getValue().equals("Bearer foo"));
    Assert.assertTrue(foundAuth, "Expected Authorization header from AUTH_TOKEN fallback");
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
  public void testUpdateSegmentURIToTarPathMap()
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
    Map<String, String> segmentUriToTarPathMap = new HashMap<>();

    // test
    _executor.updateSegmentUriToTarPathMap(taskConfigs, outputSegmentTarURI, conversionResult, segmentUriToTarPathMap,
        pushJobSpec);

    // validate
    Assert.assertEquals(segmentUriToTarPathMap.size(), 1);
    Assert.assertTrue(segmentUriToTarPathMap.containsKey(_tempDir.toURI() + "segments/segment.tar.gz"));
  }
}
