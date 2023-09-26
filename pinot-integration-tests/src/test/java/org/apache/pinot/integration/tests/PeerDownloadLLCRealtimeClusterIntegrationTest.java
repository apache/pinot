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
package org.apache.pinot.integration.tests;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.avro.reflect.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.ExternalView;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.spi.config.table.CompletionConfig;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.BasePinotFS;
import org.apache.pinot.spi.filesystem.LocalPinotFS;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/**
 * Integration test that extends RealtimeClusterIntegrationTest but uses low-level Kafka consumer and a fake PinotFS as
 * the deep store for segments. This test enables the peer to peer segment download scheme to test Pinot servers can
 * download segments from peer servers even when the deep store is down. This is done by injection of failures in
 * the fake PinotFS segment upload api (i.e., copyFromLocal) for all segments whose seq number mod 5 is 0.
 *
 * Besides standard tests, it also verifies that
 * (1) All the segments on all servers are in either ONLINE or CONSUMING states
 * (2) For segments failed during deep store upload, the corresponding segment download url string is empty in Zk.
 */
public class PeerDownloadLLCRealtimeClusterIntegrationTest extends BaseRealtimeClusterIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(PeerDownloadLLCRealtimeClusterIntegrationTest.class);

  private static final String CONSUMER_DIRECTORY = "/tmp/consumer-test";
  private static final long RANDOM_SEED = System.currentTimeMillis();
  private static final Random RANDOM = new Random(RANDOM_SEED);
  private static final int NUM_SERVERS = 2;
  public static final int UPLOAD_FAILURE_MOD = 5;

  private final boolean _isDirectAlloc = true; //Set as true; otherwise trigger indexing exception.
  private final boolean _isConsumerDirConfigured = true;
  private final boolean _enableLeadControllerResource = RANDOM.nextBoolean();
  private static File _pinotFsRootDir;

  @BeforeClass
  @Override
  public void setUp()
      throws Exception {
    System.out.println(String.format(
        "Using random seed: %s, isDirectAlloc: %s, isConsumerDirConfigured: %s, enableLeadControllerResource: %s",
        RANDOM_SEED, _isDirectAlloc, _isConsumerDirConfigured, _enableLeadControllerResource));

    _pinotFsRootDir = new File(FileUtils.getTempDirectoryPath() + File.separator + System.currentTimeMillis() + "/");
    Preconditions.checkState(_pinotFsRootDir.mkdir(), "Failed to make a dir for " + _pinotFsRootDir.getPath());

    // Remove the consumer directory
    File consumerDirectory = new File(CONSUMER_DIRECTORY);
    if (consumerDirectory.exists()) {
      FileUtils.deleteDirectory(consumerDirectory);
    }
    super.setUp();
  }

  @AfterClass
  @Override
  public void tearDown()
      throws Exception {
    FileUtils.deleteDirectory(new File(CONSUMER_DIRECTORY));
    super.tearDown();
  }

  @Override
  public void startServer()
      throws Exception {
    startServers(NUM_SERVERS);
  }

  @Override
  public void addTableConfig(TableConfig tableConfig)
      throws IOException {
    SegmentsValidationAndRetentionConfig segmentsValidationAndRetentionConfig =
        new SegmentsValidationAndRetentionConfig();
    CompletionConfig completionConfig = new CompletionConfig("DOWNLOAD");
    segmentsValidationAndRetentionConfig.setCompletionConfig(completionConfig);
    segmentsValidationAndRetentionConfig.setReplicasPerPartition(String.valueOf(NUM_SERVERS));
    // Important: enable peer to peer download.
    segmentsValidationAndRetentionConfig.setPeerSegmentDownloadScheme("http");
    tableConfig.setValidationConfig(segmentsValidationAndRetentionConfig);
    tableConfig.getValidationConfig().setTimeColumnName(this.getTimeColumnName());

    sendPostRequest(_controllerRequestURLBuilder.forTableCreate(), tableConfig.toJsonString());
  }

  @Override
  public void startController()
      throws Exception {
    Map<String, Object> controllerConfig = getDefaultControllerConfiguration();
    // Override the data dir config.
    controllerConfig.put(ControllerConf.DATA_DIR, "mockfs://" + getHelixClusterName());
    controllerConfig.put(ControllerConf.LOCAL_TEMP_DIR, FileUtils.getTempDirectory().getAbsolutePath());
    // Use the mock PinotFS as the PinotFS.
    controllerConfig.put("pinot.controller.storage.factory.class.mockfs",
        "org.apache.pinot.integration.tests.PeerDownloadLLCRealtimeClusterIntegrationTest$MockPinotFS");
    startController(controllerConfig);
    enableResourceConfigForLeadControllerResource(_enableLeadControllerResource);
  }

  @Nullable
  @Override
  protected String getLoadMode() {
    return "MMAP";
  }

  @Override
  protected void overrideServerConf(PinotConfiguration configuration) {
    configuration.setProperty(CommonConstants.Server.CONFIG_OF_REALTIME_OFFHEAP_ALLOCATION, true);
    configuration.setProperty(CommonConstants.Server.CONFIG_OF_REALTIME_OFFHEAP_DIRECT_ALLOCATION, _isDirectAlloc);
    configuration.setProperty(CommonConstants.Server.PREFIX_OF_CONFIG_OF_PINOT_FS_FACTORY + ".class.mockfs",
        "org.apache.pinot.integration.tests.PeerDownloadLLCRealtimeClusterIntegrationTest$MockPinotFS");
    // Set the segment deep store uri.
    configuration.setProperty("pinot.server.instance.segment.store.uri", "mockfs://" + getHelixClusterName());
    // For setting the HDFS segment fetcher.
    configuration
        .setProperty(CommonConstants.Server.PREFIX_OF_CONFIG_OF_SEGMENT_FETCHER_FACTORY + ".protocols", "file,http");
    if (_isConsumerDirConfigured) {
      configuration.setProperty(CommonConstants.Server.CONFIG_OF_CONSUMER_DIR, CONSUMER_DIRECTORY);
    }
  }

  @Test
  public void testConsumerDirectoryExists() {
    File consumerDirectory = new File(CONSUMER_DIRECTORY, "mytable_REALTIME");
    assertEquals(consumerDirectory.exists(), _isConsumerDirConfigured,
        "The off heap consumer directory does not exist");
  }

  @Test
  public void testSegmentFlushSize() {
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(getTableName());
    List<SegmentZKMetadata> segmentsZKMetadata =
        ZKMetadataProvider.getSegmentsZKMetadata(_propertyStore, realtimeTableName);
    for (SegmentZKMetadata segmentZKMetadata : segmentsZKMetadata) {
      assertEquals(segmentZKMetadata.getSizeThresholdToFlushSegment(),
          getRealtimeSegmentFlushSize() / getNumKafkaPartitions());
    }
  }

  @Test
  public void testSegmentDownloadURLs() {
    // Verify that all segments of even partition number have empty download url in zk.
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(getTableName());
    List<SegmentZKMetadata> segmentsZKMetadata =
        ZKMetadataProvider.getSegmentsZKMetadata(_propertyStore, realtimeTableName);
    for (SegmentZKMetadata segmentZKMetadata : segmentsZKMetadata) {
      String downloadUrl = segmentZKMetadata.getDownloadUrl();
      if (segmentZKMetadata.getTotalDocs() < 0) {
        // This is a consuming segment so the download url is null.
        assertNull(downloadUrl);
      } else {
        int sequenceNumber = new LLCSegmentName(segmentZKMetadata.getSegmentName()).getSequenceNumber();
        if (sequenceNumber % UPLOAD_FAILURE_MOD == 0) {
          assertTrue(downloadUrl.isEmpty());
        } else {
          assertTrue(downloadUrl.startsWith("mockfs://"));
        }
      }
    }
  }

  @Test
  public void testAllSegmentsAreOnlineOrConsuming() {
    ExternalView externalView = HelixHelper.getExternalViewForResource(_helixAdmin, getHelixClusterName(),
        TableNameBuilder.REALTIME.tableNameWithType(getTableName()));
    Assert.assertEquals("2", externalView.getReplicas());
    // Verify for each segment e, the state of e in its 2 hosting servers is either ONLINE or CONSUMING
    for (String segment : externalView.getPartitionSet()) {
      Map<String, String> instanceToStateMap = externalView.getStateMap(segment);
      Assert.assertEquals(2, instanceToStateMap.size());
      for (Map.Entry<String, String> instanceState : instanceToStateMap.entrySet()) {
        Assert.assertTrue("ONLINE".equalsIgnoreCase(instanceState.getValue()) || "CONSUMING"
            .equalsIgnoreCase(instanceState.getValue()));
      }
    }
  }

  @Test(expectedExceptions = IOException.class)
  public void testAddHLCTableShouldFail()
      throws IOException {
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("testTable")
        .setStreamConfigs(Collections.singletonMap("stream.kafka.consumer.type", "HIGHLEVEL")).build();
    sendPostRequest(_controllerRequestURLBuilder.forTableCreate(), tableConfig.toJsonString());
  }

  // MockPinotFS is a localPinotFS whose root directory is configured as _basePath;
  public static class MockPinotFS extends BasePinotFS {
    LocalPinotFS _localPinotFS = new LocalPinotFS();
    File _basePath;

    @Override
    public void init(PinotConfiguration config) {
      _localPinotFS.init(config);
      _basePath = PeerDownloadLLCRealtimeClusterIntegrationTest._pinotFsRootDir;
    }

    @Override
    public boolean mkdir(URI uri)
        throws IOException {
      try {
        return _localPinotFS.mkdir(new URI(_basePath + uri.getPath()));
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException(e.getMessage());
      }
    }

    @Override
    public boolean delete(URI segmentUri, boolean forceDelete)
        throws IOException {
      try {
        return _localPinotFS.delete(new URI(_basePath + segmentUri.getPath()), forceDelete);
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException(e.getMessage());
      }
    }

    @Override
    public boolean doMove(URI srcUri, URI dstUri)
        throws IOException {
      try {
        LOGGER.warn("Moving from {} to {}", srcUri, dstUri);
        return _localPinotFS.doMove(new URI(_basePath + srcUri.getPath()), new URI(_basePath + dstUri.getPath()));
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException(e.getMessage());
      }
    }

    @Override
    public boolean copyDir(URI srcUri, URI dstUri)
        throws IOException {
      try {
        return _localPinotFS.copyDir(new URI(_basePath + srcUri.getPath()), new URI(_basePath + dstUri.getPath()));
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException(e.getMessage());
      }
    }

    @Override
    public boolean exists(URI fileUri)
        throws IOException {
      try {
        return _localPinotFS.exists(new URI(_basePath + fileUri.getPath()));
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException(e.getMessage());
      }
    }

    @Override
    public long length(URI fileUri)
        throws IOException {
      try {
        return _localPinotFS.length(new URI(_basePath + fileUri.getPath()));
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException(e.getMessage());
      }
    }

    @Override
    public String[] listFiles(URI fileUri, boolean recursive)
        throws IOException {
      try {
        return _localPinotFS.listFiles(new URI(_basePath + fileUri.getPath()), recursive);
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException(e.getMessage());
      }
    }

    @Override
    public void copyToLocalFile(URI srcUri, File dstFile)
        throws Exception {
      _localPinotFS.copyToLocalFile(new URI(_basePath + srcUri.getPath()), dstFile);
    }

    @Override
    public void copyFromLocalFile(File srcFile, URI dstUri)
        throws Exception {
      // Inject failures for segments whose seq number mod 5 is 0.
      if (new LLCSegmentName(srcFile.getName()).getSequenceNumber() % UPLOAD_FAILURE_MOD == 0) {
        throw new IllegalArgumentException(srcFile.getAbsolutePath());
      }
      try {
        _localPinotFS.copyFromLocalFile(srcFile, new URI(_basePath + dstUri.getPath()));
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException(e.getMessage());
      }
    }

    @Override
    public boolean isDirectory(URI uri)
        throws IOException {
      try {
        return _localPinotFS.isDirectory(new URI(_basePath + uri.getPath()));
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException(e.getMessage());
      }
    }

    @Override
    public long lastModified(URI uri)
        throws IOException {
      try {
        return _localPinotFS.lastModified(new URI(_basePath + uri.getPath()));
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException(e.getMessage());
      }
    }

    @Override
    public boolean touch(URI uri)
        throws IOException {
      try {
        return _localPinotFS.touch(new URI(_basePath + uri.getPath()));
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException(e.getMessage());
      }
    }

    @Override
    public InputStream open(URI uri)
        throws IOException {
      try {
        return _localPinotFS.open(new URI(_basePath + uri.getPath()));
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException(e.getMessage());
      }
    }
  }
}
