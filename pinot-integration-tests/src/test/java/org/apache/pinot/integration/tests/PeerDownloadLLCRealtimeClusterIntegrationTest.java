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

import com.fasterxml.jackson.databind.JsonNode;
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
import org.apache.helix.ZNRecord;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.spi.config.table.CompletionConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.LocalPinotFS;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.controller.ControllerConf.ALLOW_HLC_TABLES;
import static org.apache.pinot.controller.ControllerConf.ENABLE_SPLIT_COMMIT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Integration test that extends RealtimeClusterIntegrationTest but uses low-level Kafka consumer and a fake PinotFS as
 * the deep store for segments. This test enables the peer to peer segment download scheme to test Pinot servers can
 * download segments from peer servers even the deep store is down.
 */
public class PeerDownloadLLCRealtimeClusterIntegrationTest extends RealtimeClusterIntegrationTest {
  private static final String CONSUMER_DIRECTORY = "/tmp/consumer-test";
  private static final String TEST_UPDATED_INVERTED_INDEX_QUERY =
      "SELECT COUNT(*) FROM mytable WHERE DivActualElapsedTime = 305";
  private static final List<String> UPDATED_INVERTED_INDEX_COLUMNS = Collections.singletonList("DivActualElapsedTime");
  private static final long RANDOM_SEED = System.currentTimeMillis();
  private static final Random RANDOM = new Random(RANDOM_SEED);

  private final boolean _isDirectAlloc = true; //Set as true; otherwise trigger indexing exception.
  private final boolean _isConsumerDirConfigured = true;
  private final boolean _enableSplitCommit = true;
  private final boolean _enableLeadControllerResource = RANDOM.nextBoolean();
  private final long _startTime = System.currentTimeMillis();
  private static File PINOT_FS_ROOT_DIR;

  @BeforeClass
  @Override
  public void setUp()
      throws Exception {
    System.out.println(String.format(
        "Using random seed: %s, isDirectAlloc: %s, isConsumerDirConfigured: %s, enableSplitCommit: %s, enableLeadControllerResource: %s",
        RANDOM_SEED, _isDirectAlloc, _isConsumerDirConfigured, _enableSplitCommit, _enableLeadControllerResource));

    PINOT_FS_ROOT_DIR = new File(FileUtils.getTempDirectoryPath() + File.separator + System.currentTimeMillis() + "/");
    Preconditions.checkState(PINOT_FS_ROOT_DIR.mkdir(), "Failed to make a dir for " + PINOT_FS_ROOT_DIR.getPath());

    // Remove the consumer directory
    File consumerDirectory = new File(CONSUMER_DIRECTORY);
    if (consumerDirectory.exists()) {
      FileUtils.deleteDirectory(consumerDirectory);
    }
    super.setUp();
  }


  @Override
  public void startServer() {
    startServers(2);
  }

  @Override
  public void addTableConfig(TableConfig tableConfig)
      throws IOException {
    SegmentsValidationAndRetentionConfig segmentsValidationAndRetentionConfig =
        new SegmentsValidationAndRetentionConfig();
    CompletionConfig completionConfig = new CompletionConfig("DOWNLOAD");
    segmentsValidationAndRetentionConfig.setCompletionConfig(completionConfig);
    segmentsValidationAndRetentionConfig.setReplicasPerPartition(String.valueOf(this.getNumReplicas()));
    // Important: enable peer to peer download.
    segmentsValidationAndRetentionConfig.setPeerSegmentDownloadScheme("http");
    tableConfig.setValidationConfig(segmentsValidationAndRetentionConfig);

    sendPostRequest(_controllerRequestURLBuilder.forTableCreate(), tableConfig.toJsonString());
  }


  @Override
  public void startController() {
    Map<String, Object> controllerConfig = getDefaultControllerConfiguration();
    controllerConfig.put(ALLOW_HLC_TABLES, false);
    controllerConfig.put(ENABLE_SPLIT_COMMIT, _enableSplitCommit);
    // Override the data dir config.
    controllerConfig.put(ControllerConf.DATA_DIR, "mockfs://" + getHelixClusterName() + "/");
    // Use the mock PinotFS as the PinotFS.
    controllerConfig.put("pinot.controller.storage.factory.class.mockfs",
        "org.apache.pinot.integration.tests.PeerDownloadLLCRealtimeClusterIntegrationTest$MockPinotFS");
    startController(controllerConfig);
    enableResourceConfigForLeadControllerResource(_enableLeadControllerResource);
  }

  @Override
  protected boolean useLlc() {
    return true;
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
    configuration.setProperty("pinot.server.instance.segment.store.uri", "mockfs://" + getHelixClusterName() + "/");
    // For setting the HDFS segment fetcher.
    configuration.setProperty(CommonConstants.Server.PREFIX_OF_CONFIG_OF_SEGMENT_FETCHER_FACTORY + ".protocols", "file,http");
    if (_isConsumerDirConfigured) {
      configuration.setProperty(CommonConstants.Server.CONFIG_OF_CONSUMER_DIR, CONSUMER_DIRECTORY);
    }
    if (_enableSplitCommit) {
      configuration.setProperty(CommonConstants.Server.CONFIG_OF_ENABLE_SPLIT_COMMIT, true);
      configuration.setProperty(CommonConstants.Server.CONFIG_OF_ENABLE_COMMIT_END_WITH_METADATA, true);
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
    String zkSegmentsPath = "/SEGMENTS/" + TableNameBuilder.REALTIME.tableNameWithType(getTableName());
    List<String> segmentNames = _propertyStore.getChildNames(zkSegmentsPath, 0);
    for (String segmentName : segmentNames) {
      ZNRecord znRecord = _propertyStore.get(zkSegmentsPath + "/" + segmentName, null, 0);
      assertEquals(znRecord.getSimpleField(CommonConstants.Segment.FLUSH_THRESHOLD_SIZE),
          Integer.toString(getRealtimeSegmentFlushSize() / getNumKafkaPartitions()),
          "Segment: " + segmentName + " does not have the expected flush size");
    }
  }

  @Test
  public void testInvertedIndexTriggering()
      throws Exception {
    long numTotalDocs = getCountStarResult();

    JsonNode queryResponse = postQuery(TEST_UPDATED_INVERTED_INDEX_QUERY);
    assertEquals(queryResponse.get("totalDocs").asLong(), numTotalDocs);
    assertTrue(queryResponse.get("numEntriesScannedInFilter").asLong() > 0L);

    {
      IndexingConfig config = getRealtimeTableConfig().getIndexingConfig();
      config.setInvertedIndexColumns(UPDATED_INVERTED_INDEX_COLUMNS);
      config.setBloomFilterColumns(null);

      sendPutRequest(_controllerRequestURLBuilder.forUpdateTableConfig(getTableName()),
          getRealtimeTableConfig().toJsonString());
    }

    sendPostRequest(_controllerRequestURLBuilder.forTableReload(getTableName(), "realtime"), null);

    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode queryResponse1 = postQuery(TEST_UPDATED_INVERTED_INDEX_QUERY);
        // Total docs should not change during reload
        assertEquals(queryResponse1.get("totalDocs").asLong(), numTotalDocs);
        assertEquals(queryResponse1.get("numConsumingSegmentsQueried").asLong(), 2);
        assertTrue(queryResponse1.get("minConsumingFreshnessTimeMs").asLong() > _startTime);
        assertTrue(queryResponse1.get("minConsumingFreshnessTimeMs").asLong() < System.currentTimeMillis());
        return queryResponse1.get("numEntriesScannedInFilter").asLong() == 0;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 600_000L, "Failed to generate inverted index");
  }

  @Test(expectedExceptions = IOException.class)
  public void testAddHLCTableShouldFail()
      throws IOException {
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("testTable")
        .setStreamConfigs(Collections.singletonMap("stream.kafka.consumer.type", "HIGHLEVEL")).build();
    sendPostRequest(_controllerRequestURLBuilder.forTableCreate(), tableConfig.toJsonString());
  }

  // MockPinotFS is a localPinotFS whose root directory is configured as _basePath;
  public static class MockPinotFS extends PinotFS {
    LocalPinotFS _localPinotFS;
    File _basePath;
    @Override
    public void init(PinotConfiguration config) {
      _basePath = PeerDownloadLLCRealtimeClusterIntegrationTest.PINOT_FS_ROOT_DIR;
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
        return _localPinotFS.doMove(new URI(_basePath + srcUri.getPath()), new URI(_basePath + dstUri.getPath()));
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException(e.getMessage());
      }
    }

    @Override
    public boolean copy(URI srcUri, URI dstUri)
        throws IOException {
      try {
        return _localPinotFS.copy(new URI(_basePath + srcUri.getPath()), new URI(_basePath + dstUri.getPath()));
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
      throw new UnsupportedOperationException();
    }

    @Override
    public void copyFromLocalFile(File srcFile, URI dstUri)
        throws Exception {
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