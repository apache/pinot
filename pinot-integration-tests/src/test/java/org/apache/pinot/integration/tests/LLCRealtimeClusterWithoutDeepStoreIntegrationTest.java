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

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Random;
import org.apache.avro.reflect.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.ExternalView;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.spi.config.table.CompletionConfig;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.controller.ControllerConf.ALLOW_HLC_TABLES;
import static org.apache.pinot.controller.ControllerConf.ENABLE_SPLIT_COMMIT;


// This test verifies that Pinot Low Level Realtime ingestion works properly when there is NO deepstore configured with
// the cluster
public class LLCRealtimeClusterWithoutDeepStoreIntegrationTest extends BaseRealtimeClusterIntegrationTest{
  private static final Logger LOGGER = LoggerFactory.getLogger(LLCRealtimeClusterWithoutDeepStoreIntegrationTest.class);

  private static final String CONSUMER_DIRECTORY = "/tmp/consumer-test";
  private static final long RANDOM_SEED = System.currentTimeMillis();
  private static final Random RANDOM = new Random(RANDOM_SEED);
  private static final int NUM_SERVERS = 2;
  public static final int UPLOAD_FAILURE_MOD = 5;

  private final boolean _isDirectAlloc = true; //Set as true; otherwise trigger indexing exception.
  private final boolean _isConsumerDirConfigured = true;
  private final boolean _enableSplitCommit = true;
  private final boolean _enableLeadControllerResource = RANDOM.nextBoolean();

  @BeforeClass
  @Override
  public void setUp()
      throws Exception {
    LOGGER.info(String.format(
        "Using random seed: %s, isDirectAlloc: %s, isConsumerDirConfigured: %s, enableSplitCommit: %s, "
            + "enableLeadControllerResource: %s",
        RANDOM_SEED, _isDirectAlloc, _isConsumerDirConfigured, _enableSplitCommit, _enableLeadControllerResource));

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
    // Skip the segment upload to deepstore
    segmentsValidationAndRetentionConfig.setSegmentUploadToDeepStoreDisabled(true);
    tableConfig.setValidationConfig(segmentsValidationAndRetentionConfig);
    tableConfig.getValidationConfig().setTimeColumnName(this.getTimeColumnName());

    sendPostRequest(_controllerRequestURLBuilder.forTableCreate(), tableConfig.toJsonString());
  }

  @Override
  public void startController()
      throws Exception {
    Map<String, Object> controllerConfig = getDefaultControllerConfiguration();
    controllerConfig.put(ALLOW_HLC_TABLES, false);
    controllerConfig.put(ENABLE_SPLIT_COMMIT, _enableSplitCommit);
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
    // For setting the HDFS segment fetcher.
    configuration
        .setProperty(CommonConstants.Server.PREFIX_OF_CONFIG_OF_SEGMENT_FETCHER_FACTORY + ".protocols", "file,http");
    if (_isConsumerDirConfigured) {
      configuration.setProperty(CommonConstants.Server.CONFIG_OF_CONSUMER_DIR, CONSUMER_DIRECTORY);
    }
    if (_enableSplitCommit) {
      configuration.setProperty(CommonConstants.Server.CONFIG_OF_ENABLE_SPLIT_COMMIT, true);
      configuration.setProperty(CommonConstants.Server.CONFIG_OF_ENABLE_COMMIT_END_WITH_METADATA, true);
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
}
