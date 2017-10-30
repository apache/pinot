/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.integration.tests;

import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.utils.CommonConstants;
import java.io.File;
import java.util.List;
import java.util.Random;
import org.apache.avro.reflect.Nullable;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.helix.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration test that extends RealtimeClusterIntegrationTest but uses low-level Kafka consumer.
 */
public class LLCRealtimeClusterIntegrationTest extends RealtimeClusterIntegrationTest {
  public static final String CONSUMER_DIRECTORY = "/tmp/consumer-test";
  public static final long seed = System.currentTimeMillis();
  public static final Random RANDOM = new Random(seed);
  public static final boolean isDirectAlloc = RANDOM.nextDouble() < 0.5;

  @BeforeClass
  @Override
  public void setUp() throws Exception {
    // TODO Avoid printing to stdout. Instead, we need to add the seed to every assert in this (and super-classes)
    System.out.println("========== Using random seed value " + seed);
    // Remove the consumer directory
    File consumerDirectory = new File(CONSUMER_DIRECTORY);
    if (consumerDirectory.exists()) {
      FileUtils.deleteDirectory(consumerDirectory);
    }

    super.setUp();
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
  protected void overrideServerConf(Configuration configuration) {
    configuration.setProperty(CommonConstants.Server.CONFIG_OF_REALTIME_OFFHEAP_ALLOCATION, "true");
    configuration.setProperty(CommonConstants.Server.CONFIG_OF_REALTIME_OFFHEAP_DIRECT_ALLOCATION, Boolean.toString(isDirectAlloc));
    if (RANDOM.nextDouble() < 0.5) {
      configuration.setProperty(CommonConstants.Server.CONFIG_OF_CONSUMER_DIR, CONSUMER_DIRECTORY);
    }
  }

  @Test
  public void testConsumerDirectoryExists() {
    File consumerDirectory = new File(CONSUMER_DIRECTORY, "mytable_REALTIME");
    if (!isDirectAlloc) {
      Assert.assertTrue(consumerDirectory.exists(), "The off heap consumer directory does not exist");
    }
  }

  @Test
  public void testSegmentFlushSize() throws Exception {
    String zkSegmentsPath = "/SEGMENTS/" + TableNameBuilder.REALTIME.tableNameWithType(getTableName());
    List<String> segmentNames = _propertyStore.getChildNames(zkSegmentsPath, 0);
    for (String segmentName : segmentNames) {
      ZNRecord znRecord = _propertyStore.get(zkSegmentsPath + "/" + segmentName, null, 0);
      Assert.assertEquals(znRecord.getSimpleField(CommonConstants.Segment.FLUSH_THRESHOLD_SIZE),
          Integer.toString(getRealtimeSegmentFlushSize() / getNumKafkaPartitions()),
          "Segment: " + segmentName + " does not have the expected flush size");
    }
  }
}
