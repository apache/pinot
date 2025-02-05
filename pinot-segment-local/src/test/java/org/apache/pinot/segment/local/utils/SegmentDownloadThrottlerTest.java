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
package org.apache.pinot.segment.local.utils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;


public class SegmentDownloadThrottlerTest {
  private final static String TABLE_NAME_WITH_TYPE = "test_OFFLINE";

  @Test
  public void testBasicAcquireRelease()
      throws Exception {
    SegmentDownloadThrottler downloadThrottler = new SegmentDownloadThrottler(4, TABLE_NAME_WITH_TYPE);
    Assert.assertEquals(downloadThrottler.availablePermits(), 4);
    Assert.assertEquals(downloadThrottler.totalPermits(), 4);

    downloadThrottler.acquire();
    Assert.assertEquals(downloadThrottler.availablePermits(), 3);
    Assert.assertEquals(downloadThrottler.totalPermits(), 4);

    downloadThrottler.release();
    Assert.assertEquals(downloadThrottler.availablePermits(), 4);
    Assert.assertEquals(downloadThrottler.totalPermits(), 4);
  }

  @Test
  public void testBasicAcquireAllPermits()
      throws Exception {
    int totalPermits = 4;
    SegmentDownloadThrottler downloadThrottler = new SegmentDownloadThrottler(totalPermits, TABLE_NAME_WITH_TYPE);
    Assert.assertEquals(downloadThrottler.totalPermits(), totalPermits);

    for (int i = 0; i < totalPermits; i++) {
      downloadThrottler.acquire();
      Assert.assertEquals(downloadThrottler.availablePermits(), totalPermits - i - 1);
      Assert.assertEquals(downloadThrottler.totalPermits(), totalPermits);
    }
    for (int i = 0; i < totalPermits; i++) {
      downloadThrottler.release();
      Assert.assertEquals(downloadThrottler.availablePermits(), i + 1);
      Assert.assertEquals(downloadThrottler.totalPermits(), totalPermits);
    }
  }

  @Test
  public void testThrowExceptionOnSettingInvalidConfigValues() {
    Assert.assertThrows(IllegalArgumentException.class, () -> new SegmentDownloadThrottler(-1, TABLE_NAME_WITH_TYPE));
    Assert.assertThrows(IllegalArgumentException.class, () -> new SegmentDownloadThrottler(0, TABLE_NAME_WITH_TYPE));
  }

  @Test
  public void testDisabledThrottlingBySettingDefault()
      throws Exception {
    // Default should be quite high. Should be able to essentially acquire as many permits as wanted
    int defaultPermits = Integer.parseInt(CommonConstants.Server.DEFAULT_MAX_PARALLEL_SEGMENT_DOWNLOADS);
    SegmentDownloadThrottler downloadThrottler = new SegmentDownloadThrottler(defaultPermits, TABLE_NAME_WITH_TYPE);
    Assert.assertEquals(downloadThrottler.totalPermits(), defaultPermits);

    Assert.assertEquals(downloadThrottler.availablePermits(), defaultPermits);
    for (int i = 0; i < defaultPermits; i++) {
      downloadThrottler.acquire();
      Assert.assertEquals(downloadThrottler.totalPermits(), defaultPermits);
      Assert.assertEquals(downloadThrottler.availablePermits(), defaultPermits - i - 1);
    }
  }

  @Test
  public void testPositiveToNegativeThrottleChange() {
    int initialPermits = 2;
    SegmentDownloadThrottler downloadThrottler = new SegmentDownloadThrottler(initialPermits, TABLE_NAME_WITH_TYPE);
    Assert.assertEquals(downloadThrottler.totalPermits(), initialPermits);
    Assert.assertEquals(downloadThrottler.availablePermits(), initialPermits);

    // Change the value of cluster config for max segment preprocess parallelism to be a negative value
    // If maxConcurrentDownload is <= 0, this is an invalid configuration change. Do nothing other than log a warning
    Map<String, String> updatedClusterConfigs = new HashMap<>();
    updatedClusterConfigs.put(CommonConstants.Server.CONFIG_OF_MAX_PARALLEL_SEGMENT_DOWNLOADS, "-1");
    downloadThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);

    Assert.assertEquals(downloadThrottler.totalPermits(), initialPermits);
    Assert.assertEquals(downloadThrottler.availablePermits(), initialPermits);

    updatedClusterConfigs.put(CommonConstants.Server.CONFIG_OF_MAX_PARALLEL_SEGMENT_DOWNLOADS, "0");
    downloadThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);

    Assert.assertEquals(downloadThrottler.totalPermits(), initialPermits);
    Assert.assertEquals(downloadThrottler.availablePermits(), initialPermits);
  }

  @Test
  public void testIncreaseSegmentPreprocessParallelism()
      throws Exception {
    int initialPermits = 4;
    SegmentDownloadThrottler downloadThrottler = new SegmentDownloadThrottler(initialPermits, TABLE_NAME_WITH_TYPE);
    Assert.assertEquals(downloadThrottler.totalPermits(), initialPermits);

    for (int i = 0; i < initialPermits; i++) {
      downloadThrottler.acquire();
    }
    Assert.assertEquals(downloadThrottler.totalPermits(), initialPermits);
    Assert.assertEquals(downloadThrottler.availablePermits(), 0);

    // Increase the value of cluster config for max segment preprocess parallelism
    Map<String, String> updatedClusterConfigs = new HashMap<>();
    updatedClusterConfigs.put(CommonConstants.Server.CONFIG_OF_MAX_PARALLEL_SEGMENT_DOWNLOADS,
        String.valueOf(initialPermits * 2));
    downloadThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
    Assert.assertEquals(downloadThrottler.totalPermits(), initialPermits * 2);

    Assert.assertEquals(downloadThrottler.availablePermits(), initialPermits);
    for (int i = 0; i < initialPermits; i++) {
      downloadThrottler.acquire();
    }
    Assert.assertEquals(downloadThrottler.totalPermits(), initialPermits * 2);
    Assert.assertEquals(downloadThrottler.availablePermits(), 0);
    for (int i = 0; i < (initialPermits * 2); i++) {
      downloadThrottler.release();
    }
    Assert.assertEquals(downloadThrottler.totalPermits(), initialPermits * 2);
    Assert.assertEquals(downloadThrottler.availablePermits(), initialPermits * 2);
  }

  @Test
  public void testDecreaseSegmentPreprocessParallelism()
      throws Exception {
    int initialPermits = 4;
    SegmentDownloadThrottler downloadThrottler = new SegmentDownloadThrottler(initialPermits, TABLE_NAME_WITH_TYPE);
    Assert.assertEquals(downloadThrottler.totalPermits(), initialPermits);

    for (int i = 0; i < initialPermits; i++) {
      downloadThrottler.acquire();
    }
    Assert.assertEquals(downloadThrottler.totalPermits(), initialPermits);
    Assert.assertEquals(downloadThrottler.availablePermits(), 0);

    // Increase the value of cluster config for max segment preprocess parallelism
    Map<String, String> updatedClusterConfigs = new HashMap<>();
    updatedClusterConfigs.put(CommonConstants.Server.CONFIG_OF_MAX_PARALLEL_SEGMENT_DOWNLOADS,
        String.valueOf(initialPermits / 2));
    downloadThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
    Assert.assertEquals(downloadThrottler.totalPermits(), initialPermits / 2);

    Assert.assertEquals(downloadThrottler.availablePermits(), -(initialPermits / 2));
    for (int i = 0; i < 4; i++) {
      downloadThrottler.release();
    }
    Assert.assertEquals(downloadThrottler.totalPermits(), initialPermits / 2);
    Assert.assertEquals(downloadThrottler.availablePermits(), initialPermits / 2);
  }

  @Test
  public void testThrowException()
      throws Exception {
    SegmentDownloadThrottler downloadThrottler = new SegmentDownloadThrottler(1, TABLE_NAME_WITH_TYPE);
    SegmentDownloadThrottler spy = spy(downloadThrottler);
    spy.acquire();
    Assert.assertEquals(spy.availablePermits(), 0);
    doThrow(new InterruptedException("interrupt")).when(spy).acquire();

    Assert.assertThrows(InterruptedException.class, spy::acquire);
    Assert.assertEquals(spy.availablePermits(), 0);
    spy.release();
    Assert.assertEquals(spy.availablePermits(), 1);
  }

  @Test
  public void testChangeConfigsEmpty() {
    int initialPermits = 4;
    SegmentDownloadThrottler downloadThrottler = new SegmentDownloadThrottler(initialPermits, TABLE_NAME_WITH_TYPE);
    Assert.assertEquals(downloadThrottler.totalPermits(), initialPermits);

    // Pass empty configs and keyset
    Map<String, String> updatedClusterConfigs = new HashMap<>();
    downloadThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
    Assert.assertEquals(downloadThrottler.totalPermits(), initialPermits);
  }

  @Test
  public void testChangeConfigDeletedConfigsEmpty() {
    int initialPermits = 4;
    SegmentDownloadThrottler downloadThrottler = new SegmentDownloadThrottler(initialPermits, TABLE_NAME_WITH_TYPE);
    Assert.assertEquals(downloadThrottler.totalPermits(), initialPermits);

    // Create a set of valid keys and pass clusterConfigs as null, the config should reset to the default
    Set<String> keys = new HashSet<>();
    keys.add(CommonConstants.Server.CONFIG_OF_MAX_PARALLEL_SEGMENT_DOWNLOADS);
    downloadThrottler.onChange(keys, null);
    Assert.assertEquals(downloadThrottler.totalPermits(),
        Integer.parseInt(CommonConstants.Server.DEFAULT_MAX_PARALLEL_SEGMENT_DOWNLOADS));
  }

  @Test
  public void testChangeConfigsOtherThanRelevant() {
    int initialPermits = 4;
    SegmentDownloadThrottler downloadThrottler = new SegmentDownloadThrottler(initialPermits, TABLE_NAME_WITH_TYPE);
    Assert.assertEquals(downloadThrottler.totalPermits(), initialPermits);

    // Add some random configs and call 'onChange'
    Map<String, String> updatedClusterConfigs = new HashMap<>();
    updatedClusterConfigs.put("random.config.key", "random.config.value");
    downloadThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
    Assert.assertEquals(downloadThrottler.totalPermits(), initialPermits);
  }

  @Test
  public void testChangeConfigs() {
    int initialPermits = 4;
    SegmentDownloadThrottler downloadThrottler = new SegmentDownloadThrottler(initialPermits, TABLE_NAME_WITH_TYPE);
    Assert.assertEquals(downloadThrottler.totalPermits(), initialPermits);

    // Add relevant and random configs and call 'onChange'
    Map<String, String> updatedClusterConfigs = new HashMap<>();
    // Random config
    updatedClusterConfigs.put("random.config.key", "random.config.value");
    // Config not relevant for download config
    updatedClusterConfigs.put(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM,
        String.valueOf(initialPermits * 2));
    // Relevant config
    updatedClusterConfigs.put(CommonConstants.Server.CONFIG_OF_MAX_PARALLEL_SEGMENT_DOWNLOADS,
        String.valueOf(initialPermits * 4));
    downloadThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
    Assert.assertEquals(downloadThrottler.totalPermits(), initialPermits * 4);
  }

  @Test
  public void testChangeConfigsTwoTables() {
    int initialPermits = 4;
    // Each table will have its own download throttler. For now they all take the same configuration as input
    SegmentDownloadThrottler downloadThrottler = new SegmentDownloadThrottler(initialPermits, TABLE_NAME_WITH_TYPE);
    SegmentDownloadThrottler downloadThrottlerTable2 = new SegmentDownloadThrottler(initialPermits, "test_REALTIME");
    Assert.assertEquals(downloadThrottler.totalPermits(), initialPermits);
    Assert.assertEquals(downloadThrottlerTable2.totalPermits(), initialPermits);

    // Add relevant and random configs and call 'onChange'
    Map<String, String> updatedClusterConfigs = new HashMap<>();
    // Random config
    updatedClusterConfigs.put("random.config.key", "random.config.value");
    // Config not relevant for download config
    updatedClusterConfigs.put(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM,
        String.valueOf(initialPermits * 2));
    // Relevant config
    updatedClusterConfigs.put(CommonConstants.Server.CONFIG_OF_MAX_PARALLEL_SEGMENT_DOWNLOADS,
        String.valueOf(initialPermits * 4));
    downloadThrottler.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);
    downloadThrottlerTable2.onChange(updatedClusterConfigs.keySet(), updatedClusterConfigs);

    Assert.assertEquals(downloadThrottler.totalPermits(), initialPermits * 4);
    Assert.assertEquals(downloadThrottlerTable2.totalPermits(), initialPermits * 4);
  }
}
