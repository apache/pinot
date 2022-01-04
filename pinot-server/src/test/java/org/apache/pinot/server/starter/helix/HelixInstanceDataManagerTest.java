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
package org.apache.pinot.server.starter.helix;

import java.io.File;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class HelixInstanceDataManagerTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "HelixInstanceDataManagerTest");

  @AfterMethod
  public void tearDown()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  @Test
  public void testReloadMutableSegment()
      throws ConfigurationException {
    // Provides required configs to init the instance data manager.
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty("id", "id01");
    config.setProperty("dataDir", TEMP_DIR.getAbsolutePath());
    config.setProperty("segmentTarDir", TEMP_DIR.getAbsolutePath());
    config.setProperty("readMode", "mmap");
    config.setProperty("reload.consumingSegment", "false");

    HelixInstanceDataManager mgr = new HelixInstanceDataManager();
    mgr.init(config, mock(HelixManager.class), mock(ServerMetrics.class));

    SegmentDataManager segMgr = mock(SegmentDataManager.class);

    when(segMgr.getSegment()).thenReturn(mock(ImmutableSegment.class));
    assertFalse(mgr.reloadMutableSegment("table01", "seg01", segMgr, null));

    when(segMgr.getSegment()).thenReturn(mock(MutableSegment.class));
    assertTrue(mgr.reloadMutableSegment("table01", "seg01", segMgr, null));
  }
}
