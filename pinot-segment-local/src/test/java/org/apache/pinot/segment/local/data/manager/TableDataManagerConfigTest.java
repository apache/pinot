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
package org.apache.pinot.segment.local.data.manager;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


public class TableDataManagerConfigTest {

  @Test
  public void testOverridePeerDownloadScheme() {
    // Use tableConfig's peer download scheme is present
    Configuration defaultConfig = new PropertiesConfiguration();
    defaultConfig.addProperty("peerDownloadScheme", "foo");
    InstanceDataManagerConfig instanceDataManagerConfig = mock(InstanceDataManagerConfig.class);
    TableDataManagerConfig finalConfig = new TableDataManagerConfig(defaultConfig, instanceDataManagerConfig);
    TableConfig tableConfig = mock(TableConfig.class);
    SegmentsValidationAndRetentionConfig segConfig = mock(SegmentsValidationAndRetentionConfig.class);
    when(tableConfig.getValidationConfig()).thenReturn(segConfig);
    when(segConfig.getPeerSegmentDownloadScheme()).thenReturn("http");
    finalConfig.overrideConfigs(tableConfig);
    assertEquals("http", finalConfig.getTablePeerDownloadScheme());

    // Use default value if tableConfig's peer download scheme is absent
    finalConfig = new TableDataManagerConfig(new PropertiesConfiguration(), instanceDataManagerConfig);
    when(segConfig.getPeerSegmentDownloadScheme()).thenReturn(null);
    finalConfig.overrideConfigs(tableConfig);
    assertNull(finalConfig.getTablePeerDownloadScheme());
  }
}
