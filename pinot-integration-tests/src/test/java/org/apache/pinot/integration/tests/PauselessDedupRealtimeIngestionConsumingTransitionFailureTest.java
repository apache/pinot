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

import org.apache.pinot.integration.tests.realtime.utils.FailureInjectingTableConfig;
import org.apache.pinot.integration.tests.realtime.utils.FailureInjectingTableDataManagerProvider;
import org.apache.pinot.server.starter.helix.HelixInstanceDataManagerConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;


public class PauselessDedupRealtimeIngestionConsumingTransitionFailureTest
    extends PauselessDedupRealtimeIngestionSegmentCommitFailureTest {
  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    serverConf.setProperty("pinot.server.instance.segment.store.uri", "file:" + _controllerConfig.getDataDir());
    serverConf.setProperty("pinot.server.instance." + HelixInstanceDataManagerConfig.UPLOAD_SEGMENT_TO_DEEP_STORE,
        "true");
    serverConf.setProperty("pinot.server.instance." + CommonConstants.Server.TABLE_DATA_MANAGER_PROVIDER_CLASS,
        "org.apache.pinot.integration.tests.realtime.utils.FailureInjectingTableDataManagerProvider");
    serverConf.setProperty("pinot.server.instance." + FailureInjectingTableDataManagerProvider.FAILURE_CONFIG_KEY + "."
        + getPauselessTableName(), new FailureInjectingTableConfig(false, true, getExpectedMaxFailures()).toJson());
  }

  @Override
  protected int getExpectedMaxFailures() {
    return 2;
  }
}
