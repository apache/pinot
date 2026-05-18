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
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * Re-runs all cursor integration tests using {@code FsResponseStore} (local filesystem) instead of
 * {@code MemoryResponseStore}. This validates the single-pass {@code deleteExpiredResponses} override,
 * real PinotFS file I/O, JSON serde roundtrip, and brokerId filtering against actual metadata files.
 */
public class CursorFsIntegrationTest extends CursorIntegrationTest {

  @Override
  protected void overrideBrokerConf(PinotConfiguration configuration) {
    configuration.setProperty(
        CommonConstants.CursorConfigs.PREFIX_OF_CONFIG_OF_RESPONSE_STORE + ".type", "file");
    File responseStoreDir = new File(_tempDir, "responseStore");
    configuration.setProperty(
        CommonConstants.CursorConfigs.PREFIX_OF_CONFIG_OF_RESPONSE_STORE + ".file.data.dir",
        new File(responseStoreDir, "data").toURI().toString());
    configuration.setProperty(
        CommonConstants.CursorConfigs.PREFIX_OF_CONFIG_OF_RESPONSE_STORE + ".file.temp.dir",
        new File(responseStoreDir, "temp").getAbsolutePath());
  }

  @Override
  protected Object[][] getPageSizesAndQueryEngine() {
    return new Object[][]{
        {false, 1000}, {false, 0},
        {true, 1000}, {true, 0}
    };
  }
}
