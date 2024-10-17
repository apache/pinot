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


public class CursorFsIntegrationTest extends CursorIntegrationTest {
  @Override
  protected void overrideBrokerConf(PinotConfiguration configuration) {
    configuration.setProperty(CommonConstants.CursorConfigs.PREFIX_OF_CONFIG_OF_CURSOR + ".protocol", "file");
    File tmpPath = new File(_tempDir, "tmp");
    File dataPath = new File(_tempDir, "data");
    configuration.setProperty(CommonConstants.CursorConfigs.PREFIX_OF_CONFIG_OF_CURSOR + ".temp.dir", tmpPath);
    configuration.setProperty(
        CommonConstants.CursorConfigs.PREFIX_OF_CONFIG_OF_CURSOR + ".file.data.dir", "file://" + dataPath);
  }

  @Override
  protected Object[][] getPageSizesAndQueryEngine() {
    return new Object[][]{
        {false, 1000}, {false, 0}, // 0 triggers default behaviour
        {true, 1000}, {true, 0}, // 0 triggers default behaviour
    };
  }
}
