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
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;


public class PinotAdministratorIntegrationTest extends PinotAdministratorIntegrationTestBase {

  @Test
  public void testStartFromPinotAdministratorCLI()
      throws Exception {
    Process zookeeper = startZookeeper();
    Thread.sleep(1000L);

    startController();
    Thread.sleep(3000L);

    startServer();
    startBroker();
    Thread.sleep(3000L);

    createTable();
    generateData();
    convertData();
    uploadData();
    Thread.sleep(5000L);

    long timeoutThreshold = System.currentTimeMillis() + 30000L;
    int currentRecordCount = 0;
    int expectedRecordCount = Integer.parseInt(TOTAL_RECORD_COUNT);
    while (currentRecordCount != expectedRecordCount && System.currentTimeMillis() < timeoutThreshold) {
      Thread.sleep(1000L);
      currentRecordCount = countRecords();
    }
    Assert.assertEquals(currentRecordCount, expectedRecordCount, "All segments did not load within 120 seconds");
  }

  @AfterMethod
  public void tearDown() {
    for (Process process : _processes) {
      process.destroy();
    }
    FileUtils.deleteQuietly(new File(AVRO_DIR));
    FileUtils.deleteQuietly(new File(SEGMENT_DIR));
  }
}
