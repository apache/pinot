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
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.StringUtil;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


@Test(groups = {"integration-suite-1"})
public class DedupIntegrationTest extends BaseClusterIntegrationTestSet {

  private List<File> _avroFiles;

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    // Start a customized controller with more frequent realtime segment validation
    startController();
    startBroker();
    startServers(1);

    _avroFiles = unpackAvroData(_tempDir);
    startKafka();
    pushAvroIntoKafka(_avroFiles);

    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createDedupTableConfig(_avroFiles.get(0), "id", getNumKafkaPartitions());
    addTableConfig(tableConfig);

    waitForAllDocsLoaded(600_000L);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    dropRealtimeTable(getTableName());
    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  @Override
  protected int getRealtimeSegmentFlushSize() {
    // Create > 1 segments
    return 2;
  }

  @Override
  protected String getSchemaFileName() {
    return "dedupIngestionTestSchema.schema";
  }

  @Override
  protected String getAvroTarFileName() {
    return "dedupIngestionTestData.tar.gz";
  }

  @Override
  protected String getPartitionColumn() {
    return "id";
  }

  @Override
  protected long getCountStarResult() {
    // Three distinct records are expected with pk values of 100000, 100001, 100002
    return 5;
  }

  @Test
  public void testValues()
      throws Exception {
    assertEquals(getCurrentCountStarResult(), getCountStarResult());

    // Validate the older value persist
    for (int i = 0; i < getCountStarResult(); i++) {
      Assert.assertEquals(getPinotConnection()
          .execute("SELECT name FROM " + getTableName() + " WHERE id = " + i)
          .getResultSet(0)
          .getString(0),
          "" + i);
    }
  }

  @Test
  public void testSegmentReload()
      throws Exception {
    ControllerTest.sendPostRequest(
        StringUtil.join("/", getControllerBaseApiUrl(), "segments", getTableName(),
            "reload?forceDownload=false"), null);

    // wait for reload to finish
    Thread.sleep(1000);

    // Push data again
    pushAvroIntoKafka(_avroFiles);

    // Validate no change
    assertEquals(getCurrentCountStarResult(), getCountStarResult());
    for (int i = 0; i < getCountStarResult(); i++) {
      Assert.assertEquals(getPinotConnection()
              .execute("SELECT name FROM " + getTableName() + " WHERE id = " + i)
              .getResultSet(0)
              .getString(0),
          "" + i);
    }
  }
}
