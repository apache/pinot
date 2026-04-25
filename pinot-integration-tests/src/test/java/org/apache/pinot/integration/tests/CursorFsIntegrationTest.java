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

import com.fasterxml.jackson.core.type.TypeReference;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.response.broker.CursorResponseNative;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;


/**
 * Re-runs all cursor integration tests using {@code FsResponseStore} (local filesystem) instead of
 * {@code MemoryResponseStore}. This validates the single-pass {@code deleteExpiredResponses} override,
 * real PinotFS file I/O, JSON serde roundtrip, and brokerId filtering against actual metadata files.
 */
public class CursorFsIntegrationTest extends CursorIntegrationTest {
  private static final int NUM_OFFLINE_SEGMENTS = 8;

  private File _classTempDir;
  private File _classSegmentDir;
  private File _classTarDir;

  @Override
  protected void overrideBrokerConf(PinotConfiguration configuration) {
    configuration.setProperty(
        CommonConstants.CursorConfigs.PREFIX_OF_CONFIG_OF_RESPONSE_STORE + ".type", "file");
    File responseStoreDir = getResponseStoreDir();
    configuration.setProperty(
        CommonConstants.CursorConfigs.PREFIX_OF_CONFIG_OF_RESPONSE_STORE + ".file.data.dir",
        new File(responseStoreDir, "data").toURI().toString());
    configuration.setProperty(
        CommonConstants.CursorConfigs.PREFIX_OF_CONFIG_OF_RESPONSE_STORE + ".file.temp.dir",
        new File(responseStoreDir, "temp").getAbsolutePath());
  }

  @Override
  @BeforeClass
  public void setUp()
      throws Exception {
    _classTempDir = getClassTempDir();
    _classSegmentDir = new File(_classTempDir, "segmentDir");
    _classTarDir = new File(_classTempDir, "tarDir");
    TestUtils.ensureDirectoriesExistAndEmpty(_classTempDir, _classSegmentDir, _classTarDir);

    startZk();
    startController();
    startBroker();
    startServer();

    cleanTableAndSchema();
    deleteAllResponses();

    List<File> avroFiles = getAllAvroFiles(_classTempDir);
    List<File> offlineAvroFiles = getOfflineAvroFiles(avroFiles, NUM_OFFLINE_SEGMENTS);

    Schema schema = createSchema();
    addSchema(schema);
    TableConfig offlineTableConfig = createOfflineTableConfig();
    addTableConfig(offlineTableConfig);

    ClusterIntegrationTestUtils.buildSegmentsFromAvro(offlineAvroFiles, offlineTableConfig, schema, 0,
        _classSegmentDir, _classTarDir);
    uploadSegments(getTableName(), _classTarDir);

    setUpQueryGenerator(avroFiles);

    waitForAllDocsLoaded(100_000L);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
    Exception exception = null;
    exception = runCleanup(exception, this::deleteAllResponses);
    exception = runCleanup(exception, this::cleanTableAndSchema);
    exception = runCleanup(exception, this::stopServer);
    exception = runCleanup(exception, this::stopBroker);
    exception = runCleanup(exception, this::stopController);
    exception = runCleanup(exception, this::stopZk);
    exception = runCleanup(exception, this::cleanTempDirectory);
    if (exception != null) {
      throw exception;
    }
  }

  @Override
  protected Object[][] getPageSizesAndQueryEngine() {
    return new Object[][]{
        {false, 1000}, {false, 0},
        {true, 1000}, {true, 0}
    };
  }

  @Override
  protected void deleteAllResponses()
      throws Exception {
    if (!isBrokerStarted()) {
      return;
    }

    List<CursorResponseNative> responses = JsonUtils.stringToObject(
        ClusterTest.sendGetRequest(getBrokerGetAllResponseStoresApiUrl(getBrokerBaseApiUrl()), getHeaders()),
        new TypeReference<>() {
        });
    for (CursorResponseNative response : responses) {
      try {
        ClusterTest.sendDeleteRequest(
            getBrokerDeleteResponseStoresApiUrl(getBrokerBaseApiUrl(), response.getRequestId()), getHeaders());
      } catch (IOException e) {
        if (!isNotFound(e)) {
          throw e;
        }
      }
    }
  }

  private File getClassTempDir() {
    return isSharedRichClusterEnabled() ? new File(_tempDir, "testData") : _tempDir;
  }

  private File getResponseStoreDir() {
    return isSharedRichClusterEnabled() ? new File(_tempDir, "CursorFsIntegrationTest-responseStore")
        : new File(_tempDir, "responseStore");
  }

  private List<File> getAllAvroFiles(File tempDir)
      throws Exception {
    int numSegments = unpackAvroData(tempDir).size();

    List<File> avroFiles = new ArrayList<>(numSegments);
    for (int i = 1; i <= numSegments; i++) {
      avroFiles.add(new File(tempDir, "On_Time_On_Time_Performance_2014_" + i + ".avro"));
    }
    return avroFiles;
  }

  private void cleanTableAndSchema()
      throws Exception {
    if (_helixResourceManager == null) {
      return;
    }

    String tableName = getTableName();
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    if (_helixResourceManager.getAllTables().contains(offlineTableName) || _helixResourceManager.hasOfflineTable(
        tableName)) {
      dropOfflineTable(tableName);
      waitForTableDataManagerRemoved(offlineTableName);
      waitForEVToDisappear(offlineTableName);
    }
    if (_helixResourceManager.getSchema(tableName) != null) {
      deleteSchema(tableName);
    }
  }

  private void cleanTempDirectory()
      throws Exception {
    if (_classTempDir != null) {
      FileUtils.deleteDirectory(_classTempDir);
    }
  }

  private boolean isBrokerStarted() {
    if (isSharedRichClusterEnabled()) {
      return _sharedRichClusterTestSuite != null && _sharedRichClusterTestSuite._brokerBaseApiUrl != null;
    }
    return _brokerBaseApiUrl != null;
  }

  private static boolean isNotFound(IOException e) {
    return e.getCause() instanceof HttpErrorStatusException
        && ((HttpErrorStatusException) e.getCause()).getStatusCode() == 404;
  }

  private Exception runCleanup(Exception firstException, Cleanup cleanup) {
    try {
      cleanup.run();
    } catch (Exception e) {
      if (firstException == null) {
        return e;
      }
      firstException.addSuppressed(e);
    }
    return firstException;
  }

  private interface Cleanup {
    void run()
        throws Exception;
  }
}
