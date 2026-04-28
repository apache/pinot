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
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Verifies that the broker-side background cleanup scheduler automatically deletes expired cursor responses
 * without any manual DELETE API call. Uses a short expiration (3s) and aggressive cron frequency (2s) to
 * keep test runtime low.
 */
public class CursorCronCleanupIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final Logger LOGGER = LoggerFactory.getLogger(CursorCronCleanupIntegrationTest.class);
  private static final int NUM_OFFLINE_SEGMENTS = 8;
  private static final int COUNT_STAR_RESULT = 79003;
  private static final String SHARED_TABLE_NAME = "cursor_cron_cleanup";
  private static final String TEST_QUERY =
      "SELECT SUM(CAST(CAST(ArrTime AS varchar) AS LONG)) FROM %s WHERE DaysSinceEpoch <> 16312 AND Carrier = 'DL'";

  private File _classTempDir;
  private File _classSegmentDir;
  private File _classTarDir;

  @Override
  protected void overrideBrokerConf(PinotConfiguration configuration) {
    configuration.setProperty(
        CommonConstants.CursorConfigs.PREFIX_OF_CONFIG_OF_RESPONSE_STORE + ".type", "memory");
    configuration.setProperty(CommonConstants.CursorConfigs.RESULTS_EXPIRATION_INTERVAL, "3s");
    configuration.setProperty(CommonConstants.CursorConfigs.RESPONSE_STORE_CLEANER_FREQUENCY_PERIOD, "2s");
  }

  @Override
  protected long getCountStarResult() {
    return COUNT_STAR_RESULT;
  }

  @Override
  protected String getTableName() {
    return isSharedRichClusterEnabled() ? SHARED_TABLE_NAME : super.getTableName();
  }

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

    ClusterIntegrationTestUtils.buildSegmentsFromAvro(offlineAvroFiles, offlineTableConfig, schema, 0, _classSegmentDir,
        _classTarDir);
    uploadSegments(getTableName(), _classTarDir);

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

  protected Map<String, String> getHeaders() {
    return Collections.emptyMap();
  }

  protected String getBrokerGetAllResponseStoresApiUrl(String brokerBaseApiUrl) {
    return brokerBaseApiUrl + "/responseStore";
  }

  protected String getBrokerDeleteResponseStoresApiUrl(String brokerBaseApiUrl, String requestId) {
    return getBrokerGetAllResponseStoresApiUrl(brokerBaseApiUrl) + "/" + requestId;
  }

  private File getClassTempDir() {
    return isSharedRichClusterEnabled() ? new File(_tempDir, "testData") : _tempDir;
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

  @Test
  public void testCronCleanupDeletesExpiredResponses()
      throws Exception {
    deleteAllResponses();
    String brokerUrl = getBrokerBaseApiUrl();

    List<CursorResponseNative> before = JsonUtils.stringToObject(
        ClusterTest.sendGetRequest(getBrokerGetAllResponseStoresApiUrl(brokerUrl), getHeaders()),
        new TypeReference<>() {
        });
    int countBefore = before.size();

    // Submit a cursor query -- response will expire in ~3s
    ClusterTest.postQuery(String.format(TEST_QUERY, getTableName()),
        ClusterIntegrationTestUtils.getBrokerQueryApiUrl(brokerUrl, false) + "?getCursor=true&numRows=100000",
        getHeaders(), Collections.emptyMap());

    // Verify it was stored
    List<CursorResponseNative> afterCreate = JsonUtils.stringToObject(
        ClusterTest.sendGetRequest(getBrokerGetAllResponseStoresApiUrl(brokerUrl), getHeaders()),
        new TypeReference<>() {
        });
    Assert.assertEquals(afterCreate.size(), countBefore + 1, "Expected one new cursor response after query");

    // Wait for cron to clean it up automatically (no manual DELETE call).
    // Expiration=3s, frequency=2s, first run after ~2s + jitter (0..500ms), then every 2s.
    TestUtils.waitForCondition(aVoid -> {
      try {
        List<CursorResponseNative> remaining = JsonUtils.stringToObject(
            ClusterTest.sendGetRequest(getBrokerGetAllResponseStoresApiUrl(brokerUrl), getHeaders()),
            new TypeReference<>() {
            });
        return remaining.size() <= countBefore;
      } catch (Exception e) {
        LOGGER.warn("Error polling response store during cron cleanup wait", e);
        return false;
      }
    }, 1_000L, 15_000L, "Cron cleanup did not remove expired cursor response within timeout");
  }

  private void deleteAllResponses()
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

  private void cleanTempDirectory()
      throws Exception {
    if (_classTempDir != null) {
      FileUtils.deleteDirectory(_classTempDir);
    }
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
