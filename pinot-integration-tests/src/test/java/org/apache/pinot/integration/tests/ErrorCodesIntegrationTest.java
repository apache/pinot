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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;


public abstract class ErrorCodesIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final String SHARED_TABLE_NAME_PREFIX = "error_codes_";

  private File _classTempDir;
  private File _classSegmentDir;
  private File _classTarDir;

  @BeforeClass
  public void setUp()
      throws Exception {
    _classTempDir = getClassTempDir();
    _classSegmentDir = new File(_classTempDir, "segmentDir");
    _classTarDir = new File(_classTempDir, "tarDir");
    TestUtils.ensureDirectoriesExistAndEmpty(_classTempDir, _classSegmentDir, _classTarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(getHelixClusterName())
            .build();
    startBrokers(1);
    startServers(1);

    cleanTableAndSchema();

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_classTempDir);

    // Create and upload segments. For exhaustive testing, concurrently upload multiple segments with the same name
    // and validate correctness with parallel push protection enabled.
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _classSegmentDir,
        _classTarDir);
    // Create a copy of _tarDir to create multiple segments with the same name.
    File tarDir2 = new File(_classTempDir, "tarDir2");
    FileUtils.copyDirectory(_classTarDir, tarDir2);

    List<File> tarDirs = new ArrayList<>();
    tarDirs.add(_classTarDir);
    tarDirs.add(tarDir2);
    try {
      uploadSegments(getTableName(), TableType.OFFLINE, tarDirs);
    } catch (Exception e) {
      // If enableParallelPushProtection is enabled and the same segment is uploaded concurrently, we could get one
      // of the three exception:
      //   - 409 conflict of the second call enters ProcessExistingSegment
      //   - segmentZkMetadata creation failure if both calls entered ProcessNewSegment
      //   - Failed to copy segment tar file to final location due to the same segment pushed twice concurrently
      // In such cases we upload all the segments again to ensure that the data is set up correctly.
      assertTrue(e.getMessage().contains("Another segment upload is in progress for segment") || e.getMessage()
          .contains("Failed to create ZK metadata for segment") || e.getMessage()
          .contains("java.nio.file.FileAlreadyExistsException"), e.getMessage());
      uploadSegments(getTableName(), _classTarDir);
    }
    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
    Exception firstException = null;
    try {
      cleanTableAndSchema();
    } catch (Exception e) {
      firstException = e;
    }
    try {
      closeH2Connection();
    } catch (Exception e) {
      if (firstException != null) {
        firstException.addSuppressed(e);
      } else {
        firstException = e;
      }
    }
    try {
      // Shared rich cluster mode keeps services alive for the suite; these calls are no-ops when sharing is enabled.
      stopController();
      stopZk();
    } finally {
      FileUtils.deleteDirectory(_classTempDir);
    }
    if (firstException != null) {
      throw firstException;
    }
  }

  public abstract boolean useMultiStageQueryEngine();

  /**
   * If true, tests will query the controller instead of the broker.
   */
  public abstract boolean queryController();

  @Override
  protected String getTableName() {
    return isSharedRichClusterEnabled()
        ? SHARED_TABLE_NAME_PREFIX + getClass().getSimpleName().toLowerCase(Locale.ROOT)
        : super.getTableName();
  }

  @Override
  protected List<FieldConfig> getFieldConfigs() {
    return Collections.singletonList(
        new FieldConfig("DivAirports", FieldConfig.EncodingType.DICTIONARY, Collections.emptyList(),
            FieldConfig.CompressionCodec.MV_ENTRY_DICT, null));
  }

  @Test
  public void testParsingError()
      throws Exception {
    testQueryException("POTATO", QueryErrorCode.SQL_PARSING);
  }

  @Test
  public void testTableDoesNotExist()
      throws Exception {
    testQueryException("SELECT COUNT(*) FROM potato", QueryErrorCode.TABLE_DOES_NOT_EXIST);
  }

  @Test
  public void testFunctionDoesNotExist()
      throws Exception {
    testQueryException("SELECT POTATO(ArrTime) FROM " + getTableName(), QueryErrorCode.QUERY_VALIDATION);
  }

  @Test
  public void testInvalidCasting()
      throws Exception {
    // ArrTime expects a numeric type
    testQueryException("SELECT COUNT(*) FROM " + getTableName() + " where ArrTime = 'potato'",
        useMultiStageQueryEngine() ? QueryErrorCode.QUERY_PLANNING : QueryErrorCode.QUERY_VALIDATION);
  }

  private void testQueryException(@Language("sql") String query, QueryErrorCode errorCode)
      throws Exception {
    QueryAssert queryAssert;
    if (queryController()) {
      queryAssert = assertControllerQuery(query);
    } else {
      queryAssert = assertQuery(query);
    }
    queryAssert
        .firstException()
        .hasErrorCode(errorCode);
  }

  private File getClassTempDir() {
    if (!isSharedRichClusterEnabled()) {
      return _tempDir;
    }
    return new File(FileUtils.getTempDirectory(), ErrorCodesIntegrationTest.class.getSimpleName() + "-"
        + getClass().getSimpleName());
  }

  private void cleanTableAndSchema()
      throws Exception {
    if (_helixResourceManager == null) {
      return;
    }

    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
    if (_helixResourceManager.getAllTables().contains(offlineTableName) || _helixResourceManager.hasOfflineTable(
        getTableName())) {
      dropOfflineTable(getTableName());
      waitForTableDataManagerRemoved(offlineTableName);
      waitForEVToDisappear(offlineTableName);
    }
    if (_helixResourceManager.getSchema(getTableName()) != null) {
      deleteSchema(getTableName());
    }
  }

  private void closeH2Connection()
      throws SQLException {
    if (_h2Connection != null) {
      _h2Connection.close();
      _h2Connection = null;
    }
  }

  public static class MultiStageBrokerTestCase extends ErrorCodesIntegrationTest {
    @Override
    public boolean useMultiStageQueryEngine() {
      return true;
    }

    @Override
    public boolean queryController() {
      return false;
    }
  }

  public static class SingleStageBrokerTestCase extends ErrorCodesIntegrationTest {
    @Override
    public boolean useMultiStageQueryEngine() {
      return false;
    }

    @Override
    public boolean queryController() {
      return false;
    }
  }

  public static class MultiStageControllerTestCase extends ErrorCodesIntegrationTest {
    @Override
    public boolean useMultiStageQueryEngine() {
      return true;
    }

    @Override
    public boolean queryController() {
      return true;
    }
  }

  public static class SingleStageControllerTestCase extends ErrorCodesIntegrationTest {
    @Override
    public boolean useMultiStageQueryEngine() {
      return false;
    }

    @Override
    public boolean queryController() {
      return true;
    }
  }
}
