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
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.helix.zookeeper.constant.ZkSystemPropertyKeys;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * This test is created to show the bug in Helix 0.9.8 that if a ZooKeeper IdealState is larger than 1MB after
 * compression, it cannot be updated anymore. Somehow this test can also make sure in future we will support
 * large IdealStates
 */
public class HelixZNodeSizeLimitTest extends SharedRichClusterIntegrationTest {
  private static final String SHARED_TABLE_NAME_PREFIX = "helix_znode_size_limit";
  private static final String LARGE_ZNODE_SIZE_LIMIT_BYTES = "4000000";

  private final String _sharedResourceSuffix = Long.toUnsignedString(RANDOM.nextLong(), Character.MAX_RADIX);
  private File _classTempDir;
  private String _previousJuteMaxBuffer;
  private String _previousZkSerializerWriteSizeLimit;

  @Override
  protected String getTableName() {
    return isSharedRichClusterEnabled() ? SHARED_TABLE_NAME_PREFIX + "_" + _sharedResourceSuffix
        : super.getTableName();
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    // This line of code has to be executed before the org.apache.helix.zookeeper.zkclient.ZkClient.WRITE_SIZE_LIMIT
    // is initialized. The code is in:
    // https://github.com/apache/helix/blob/master/zookeeper-api/
    // src/main/java/org/apache/helix/zookeeper/zkclient/ZkClient.java#L105
    // The below line gets executed before ZkClient.WRITE_SIZE_LIMIT is created
    _previousJuteMaxBuffer = System.getProperty(ZkSystemPropertyKeys.JUTE_MAXBUFFER);
    _previousZkSerializerWriteSizeLimit =
        System.getProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES);
    System.setProperty(ZkSystemPropertyKeys.JUTE_MAXBUFFER, LARGE_ZNODE_SIZE_LIMIT_BYTES);
    System.setProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES,
        LARGE_ZNODE_SIZE_LIMIT_BYTES);

    _classTempDir = getClassTempDir();
    TestUtils.ensureDirectoriesExistAndEmpty(_classTempDir);

    // Start Zookeeper and Pinot. In shared mode these attach to the shared suite-owned cluster.
    startZk();
    startController();
    startBroker();
    startServer();

    cleanTableAndSchema();

    addSchema(createSchema());
    addTableConfig(createOfflineTableConfig());
  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
    Exception exception = null;
    exception = runCleanup(exception, this::cleanTableAndSchema);
    exception = runCleanup(exception, this::stopServer);
    exception = runCleanup(exception, this::stopBroker);
    exception = runCleanup(exception, this::stopController);
    exception = runCleanup(exception, this::stopZk);
    exception = runCleanup(exception, this::cleanTempDirectory);
    exception = runCleanup(exception, this::restoreSystemProperties);
    if (exception != null) {
      throw exception;
    }
  }

  private File getClassTempDir() {
    return isSharedRichClusterEnabled() ? new File(_tempDir, "testData-" + _sharedResourceSuffix) : _tempDir;
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
    }
    waitForEVToDisappear(offlineTableName);
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

  private void restoreSystemProperties() {
    restoreSystemProperty(ZkSystemPropertyKeys.JUTE_MAXBUFFER, _previousJuteMaxBuffer);
    restoreSystemProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES,
        _previousZkSerializerWriteSizeLimit);
  }

  private void restoreSystemProperty(String propertyName, String previousValue) {
    if (previousValue == null) {
      System.clearProperty(propertyName);
    } else {
      System.setProperty(propertyName, previousValue);
    }
  }

  @Test
  public void testUpdateIdealState() {
    // In Helix 0.9.8, we get error logs like below:
    // 13:03:51.576 ERROR [ZkBaseDataAccessor] [main] Exception while setting path:
    // /HelixZNodeSizeLimitTest/IDEALSTATES/mytable_OFFLINE
    //  org.apache.helix.HelixException: Data size larger than 1M
    //  at org.apache.helix.manager.zk.zookeeper.ZkClient.checkDataSizeLimit(ZkClient.java:1513) ~[helix-core-0.9.8
    //  .jar:0.9.8]
    //  at org.apache.helix.manager.zk.zookeeper.ZkClient.writeDataReturnStat(ZkClient.java:1406) ~[helix-core-0.9.8
    //  .jar:0.9.8]
    String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
    // The updated IdealState after compression is roughly 2MB
    try {
      HelixHelper.updateIdealState(_helixManager, tableNameWithType, idealState -> {
        Map<String, Map<String, String>> currentAssignment = idealState.getRecord().getMapFields();
        for (int i = 0; i < 500_000; i++) {
          currentAssignment.put("segment_" + i,
              Map.of("Server_with_some_reasonable_long_prefix_" + (i % 10), "ONLINE"));
          currentAssignment.put("segment_" + i,
              Map.of("Server_with_some_reasonable_long_prefix_" + (i % 9), "ONLINE"));
        }
        return idealState;
      });
    } catch (Exception e) {
      Assert.fail("Exception shouldn't be thrown even if the data size of the ideal state is larger than 1M");
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
