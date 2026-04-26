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
package org.apache.pinot.plugin.minion.tasks;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.restlet.resources.ValidDocIdsBitmapResponse;
import org.apache.pinot.common.restlet.resources.ValidDocIdsType;
import org.apache.pinot.common.utils.RoaringBitmapUtils;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.minion.ClusterInfoAccessor;
import org.apache.pinot.controller.util.ServerSegmentMetadataReader;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.common.MinionConstants.UpsertCompactionTask;
import org.apache.pinot.minion.MinionContext;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.filesystem.LocalPinotFS;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.Enablement;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.MockedConstruction;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class MinionTaskUtilsTest {

  TableConfig _tableConfig =
      new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").setTimeColumnName("dateTime").build();

  public ClusterInfoAccessor getMockClusterInfo(String dataDir, String vipUrl) {
    ClusterInfoAccessor mockClusterInfo = mock(ClusterInfoAccessor.class);
    when(mockClusterInfo.getDataDir()).thenReturn(dataDir);
    when(mockClusterInfo.getVipUrlForLeadController(any())).thenReturn(vipUrl);
    return mockClusterInfo;
  }

  @Test
  public void testGetInputPinotFS()
      throws Exception {
    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put("input.fs.className", "org.apache.pinot.spi.filesystem.LocalPinotFS");
    URI fileURI = new URI("file:///path/to/file");

    PinotFS pinotFS = MinionTaskUtils.getInputPinotFS(taskConfigs, fileURI);

    assertTrue(pinotFS instanceof LocalPinotFS);
  }

  @Test
  public void testGetOutputPinotFS()
      throws Exception {
    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put("output.fs.className", "org.apache.pinot.spi.filesystem.LocalPinotFS");
    URI fileURI = new URI("file:///path/to/file");

    PinotFS pinotFS = MinionTaskUtils.getOutputPinotFS(taskConfigs, fileURI);

    assertTrue(pinotFS instanceof LocalPinotFS);
  }

  @Test
  public void testIsLocalOutputDir() {
    assertTrue(MinionTaskUtils.isLocalOutputDir("file"));
    assertFalse(MinionTaskUtils.isLocalOutputDir("hdfs"));
  }

  @Test
  public void testGetLocalPinotFs() {
    assertTrue(MinionTaskUtils.getLocalPinotFs() instanceof LocalPinotFS);
  }

  @Test
  public void testNormalizeDirectoryURI() {
    assertEquals("file:///path/to/dir/", MinionTaskUtils.normalizeDirectoryURI("file:///path/to/dir"));
    assertEquals("file:///path/to/dir/", MinionTaskUtils.normalizeDirectoryURI("file:///path/to/dir/"));
  }

  @Test
  public void testExtractMinionAllowDownloadFromServer() {
    Map<String, String> configs = new HashMap<>();
    TableTaskConfig tableTaskConfig = new TableTaskConfig(
        Collections.singletonMap(MinionConstants.MergeRollupTask.TASK_TYPE, configs));
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("sampleTable")
        .setTaskConfig(tableTaskConfig).build();

    // Test when the configuration is not set, should return the default value which is false
    assertFalse(MinionTaskUtils.extractMinionAllowDownloadFromServer(tableConfig,
        MinionConstants.MergeRollupTask.TASK_TYPE, false));

    // Test when the configuration is set to true
    configs.put(TableTaskConfig.MINION_ALLOW_DOWNLOAD_FROM_SERVER, "true");
    tableTaskConfig = new TableTaskConfig(Collections.singletonMap(MinionConstants.MergeRollupTask.TASK_TYPE, configs));
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("sampleTable")
        .setTaskConfig(tableTaskConfig).build();
    assertTrue(MinionTaskUtils.extractMinionAllowDownloadFromServer(tableConfig,
        MinionConstants.MergeRollupTask.TASK_TYPE, false));

    // Test when the configuration is set to false
    configs.put(TableTaskConfig.MINION_ALLOW_DOWNLOAD_FROM_SERVER, "false");
    tableTaskConfig = new TableTaskConfig(Collections.singletonMap(MinionConstants.MergeRollupTask.TASK_TYPE, configs));
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("sampleTable")
        .setTaskConfig(tableTaskConfig).build();
    assertFalse(MinionTaskUtils.extractMinionAllowDownloadFromServer(tableConfig,
        MinionConstants.MergeRollupTask.TASK_TYPE, false));
  }

  @Test
  public void testGetValidDocIdsTypeDefaultBehavior() {
    // Test default behavior scenarios
    UpsertConfig upsertConfig = new UpsertConfig();
    Map<String, String> taskConfigs = new HashMap<>();

    // Test 1: Default when delete is not enabled
    ValidDocIdsType result1 =
        MinionTaskUtils.getValidDocIdsType(upsertConfig, taskConfigs, UpsertCompactionTask.VALID_DOC_IDS_TYPE);
    assertEquals(result1, ValidDocIdsType.SNAPSHOT);

    // Test 2: Default when delete is enabled
    upsertConfig.setDeleteRecordColumn("deleted");
    ValidDocIdsType result2 =
        MinionTaskUtils.getValidDocIdsType(upsertConfig, taskConfigs, UpsertCompactionTask.VALID_DOC_IDS_TYPE);
    assertEquals(result2, ValidDocIdsType.SNAPSHOT_WITH_DELETE);
  }

  @Test
  public void testGetValidDocIdsTypeExplicitConfigurations() {
    // Test explicit configuration scenarios
    UpsertConfig upsertConfig = new UpsertConfig();
    Map<String, String> taskConfigs = new HashMap<>();

    // Test 1: Explicit SNAPSHOT with enabled snapshot
    upsertConfig.setSnapshot(Enablement.ENABLE);
    taskConfigs.put(UpsertCompactionTask.VALID_DOC_IDS_TYPE, "SNAPSHOT");
    ValidDocIdsType result1 =
        MinionTaskUtils.getValidDocIdsType(upsertConfig, taskConfigs, UpsertCompactionTask.VALID_DOC_IDS_TYPE);
    assertEquals(result1, ValidDocIdsType.SNAPSHOT);

    // Test 2: Explicit SNAPSHOT with default snapshot enablement
    upsertConfig = new UpsertConfig(); // Reset to default
    taskConfigs.put(UpsertCompactionTask.VALID_DOC_IDS_TYPE, "SNAPSHOT");
    ValidDocIdsType result2 =
        MinionTaskUtils.getValidDocIdsType(upsertConfig, taskConfigs, UpsertCompactionTask.VALID_DOC_IDS_TYPE);
    assertEquals(result2, ValidDocIdsType.SNAPSHOT);

    // Test 3: Explicit IN_MEMORY without delete
    taskConfigs.put(UpsertCompactionTask.VALID_DOC_IDS_TYPE, "IN_MEMORY");
    ValidDocIdsType result3 =
        MinionTaskUtils.getValidDocIdsType(upsertConfig, taskConfigs, UpsertCompactionTask.VALID_DOC_IDS_TYPE);
    assertEquals(result3, ValidDocIdsType.IN_MEMORY);

    // Test 4: Explicit IN_MEMORY_WITH_DELETE with delete column
    upsertConfig.setDeleteRecordColumn("deleted");
    taskConfigs.put(UpsertCompactionTask.VALID_DOC_IDS_TYPE, "IN_MEMORY_WITH_DELETE");
    ValidDocIdsType result4 =
        MinionTaskUtils.getValidDocIdsType(upsertConfig, taskConfigs, UpsertCompactionTask.VALID_DOC_IDS_TYPE);
    assertEquals(result4, ValidDocIdsType.IN_MEMORY_WITH_DELETE);

    // Test 5: Explicit SNAPSHOT_WITH_DELETE with delete column
    taskConfigs.put(UpsertCompactionTask.VALID_DOC_IDS_TYPE, "SNAPSHOT_WITH_DELETE");
    ValidDocIdsType result5 =
        MinionTaskUtils.getValidDocIdsType(upsertConfig, taskConfigs, UpsertCompactionTask.VALID_DOC_IDS_TYPE);
    assertEquals(result5, ValidDocIdsType.SNAPSHOT_WITH_DELETE);
  }

  @Test
  public void testGetValidDocIdsTypeBackwardCompatibilityAndOverride() {
    // Test backward compatibility behavior when delete is enabled
    UpsertConfig upsertConfig = new UpsertConfig();
    upsertConfig.setDeleteRecordColumn("deleted");
    Map<String, String> taskConfigs = new HashMap<>();

    // Test 1: SNAPSHOT gets overridden to SNAPSHOT_WITH_DELETE
    taskConfigs.put(UpsertCompactionTask.VALID_DOC_IDS_TYPE, "SNAPSHOT");
    ValidDocIdsType result1 =
        MinionTaskUtils.getValidDocIdsType(upsertConfig, taskConfigs, UpsertCompactionTask.VALID_DOC_IDS_TYPE);
    assertEquals(result1, ValidDocIdsType.SNAPSHOT_WITH_DELETE);

    // Test 2: IN_MEMORY gets overridden to SNAPSHOT_WITH_DELETE
    taskConfigs.put(UpsertCompactionTask.VALID_DOC_IDS_TYPE, "IN_MEMORY");
    ValidDocIdsType result2 =
        MinionTaskUtils.getValidDocIdsType(upsertConfig, taskConfigs, UpsertCompactionTask.VALID_DOC_IDS_TYPE);
    assertEquals(result2, ValidDocIdsType.SNAPSHOT_WITH_DELETE);

    // Test 3: SNAPSHOT_WITH_DELETE stays the same
    taskConfigs.put(UpsertCompactionTask.VALID_DOC_IDS_TYPE, "SNAPSHOT_WITH_DELETE");
    ValidDocIdsType result3 =
        MinionTaskUtils.getValidDocIdsType(upsertConfig, taskConfigs, UpsertCompactionTask.VALID_DOC_IDS_TYPE);
    assertEquals(result3, ValidDocIdsType.SNAPSHOT_WITH_DELETE);

    // Test 4: IN_MEMORY_WITH_DELETE stays the same (not overridden)
    taskConfigs.put(UpsertCompactionTask.VALID_DOC_IDS_TYPE, "IN_MEMORY_WITH_DELETE");
    ValidDocIdsType result4 =
        MinionTaskUtils.getValidDocIdsType(upsertConfig, taskConfigs, UpsertCompactionTask.VALID_DOC_IDS_TYPE);
    assertEquals(result4, ValidDocIdsType.IN_MEMORY_WITH_DELETE);

    // Test 5: Case insensitive override behavior
    taskConfigs.put(UpsertCompactionTask.VALID_DOC_IDS_TYPE, "in_memory_with_delete");
    ValidDocIdsType result5 =
        MinionTaskUtils.getValidDocIdsType(upsertConfig, taskConfigs, UpsertCompactionTask.VALID_DOC_IDS_TYPE);
    assertEquals(result5, ValidDocIdsType.IN_MEMORY_WITH_DELETE);
  }

  @Test
  public void testGetValidDocIdsTypeValidationErrors() {
    // Test validation error scenarios

    // Test 1: SNAPSHOT with disabled snapshot
    UpsertConfig upsertConfig1 = new UpsertConfig();
    upsertConfig1.setSnapshot(Enablement.DISABLE);
    Map<String, String> taskConfigs1 = new HashMap<>();
    taskConfigs1.put(UpsertCompactionTask.VALID_DOC_IDS_TYPE, "SNAPSHOT");
    IllegalStateException exception1 = expectThrows(
        "Expected IllegalStateException when using SNAPSHOT with snapshot disabled",
        IllegalStateException.class,
        () -> MinionTaskUtils.getValidDocIdsType(upsertConfig1, taskConfigs1, UpsertCompactionTask.VALID_DOC_IDS_TYPE));
    assertEquals(exception1.getMessage(), "'snapshot' must not be 'DISABLE' with validDocIdsType: SNAPSHOT");

    // Test 2: IN_MEMORY_WITH_DELETE without delete column
    UpsertConfig upsertConfig2 = new UpsertConfig();
    Map<String, String> taskConfigs2 = new HashMap<>();
    taskConfigs2.put(UpsertCompactionTask.VALID_DOC_IDS_TYPE, "IN_MEMORY_WITH_DELETE");
    IllegalStateException exception2 = expectThrows(
        "Expected IllegalStateException when using IN_MEMORY_WITH_DELETE without delete column",
        IllegalStateException.class,
        () -> MinionTaskUtils.getValidDocIdsType(upsertConfig2, taskConfigs2, UpsertCompactionTask.VALID_DOC_IDS_TYPE));
    assertEquals(exception2.getMessage(),
        "'deleteRecordColumn' must be provided with validDocIdsType: IN_MEMORY_WITH_DELETE");

    // Test 3: SNAPSHOT_WITH_DELETE without delete column
    UpsertConfig upsertConfig3 = new UpsertConfig();
    Map<String, String> taskConfigs3 = new HashMap<>();
    taskConfigs3.put(UpsertCompactionTask.VALID_DOC_IDS_TYPE, "SNAPSHOT_WITH_DELETE");
    IllegalStateException exception3 = expectThrows(
        "Expected IllegalStateException when using SNAPSHOT_WITH_DELETE without delete column",
        IllegalStateException.class,
        () -> MinionTaskUtils.getValidDocIdsType(upsertConfig3, taskConfigs3, UpsertCompactionTask.VALID_DOC_IDS_TYPE));
    assertEquals(exception3.getMessage(),
        "'deleteRecordColumn' must be provided with validDocIdsType: SNAPSHOT_WITH_DELETE");

    // Test 4: SNAPSHOT_WITH_DELETE with disabled snapshot
    UpsertConfig upsertConfig4 = new UpsertConfig();
    upsertConfig4.setDeleteRecordColumn("deleted");
    upsertConfig4.setSnapshot(Enablement.DISABLE);
    Map<String, String> taskConfigs4 = new HashMap<>();
    taskConfigs4.put(UpsertCompactionTask.VALID_DOC_IDS_TYPE, "SNAPSHOT_WITH_DELETE");
    IllegalStateException exception4 = expectThrows(
        "Expected IllegalStateException when using SNAPSHOT_WITH_DELETE with snapshot disabled",
        IllegalStateException.class,
        () -> MinionTaskUtils.getValidDocIdsType(upsertConfig4, taskConfigs4, UpsertCompactionTask.VALID_DOC_IDS_TYPE));
    assertEquals(exception4.getMessage(),
        "'snapshot' must not be 'DISABLE' with validDocIdsType: SNAPSHOT_WITH_DELETE");
  }

  @Test
  public void testParseValidDocIdsConsensusMode() {
    // Null or blank defaults to EQUAL
    assertEquals(MinionTaskUtils.parseValidDocIdsConsensusMode(null),
        MinionConstants.ValidDocIdsConsensusMode.EQUAL);
    assertEquals(MinionTaskUtils.parseValidDocIdsConsensusMode(""),
        MinionConstants.ValidDocIdsConsensusMode.EQUAL);
    assertEquals(MinionTaskUtils.parseValidDocIdsConsensusMode("   "),
        MinionConstants.ValidDocIdsConsensusMode.EQUAL);

    // UNSAFE
    assertEquals(MinionTaskUtils.parseValidDocIdsConsensusMode("UNSAFE"),
        MinionConstants.ValidDocIdsConsensusMode.UNSAFE);
    assertEquals(MinionTaskUtils.parseValidDocIdsConsensusMode("unsafe"),
        MinionConstants.ValidDocIdsConsensusMode.UNSAFE);

    // EQUAL
    assertEquals(MinionTaskUtils.parseValidDocIdsConsensusMode("EQUAL"),
        MinionConstants.ValidDocIdsConsensusMode.EQUAL);
    assertEquals(MinionTaskUtils.parseValidDocIdsConsensusMode("  EQUAL  "),
        MinionConstants.ValidDocIdsConsensusMode.EQUAL);

    // MOST_VALID_DOCS
    assertEquals(MinionTaskUtils.parseValidDocIdsConsensusMode("MOST_VALID_DOCS"),
        MinionConstants.ValidDocIdsConsensusMode.MOST_VALID_DOCS);
    assertEquals(MinionTaskUtils.parseValidDocIdsConsensusMode("most_valid_docs"),
        MinionConstants.ValidDocIdsConsensusMode.MOST_VALID_DOCS);

    // Invalid value throws
    expectThrows(IllegalArgumentException.class,
        () -> MinionTaskUtils.parseValidDocIdsConsensusMode("INVALID_MODE"));
  }

  /**
   * Builds a RoaringBitmap with {@code numDocs} valid doc ids (0..numDocs-1).
   */
  private static RoaringBitmap makeBitmap(int numDocs) {
    RoaringBitmap b = new RoaringBitmap();
    for (int i = 0; i < numDocs; i++) {
      b.add(i);
    }
    return b;
  }

  /**
   * Builds a ValidDocIdsBitmapResponse for testing: same segmentCrc and GOOD status.
   */
  private static ValidDocIdsBitmapResponse makeResponse(String segmentName, String crc, String instanceId,
      RoaringBitmap bitmap) {
    return new ValidDocIdsBitmapResponse(segmentName, crc, ValidDocIdsType.SNAPSHOT,
        RoaringBitmapUtils.serialize(bitmap), instanceId, ServiceStatus.Status.GOOD);
  }

  /**
   * Creates an InstanceConfig so that InstanceUtils.getServerAdminEndpoint() returns a valid URL.
   */
  private static InstanceConfig makeInstanceConfig(String instanceId) {
    InstanceConfig config = new InstanceConfig(instanceId);
    config.setHostName("localhost");
    config.getRecord().setIntField(Helix.Instance.ADMIN_PORT_KEY, 8098);
    return config;
  }

  /**
   * Sets up MinionContext with mock Helix so getServers() returns the given server list.
   */
  private void setupMinionContextWithServers(String tableNameWithType, String segmentName, String[] servers) {
    ExternalView externalView = new ExternalView(tableNameWithType);
    Map<String, String> assignment = new HashMap<>();
    for (String s : servers) {
      assignment.put(s, SegmentStateModel.ONLINE);
    }
    externalView.getRecord().getMapFields().put(segmentName, assignment);

    HelixAdmin helixAdmin = mock(HelixAdmin.class);
    when(helixAdmin.getResourceExternalView(anyString(), eq(tableNameWithType))).thenReturn(externalView);
    for (String server : servers) {
      when(helixAdmin.getInstanceConfig(anyString(), eq(server))).thenReturn(makeInstanceConfig(server));
    }

    HelixManager helixManager = mock(HelixManager.class);
    when(helixManager.getClusterName()).thenReturn("testCluster");
    when(helixManager.getClusterManagmentTool()).thenReturn(helixAdmin);

    MinionContext.getInstance().setHelixManager(helixManager);
  }

  /**
   * Calls getValidDocIdFromServerMatchingCrc with ServerSegmentMetadataReader mocked. Each invocation of
   * getValidDocIdsBitmapFromServer returns the next element of responseOrThrowByCallOrder; if it is an Exception,
   * that exception is thrown (simulating fetch failure).
   */
  private static RoaringBitmap getValidDocIdFromServerMatchingCrcWithMockedReader(String tableName,
      String segmentName, String expectedCrc, String consensusMode, List<Object> responseOrThrowByCallOrder,
      String[] servers, MinionTaskUtilsTest testInstance) {
    testInstance.setupMinionContextWithServers(tableName, segmentName, servers);
    // Shared across all mock instances (production creates one reader per server).
    AtomicInteger callIndex = new AtomicInteger(0);
    try (MockedConstruction<ServerSegmentMetadataReader> ignored = mockConstruction(ServerSegmentMetadataReader.class,
        (mock, context) -> {
          when(mock.getValidDocIdsBitmapFromServer(anyString(), anyString(), anyString(), anyString(), anyInt()))
              .thenAnswer(inv -> {
                int i = callIndex.getAndIncrement();
                if (i >= responseOrThrowByCallOrder.size()) {
                  throw new IllegalStateException("Mock received more calls than expected");
                }
                Object action = responseOrThrowByCallOrder.get(i);
                if (action instanceof Exception) {
                  throw (Exception) action;
                }
                return (ValidDocIdsBitmapResponse) action;
              });
        })) {
      return MinionTaskUtils.getValidDocIdFromServerMatchingCrc(tableName, segmentName,
          ValidDocIdsType.SNAPSHOT.name(), MinionContext.getInstance(), expectedCrc, consensusMode);
    }
  }

  @Test
  public void testSameValidDocsEqualConsensus() {
    String tableName = "myTable_REALTIME";
    String segmentName = "seg1";
    String expectedCrc = "crc1";
    List<Object> responses = List.of(
        makeResponse(segmentName, expectedCrc, "server1", makeBitmap(5)),
        makeResponse(segmentName, expectedCrc, "server2", makeBitmap(5)),
        makeResponse(segmentName, expectedCrc, "server3", makeBitmap(5)));
    RoaringBitmap result = getValidDocIdFromServerMatchingCrcWithMockedReader(tableName, segmentName, expectedCrc,
        "EQUAL", responses, new String[]{"server1", "server2", "server3"}, this);
    assertNotNull(result);
    assertEquals(result.getCardinality(), 5);
  }

  @Test
  public void testSameValidDocsMaxValidDocs() {
    String tableName = "myTable_REALTIME";
    String segmentName = "seg1";
    String expectedCrc = "crc1";
    List<Object> responses = List.of(
        makeResponse(segmentName, expectedCrc, "server1", makeBitmap(5)),
        makeResponse(segmentName, expectedCrc, "server2", makeBitmap(5)),
        makeResponse(segmentName, expectedCrc, "server3", makeBitmap(5)));
    RoaringBitmap result = getValidDocIdFromServerMatchingCrcWithMockedReader(tableName, segmentName, expectedCrc,
        "MOST_VALID_DOCS", responses, new String[]{"server1", "server2", "server3"}, this);
    assertNotNull(result);
    assertEquals(result.getCardinality(), 5);
  }

  @Test
  public void testSameValidDocsNone() {
    String tableName = "myTable_REALTIME";
    String segmentName = "seg1";
    String expectedCrc = "crc1";
    List<Object> responses = List.of(
        makeResponse(segmentName, expectedCrc, "server1", makeBitmap(5)),
        makeResponse(segmentName, expectedCrc, "server2", makeBitmap(5)),
        makeResponse(segmentName, expectedCrc, "server3", makeBitmap(5)));
    RoaringBitmap result = getValidDocIdFromServerMatchingCrcWithMockedReader(tableName, segmentName, expectedCrc,
        "UNSAFE", responses, new String[]{"server1", "server2", "server3"}, this);
    assertNotNull(result);
    assertEquals(result.getCardinality(), 5);
  }

  @Test
  public void testDifferentValidDocsMaxValidDocsMax() {
    String tableName = "myTable_REALTIME";
    String segmentName = "seg1";
    String expectedCrc = "crc1";
    List<Object> responses = List.of(
        makeResponse(segmentName, expectedCrc, "server1", makeBitmap(5)),
        makeResponse(segmentName, expectedCrc, "server2", makeBitmap(3)),
        makeResponse(segmentName, expectedCrc, "server3", makeBitmap(4)));
    RoaringBitmap result = getValidDocIdFromServerMatchingCrcWithMockedReader(tableName, segmentName, expectedCrc,
        "MOST_VALID_DOCS", responses, new String[]{"server1", "server2", "server3"}, this);
    assertNotNull(result);
    assertEquals(result.getCardinality(), 5);
  }

  @Test
  public void testsomeServersNoValidDocsEqualConsensus() {
    String tableName = "myTable_REALTIME";
    String segmentName = "seg1";
    String expectedCrc = "crc1";
    List<Object> responses = List.of(
        makeResponse(segmentName, expectedCrc, "server1", makeBitmap(0)),
        makeResponse(segmentName, expectedCrc, "server2", makeBitmap(0)),
        makeResponse(segmentName, expectedCrc, "server3", makeBitmap(3)));
    expectThrows(IllegalStateException.class,
        () -> getValidDocIdFromServerMatchingCrcWithMockedReader(tableName, segmentName, expectedCrc,
            "EQUAL", responses, new String[]{"server1", "server2", "server3"}, this));
  }

  @Test
  public void testsomeServersNoValidDocsMaxValidDocs() {
    String tableName = "myTable_REALTIME";
    String segmentName = "seg1";
    String expectedCrc = "crc1";
    List<Object> responses = List.of(
        makeResponse(segmentName, expectedCrc, "server1", makeBitmap(0)),
        makeResponse(segmentName, expectedCrc, "server2", makeBitmap(0)),
        makeResponse(segmentName, expectedCrc, "server3", makeBitmap(3)));
    RoaringBitmap result = getValidDocIdFromServerMatchingCrcWithMockedReader(tableName, segmentName, expectedCrc,
        "MOST_VALID_DOCS", responses, new String[]{"server1", "server2", "server3"}, this);
    assertNotNull(result);
    assertEquals(result.getCardinality(), 3);
  }

  @Test
  public void testSomeServersNoValidDocsNone() {
    String tableName = "myTable_REALTIME";
    String segmentName = "seg1";
    String expectedCrc = "crc1";
    List<Object> responses = List.of(
        makeResponse(segmentName, expectedCrc, "server1", makeBitmap(0)),
        makeResponse(segmentName, expectedCrc, "server2", makeBitmap(0)),
        makeResponse(segmentName, expectedCrc, "server3", makeBitmap(3)));
    RoaringBitmap result = getValidDocIdFromServerMatchingCrcWithMockedReader(tableName, segmentName, expectedCrc,
        "UNSAFE", responses, new String[]{"server1", "server2", "server3"}, this);
    assertNotNull(result);
    assertEquals(result.getCardinality(), 0);
  }

  // --- one server fails (returns null): EQUAL throws; others skip and use remaining ---

  @Test
  public void testOneServerFailsEqualConsensus() {
    String tableName = "myTable_REALTIME";
    String segmentName = "seg1";
    String expectedCrc = "crc1";
    List<Object> responses = List.of(
        makeResponse(segmentName, expectedCrc, "server1", makeBitmap(5)),
        new RuntimeException("simulated fetch failure"),
        makeResponse(segmentName, expectedCrc, "server3", makeBitmap(5)));
    expectThrows(IllegalStateException.class,
        () -> getValidDocIdFromServerMatchingCrcWithMockedReader(tableName, segmentName, expectedCrc,
            "EQUAL", responses, new String[]{"server1", "server2", "server3"}, this));
  }

  @Test
  public void testOneServerFailsNone() {
    String tableName = "myTable_REALTIME";
    String segmentName = "seg1";
    String expectedCrc = "crc1";
    List<Object> responses = List.of(
        new RuntimeException("simulated fetch failure"),
        makeResponse(segmentName, expectedCrc, "server2", makeBitmap(3)),
        makeResponse(segmentName, expectedCrc, "server3", makeBitmap(5)));
    RoaringBitmap result = getValidDocIdFromServerMatchingCrcWithMockedReader(tableName, segmentName, expectedCrc,
        "UNSAFE", responses, new String[]{"server1", "server2", "server3"}, this);
    assertNotNull(result);
    assertEquals(result.getCardinality(), 3);
  }

  @Test
  public void testAllServersFailMostValidDocs() {
    String tableName = "myTable_REALTIME";
    String segmentName = "seg1";
    String expectedCrc = "crc1";
    List<Object> responses = List.of(new RuntimeException("simulated"), new RuntimeException("simulated"),
        new RuntimeException("simulated"));
    expectThrows(IllegalStateException.class,
        () -> getValidDocIdFromServerMatchingCrcWithMockedReader(tableName, segmentName, expectedCrc, "MOST_VALID_DOCS",
            responses, new String[]{"server1", "server2", "server3"}, this));
  }

  @Test
  public void testGetPushTaskConfigNoConfig() {
    Map<String, String> taskConfig = new HashMap<>();
    Map<String, String> pushTaskConfigs = MinionTaskUtils.getPushTaskConfig(_tableConfig.getTableName(), taskConfig,
        getMockClusterInfo("/data/dir", "http://localhost:9000"));
    assertEquals(pushTaskConfigs.size(), 2);
    assertEquals(pushTaskConfigs.get(BatchConfigProperties.PUSH_MODE),
        BatchConfigProperties.SegmentPushType.TAR.toString());
  }

  @Test
  public void testGetPushTaskConfigURIPushMode() {
    Map<String, String> taskConfig = new HashMap<>();
    taskConfig.put(BatchConfigProperties.PUSH_MODE, BatchConfigProperties.SegmentPushType.URI.toString());
    Map<String, String> pushTaskConfigs = MinionTaskUtils.getPushTaskConfig(_tableConfig.getTableName(), taskConfig,
        getMockClusterInfo("/data/dir", "http://localhost:9000"));
    assertEquals(pushTaskConfigs.size(), 2);
    assertEquals(pushTaskConfigs.get(BatchConfigProperties.PUSH_MODE),
        BatchConfigProperties.SegmentPushType.TAR.toString());
    assertEquals(pushTaskConfigs.get(BatchConfigProperties.PUSH_CONTROLLER_URI), "http://localhost:9000");
  }

  @Test
  public void testGetPushTaskConfigURIPushModeDeepStoreControllerInfo() {
    Map<String, String> taskConfig = new HashMap<>();
    taskConfig.put(BatchConfigProperties.PUSH_MODE, BatchConfigProperties.SegmentPushType.URI.toString());
    Map<String, String> pushTaskConfigs = MinionTaskUtils.getPushTaskConfig(_tableConfig.getTableName(), taskConfig,
        getMockClusterInfo("hdfs://data/dir", "http://localhost:9000"));
    assertEquals(pushTaskConfigs.size(), 3);
    assertEquals(pushTaskConfigs.get(BatchConfigProperties.PUSH_MODE),
        BatchConfigProperties.SegmentPushType.METADATA.toString());
    assertEquals(pushTaskConfigs.get(BatchConfigProperties.OUTPUT_SEGMENT_DIR_URI), "hdfs://data/dir/myTable");
    assertEquals(pushTaskConfigs.get(BatchConfigProperties.PUSH_CONTROLLER_URI), "http://localhost:9000");
  }

  @Test
  public void testGetPushTaskConfigMETADATAPushModeDeepStoreControllerInfo() {
    Map<String, String> taskConfig = new HashMap<>();
    taskConfig.put(BatchConfigProperties.PUSH_MODE, BatchConfigProperties.SegmentPushType.METADATA.toString());
    ClusterInfoAccessor mockClusterInfo = getMockClusterInfo("hdfs://data/dir", "http://localhost:9000");
    Map<String, String> pushTaskConfigs =
        MinionTaskUtils.getPushTaskConfig(_tableConfig.getTableName(), taskConfig, mockClusterInfo);
    assertEquals(pushTaskConfigs.get(BatchConfigProperties.OUTPUT_SEGMENT_DIR_URI), "hdfs://data/dir/myTable");
    assertEquals(pushTaskConfigs.get(BatchConfigProperties.PUSH_MODE),
        BatchConfigProperties.SegmentPushType.METADATA.toString());
    assertEquals(pushTaskConfigs.size(), 3);
  }

  @Test
  public void testGetPushTaskConfigMETADATAPushModeDeepStoreOutputUriTaskConfig() {
    Map<String, String> taskConfig = new HashMap<>();
    taskConfig.put(BatchConfigProperties.PUSH_MODE, BatchConfigProperties.SegmentPushType.METADATA.toString());
    taskConfig.put(BatchConfigProperties.OUTPUT_SEGMENT_DIR_URI, "hdfs://data/dir/myTable");
    Map<String, String> pushTaskConfigs = MinionTaskUtils.getPushTaskConfig(_tableConfig.getTableName(), taskConfig,
        getMockClusterInfo("/data/dir", "http://localhost:9000"));

    assertEquals(pushTaskConfigs.get(BatchConfigProperties.OUTPUT_SEGMENT_DIR_URI), "hdfs://data/dir/myTable");
    assertEquals(pushTaskConfigs.get(BatchConfigProperties.PUSH_MODE),
        BatchConfigProperties.SegmentPushType.METADATA.toString());
    assertEquals(pushTaskConfigs.get(BatchConfigProperties.PUSH_CONTROLLER_URI), "http://localhost:9000");
    assertEquals(pushTaskConfigs.size(), 3);
  }

  @Test
  public void testGetPushTaskConfigTARPushMode() {
    Map<String, String> taskConfig = new HashMap<>();
    taskConfig.put(BatchConfigProperties.PUSH_MODE, BatchConfigProperties.SegmentPushType.TAR.toString());
    Map<String, String> pushTaskConfigs = MinionTaskUtils.getPushTaskConfig(_tableConfig.getTableName(), taskConfig,
        getMockClusterInfo("/data/dir", "http://localhost:9000"));

    assertEquals(pushTaskConfigs.size(), 2);
    assertEquals(pushTaskConfigs.get(BatchConfigProperties.PUSH_MODE),
        BatchConfigProperties.SegmentPushType.TAR.toString());
    assertEquals(pushTaskConfigs.get(BatchConfigProperties.PUSH_CONTROLLER_URI), "http://localhost:9000");
  }

  @Test
  public void testGetPushTaskConfigMETADATAPushModeWithLocalOutputDir() {
    Map<String, String> taskConfig = new HashMap<>();
    taskConfig.put(BatchConfigProperties.PUSH_MODE, BatchConfigProperties.SegmentPushType.METADATA.toString());
    taskConfig.put(MinionTaskUtils.ALLOW_METADATA_PUSH_WITH_LOCAL_FS, "true");
    Map<String, String> pushTaskConfigs = MinionTaskUtils.getPushTaskConfig(_tableConfig.getTableName(), taskConfig,
        getMockClusterInfo("/data/dir", "http://localhost:9000"));

    assertEquals(pushTaskConfigs.get(BatchConfigProperties.OUTPUT_SEGMENT_DIR_URI), "/data/dir/myTable");
    assertEquals(pushTaskConfigs.get(BatchConfigProperties.PUSH_MODE),
        BatchConfigProperties.SegmentPushType.METADATA.toString());
    assertEquals(pushTaskConfigs.get(BatchConfigProperties.PUSH_CONTROLLER_URI), "http://localhost:9000");
    assertEquals(pushTaskConfigs.size(), 4);
  }

  private static SegmentZKMetadata makeSegmentWithEndTimeMs(String name, long endTimeMs) {
    SegmentZKMetadata segment = new SegmentZKMetadata(name);
    segment.setEndTime(endTimeMs);
    segment.setTimeUnit(TimeUnit.MILLISECONDS);
    return segment;
  }

  private static SegmentZKMetadata makeSegmentWithCreationTime(String name, long endTimeMs, long creationTimeMs) {
    SegmentZKMetadata segment = new SegmentZKMetadata(name);
    segment.setEndTime(endTimeMs);
    segment.setTimeUnit(TimeUnit.MILLISECONDS);
    segment.setCreationTime(creationTimeMs);
    return segment;
  }

  @Test
  public void testFilterSegmentsPastRetentionExcludesExpiredSegments() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testTable")
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("7")
        .build();

    long nowMs = System.currentTimeMillis();
    long oneDayMs = 86_400_000L;

    SegmentZKMetadata recentSegment = makeSegmentWithEndTimeMs("segment_recent", nowMs - 2 * oneDayMs);
    SegmentZKMetadata expiredSegment = makeSegmentWithEndTimeMs("segment_expired", nowMs - 10 * oneDayMs);

    List<SegmentZKMetadata> segments = new ArrayList<>(List.of(recentSegment, expiredSegment));
    List<SegmentZKMetadata> filtered =
        MinionTaskUtils.filterSegmentsPastRetention(segments, tableConfig, null, nowMs, false);

    assertEquals(filtered.size(), 1);
    assertEquals(filtered.get(0).getSegmentName(), "segment_recent");
  }

  @Test
  public void testFilterSegmentsPastRetentionReturnsAllWhenNoRetention() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testTable")
        .build();

    long nowMs = System.currentTimeMillis();

    SegmentZKMetadata segment1 = makeSegmentWithEndTimeMs("segment_1", nowMs - 365L * 86_400_000L);
    SegmentZKMetadata segment2 = makeSegmentWithEndTimeMs("segment_2", nowMs - 86_400_000L);

    List<SegmentZKMetadata> segments = new ArrayList<>(List.of(segment1, segment2));
    List<SegmentZKMetadata> filtered =
        MinionTaskUtils.filterSegmentsPastRetention(segments, tableConfig, null, nowMs, false);

    assertEquals(filtered.size(), 2);
  }

  @Test
  public void testFilterSegmentsPastRetentionKeepsSegmentsWithInvalidEndTime() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testTable")
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("7")
        .build();

    SegmentZKMetadata invalidTimeSegment = new SegmentZKMetadata("segment_invalid_time");
    invalidTimeSegment.setEndTime(-1);
    invalidTimeSegment.setTimeUnit(TimeUnit.MILLISECONDS);

    List<SegmentZKMetadata> segments = new ArrayList<>(List.of(invalidTimeSegment));
    List<SegmentZKMetadata> filtered =
        MinionTaskUtils.filterSegmentsPastRetention(segments, tableConfig, null, System.currentTimeMillis(), false);

    assertEquals(filtered.size(), 1);
    assertEquals(filtered.get(0).getSegmentName(), "segment_invalid_time");
  }

  @Test
  public void testFilterSegmentsPastRetentionMalformedRetentionConfigReturnsAll() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testTable")
        .setRetentionTimeUnit("INVALID_UNIT")
        .setRetentionTimeValue("7")
        .build();

    long nowMs = System.currentTimeMillis();
    SegmentZKMetadata segment = makeSegmentWithEndTimeMs("segment_1", nowMs - 365L * 86_400_000L);

    List<SegmentZKMetadata> segments = new ArrayList<>(List.of(segment));
    List<SegmentZKMetadata> filtered =
        MinionTaskUtils.filterSegmentsPastRetention(segments, tableConfig, null, nowMs, false);

    assertEquals(filtered.size(), 1, "Malformed unit should fall through to catch block and return original list");
  }

  @Test
  public void testFilterSegmentsPastRetentionMalformedRetentionValueReturnsAll() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testTable")
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("abc")
        .build();

    long nowMs = System.currentTimeMillis();
    SegmentZKMetadata segment = makeSegmentWithEndTimeMs("segment_1", nowMs - 365L * 86_400_000L);

    List<SegmentZKMetadata> segments = new ArrayList<>(List.of(segment));
    List<SegmentZKMetadata> filtered =
        MinionTaskUtils.filterSegmentsPastRetention(segments, tableConfig, null, nowMs, false);

    assertEquals(filtered.size(), 1, "Malformed value should fall through to catch block and return original list");
  }

  @Test
  public void testFilterSegmentsPastRetentionZeroRetentionReturnsAll() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testTable")
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("0")
        .build();

    long nowMs = System.currentTimeMillis();
    SegmentZKMetadata segment = makeSegmentWithEndTimeMs("segment_1", nowMs - 365L * 86_400_000L);

    List<SegmentZKMetadata> segments = new ArrayList<>(List.of(segment));
    List<SegmentZKMetadata> filtered =
        MinionTaskUtils.filterSegmentsPastRetention(segments, tableConfig, null, nowMs, false);

    assertEquals(filtered.size(), 1, "Zero retention should return original list unchanged");
  }

  @Test
  public void testFilterSegmentsPastRetentionNegativeRetentionReturnsAll() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testTable")
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("-1")
        .build();

    long nowMs = System.currentTimeMillis();
    SegmentZKMetadata segment = makeSegmentWithEndTimeMs("segment_1", nowMs - 365L * 86_400_000L);

    List<SegmentZKMetadata> segments = new ArrayList<>(List.of(segment));
    List<SegmentZKMetadata> filtered =
        MinionTaskUtils.filterSegmentsPastRetention(segments, tableConfig, null, nowMs, false);

    assertEquals(filtered.size(), 1, "Negative retention should return original list unchanged");
  }

  @Test
  public void testFilterSegmentsPastRetentionExactBoundaryIsKept() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testTable")
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("7")
        .build();

    long nowMs = System.currentTimeMillis();
    long sevenDaysMs = 7L * 86_400_000L;

    // Exact boundary: endTime is exactly 7 days ago, so (now - endTime) == retentionMs. The filter uses
    // strict greater-than, so this segment should be kept.
    SegmentZKMetadata boundarySegment = makeSegmentWithEndTimeMs("segment_boundary", nowMs - sevenDaysMs);
    // 1ms past boundary: (now - endTime) > retentionMs, so this should be excluded.
    SegmentZKMetadata justExpiredSegment = makeSegmentWithEndTimeMs("segment_just_expired", nowMs - sevenDaysMs - 1);

    List<SegmentZKMetadata> segments = new ArrayList<>(List.of(boundarySegment, justExpiredSegment));
    List<SegmentZKMetadata> filtered =
        MinionTaskUtils.filterSegmentsPastRetention(segments, tableConfig, null, nowMs, false);

    assertEquals(filtered.size(), 1);
    assertEquals(filtered.get(0).getSegmentName(), "segment_boundary",
        "Segment at exact retention boundary should be kept (strict greater-than comparison)");
  }

  @Test
  public void testFilterSegmentsPastRetentionWithBuffer() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testTable")
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("7")
        .build();

    long nowMs = System.currentTimeMillis();
    long oneDayMs = 86_400_000L;

    // 5 days old — within 7d retention but outside effective retention of (7d - 3d = 4d)
    SegmentZKMetadata borderlineSegment = makeSegmentWithEndTimeMs("segment_borderline", nowMs - 5 * oneDayMs);
    // 2 days old — within both retention and effective retention
    SegmentZKMetadata recentSegment = makeSegmentWithEndTimeMs("segment_recent", nowMs - 2 * oneDayMs);

    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put(MinionTaskUtils.RETENTION_EXPIRY_BUFFER_PERIOD_KEY, "3d");

    List<SegmentZKMetadata> segments = new ArrayList<>(List.of(borderlineSegment, recentSegment));
    List<SegmentZKMetadata> filtered =
        MinionTaskUtils.filterSegmentsPastRetention(segments, tableConfig, taskConfigs, nowMs, false);

    assertEquals(filtered.size(), 1);
    assertEquals(filtered.get(0).getSegmentName(), "segment_recent",
        "With 3d buffer, effective retention is 4d — 5-day-old segment should be excluded");
  }

  @Test
  public void testFilterSegmentsPastRetentionBufferExceedsRetentionReturnsAll() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testTable")
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("7")
        .build();

    long nowMs = System.currentTimeMillis();
    SegmentZKMetadata segment = makeSegmentWithEndTimeMs("segment_1", nowMs - 365L * 86_400_000L);

    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put(MinionTaskUtils.RETENTION_EXPIRY_BUFFER_PERIOD_KEY, "10d");

    List<SegmentZKMetadata> segments = new ArrayList<>(List.of(segment));
    List<SegmentZKMetadata> filtered =
        MinionTaskUtils.filterSegmentsPastRetention(segments, tableConfig, taskConfigs, nowMs, false);

    assertEquals(filtered.size(), 1,
        "Buffer exceeding retention should fail-open and return all segments");
  }

  @Test
  public void testFilterSegmentsPastRetentionNullTaskConfigsNoBuffer() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testTable")
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("7")
        .build();

    long nowMs = System.currentTimeMillis();
    SegmentZKMetadata expired = makeSegmentWithEndTimeMs("segment_expired", nowMs - 10L * 86_400_000L);

    List<SegmentZKMetadata> segments = new ArrayList<>(List.of(expired));
    List<SegmentZKMetadata> filtered =
        MinionTaskUtils.filterSegmentsPastRetention(segments, tableConfig, null, nowMs, false);

    assertEquals(filtered.size(), 0, "Null taskConfigs means no buffer — expired segment should be filtered");
  }

  @Test
  public void testFilterSegmentsPastRetentionMalformedBufferReturnsZeroBuffer() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testTable")
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("7")
        .build();

    long nowMs = System.currentTimeMillis();
    long oneDayMs = 86_400_000L;
    SegmentZKMetadata expired = makeSegmentWithEndTimeMs("segment_expired", nowMs - 10 * oneDayMs);
    SegmentZKMetadata recent = makeSegmentWithEndTimeMs("segment_recent", nowMs - 2 * oneDayMs);

    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put(MinionTaskUtils.RETENTION_EXPIRY_BUFFER_PERIOD_KEY, "invalid_period");

    List<SegmentZKMetadata> segments = new ArrayList<>(List.of(expired, recent));
    List<SegmentZKMetadata> filtered =
        MinionTaskUtils.filterSegmentsPastRetention(segments, tableConfig, taskConfigs, nowMs, false);

    assertEquals(filtered.size(), 1,
        "Malformed buffer should fall back to 0 — expired segment still filtered by raw retention");
    assertEquals(filtered.get(0).getSegmentName(), "segment_recent");
  }

  @Test
  public void testFilterSegmentsPastRetentionEmptyTaskConfigsNoBuffer() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testTable")
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("7")
        .build();

    long nowMs = System.currentTimeMillis();
    SegmentZKMetadata expired = makeSegmentWithEndTimeMs("segment_expired", nowMs - 10L * 86_400_000L);

    List<SegmentZKMetadata> segments = new ArrayList<>(List.of(expired));
    List<SegmentZKMetadata> filtered =
        MinionTaskUtils.filterSegmentsPastRetention(segments, tableConfig, new HashMap<>(), nowMs, false);

    assertEquals(filtered.size(), 0,
        "Empty taskConfigs (no buffer key) means no buffer — expired segment should be filtered");
  }

  // --- isCreationTimeFallbackEnabled tests ---

  @Test
  public void testIsCreationTimeFallbackEnabledReturnsDefaultWhenKeyAbsent() {
    ClusterInfoAccessor mockAccessor = mock(ClusterInfoAccessor.class);
    when(mockAccessor.getClusterConfig(
        ControllerConf.ControllerPeriodicTasksConf.ENABLE_RETENTION_CREATION_TIME_FALLBACK)).thenReturn(null);
    assertFalse(MinionTaskUtils.isCreationTimeFallbackEnabled(mockAccessor));
  }

  @Test
  public void testIsCreationTimeFallbackEnabledReturnsTrueWhenSet() {
    ClusterInfoAccessor mockAccessor = mock(ClusterInfoAccessor.class);
    when(mockAccessor.getClusterConfig(
        ControllerConf.ControllerPeriodicTasksConf.ENABLE_RETENTION_CREATION_TIME_FALLBACK)).thenReturn("true");
    assertTrue(MinionTaskUtils.isCreationTimeFallbackEnabled(mockAccessor));
  }

  @Test
  public void testIsCreationTimeFallbackEnabledReturnsFalseWhenExplicitlyDisabled() {
    ClusterInfoAccessor mockAccessor = mock(ClusterInfoAccessor.class);
    when(mockAccessor.getClusterConfig(
        ControllerConf.ControllerPeriodicTasksConf.ENABLE_RETENTION_CREATION_TIME_FALLBACK)).thenReturn("false");
    assertFalse(MinionTaskUtils.isCreationTimeFallbackEnabled(mockAccessor));
  }

  @Test
  public void testIsCreationTimeFallbackEnabledNonBooleanStringReturnsFalse() {
    ClusterInfoAccessor mockAccessor = mock(ClusterInfoAccessor.class);
    when(mockAccessor.getClusterConfig(
        ControllerConf.ControllerPeriodicTasksConf.ENABLE_RETENTION_CREATION_TIME_FALLBACK)).thenReturn("abc");
    assertFalse(MinionTaskUtils.isCreationTimeFallbackEnabled(mockAccessor));
  }

  // --- filterSegmentsPastRetention with creation-time fallback tests ---

  @Test
  public void testFilterSegmentsPastRetentionFallbackEnabledFiltersInvalidEndTimeOldCreationTime() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testTable")
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("7")
        .build();

    long nowMs = System.currentTimeMillis();
    long oneDayMs = 86_400_000L;

    SegmentZKMetadata invalidEndTimeOldCreation =
        makeSegmentWithCreationTime("segment_invalid_old", -1, nowMs - 10 * oneDayMs);
    SegmentZKMetadata recent = makeSegmentWithEndTimeMs("segment_recent", nowMs - 2 * oneDayMs);

    List<SegmentZKMetadata> segments = new ArrayList<>(List.of(invalidEndTimeOldCreation, recent));
    List<SegmentZKMetadata> filtered =
        MinionTaskUtils.filterSegmentsPastRetention(segments, tableConfig, null, nowMs, true);

    assertEquals(filtered.size(), 1);
    assertEquals(filtered.get(0).getSegmentName(), "segment_recent",
        "With fallback enabled, segment with invalid end time and old creation time should be filtered");
  }

  @Test
  public void testFilterSegmentsPastRetentionFallbackDisabledKeepsInvalidEndTimeOldCreationTime() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testTable")
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("7")
        .build();

    long nowMs = System.currentTimeMillis();
    long oneDayMs = 86_400_000L;

    SegmentZKMetadata invalidEndTimeOldCreation =
        makeSegmentWithCreationTime("segment_invalid_old", -1, nowMs - 10 * oneDayMs);

    List<SegmentZKMetadata> segments = new ArrayList<>(List.of(invalidEndTimeOldCreation));
    List<SegmentZKMetadata> filtered =
        MinionTaskUtils.filterSegmentsPastRetention(segments, tableConfig, null, nowMs, false);

    assertEquals(filtered.size(), 1,
        "With fallback disabled, segment with invalid end time should be kept regardless of creation time");
  }

  @Test
  public void testFilterSegmentsPastRetentionFallbackEnabledKeepsRecentCreationTime() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("testTable")
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("7")
        .build();

    long nowMs = System.currentTimeMillis();
    long oneDayMs = 86_400_000L;

    SegmentZKMetadata invalidEndTimeRecentCreation =
        makeSegmentWithCreationTime("segment_invalid_recent", -1, nowMs - 2 * oneDayMs);

    List<SegmentZKMetadata> segments = new ArrayList<>(List.of(invalidEndTimeRecentCreation));
    List<SegmentZKMetadata> filtered =
        MinionTaskUtils.filterSegmentsPastRetention(segments, tableConfig, null, nowMs, true);

    assertEquals(filtered.size(), 1,
        "With fallback enabled, segment with invalid end time but recent creation time should be kept");
  }
}
