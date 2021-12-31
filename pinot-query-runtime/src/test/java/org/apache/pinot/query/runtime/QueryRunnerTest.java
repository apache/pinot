package org.apache.pinot.query.runtime;

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.QueryEnvironmentTestUtils;
import org.apache.pinot.query.dispatch.QueryDispatcher;
import org.apache.pinot.query.dispatch.WorkerQueryRequest;
import org.apache.pinot.query.planner.QueryPlan;
import org.apache.pinot.query.runtime.blocks.DataTableBlock;
import org.apache.pinot.query.runtime.mailbox.GrpcMailboxService;
import org.apache.pinot.query.runtime.mailbox.StringMailboxIdentifier;
import org.apache.pinot.query.runtime.operator.MailboxReceiveOperator;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class QueryRunnerTest extends QueryRuntimeTestBase {
  private static final Random RANDOM_REQUEST_ID_GEN = new Random();
  private static final File INDEX_DIR_A = new File(FileUtils.getTempDirectory(), "QueryRunnerTest_tableA");
  private static final File INDEX_DIR_B = new File(FileUtils.getTempDirectory(), "QueryRunnerTest_tableB");
  private static final ExecutorService TEST_EXECUTOR_SERVICE = Executors.newFixedThreadPool(1);
  private static final int NUM_ROWS = 5;
  private GrpcMailboxService _mailboxService;

  private QueryRunner _queryRunner;
  private ImmutableSegment _segmentA1;
  private ImmutableSegment _segmentA2;
  private ImmutableSegment _segmentA3;
  private ImmutableSegment _segmentB1;

  @BeforeClass
  public void setUp() throws Exception {
    Map<String, Object> runnerConfig = new HashMap<>();
    _reducerGrpcPort = QueryEnvironmentTestUtils.getAvailablePort();
    runnerConfig.put(CommonConstants.Server.CONFIG_OF_GRPC_PORT, _reducerGrpcPort);
    runnerConfig.put(CommonConstants.Server.CONFIG_OF_INSTANCE_ID,
        String.format("Server_localhost_%d", _reducerGrpcPort));

    FileUtils.deleteQuietly(INDEX_DIR_A);
    FileUtils.deleteQuietly(INDEX_DIR_B);

    _segmentA1 = buildSegment(INDEX_DIR_A, "a", "a1");
    _segmentA2 = buildSegment(INDEX_DIR_A, "a", "a2");
    _segmentA3 = buildSegment(INDEX_DIR_A, "a", "a3");
    _segmentB1 = buildSegment(INDEX_DIR_B, "b", "b1");

    setupRoutingManager(_reducerGrpcPort, 2, (int) runnerConfig.get(CommonConstants.Server.CONFIG_OF_GRPC_PORT));

    // Make query runner run on 2 different hosts.
    PinotConfiguration configuration = new PinotConfiguration(runnerConfig);
    _mailboxService = new GrpcMailboxService(configuration);
    _mailboxService.start();
    _queryRunner = new QueryRunner();
    _queryRunner.init(configuration, mockInstanceDataManager(), _mailboxService, mockServiceMetrics());
  }

  @AfterClass
  public void tearDown() {
    _queryRunner.shutDown();
    _mailboxService.shutdown();
    _segmentA1.destroy();
    _segmentA2.destroy();
    _segmentA3.destroy();
    _segmentB1.destroy();
    FileUtils.deleteQuietly(INDEX_DIR_A);
    FileUtils.deleteQuietly(INDEX_DIR_B);
  }

  @Test
  public void testRunningTableScanOnlyQuery()
      throws Exception {
    QueryPlan queryPlan = _queryEnvironment.sqlQuery("SELECT * FROM b");
    String singleServerStage = getStage(queryPlan, 1);
    ServerInstance serverInstance = queryPlan.getStageMetadataMap().get(singleServerStage).getServerInstances().get(0);
    WorkerQueryRequest workerQueryRequest = QueryDispatcher.constructStageQueryRequest(queryPlan, singleServerStage, serverInstance);

    String stageRoodId = workerQueryRequest.getStageRoot().getStageId();
    Map<String, String> requestMetadataMap = ImmutableMap.of(
        "RequestId", String.valueOf(RANDOM_REQUEST_ID_GEN.nextLong())
    );

    // TODO: availability Grpc Port is long before last check available.
    MailboxReceiveOperator mailboxReceiveOperator =
        createReduceStageOperator(queryPlan.getStageMetadataMap().get(stageRoodId).getServerInstances(),
            requestMetadataMap.get("RequestId"), stageRoodId, _reducerGrpcPort);

    // execute this single stage.
    _queryRunner.processQuery(workerQueryRequest, TEST_EXECUTOR_SERVICE, requestMetadataMap);

    DataTableBlock dataTableBlock = mailboxReceiveOperator.nextBlock();
    Assert.assertNotNull(dataTableBlock);
  }

  protected MailboxReceiveOperator createReduceStageOperator(
      List<ServerInstance> sendingInstances, String jobId, String stageId, int port) {
    StringMailboxIdentifier mailboxId =
        new StringMailboxIdentifier(String.format("%s_%s", jobId, stageId), "NULL", "localhost", "localhost", port);
    MailboxReceiveOperator mailboxReceiveOperator = new MailboxReceiveOperator(_mailboxService, sendingInstances,
        "localhost", port, jobId, stageId);
    return mailboxReceiveOperator;
  }

  protected ImmutableSegment buildSegment(File indexDir, String tableName, String segmentName) throws Exception {
    GenericRow row = new GenericRow();
    row.putValue("c1", "foo");
    row.putValue("c2", "bar");
    row.putValue("c3", 1);
    row.putValue("t", System.currentTimeMillis());

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      rows.add(row);
    }

    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName).setTimeColumnName("t").build();
    Schema schema = QueryEnvironmentTestUtils.SCHEMA;
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(indexDir.getPath());
    config.setTableName(tableName);
    config.setSegmentName(segmentName);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }
    return ImmutableSegmentLoader.load(new File(indexDir, segmentName), ReadMode.mmap);
  }

  protected ServerMetrics mockServiceMetrics() {
    return mock(ServerMetrics.class);
  }

  protected InstanceDataManager mockInstanceDataManager() {
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    TableDataManager tableDataManagerA = mockTableDataManager(_segmentA1, _segmentA2);
    TableDataManager tableDataManagerB = mockTableDataManager(_segmentB1);
    when(instanceDataManager.getTableDataManager(matches("a.*"))).thenReturn(tableDataManagerA);
    when(instanceDataManager.getTableDataManager(matches("b.*"))).thenReturn(tableDataManagerB);
    return instanceDataManager;
  }

  protected TableDataManager mockTableDataManager(ImmutableSegment... immutableSegment) {
    List<SegmentDataManager> tableSegmentDataManagers = Arrays.stream(immutableSegment)
        .map(ImmutableSegmentDataManager::new).collect(Collectors.toList());
    TableDataManager tableDataManager = mock(TableDataManager.class);
    when(tableDataManager.acquireSegments(any(), any())).thenReturn(tableSegmentDataManagers);
    return tableDataManager;
  }
}
