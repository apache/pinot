package org.apache.pinot.query.runtime;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.QueryEnvironmentTestUtils;
import org.apache.pinot.query.QueryServerEnclosure;
import org.apache.pinot.query.dispatch.QueryDispatcher;
import org.apache.pinot.query.dispatch.WorkerQueryRequest;
import org.apache.pinot.query.planner.QueryPlan;
import org.apache.pinot.query.runtime.blocks.DataTableBlock;
import org.apache.pinot.query.runtime.mailbox.GrpcMailboxService;
import org.apache.pinot.query.runtime.operator.MailboxReceiveOperator;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class QueryRunnerTest {
  private static final Random RANDOM_REQUEST_ID_GEN = new Random();
  private static final File INDEX_DIR_S1_A = new File(FileUtils.getTempDirectory(), "QueryRunnerTest_server1_tableA");
  private static final File INDEX_DIR_S1_B = new File(FileUtils.getTempDirectory(), "QueryRunnerTest_server1_tableB");
  private static final File INDEX_DIR_S2_A = new File(FileUtils.getTempDirectory(), "QueryRunnerTest_server2_tableA");

  private QueryEnvironment _queryEnvironment;
  private int _reducerGrpcPort;
  private Map<ServerInstance, QueryServerEnclosure> _servers = new HashMap<>();
  private GrpcMailboxService _mailboxService;

  @BeforeClass
  public void setUp() throws Exception {
    QueryServerEnclosure server1 = new QueryServerEnclosure(Lists.newArrayList("a", "b"),
        ImmutableMap.of("a", INDEX_DIR_S1_A, "b", INDEX_DIR_S1_B), QueryEnvironmentTestUtils.SERVER1_SEGMENTS);
    QueryServerEnclosure server2 =
        new QueryServerEnclosure(Lists.newArrayList("a"), ImmutableMap.of("a", INDEX_DIR_S2_A),
            QueryEnvironmentTestUtils.SERVER2_SEGMENTS);

    _reducerGrpcPort = QueryEnvironmentTestUtils.getAvailablePort();
    Map<String, Object> reducerConfig = new HashMap<>();
    reducerConfig.put(CommonConstants.Server.CONFIG_OF_GRPC_PORT, _reducerGrpcPort);
    reducerConfig.put(CommonConstants.Server.CONFIG_OF_INSTANCE_ID,
        String.format("Broker_localhost_%d", _reducerGrpcPort));
    _mailboxService = new GrpcMailboxService(new PinotConfiguration(reducerConfig));
    _mailboxService.start();

    _queryEnvironment = QueryEnvironmentTestUtils.getQueryEnvironment(
        _reducerGrpcPort, server1.getPort(), server2.getPort());
    server1.start();
    server2.start();
    _servers.put(QueryEnvironmentTestUtils.getServerInstance(server1.getPort()), server1);
    _servers.put(QueryEnvironmentTestUtils.getServerInstance(server2.getPort()), server2);
  }

  @AfterClass
  public void tearDown() {
    for (QueryServerEnclosure server : _servers.values()) {
      server.shutDown();
    }
  }

  @Test
  public void testRunningTableScanOnlyQuery()
      throws Exception {
    QueryPlan queryPlan = _queryEnvironment.sqlQuery("SELECT * FROM b");
    String stageRoodId = QueryEnvironmentTestUtils.getTestStageByServerCount(queryPlan, 1);
    Map<String, String> requestMetadataMap = ImmutableMap.of(
        "RequestId", String.valueOf(RANDOM_REQUEST_ID_GEN.nextLong()));

    ServerInstance serverInstance = queryPlan.getStageMetadataMap().get(stageRoodId).getServerInstances().get(0);
    WorkerQueryRequest workerQueryRequest = QueryDispatcher.constructStageQueryRequest(queryPlan, stageRoodId, serverInstance);

    MailboxReceiveOperator mailboxReceiveOperator =
        createReduceStageOperator(queryPlan.getStageMetadataMap().get(stageRoodId).getServerInstances(),
            requestMetadataMap.get("RequestId"), stageRoodId, _reducerGrpcPort);

    // execute this single stage.
    _servers.get(serverInstance).processQuery(workerQueryRequest, requestMetadataMap);

    DataTableBlock dataTableBlock;
    // get the block back and it should have 5 rows
    dataTableBlock = mailboxReceiveOperator.nextBlock();
    Assert.assertEquals(dataTableBlock.getDataTable().getNumberOfRows(), 5);
    // next block should be null as all servers finished sending.
    dataTableBlock = mailboxReceiveOperator.nextBlock();
    Assert.assertNull(dataTableBlock);
  }

  @Test
  public void testRunningTableScanMultipleServer()
      throws Exception {
    QueryPlan queryPlan = _queryEnvironment.sqlQuery("SELECT * FROM a");
    String stageRoodId = QueryEnvironmentTestUtils.getTestStageByServerCount(queryPlan, 2);
    Map<String, String> requestMetadataMap = ImmutableMap.of("RequestId", String.valueOf(RANDOM_REQUEST_ID_GEN.nextLong()));

    for (ServerInstance serverInstance : queryPlan.getStageMetadataMap().get(stageRoodId).getServerInstances()) {
      WorkerQueryRequest workerQueryRequest = QueryDispatcher.constructStageQueryRequest(queryPlan, stageRoodId, serverInstance);

      // execute this single stage.
      _servers.get(serverInstance).processQuery(workerQueryRequest, requestMetadataMap);
    }

    MailboxReceiveOperator mailboxReceiveOperator = createReduceStageOperator(
        queryPlan.getStageMetadataMap().get(stageRoodId).getServerInstances(),
        requestMetadataMap.get("RequestId"), stageRoodId, _reducerGrpcPort);

    int count = 0;
    int rowCount = 0;
    DataTableBlock dataTableBlock;
    while (count < 2) { // we have 2 servers sending data.
      dataTableBlock = mailboxReceiveOperator.nextBlock();
      rowCount += dataTableBlock.getDataTable().getNumberOfRows();
      count++;
    }
    // assert that all table A segments returned successfully.
    Assert.assertEquals(rowCount, 15);
    // assert that the next block is null (e.g. finished receiving).
    dataTableBlock = mailboxReceiveOperator.nextBlock();
    Assert.assertNull(dataTableBlock);
  }

  protected MailboxReceiveOperator createReduceStageOperator(
      List<ServerInstance> sendingInstances, String jobId, String stageId, int port) {
    MailboxReceiveOperator mailboxReceiveOperator = new MailboxReceiveOperator(_mailboxService, sendingInstances,
        "localhost", port, jobId, stageId);
    return mailboxReceiveOperator;
  }
}
