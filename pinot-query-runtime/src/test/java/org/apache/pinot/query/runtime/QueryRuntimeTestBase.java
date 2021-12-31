package org.apache.pinot.query.runtime;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.jdbc.CalciteSchemaBuilder;
import org.apache.pinot.broker.routing.RoutingManager;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.QueryEnvironmentTestUtils;
import org.apache.pinot.query.catalog.PinotCatalog;
import org.apache.pinot.query.planner.QueryPlan;
import org.apache.pinot.query.routing.WorkerManager;
import org.apache.pinot.query.type.TypeFactory;
import org.apache.pinot.query.type.TypeSystem;
import org.mockito.Mockito;


public abstract class QueryRuntimeTestBase {
  protected QueryEnvironment _queryEnvironment;
  protected int _reducerGrpcPort;

  protected void setupRoutingManager(int reducerPort, int portA, int portB) {
    RoutingManager routingManager = QueryEnvironmentTestUtils.getMockRoutingManager(portA, portB);
    _queryEnvironment = new QueryEnvironment(
        new TypeFactory(new TypeSystem()),
        CalciteSchemaBuilder.asRootSchema(new PinotCatalog(QueryEnvironmentTestUtils.mockTableCache())),
        new WorkerManager("localhost", reducerPort, routingManager)
    );
  }

  protected ServerInstance mockServerInstance(int availablePort) {
    ServerInstance mock = Mockito.mock(ServerInstance.class);
    Mockito.when(mock.getGrpcPort()).thenReturn(availablePort);
    Mockito.when(mock.getHostname()).thenReturn("localhost");
    return mock;
  }

  protected static String getStage(QueryPlan queryPlan, int serverCount) {
    List<String> stageIds = queryPlan.getStageMetadataMap().entrySet().stream()
        .filter(e -> !e.getKey().equals("ROOT") && e.getValue().getServerInstances().size() == 1)
        .map(Map.Entry::getKey)
        .collect(Collectors.toList());
    return stageIds.size() > 0 ? stageIds.get(0) : null;
  }

  protected QueryRunner mockQueryRunner() {
    return Mockito.mock(QueryRunner.class);
  }
}
