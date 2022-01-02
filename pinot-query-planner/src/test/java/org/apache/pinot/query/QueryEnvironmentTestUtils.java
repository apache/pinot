package org.apache.pinot.query;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.jdbc.CalciteSchemaBuilder;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.broker.routing.RoutingManager;
import org.apache.pinot.broker.routing.RoutingTable;
import org.apache.pinot.common.utils.helix.TableCache;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.catalog.PinotCatalog;
import org.apache.pinot.query.planner.QueryPlan;
import org.apache.pinot.query.routing.WorkerManager;
import org.apache.pinot.query.type.TypeFactory;
import org.apache.pinot.query.type.TypeSystem;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Base query environment test that provides a bunch of mock tables / schemas so that
 * we can run a simple query planning, produce stages / metadata for other components to test.
 */
public class QueryEnvironmentTestUtils {
  public static Schema SCHEMA;
  public static Map<String, List<String>> SERVER1_SEGMENTS =
      ImmutableMap.of("a", Lists.newArrayList("a1", "a2"), "b", Lists.newArrayList("b1"));
  public static Map<String, List<String>> SERVER2_SEGMENTS =
      ImmutableMap.of("a", Lists.newArrayList("a3"));

  static {
    SCHEMA =
        new Schema.SchemaBuilder().setSchemaName("a")
            .addSingleValueDimension("c1", FieldSpec.DataType.STRING, "")
            .addSingleValueDimension("c2", FieldSpec.DataType.STRING, "")
            .addDateTime("t", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:HOURS")
            .addMetric("c3", FieldSpec.DataType.INT, 0).build();
  }

  public static TableCache mockTableCache() {
    TableCache mock = mock(TableCache.class);
    when(mock.getSchema("a")).thenReturn(SCHEMA);
    when(mock.getSchema("b")).thenReturn(SCHEMA);
    return mock;
  }

  public static QueryEnvironment getQueryEnvironment(int reducerPort, int port1, int port2) {
    RoutingManager routingManager = QueryEnvironmentTestUtils.getMockRoutingManager(port1, port2);
    return new QueryEnvironment(
        new TypeFactory(new TypeSystem()),
        CalciteSchemaBuilder.asRootSchema(new PinotCatalog(QueryEnvironmentTestUtils.mockTableCache())),
        new WorkerManager("localhost", reducerPort, routingManager)
    );
  }

  public static RoutingManager getMockRoutingManager(int port1, int port2) {
    String server1 = String.format("localhost_%d", port1);
    String server2 = String.format("localhost_%d", port2);
    ServerInstance host1 = getServerInstance(port1);
    ServerInstance host2 = getServerInstance(port2);
    RoutingManager mock = mock(RoutingManager.class);
    RoutingTable rtA = mock(RoutingTable.class);
    when(rtA.getServerInstanceToSegmentsMap()).thenReturn(ImmutableMap.of(host1, SERVER1_SEGMENTS.get("a"),
        host2, SERVER2_SEGMENTS.get("a")));
    RoutingTable rtB = mock(RoutingTable.class);
    when(rtB.getServerInstanceToSegmentsMap()).thenReturn(ImmutableMap.of(host1, SERVER1_SEGMENTS.get("b")));
    when(mock.getRoutingTable("a")).thenReturn(rtA);
    when(mock.getRoutingTable("b")).thenReturn(rtB);
    when(mock.getEnabledServerInstanceMap()).thenReturn(ImmutableMap.of(server1, host1, server2, host2));
    return mock;
  }

  public static String getTestStageByServerCount(QueryPlan queryPlan, int serverCount) {
    List<String> stageIds = queryPlan.getStageMetadataMap().entrySet().stream()
        .filter(e -> !e.getKey().equals("ROOT") && e.getValue().getServerInstances().size() == serverCount)
        .map(Map.Entry::getKey)
        .collect(Collectors.toList());
    return stageIds.size() > 0 ? stageIds.get(0) : null;
  }

  public static int getAvailablePort() {
    try {
      try (ServerSocket socket = new ServerSocket(0)) {
        return socket.getLocalPort();
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to find an available port to use", e);
    }
  }

  public static ServerInstance getServerInstance(int port) {
    String server = String.format("localhost_%d", port);
    InstanceConfig instanceConfig = InstanceConfig.toInstanceConfig(server);
    ZNRecord znRecord = instanceConfig.getRecord();
    Map<String, String> simpleFields = znRecord.getSimpleFields();
    simpleFields.put(CommonConstants.Helix.Instance.GRPC_PORT_KEY, instanceConfig.getPort());
    return new ServerInstance(instanceConfig);
  }
}
