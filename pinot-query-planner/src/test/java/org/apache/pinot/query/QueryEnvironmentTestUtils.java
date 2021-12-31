package org.apache.pinot.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Map;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.broker.routing.RoutingManager;
import org.apache.pinot.broker.routing.RoutingTable;
import org.apache.pinot.common.utils.helix.TableCache;
import org.apache.pinot.core.transport.ServerInstance;
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

  public static RoutingManager getMockRoutingManager(int portA, int portB) {
    String serverA = String.format("localhost_%d", portA);
    String serverB = String.format("localhost_%d", portB);
    ServerInstance hostA = getServerInstance(serverA);
    ServerInstance hostB = getServerInstance(serverB);
    RoutingManager mock = mock(RoutingManager.class);
    RoutingTable rtA = mock(RoutingTable.class);
    when(rtA.getServerInstanceToSegmentsMap()).thenReturn(ImmutableMap.of(hostA, ImmutableList.of("a1", "a2"),
        hostB, ImmutableList.of("a3")));
    RoutingTable rtB = mock(RoutingTable.class);
    when(rtB.getServerInstanceToSegmentsMap()).thenReturn(ImmutableMap.of(hostB, ImmutableList.of("b1")
    ));
    when(mock.getRoutingTable("a")).thenReturn(rtA);
    when(mock.getRoutingTable("b")).thenReturn(rtB);
    when(mock.getEnabledServerInstanceMap()).thenReturn(ImmutableMap.of(serverA, hostA, serverB, hostB));
    return mock;
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

  private static ServerInstance getServerInstance(String server) {
    InstanceConfig instanceConfig = InstanceConfig.toInstanceConfig(server);
    ZNRecord znRecord = instanceConfig.getRecord();
    Map<String, String> simpleFields = znRecord.getSimpleFields();
    simpleFields.put(CommonConstants.Helix.Instance.GRPC_PORT_KEY, instanceConfig.getPort());
    return new ServerInstance(instanceConfig);
  }
}
