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
package org.apache.pinot.query;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.calcite.jdbc.CalciteSchemaBuilder;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.catalog.PinotCatalog;
import org.apache.pinot.query.planner.QueryPlan;
import org.apache.pinot.query.routing.WorkerInstance;
import org.apache.pinot.query.routing.WorkerManager;
import org.apache.pinot.query.type.TypeFactory;
import org.apache.pinot.query.type.TypeSystem;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Base query environment test that provides a bunch of mock tables / schemas so that
 * we can run a simple query planning, produce stages / metadata for other components to test.
 */
public class QueryEnvironmentTestUtils {
  public static final Schema.SchemaBuilder SCHEMA_BUILDER;
  public static final Map<String, List<String>> SERVER1_SEGMENTS =
      ImmutableMap.of("a", Lists.newArrayList("a1", "a2"), "b", Lists.newArrayList("b1"), "c",
          Lists.newArrayList("c1"), "d_O", Lists.newArrayList("d1"));
  public static final Map<String, List<String>> SERVER2_SEGMENTS =
      ImmutableMap.of("a", Lists.newArrayList("a3"), "c", Lists.newArrayList("c2", "c3"),
          "d_R", Lists.newArrayList("d2"), "d_O", Lists.newArrayList("d3"));

  static {
    SCHEMA_BUILDER = new Schema.SchemaBuilder().addSingleValueDimension("col1", FieldSpec.DataType.STRING, "")
        .addSingleValueDimension("col2", FieldSpec.DataType.STRING, "")
        .addDateTime("ts", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:HOURS")
        .addMetric("col3", FieldSpec.DataType.INT, 0);
  }

  private QueryEnvironmentTestUtils() {
    // do not instantiate.
  }

  public static TableCache mockTableCache() {
    TableCache mock = mock(TableCache.class);
    when(mock.getTableNameMap()).thenReturn(ImmutableMap.of("a_REALTIME", "a", "b_REALTIME", "b", "c_REALTIME", "c",
        "d_OFFLINE", "d", "d_REALTIME", "d"));
    when(mock.getSchema("a")).thenReturn(SCHEMA_BUILDER.setSchemaName("a").build());
    when(mock.getSchema("b")).thenReturn(SCHEMA_BUILDER.setSchemaName("b").build());
    when(mock.getSchema("c")).thenReturn(SCHEMA_BUILDER.setSchemaName("c").build());
    when(mock.getSchema("d")).thenReturn(SCHEMA_BUILDER.setSchemaName("d").build());
    return mock;
  }

  public static QueryEnvironment getQueryEnvironment(int reducerPort, int port1, int port2) {
    RoutingManager routingManager = QueryEnvironmentTestUtils.getMockRoutingManager(port1, port2);
    return new QueryEnvironment(new TypeFactory(new TypeSystem()),
        CalciteSchemaBuilder.asRootSchema(new PinotCatalog(QueryEnvironmentTestUtils.mockTableCache())),
        new WorkerManager("localhost", reducerPort, routingManager));
  }

  public static RoutingManager getMockRoutingManager(int port1, int port2) {
    String server1 = String.format("localhost_%d", port1);
    String server2 = String.format("localhost_%d", port2);
    // this doesn't test the QueryServer functionality so the server port can be the same as the mailbox port.
    // this is only use for test identifier purpose.
    ServerInstance host1 = new WorkerInstance("localhost", port1, port1, port1, port1);
    ServerInstance host2 = new WorkerInstance("localhost", port2, port2, port2, port2);

    RoutingTable rtA = mock(RoutingTable.class);
    when(rtA.getServerInstanceToSegmentsMap()).thenReturn(
        ImmutableMap.of(host1, SERVER1_SEGMENTS.get("a"), host2, SERVER2_SEGMENTS.get("a")));
    RoutingTable rtB = mock(RoutingTable.class);
    when(rtB.getServerInstanceToSegmentsMap()).thenReturn(ImmutableMap.of(host1, SERVER1_SEGMENTS.get("b")));
    RoutingTable rtC = mock(RoutingTable.class);
    when(rtC.getServerInstanceToSegmentsMap()).thenReturn(
        ImmutableMap.of(host1, SERVER1_SEGMENTS.get("c"), host2, SERVER2_SEGMENTS.get("c")));

    // hybrid table
    RoutingTable rtDOffline = mock(RoutingTable.class);
    RoutingTable rtDRealtime = mock(RoutingTable.class);
    when(rtDOffline.getServerInstanceToSegmentsMap()).thenReturn(
        ImmutableMap.of(host1, SERVER1_SEGMENTS.get("d_O"), host2, SERVER2_SEGMENTS.get("d_O")));
    when(rtDRealtime.getServerInstanceToSegmentsMap()).thenReturn(ImmutableMap.of(host2, SERVER2_SEGMENTS.get("d_R")));
    Map<String, RoutingTable> mockRoutingTableMap = ImmutableMap.of("a", rtA, "b", rtB, "c", rtC,
        "d_OFFLINE", rtDOffline, "d_REALTIME", rtDRealtime);

    RoutingManager mock = mock(RoutingManager.class);
    when(mock.getRoutingTable(any())).thenAnswer(invocation -> {
      BrokerRequest brokerRequest = invocation.getArgument(0);
      String tableName = brokerRequest.getPinotQuery().getDataSource().getTableName();
      return mockRoutingTableMap.getOrDefault(tableName,
          mockRoutingTableMap.get(TableNameBuilder.extractRawTableName(tableName)));
    });
    when(mock.getEnabledServerInstanceMap()).thenReturn(ImmutableMap.of(server1, host1, server2, host2));
    when(mock.getTimeBoundaryInfo(anyString())).thenAnswer(invocation -> {
      String offlineTableName = invocation.getArgument(0);
      return "d_OFFLINE".equals(offlineTableName) ? new TimeBoundaryInfo("ts",
          String.valueOf(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1))) : null;
    });
    return mock;
  }

  public static int getTestStageByServerCount(QueryPlan queryPlan, int serverCount) {
    List<Integer> stageIds = queryPlan.getStageMetadataMap().entrySet().stream()
        .filter(e -> !e.getKey().equals(0) && e.getValue().getServerInstances().size() == serverCount)
        .map(Map.Entry::getKey).collect(Collectors.toList());
    return stageIds.size() > 0 ? stageIds.get(0) : -1;
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
}
