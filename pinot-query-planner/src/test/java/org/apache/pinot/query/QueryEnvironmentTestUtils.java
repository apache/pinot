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
import org.apache.pinot.query.testutils.MockRoutingManagerFactory;
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
    SCHEMA_BUILDER = new Schema.SchemaBuilder()
        .addSingleValueDimension("col1", FieldSpec.DataType.STRING, "")
        .addSingleValueDimension("col2", FieldSpec.DataType.STRING, "")
        .addDateTime("ts", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:HOURS")
        .addMetric("col3", FieldSpec.DataType.INT, 0)
        .setSchemaName("defaultSchemaName");
  }

  private QueryEnvironmentTestUtils() {
    // do not instantiate.
  }

  public static QueryEnvironment getQueryEnvironment(int reducerPort, int port1, int port2) {
    MockRoutingManagerFactory factory = new MockRoutingManagerFactory(port1, port2)
        .registerTable(SCHEMA_BUILDER.setSchemaName("a").build(), "a_REALTIME")
        .registerTable(SCHEMA_BUILDER.setSchemaName("b").build(), "b_REALTIME")
        .registerTable(SCHEMA_BUILDER.setSchemaName("c").build(), "c_OFFLINE")
        .registerTable(SCHEMA_BUILDER.setSchemaName("d").build(), "d")
        .registerFakeSegments(port1, "a_REALTIME", 2)
        .registerFakeSegments(port1, "b_REALTIME", 1)
        .registerFakeSegments(port1, "c_OFFLINE", 1)
        .registerFakeSegments(port1, "d_OFFLINE", 1)
        .registerFakeSegments(port2, "a_REALTIME", 1)
        .registerFakeSegments(port2, "c_OFFLINE", 2)
        .registerFakeSegments(port2, "d_OFFLINE", 1)
        .registerFakeSegments(port2, "d_REALTIME", 1);
    RoutingManager routingManager = factory.buildRoutingManager();
    TableCache tableCache = factory.buildTableCache();
    return new QueryEnvironment(new TypeFactory(new TypeSystem()),
        CalciteSchemaBuilder.asRootSchema(new PinotCatalog(tableCache)),
        new WorkerManager("localhost", reducerPort, routingManager));
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
