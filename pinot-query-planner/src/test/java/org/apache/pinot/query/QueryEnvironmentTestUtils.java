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

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.calcite.jdbc.CalciteSchemaBuilder;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.query.catalog.PinotCatalog;
import org.apache.pinot.query.routing.WorkerManager;
import org.apache.pinot.query.testutils.MockRoutingManagerFactory;
import org.apache.pinot.query.type.TypeFactory;
import org.apache.pinot.query.type.TypeSystem;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;



/**
 * Base query environment test that provides a bunch of mock tables / schemas so that
 * we can run a simple query planning, produce stages / metadata for other components to test.
 */
public class QueryEnvironmentTestUtils {
  public static final Schema.SchemaBuilder SCHEMA_BUILDER;
  private static final int NUM_ROWS = 5;
  private static final String[] STRING_FIELD_LIST = new String[]{"foo", "bar", "alice", "bob", "charlie"};
  private static final int[] INT_FIELD_LIST = new int[]{1, 42};
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

  public static List<GenericRow> buildRows(String tableName) {
    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow row = new GenericRow();
      row.putValue("col1", STRING_FIELD_LIST[i % STRING_FIELD_LIST.length]);
      row.putValue("col2", STRING_FIELD_LIST[i % (STRING_FIELD_LIST.length - 2)]);
      row.putValue("col3", INT_FIELD_LIST[i % INT_FIELD_LIST.length]);
      row.putValue("ts", tableName.endsWith("_O")
          ? System.currentTimeMillis() - TimeUnit.DAYS.toMillis(2) : System.currentTimeMillis());
      rows.add(row);
    }
    return rows;
  }

  public static QueryEnvironment getQueryEnvironment(int reducerPort, int port1, int port2,
      Map<String, List<String>> segmentMap1, Map<String, List<String>> segmentMap2) {
    MockRoutingManagerFactory factory = new MockRoutingManagerFactory(port1, port2)
        .registerTable(SCHEMA_BUILDER.setSchemaName("a").build(), "a_REALTIME")
        .registerTable(SCHEMA_BUILDER.setSchemaName("b").build(), "b_REALTIME")
        .registerTable(SCHEMA_BUILDER.setSchemaName("c").build(), "c_OFFLINE")
        .registerTable(SCHEMA_BUILDER.setSchemaName("d").build(), "d");
    for (Map.Entry<String, List<String>> entry : segmentMap1.entrySet()) {
      for (String segment : entry.getValue()) {
        factory.registerSegment(port1, entry.getKey(), segment);
      }
    }
    for (Map.Entry<String, List<String>> entry : segmentMap2.entrySet()) {
      for (String segment : entry.getValue()) {
        factory.registerSegment(port2, entry.getKey(), segment);
      }
    }
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
