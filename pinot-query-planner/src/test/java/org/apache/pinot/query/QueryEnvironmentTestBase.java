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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;


public class QueryEnvironmentTestBase {
  public static final Map<String, List<String>> SERVER1_SEGMENTS =
      ImmutableMap.of("a_REALTIME", ImmutableList.of("a1", "a2"), "b_REALTIME", ImmutableList.of("b1"), "c_OFFLINE",
          ImmutableList.of("c1"), "d_OFFLINE", ImmutableList.of("d1"));
  public static final Map<String, List<String>> SERVER2_SEGMENTS =
      ImmutableMap.of("a_REALTIME", ImmutableList.of("a3"), "c_OFFLINE", ImmutableList.of("c2", "c3"),
          "d_REALTIME", ImmutableList.of("d2"), "d_OFFLINE", ImmutableList.of("d3"));
  public static final Map<String, Schema> TABLE_SCHEMAS = new HashMap<>();
  private static final Schema.SchemaBuilder SCHEMA_BUILDER;
  static {
    SCHEMA_BUILDER = new Schema.SchemaBuilder()
        .addSingleValueDimension("col1", FieldSpec.DataType.STRING, "")
        .addSingleValueDimension("col2", FieldSpec.DataType.STRING, "")
        .addDateTime("ts", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:HOURS")
        .addMetric("col3", FieldSpec.DataType.INT, 0)
        .setSchemaName("defaultSchemaName");
    TABLE_SCHEMAS.put("a_REALTIME", SCHEMA_BUILDER.setSchemaName("a").build());
    TABLE_SCHEMAS.put("b_REALTIME", SCHEMA_BUILDER.setSchemaName("b").build());
    TABLE_SCHEMAS.put("c_OFFLINE", SCHEMA_BUILDER.setSchemaName("c").build());
    TABLE_SCHEMAS.put("d", SCHEMA_BUILDER.setSchemaName("d").build());
  }

  protected QueryEnvironment _queryEnvironment;

  @BeforeClass
  public void setUp() {
    // the port doesn't matter as we are not actually making a server call.
    _queryEnvironment = getQueryEnvironment(3, 1, 2, TABLE_SCHEMAS, SERVER1_SEGMENTS, SERVER2_SEGMENTS);
  }

  @DataProvider(name = "testQueryDataProvider")
  protected Object[][] provideQueries() {
    return new Object[][] {
        new Object[]{"SELECT * FROM a ORDER BY col1 LIMIT 10"},
        new Object[]{"SELECT * FROM b ORDER BY col1, col2 DESC LIMIT 10"},
        new Object[]{"SELECT * FROM d"},
        new Object[]{"SELECT * FROM a JOIN b ON a.col1 = b.col2"},
        new Object[]{"SELECT * FROM a JOIN b ON a.col1 = b.col2 WHERE a.col3 >= 0"},
        new Object[]{"SELECT * FROM a JOIN b ON a.col1 = b.col2 WHERE a.col3 >= 0 AND a.col3 > b.col3"},
        new Object[]{"SELECT * FROM a JOIN b on a.col1 = b.col1 AND a.col2 = b.col2"},
        new Object[]{"SELECT a.col1, a.ts, b.col3 FROM a JOIN b ON a.col1 = b.col2 "
            + " WHERE a.col3 >= 0 AND a.col2 = 'a' AND b.col3 < 0"},
        new Object[]{"SELECT a.col1, a.col3 + a.ts FROM a WHERE a.col3 >= 0 AND a.col2 = 'a'"},
        new Object[]{"SELECT SUM(a.col3), COUNT(*) FROM a WHERE a.col3 >= 0 AND a.col2 = 'a'"},
        new Object[]{"SELECT a.col1, SUM(a.col3) FROM a WHERE a.col3 >= 0 AND a.col2 = 'a' GROUP BY a.col1"},
        new Object[]{"SELECT a.col1, COUNT(*) FROM a WHERE a.col3 >= 0 AND a.col2 = 'a' GROUP BY a.col1"},
        new Object[]{"SELECT a.col2, a.col1, SUM(a.col3) FROM a WHERE a.col3 >= 0 AND a.col1 = 'a' "
            + " GROUP BY a.col1, a.col2"},
        new Object[]{"SELECT a.col1, AVG(b.col3) FROM a JOIN b ON a.col1 = b.col2 "
            + " WHERE a.col3 >= 0 AND a.col2 = 'a' AND b.col3 < 0 GROUP BY a.col1"},
        new Object[]{"SELECT a.col1, COUNT(*), SUM(a.col3) FROM a WHERE a.col3 >= 0 AND a.col2 = 'a' GROUP BY a.col1 "
            + "HAVING COUNT(*) > 10 AND MAX(a.col3) >= 0 AND MIN(a.col3) < 20 AND SUM(a.col3) <= 10 "
            + "AND AVG(a.col3) = 5"},
        new Object[]{"SELECT dateTrunc('DAY', ts) FROM a LIMIT 10"},
        new Object[]{"SELECT dateTrunc('DAY', a.ts + b.ts) FROM a JOIN b on a.col1 = b.col1 AND a.col2 = b.col2"},
        new Object[]{"SELECT a.col2, a.col3 FROM a JOIN b ON a.col1 = b.col1 "
            + " WHERE a.col3 >= 0 GROUP BY a.col2, a.col3"},
        new Object[]{"SELECT a.col1, b.col2 FROM a JOIN b ON a.col1 = b.col1 WHERE a.col2 IN ('foo', 'bar') AND"
            + " b.col2 NOT IN ('alice', 'charlie')"},
    };
  }

  public static QueryEnvironment getQueryEnvironment(int reducerPort, int port1, int port2,
      Map<String, Schema> schemaMap, Map<String, List<String>> segmentMap1, Map<String, List<String>> segmentMap2) {
    MockRoutingManagerFactory factory = new MockRoutingManagerFactory(port1, port2);
    for (Map.Entry<String, Schema> entry : schemaMap.entrySet()) {
      factory.registerTable(entry.getValue(), entry.getKey());
    }
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
}
