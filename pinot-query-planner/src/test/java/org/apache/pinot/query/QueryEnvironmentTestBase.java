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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import javax.annotation.Nullable;
import org.apache.calcite.jdbc.CalciteSchemaBuilder;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.TablePartitionInfo;
import org.apache.pinot.core.routing.TablePartitionInfo.PartitionInfo;
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

  protected static final Random RANDOM_REQUEST_ID_GEN = new Random();
  public static final Map<String, List<String>> SERVER1_SEGMENTS =
      ImmutableMap.of("a_REALTIME", ImmutableList.of("a1", "a2"), "b_REALTIME", ImmutableList.of("b1"), "c_OFFLINE",
          ImmutableList.of("c1"), "d_OFFLINE", ImmutableList.of("d1"), "e_OFFLINE", ImmutableList.of("e1"));
  public static final Map<String, List<String>> SERVER2_SEGMENTS =
      ImmutableMap.of("a_REALTIME", ImmutableList.of("a3"), "c_OFFLINE", ImmutableList.of("c2", "c3"),
          "d_REALTIME", ImmutableList.of("d2"), "d_OFFLINE", ImmutableList.of("d3"), "e_REALTIME",
          ImmutableList.of("e2"), "e_OFFLINE", ImmutableList.of("e3"));
  public static final Map<String, Schema> TABLE_SCHEMAS = new HashMap<>();

  static {
    TABLE_SCHEMAS.put("a_REALTIME", getSchemaBuilder("a").build());
    TABLE_SCHEMAS.put("b_REALTIME", getSchemaBuilder("b").build());
    TABLE_SCHEMAS.put("c_OFFLINE", getSchemaBuilder("c").build());
    TABLE_SCHEMAS.put("d", getSchemaBuilder("d").build());
    TABLE_SCHEMAS.put("e", getSchemaBuilder("e")
        .addMultiValueDimension("mcol1", FieldSpec.DataType.STRING).build());
  }

  static Schema.SchemaBuilder getSchemaBuilder(String schemaName) {
    return new Schema.SchemaBuilder()
        .addSingleValueDimension("col1", FieldSpec.DataType.STRING, "")
        .addSingleValueDimension("col2", FieldSpec.DataType.STRING, "")
        .addSingleValueDimension("col5", FieldSpec.DataType.BOOLEAN, false)
        .addDateTime("ts", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:HOURS")
        .addMetric("col3", FieldSpec.DataType.INT, 0)
        .addMetric("col4", FieldSpec.DataType.BIG_DECIMAL, 0)
        .addMetric("col6", FieldSpec.DataType.INT, 0)
        .setSchemaName(schemaName);
  }

  protected QueryEnvironment _queryEnvironment;

  @BeforeClass
  public void setUp() {
    // the port doesn't matter as we are not actually making a server call.
    _queryEnvironment = getQueryEnvironment(3, 1, 2, TABLE_SCHEMAS, SERVER1_SEGMENTS, SERVER2_SEGMENTS, null);
  }

  @DataProvider(name = "testQueryDataProvider")
  protected Object[][] provideQueries() {
    return new Object[][]{
        new Object[]{"SELECT * FROM a UNION SELECT * FROM b"},
        new Object[]{"SELECT * FROM a UNION ALL SELECT * FROM b"},
        new Object[]{"SELECT * FROM a INTERSECT SELECT * FROM b"},
        new Object[]{"SELECT * FROM a EXCEPT SELECT * FROM b"},
        new Object[]{"SELECT * FROM a MINUS SELECT * FROM b"},
        new Object[]{"SELECT * FROM a ORDER BY col1 LIMIT 10"},
        new Object[]{"SELECT * FROM b ORDER BY col1, col2 DESC LIMIT 10"},
        new Object[]{"SELECT * FROM d"},
        new Object[]{"SELECT * FROM a JOIN b ON a.col1 = b.col2"},
        new Object[]{"SELECT * FROM a JOIN b ON a.col1 = b.col2 WHERE a.col3 >= 0"},
        new Object[]{"SELECT * FROM a JOIN b ON a.col1 = b.col2 WHERE a.col3 >= 0 AND a.col3 > b.col3"},
        new Object[]{"SELECT * FROM a JOIN b on a.col1 = b.col1 AND a.col2 = b.col2"},
        new Object[]{
            "SELECT a.col1, a.ts, b.col3 FROM a JOIN b ON a.col1 = b.col2 "
                + " WHERE a.col3 >= 0 AND a.col2 = 'a' AND b.col3 < 0"
        },
        new Object[]{"SELECT a.col1, a.col3 + a.ts FROM a WHERE a.col3 >= 0 AND a.col2 = 'a'"},
        new Object[]{"SELECT SUM(a.col3), COUNT(*) FROM a WHERE a.col3 >= 0 AND a.col2 = 'a'"},
        new Object[]{"SELECT AVG(a.col3), SUM(a.col3), COUNT(a.col3) FROM a"},
        new Object[]{"SELECT a.col1, AVG(a.col3), SUM(a.col3), COUNT(a.col3) FROM a GROUP BY a.col1"},
        new Object[]{"SELECT BOOL_AND(a.col5), BOOL_OR(a.col5) FROM a"},
        new Object[]{"SELECT a.col3, BOOL_AND(a.col5), BOOL_OR(a.col5) FROM a GROUP BY a.col3"},
        new Object[]{"SELECT KURTOSIS(a.col2), COUNT(DISTINCT a.col3), SKEWNESS(a.col3) FROM a"},
        new Object[]{"SELECT a.col1, KURTOSIS(a.col2), SKEWNESS(a.col3) FROM a GROUP BY a.col1"},
        new Object[]{"SELECT COUNT(a.col3), AVG(a.col3), SUM(a.col3), MIN(a.col3), MAX(a.col3) FROM a"},
        new Object[]{"SELECT DISTINCTCOUNT(a.col3), COUNT(a.col4), COUNT(*), COUNT(DISTINCT a.col1) FROM a"},
        new Object[]{
            "SELECT a.col2, DISTINCTCOUNT(a.col3), COUNT(a.col4), COUNT(*), COUNT(DISTINCT a.col1) FROM a "
                + "GROUP BY a.col2 ORDER BY a.col2"
        },
        new Object[]{"SELECT a.col1, SKEWNESS(a.col3), KURTOSIS(a.col3), DISTINCTCOUNT(a.col1) FROM a GROUP BY a.col1"},
        new Object[]{"SELECT a.col1, SUM(a.col3) FROM a WHERE a.col3 >= 0 AND a.col2 = 'a' GROUP BY a.col1"},
        new Object[]{"SELECT a.col1, COUNT(*) FROM a WHERE a.col3 >= 0 AND a.col2 = 'a' GROUP BY a.col1"},
        new Object[]{
            "SELECT a.col2, a.col1, SUM(a.col3) FROM a WHERE a.col3 >= 0 AND a.col1 = 'a' "
                + " GROUP BY a.col1, a.col2"
        },
        new Object[]{
            "SELECT a.col1, AVG(b.col3) FROM a JOIN b ON a.col1 = b.col2 "
                + " WHERE a.col3 >= 0 AND a.col2 = 'a' AND b.col3 < 0 GROUP BY a.col1"
        },
        new Object[]{
            "SELECT a.col1, COUNT(*), SUM(a.col3) FROM a WHERE a.col3 >= 0 AND a.col2 = 'a' GROUP BY a.col1 "
                + "HAVING COUNT(*) > 10 AND MAX(a.col3) >= 0 AND MIN(a.col3) < 20 AND SUM(a.col3) <= 10 "
                + "AND AVG(a.col3) = 5"
        },
        new Object[]{"SELECT dateTrunc('DAY', ts) FROM a LIMIT 10"},
        new Object[]{"SELECT dateTrunc('DAY', a.ts + b.ts) FROM a JOIN b on a.col1 = b.col1 AND a.col2 = b.col2"},
        new Object[]{
            "SELECT a.col2, a.col3 FROM a JOIN b ON a.col1 = b.col1 "
                + " WHERE a.col3 >= 0 GROUP BY a.col2, a.col3"
        },
        new Object[]{
            "SELECT a.col1, b.col2 FROM a JOIN b ON a.col1 = b.col1 WHERE a.col2 IN ('foo', 'bar') AND"
                + " b.col2 NOT IN ('alice', 'charlie')"
        },
        new Object[]{"SELECT COUNT(*) OVER() FROM a"},
        new Object[]{"SELECT 42, COUNT(*) OVER() FROM a"},
        new Object[]{"SELECT a.col1, SUM(a.col3) OVER () FROM a"},
        new Object[]{"SELECT a.col1, SUM(a.col3) OVER (PARTITION BY a.col2) FROM a"},
        new Object[]{"SELECT a.col1, SUM(a.col3) OVER (PARTITION BY a.col2 ORDER BY a.col2) FROM a"},
        new Object[]{"SELECT a.col1, AVG(a.col3) OVER (), SUM(a.col3) OVER () FROM a"},
        new Object[]{"SELECT a.col1, SUM(a.col3) OVER () FROM a WHERE a.col3 >= 0"},
        new Object[]{
            "SELECT a.col1, SUM(a.col3) OVER (PARTITION BY a.col2), MIN(a.col3) OVER (PARTITION BY a.col2) "
                + "FROM a"
        },
        new Object[]{"SELECT a.col1, SUM(a.col3) OVER (PARTITION BY a.col2, a.col1) FROM a"},
        new Object[]{
            "SELECT a.col1, SUM(a.col3) OVER (ORDER BY a.col2, a.col1), MIN(a.col3) OVER (ORDER BY a.col2, "
                + "a.col1) FROM a"
        },
        new Object[]{"SELECT a.col1, ROW_NUMBER() OVER(PARTITION BY a.col2 ORDER BY a.col3) FROM a"},
        new Object[]{"SELECT RANK() OVER(PARTITION BY a.col2 ORDER BY a.col2) FROM a"},
        new Object[]{
            "SELECT col1, total, rank FROM (SELECT a.col1 as col1, count(*) as total, "
                + "RANK() OVER(ORDER BY count(*) DESC) AS rank FROM a GROUP BY a.col1) WHERE rank < 5"
        },
        new Object[]{"SELECT RANK() OVER(PARTITION BY a.col2 ORDER BY a.col1) FROM a"},
        new Object[]{"SELECT DENSE_RANK() OVER(ORDER BY a.col1) FROM a"},
        new Object[]{"SELECT a.col1, SUM(a.col3) OVER (ORDER BY a.col2), MIN(a.col3) OVER (ORDER BY a.col2) FROM a"},
        new Object[]{
            "SELECT /*+ aggOptions(is_partitioned_by_group_by_keys='true') */ a.col3, a.col1, SUM(b.col3) "
                + "FROM a JOIN b ON a.col3 = b.col3 GROUP BY a.col3, a.col1"
        },
        new Object[]{
            "SELECT /*+ aggOptions(is_skip_leaf_stage_group_by='true') */ a.col2, COUNT(*), SUM(a.col3), "
                + "SUM(a.col1) FROM a WHERE a.col3 >= 0 AND a.col2 = 'a' GROUP BY a.col2 HAVING COUNT(*) > 10 "
                + "AND MAX(a.col3) >= 0 AND MIN(a.col3) < 20 AND SUM(a.col3) <= 10 AND AVG(a.col3) = 5"
        },
        new Object[]{
            "SELECT /*+ aggOptions(is_skip_leaf_stage_group_by='true') */ a.col1, SUM(a.col3) FROM a "
                + "WHERE a.col3 >= 0 AND a.col2 = 'a' GROUP BY a.col1"
        },
        new Object[]{
            "SELECT /*+ aggOptions(is_skip_leaf_stage_group_by='true') */ a.col1, COUNT(*) FROM a "
                + "WHERE a.col3 >= 0 AND a.col2 = 'a' GROUP BY a.col1"
        },
        new Object[]{
            "SELECT /*+ aggOptions(is_skip_leaf_stage_group_by='true') */ a.col2, a.col1, SUM(a.col3) FROM a "
                + "WHERE a.col3 >= 0 AND a.col1 = 'a'  GROUP BY a.col1, a.col2"
        },
        new Object[]{
            "SELECT /*+ aggOptions(is_skip_leaf_stage_group_by='true') */ a.col1, AVG(b.col3) FROM a JOIN b "
                + "ON a.col1 = b.col2  WHERE a.col3 >= 0 AND a.col2 = 'a' AND b.col3 < 0 GROUP BY a.col1"
        },
        new Object[]{
            "SELECT /*+ aggOptions(is_skip_leaf_stage_group_by='true') */ a.col1 as v1, a.col1 as v2, "
                + "AVG(a.col3) FROM a GROUP BY v1, v2"
        },
        new Object[]{
            "SELECT /*+ aggOptions(is_skip_leaf_stage_group_by='true') */ a.col2, COUNT(*), SUM(a.col3), "
                + "SUM(a.col1) FROM a WHERE a.col3 >= 0 AND a.col2 = 'a' GROUP BY a.col2 HAVING COUNT(*) > 10 "
                + "AND MAX(a.col3) >= 0 AND MIN(a.col3) < 20 AND SUM(a.col3) <= 10 AND AVG(a.col3) = 5"
        },
        new Object[]{
            "SELECT /*+ aggOptions(is_skip_leaf_stage_group_by='true') */ a.col2, a.col3 FROM a JOIN b "
                + "ON a.col1 = b.col1  WHERE a.col3 >= 0 GROUP BY a.col2, a.col3"
        },
    };
  }

  public static QueryEnvironment getQueryEnvironment(int reducerPort, int port1, int port2,
      Map<String, Schema> schemaMap, Map<String, List<String>> segmentMap1, Map<String, List<String>> segmentMap2,
      @Nullable Map<String, Pair<String, List<List<String>>>> partitionedSegmentsMap) {
    return getQueryEnvironment(reducerPort, port1, port2, schemaMap, segmentMap1, segmentMap2, partitionedSegmentsMap,
        Collections.emptyMap());
  }

  public static QueryEnvironment getQueryEnvironment(int reducerPort, int port1, int port2,
      Map<String, Schema> schemaMap, Map<String, List<String>> segmentMap1, Map<String, List<String>> segmentMap2,
      @Nullable Map<String, Pair<String, List<List<String>>>> partitionedSegmentsMap,
      Map<String, Boolean> nullHandlingMap) {
    MockRoutingManagerFactory factory = new MockRoutingManagerFactory(port1, port2);
    for (Map.Entry<String, Schema> entry : schemaMap.entrySet()) {
      factory.registerTable(entry.getValue(), entry.getKey(), nullHandlingMap.getOrDefault(entry.getKey(), false));
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
    Map<String, TablePartitionInfo> partitionInfoMap = null;
    if (MapUtils.isNotEmpty(partitionedSegmentsMap)) {
      partitionInfoMap = new HashMap<>();
      for (Map.Entry<String, Pair<String, List<List<String>>>> entry : partitionedSegmentsMap.entrySet()) {
        String tableNameWithType = entry.getKey();
        String partitionColumn = entry.getValue().getLeft();
        List<List<String>> partitionIdToSegmentsMap = entry.getValue().getRight();
        int numPartitions = partitionIdToSegmentsMap.size();
        String hostname1 = MockRoutingManagerFactory.toHostname(port1);
        String hostname2 = MockRoutingManagerFactory.toHostname(port2);
        PartitionInfo[] partitionIdToInfoMap = new PartitionInfo[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
          String hostname = i < (numPartitions / 2) ? hostname1 : hostname2;
          partitionIdToInfoMap[i] = new PartitionInfo(Collections.singleton(hostname), partitionIdToSegmentsMap.get(i));
        }
        TablePartitionInfo tablePartitionInfo =
            new TablePartitionInfo(tableNameWithType, partitionColumn, "hashCode", numPartitions, partitionIdToInfoMap,
                Collections.emptyList());
        partitionInfoMap.put(tableNameWithType, tablePartitionInfo);
      }
    }
    RoutingManager routingManager = factory.buildRoutingManager(partitionInfoMap);
    TableCache tableCache = factory.buildTableCache();
    return new QueryEnvironment(new TypeFactory(new TypeSystem()),
        CalciteSchemaBuilder.asRootSchema(new PinotCatalog(tableCache)),
        new WorkerManager("localhost", reducerPort, routingManager), tableCache);
  }

  /**
   * JSON test case definition for query planner test cases. Tables and schemas will come from those already defined
   * and part of the {@code QueryEnvironment} in this base and are not part of the JSON definition for now.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class QueryPlanTestCase {
    // ignores the entire query test case
    @JsonProperty("ignored")
    public boolean _ignored;
    @JsonProperty("queries")
    public List<Query> _queries;

    @Override
    public String toString() {
      return "QueryPlanTestCase{" + "_ignored=" + _ignored + ", _queries=" + _queries + '}';
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Query {
      // ignores just a single query test from the test case
      @JsonProperty("ignored")
      public boolean _ignored;
      @JsonProperty("sql")
      public String _sql;
      @JsonProperty("description")
      public String _description;
      @JsonProperty("output")
      public List<String> _output = null;
      @JsonProperty("expectedException")
      public String _expectedException;

      @Override
      public String toString() {
        return "Query{" + "_ignored=" + _ignored + ", _sql='" + _sql + '\'' + ", _description='" + _description + '\''
            + ", _outputs=" + _output + ", _expectedException='" + _expectedException + '\'' + '}';
      }
    }
  }
}
