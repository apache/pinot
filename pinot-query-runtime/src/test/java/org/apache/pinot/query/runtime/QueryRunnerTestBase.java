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
package org.apache.pinot.query.runtime;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.math.DoubleMath;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.core.query.reduce.ExecutionStatsAggregator;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.QueryServerEnclosure;
import org.apache.pinot.query.QueryTestSet;
import org.apache.pinot.query.mailbox.GrpcMailboxService;
import org.apache.pinot.query.planner.QueryPlan;
import org.apache.pinot.query.planner.stage.MailboxReceiveNode;
import org.apache.pinot.query.routing.VirtualServer;
import org.apache.pinot.query.runtime.plan.DistributedStagePlan;
import org.apache.pinot.query.service.QueryConfig;
import org.apache.pinot.query.service.dispatch.QueryDispatcher;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.StringUtil;
import org.testng.Assert;


public abstract class QueryRunnerTestBase extends QueryTestSet {
  protected static final double DOUBLE_CMP_EPSILON = 0.0001d;
  protected static final String SEGMENT_BREAKER_KEY = "__SEGMENT_BREAKER_KEY__";
  protected static final String SEGMENT_BREAKER_STR = "------";
  protected static final GenericRow SEGMENT_BREAKER_ROW = new GenericRow();
  protected static final Random RANDOM_REQUEST_ID_GEN = new Random();
  protected QueryEnvironment _queryEnvironment;
  protected String _reducerHostname;
  protected int _reducerGrpcPort;
  protected Map<ServerInstance, QueryServerEnclosure> _servers = new HashMap<>();
  protected GrpcMailboxService _mailboxService;

  static {
    SEGMENT_BREAKER_ROW.putValue(SEGMENT_BREAKER_KEY, SEGMENT_BREAKER_STR);
  }

  // --------------------------------------------------------------------------
  // QUERY UTILS
  // --------------------------------------------------------------------------
  protected List<Object[]> queryRunner(String sql, Map<Integer, ExecutionStatsAggregator> executionStatsAggregatorMap) {
    QueryPlan queryPlan = _queryEnvironment.planQuery(sql);
    long requestId = RANDOM_REQUEST_ID_GEN.nextLong();
    Map<String, String> requestMetadataMap =
        ImmutableMap.of(QueryConfig.KEY_OF_BROKER_REQUEST_ID, String.valueOf(requestId),
            QueryConfig.KEY_OF_BROKER_REQUEST_TIMEOUT_MS,
            String.valueOf(CommonConstants.Broker.DEFAULT_BROKER_TIMEOUT_MS));
    int reducerStageId = -1;
    for (int stageId : queryPlan.getStageMetadataMap().keySet()) {
      if (queryPlan.getQueryStageMap().get(stageId) instanceof MailboxReceiveNode) {
        reducerStageId = stageId;
      } else {
        for (VirtualServer serverInstance : queryPlan.getStageMetadataMap().get(stageId).getServerInstances()) {
          DistributedStagePlan distributedStagePlan =
              QueryDispatcher.constructDistributedStagePlan(queryPlan, stageId, serverInstance);
          _servers.get(serverInstance.getServer()).processQuery(distributedStagePlan, requestMetadataMap);
        }
      }
      if (executionStatsAggregatorMap != null) {
        executionStatsAggregatorMap.put(stageId, new ExecutionStatsAggregator(true));
      }
    }
    Preconditions.checkState(reducerStageId != -1);
    ResultTable resultTable = QueryDispatcher.runReducer(requestId, queryPlan, reducerStageId,
        Long.parseLong(requestMetadataMap.get(QueryConfig.KEY_OF_BROKER_REQUEST_TIMEOUT_MS)), _mailboxService,
        executionStatsAggregatorMap);
    return resultTable.getRows();
  }

  protected List<Object[]> queryH2(String sql)
      throws Exception {
    int firstSemi = sql.indexOf(';');
    if (firstSemi > 0 && firstSemi != sql.length() - 1) {
      // trim off any SET statements for H2
      sql = sql.substring(firstSemi + 1);
    }
    Statement h2statement = _h2Connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    h2statement.execute(sql);
    ResultSet h2ResultSet = h2statement.getResultSet();
    int columnCount = h2ResultSet.getMetaData().getColumnCount();
    List<Object[]> result = new ArrayList<>();
    while (h2ResultSet.next()) {
      Object[] row = new Object[columnCount];
      for (int i = 0; i < columnCount; i++) {
        row[i] = h2ResultSet.getObject(i + 1);
      }
      result.add(row);
    }
    return result;
  }

  protected void compareRowEquals(List<Object[]> resultRows, List<Object[]> expectedRows) {
    compareRowEquals(resultRows, expectedRows, false);
  }

  protected void compareRowEquals(List<Object[]> resultRows, List<Object[]> expectedRows,
      boolean keepOutputRowsInOrder) {
    Assert.assertEquals(resultRows.size(), expectedRows.size(),
        String.format("Mismatched number of results. expected: %s, actual: %s",
            expectedRows.stream().map(Arrays::toString).collect(Collectors.joining(",\n")),
            resultRows.stream().map(Arrays::toString).collect(Collectors.joining(",\n"))));

    Comparator<Object> valueComp = (l, r) -> {
      if (l == null && r == null) {
        return 0;
      } else if (l == null) {
        return -1;
      } else if (r == null) {
        return 1;
      }
      if (l instanceof Integer) {
        return Integer.compare((Integer) l, ((Number) r).intValue());
      } else if (l instanceof Long) {
        return Long.compare((Long) l, ((Number) r).longValue());
      } else if (l instanceof Float) {
        if (DoubleMath.fuzzyEquals((Float) l, ((Number) r).floatValue(), DOUBLE_CMP_EPSILON)) {
          return 0;
        }
        return Float.compare((Float) l, ((Number) r).floatValue());
      } else if (l instanceof Double) {
        if (DoubleMath.fuzzyEquals((Double) l, ((Number) r).doubleValue(), DOUBLE_CMP_EPSILON)) {
          return 0;
        }
        return Double.compare((Double) l, ((Number) r).doubleValue());
      } else if (l instanceof String) {
        return ((String) l).compareTo((String) r);
      } else if (l instanceof Boolean) {
        return ((Boolean) l).compareTo((Boolean) r);
      } else if (l instanceof BigDecimal) {
        if (r instanceof BigDecimal) {
          return ((BigDecimal) l).compareTo((BigDecimal) r);
        } else {
          return ((BigDecimal) l).compareTo(new BigDecimal((String) r));
        }
      } else if (l instanceof ByteArray) {
        return ((ByteArray) l).compareTo(new ByteArray((byte[]) r));
      } else if (l instanceof Timestamp) {
        return ((Timestamp) l).compareTo((Timestamp) r);
      } else {
        throw new RuntimeException("non supported type " + l.getClass());
      }
    };
    Comparator<Object[]> rowComp = (l, r) -> {
      int cmp = 0;
      for (int i = 0; i < l.length; i++) {
        cmp = valueComp.compare(l[i], r[i]);
        if (cmp != 0) {
          return cmp;
        }
      }
      return 0;
    };
    if (!keepOutputRowsInOrder) {
      resultRows.sort(rowComp);
      expectedRows.sort(rowComp);
    }
    for (int i = 0; i < resultRows.size(); i++) {
      Object[] resultRow = resultRows.get(i);
      Object[] expectedRow = expectedRows.get(i);
      Assert.assertEquals(expectedRow.length, resultRow.length,
          String.format("Unexpected row size mismatch. Expected: %s, Actual: %s", Arrays.toString(expectedRow),
              Arrays.toString(resultRow)));
      for (int j = 0; j < resultRow.length; j++) {
        Assert.assertEquals(valueComp.compare(resultRow[j], expectedRow[j]), 0,
            "Not match at (" + i + "," + j + ")! Expected: " + Arrays.toString(expectedRow) + " Actual: "
                + Arrays.toString(resultRow));
      }
    }
  }

  // --------------------------------------------------------------------------
  // TEST CASES PREP
  // --------------------------------------------------------------------------
  protected Schema constructSchema(String schemaName, List<QueryTestCase.ColumnAndType> columnAndTypes) {
    Schema.SchemaBuilder builder = new Schema.SchemaBuilder();
    for (QueryTestCase.ColumnAndType columnAndType : columnAndTypes) {
      builder.addSingleValueDimension(columnAndType._name, FieldSpec.DataType.valueOf(columnAndType._type));
    }
    // TODO: ts is built-in, but we should allow user overwrite
    builder.addDateTime("ts", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:SECONDS");
    builder.setSchemaName(schemaName);
    return builder.build();
  }

  protected List<GenericRow> toRow(List<QueryTestCase.ColumnAndType> columnAndTypes, List<List<Object>> value) {
    List<GenericRow> result = new ArrayList<>(value.size());
    for (int rowId = 0; rowId < value.size(); rowId++) {
      GenericRow row = new GenericRow();
      List<Object> rawRow = value.get(rowId);
      if (rawRow.size() == 1 && SEGMENT_BREAKER_STR.equals(rawRow.get(0))) {
        result.add(SEGMENT_BREAKER_ROW);
      } else {
        int colId = 0;
        for (QueryTestCase.ColumnAndType columnAndType : columnAndTypes) {
          row.putValue(columnAndType._name, rawRow.get(colId++));
        }
        // TODO: ts is built-in, but we should allow user overwrite
        row.putValue("ts", System.currentTimeMillis());
        result.add(row);
      }
    }
    return result;
  }

  protected Connection _h2Connection;

  protected Connection getH2Connection() {
    Assert.assertNotNull(_h2Connection, "H2 Connection has not been initialized");
    return _h2Connection;
  }

  protected void setH2Connection()
      throws Exception {
    Assert.assertNull(_h2Connection);
    Class.forName("org.h2.Driver");
    _h2Connection = DriverManager.getConnection("jdbc:h2:mem:");
  }

  protected void addTableToH2(String tableName, Schema schema)
      throws SQLException {
    List<String> h2FieldNamesAndTypes = toH2FieldNamesAndTypes(schema);
    // create table
    _h2Connection.prepareCall("DROP TABLE IF EXISTS " + tableName).execute();
    _h2Connection.prepareCall("CREATE TABLE " + tableName + " (" + StringUtil.join(",",
        h2FieldNamesAndTypes.toArray(new String[h2FieldNamesAndTypes.size()])) + ")").execute();
  }

  protected void addDataToH2(String tableName, Schema schema, List<GenericRow> rows)
      throws SQLException {
    if (rows != null && rows.size() > 0) {
      // prepare the statement for ingestion
      List<String> h2FieldNamesAndTypes = toH2FieldNamesAndTypes(schema);
      StringBuilder params = new StringBuilder("?");
      for (int i = 0; i < h2FieldNamesAndTypes.size() - 1; i++) {
        params.append(",?");
      }
      PreparedStatement h2Statement =
          _h2Connection.prepareStatement("INSERT INTO " + tableName + " VALUES (" + params.toString() + ")");

      // insert data into table
      for (GenericRow row : rows) {
        int h2Index = 1;
        for (String fieldName : schema.getColumnNames()) {
          Object value = row.getValue(fieldName);
          h2Statement.setObject(h2Index++, value);
        }
        h2Statement.execute();
      }
    }
  }

  private static List<String> toH2FieldNamesAndTypes(org.apache.pinot.spi.data.Schema pinotSchema) {
    List<String> fieldNamesAndTypes = new ArrayList<>(pinotSchema.size());
    for (String fieldName : pinotSchema.getColumnNames()) {
      FieldSpec.DataType dataType = pinotSchema.getFieldSpecFor(fieldName).getDataType();
      String fieldType;
      switch (dataType) {
        case INT:
        case LONG:
          fieldType = "bigint";
          break;
        case STRING:
          fieldType = "varchar(128)";
          break;
        case FLOAT:
          fieldType = "real";
          break;
        case DOUBLE:
          fieldType = "double";
          break;
        case BOOLEAN:
          fieldType = "BOOLEAN";
          break;
        case BIG_DECIMAL:
          fieldType = "NUMERIC";
          break;
        case BYTES:
          fieldType = "BYTEA";
          break;
        case TIMESTAMP:
          fieldType = "TIMESTAMP";
          break;
        default:
          throw new UnsupportedOperationException("Unsupported type conversion to h2 type: " + dataType);
      }
      fieldNamesAndTypes.add(fieldName + " " + fieldType);
    }
    return fieldNamesAndTypes;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class QueryTestCase {
    public static final String BLOCK_SIZE_KEY = "blockSize";
    public static final String SERVER_ASSIGN_STRATEGY_KEY = "serverSelectionStrategy";

    // ignores the entire query test case
    @JsonProperty("ignored")
    public boolean _ignored;
    @JsonProperty("tables")
    public Map<String, Table> _tables;
    @JsonProperty("queries")
    public List<Query> _queries;
    @JsonProperty("extraProps")
    public Map<String, Object> _extraProps = Collections.emptyMap();

    public static class Table {
      @JsonProperty("schema")
      public List<ColumnAndType> _schema;
      @JsonProperty("inputs")
      public List<List<Object>> _inputs;
      @JsonProperty("partitionColumns")
      public List<String> _partitionColumns;
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
      @JsonProperty("outputs")
      public List<List<Object>> _outputs = null;
      @JsonProperty("expectedException")
      public String _expectedException;
      @JsonProperty("keepOutputRowOrder")
      public boolean _keepOutputRowOrder;
    }

    public static class ColumnAndType {
      @JsonProperty("name")
      String _name;
      @JsonProperty("type")
      String _type;
    }
  }
}
