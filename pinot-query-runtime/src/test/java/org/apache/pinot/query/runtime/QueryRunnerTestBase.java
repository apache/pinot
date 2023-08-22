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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.NamedThreadFactory;
import org.apache.pinot.core.query.reduce.ExecutionStatsAggregator;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.QueryServerEnclosure;
import org.apache.pinot.query.QueryTestSet;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.DispatchableSubPlan;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.executor.OpChainSchedulerService;
import org.apache.pinot.query.runtime.operator.OperatorTestUtil;
import org.apache.pinot.query.runtime.plan.DistributedStagePlan;
import org.apache.pinot.query.runtime.plan.StageMetadata;
import org.apache.pinot.query.service.QueryConfig;
import org.apache.pinot.query.service.dispatch.QueryDispatcher;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.StringUtil;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.h2.jdbc.JdbcArray;
import org.testng.Assert;


public abstract class QueryRunnerTestBase extends QueryTestSet {
  // TODO: Find a better way to create the global test executor
  public static final ExecutorService EXECUTOR =
      Executors.newCachedThreadPool(new NamedThreadFactory("worker_on_" + OperatorTestUtil.class.getSimpleName()) {
        @Override
        public Thread newThread(Runnable r) {
          Thread thread = super.newThread(r);
          thread.setDaemon(true);
          return thread;
        }
      });
  protected static final double DOUBLE_CMP_EPSILON = 0.0001d;
  protected static final String SEGMENT_BREAKER_KEY = "__SEGMENT_BREAKER_KEY__";
  protected static final String SEGMENT_BREAKER_STR = "------";
  protected static final GenericRow SEGMENT_BREAKER_ROW = new GenericRow();
  protected static final Random RANDOM_REQUEST_ID_GEN = new Random();
  protected QueryEnvironment _queryEnvironment;
  protected String _reducerHostname;
  protected int _reducerGrpcPort;
  protected Map<QueryServerInstance, QueryServerEnclosure> _servers = new HashMap<>();
  protected MailboxService _mailboxService;
  protected OpChainSchedulerService _reducerScheduler;

  static {
    SEGMENT_BREAKER_ROW.putValue(SEGMENT_BREAKER_KEY, SEGMENT_BREAKER_STR);
  }

  // --------------------------------------------------------------------------
  // QUERY UTILS
  // --------------------------------------------------------------------------

  /**
   * Dispatch query to each pinot-server. The logic should mimic QueryDispatcher.submit() but does not actually make
   * ser/de dispatches.
   */
  protected List<Object[]> queryRunner(String sql, Map<Integer, ExecutionStatsAggregator> executionStatsAggregatorMap) {
    long requestId = RANDOM_REQUEST_ID_GEN.nextLong();
    SqlNodeAndOptions sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(sql);
    QueryEnvironment.QueryPlannerResult queryPlannerResult =
        _queryEnvironment.planQuery(sql, sqlNodeAndOptions, requestId);
    DispatchableSubPlan dispatchableSubPlan = queryPlannerResult.getQueryPlan();
    Map<String, String> requestMetadataMap = new HashMap<>();
    requestMetadataMap.put(QueryConfig.KEY_OF_BROKER_REQUEST_ID, String.valueOf(requestId));
    requestMetadataMap.put(QueryConfig.KEY_OF_BROKER_REQUEST_TIMEOUT_MS,
        String.valueOf(CommonConstants.Broker.DEFAULT_BROKER_TIMEOUT_MS));
    requestMetadataMap.put(CommonConstants.Broker.Request.QueryOptionKey.ENABLE_NULL_HANDLING, "true");
    requestMetadataMap.putAll(sqlNodeAndOptions.getOptions());

    // Putting trace testing here as extra options as it doesn't go along with the rest of the items.
    if (executionStatsAggregatorMap != null) {
      requestMetadataMap.put(CommonConstants.Broker.Request.TRACE, "true");
    }

    int reducerStageId = -1;
    for (int stageId = 0; stageId < dispatchableSubPlan.getQueryStageList().size(); stageId++) {
      if (dispatchableSubPlan.getQueryStageList().get(stageId).getPlanFragment()
          .getFragmentRoot() instanceof MailboxReceiveNode) {
        reducerStageId = stageId;
      } else {
        processDistributedStagePlans(dispatchableSubPlan, stageId, requestMetadataMap);
      }
      if (executionStatsAggregatorMap != null) {
        executionStatsAggregatorMap.put(stageId, new ExecutionStatsAggregator(true));
      }
    }
    Preconditions.checkState(reducerStageId != -1);
    ResultTable resultTable = QueryDispatcher.runReducer(requestId, dispatchableSubPlan, reducerStageId,
        Long.parseLong(requestMetadataMap.get(QueryConfig.KEY_OF_BROKER_REQUEST_TIMEOUT_MS)), _mailboxService,
        _reducerScheduler, executionStatsAggregatorMap, true);
    return resultTable.getRows();
  }

  protected void processDistributedStagePlans(DispatchableSubPlan dispatchableSubPlan, int stageId,
      Map<String, String> requestMetadataMap) {
    Map<QueryServerInstance, List<Integer>> serverInstanceToWorkerIdMap =
        dispatchableSubPlan.getQueryStageList().get(stageId).getServerInstanceToWorkerIdMap();
    for (Map.Entry<QueryServerInstance, List<Integer>> entry : serverInstanceToWorkerIdMap.entrySet()) {
      QueryServerInstance server = entry.getKey();
      for (int workerId : entry.getValue()) {
        DistributedStagePlan distributedStagePlan = constructDistributedStagePlan(
            dispatchableSubPlan, stageId, new VirtualServerAddress(server, workerId));
        _servers.get(server).processQuery(distributedStagePlan, requestMetadataMap);
      }
    }
  }

  protected static DistributedStagePlan constructDistributedStagePlan(DispatchableSubPlan dispatchableSubPlan,
      int stageId, VirtualServerAddress serverAddress) {
    return new DistributedStagePlan(stageId, serverAddress,
        dispatchableSubPlan.getQueryStageList().get(stageId).getPlanFragment().getFragmentRoot(),
        new StageMetadata.Builder().setWorkerMetadataList(
                dispatchableSubPlan.getQueryStageList().get(stageId).getWorkerMetadataList())
            .addCustomProperties(dispatchableSubPlan.getQueryStageList().get(stageId).getCustomProperties()).build());
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
        float lf = (Float) l;
        float rf = ((Number) r).floatValue();
        if (DoubleMath.fuzzyEquals(lf, rf, DOUBLE_CMP_EPSILON)) {
          return 0;
        }
        float maxf = Math.max(Math.abs(lf), Math.abs(rf));
        if (DoubleMath.fuzzyEquals(lf / maxf, rf / maxf, DOUBLE_CMP_EPSILON)) {
          return 0;
        }
        return Float.compare(lf, rf);
      } else if (l instanceof Double) {
        double ld = (Double) l;
        double rd = ((Number) r).doubleValue();
        if (DoubleMath.fuzzyEquals(ld, rd, DOUBLE_CMP_EPSILON)) {
          return 0;
        }
        double maxd = Math.max(Math.abs(ld), Math.abs(rd));
        if (DoubleMath.fuzzyEquals(ld / maxd, rd / maxd, DOUBLE_CMP_EPSILON)) {
          return 0;
        }
        return Double.compare(ld, rd);
      } else if (l instanceof String) {
        if (r instanceof byte[]) {
          return ((String) l).compareTo(BytesUtils.toHexString((byte[]) r));
        } else if (r instanceof Timestamp) {
          return ((String) l).compareTo((r).toString());
        }
        return ((String) l).compareTo((String) r);
      } else if (l instanceof Boolean) {
        return ((Boolean) l).compareTo((Boolean) r);
      } else if (l instanceof BigDecimal) {
        if (r instanceof BigDecimal) {
          return ((BigDecimal) l).compareTo((BigDecimal) r);
        } else {
          return ((BigDecimal) l).compareTo(new BigDecimal((String) r));
        }
      } else if (l instanceof byte[]) {
        if (r instanceof byte[]) {
          return ByteArray.compare((byte[]) l, (byte[]) r);
        } else {
          return ByteArray.compare((byte[]) l, ((ByteArray) r).getBytes());
        }
      } else if (l instanceof ByteArray) {
        if (r instanceof ByteArray) {
          return ((ByteArray) l).compareTo((ByteArray) r);
        } else {
          return ByteArray.compare(((ByteArray) l).getBytes(), (byte[]) r);
        }
      } else if (l instanceof Timestamp) {
        return ((Timestamp) l).compareTo((Timestamp) r);
      } else if (l instanceof int[]) {
        int[] larray = (int[]) l;
        try {
          if (r instanceof JdbcArray) {
            Object[] rarray = (Object[]) ((JdbcArray) r).getArray();
            for (int idx = 0; idx < larray.length; idx++) {
              Number relement = (Number) rarray[idx];
              if (larray[idx] != relement.intValue()) {
                return -1;
              }
            }
          } else {
            int[] rarray = (int[]) r;
            for (int idx = 0; idx < larray.length; idx++) {
              if (larray[idx] != rarray[idx]) {
                return -1;
              }
            }
          }
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
        return 0;
      } else if (l instanceof String[]) {
        String[] larray = (String[]) l;
        try {
          if (r instanceof JdbcArray) {
            Object[] rarray = (Object[]) ((JdbcArray) r).getArray();
            for (int idx = 0; idx < larray.length; idx++) {
              if (!larray[idx].equals(rarray[idx])) {
                return -1;
              }
            }
          } else {
            String[] rarray = (r instanceof List) ? ((List<String>) r).toArray(new String[0]) : (String[]) r;
            for (int idx = 0; idx < larray.length; idx++) {
              if (!larray[idx].equals(rarray[idx])) {
                return -1;
              }
            }
          }
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
        return 0;
      } else if (l instanceof JdbcArray) {
        try {
          Object[] larray = (Object[]) ((JdbcArray) l).getArray();
          Object[] rarray = (Object[]) ((JdbcArray) r).getArray();
          for (int idx = 0; idx < larray.length; idx++) {
            if (!larray[idx].equals(rarray[idx])) {
              return -1;
            }
          }
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
        return 0;
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
      if (columnAndType._isSingleValue) {
        builder.addSingleValueDimension(columnAndType._name, FieldSpec.DataType.valueOf(columnAndType._type));
      } else {
        builder.addMultiValueDimension(columnAndType._name, FieldSpec.DataType.valueOf(columnAndType._type));
      }
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
      throws SQLException, DecoderException {
    if (rows != null && rows.size() > 0) {
      // prepare the statement for ingestion
      List<String> h2FieldNamesAndTypes = toH2FieldNamesAndTypes(schema);
      StringBuilder params = new StringBuilder("?");
      for (int i = 0; i < h2FieldNamesAndTypes.size() - 1; i++) {
        params.append(",?");
      }
      PreparedStatement h2Statement =
          _h2Connection.prepareStatement("INSERT INTO " + tableName + " VALUES (" + params + ")");

      // insert data into table
      for (GenericRow row : rows) {
        int h2Index = 1;
        for (String fieldName : schema.getColumnNames()) {
          Object value = row.getValue(fieldName);
          if (value instanceof List) {
            h2Statement.setArray(h2Index++,
                _h2Connection.createArrayOf(getH2FieldType(schema.getFieldSpecFor(fieldName).getDataType()),
                    ((List) value).toArray()));
          } else {
            switch (schema.getFieldSpecFor(fieldName).getDataType()) {
              case BYTES:
                h2Statement.setBytes(h2Index++, Hex.decodeHex((String) value));
                break;
              default:
                h2Statement.setObject(h2Index++, value);
                break;
            }
          }
        }
        h2Statement.execute();
      }
    }
  }

  private static List<String> toH2FieldNamesAndTypes(org.apache.pinot.spi.data.Schema pinotSchema) {
    List<String> fieldNamesAndTypes = new ArrayList<>(pinotSchema.size());
    for (String fieldName : pinotSchema.getColumnNames()) {
      FieldSpec.DataType dataType = pinotSchema.getFieldSpecFor(fieldName).getDataType();
      String fieldType = getH2FieldType(dataType);
      if (!pinotSchema.getFieldSpecFor(fieldName).isSingleValueField()) {
        fieldType += " ARRAY";
      }
      fieldNamesAndTypes.add(fieldName + " " + fieldType);
    }
    return fieldNamesAndTypes;
  }

  private static String getH2FieldType(FieldSpec.DataType dataType) {
    switch (dataType) {
      case INT:
      case LONG:
        return "bigint";
      case STRING:
        return "varchar(128)";
      case FLOAT:
        return "real";
      case DOUBLE:
        return "double";
      case BOOLEAN:
        return "BOOLEAN";
      case BIG_DECIMAL:
        return "NUMERIC(1200, 600)";
      case BYTES:
        return "BYTEA";
      case TIMESTAMP:
        return "TIMESTAMP";
      default:
        throw new UnsupportedOperationException("Unsupported type conversion to h2 type: " + dataType);
    }
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
      @JsonProperty("h2Sql")
      public String _h2Sql;
      @JsonProperty("description")
      public String _description;
      @JsonProperty("outputs")
      public List<List<Object>> _outputs = null;
      @JsonProperty("expectedException")
      public String _expectedException;
      @JsonProperty("keepOutputRowOrder")
      public boolean _keepOutputRowOrder;
      @JsonProperty("expectedNumSegments")
      public Integer _expectedNumSegments;
    }

    public static class ColumnAndType {
      @JsonProperty("name")
      String _name;
      @JsonProperty("type")
      String _type;
      @JsonProperty("isSingleValue")
      boolean _isSingleValue = true;
    }
  }
}
