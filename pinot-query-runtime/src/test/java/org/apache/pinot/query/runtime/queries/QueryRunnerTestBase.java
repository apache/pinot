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
package org.apache.pinot.query.runtime.queries;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.query.reduce.ExecutionStatsAggregator;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.QueryServerEnclosure;
import org.apache.pinot.query.QueryTestSet;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.DispatchablePlanFragment;
import org.apache.pinot.query.planner.DispatchableSubPlan;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.plan.DistributedStagePlan;
import org.apache.pinot.query.runtime.plan.StageMetadata;
import org.apache.pinot.query.service.dispatch.QueryDispatcher;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.StringUtil;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.h2.jdbc.JdbcArray;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public abstract class QueryRunnerTestBase extends QueryTestSet {
  protected static final double DOUBLE_CMP_EPSILON = 0.0001d;
  protected static final String SEGMENT_BREAKER_KEY = "__SEGMENT_BREAKER_KEY__";
  protected static final String SEGMENT_BREAKER_STR = "------";
  protected static final GenericRow SEGMENT_BREAKER_ROW = new GenericRow();
  protected static final AtomicLong REQUEST_ID_GEN = new AtomicLong();

  static {
    SEGMENT_BREAKER_ROW.putValue(SEGMENT_BREAKER_KEY, SEGMENT_BREAKER_STR);
  }

  protected String _reducerHostname;
  protected int _reducerPort;
  protected MailboxService _mailboxService;
  protected QueryEnvironment _queryEnvironment;
  protected Map<QueryServerInstance, QueryServerEnclosure> _servers = new HashMap<>();

  // --------------------------------------------------------------------------
  // QUERY UTILS
  // --------------------------------------------------------------------------

  /**
   * Dispatch query to each pinot-server. The logic should mimic QueryDispatcher.submit() but does not actually make
   * ser/de dispatches.
   */
  protected ResultTable queryRunner(String sql, Map<Integer, ExecutionStatsAggregator> executionStatsAggregatorMap) {
    long requestId = REQUEST_ID_GEN.getAndIncrement();
    SqlNodeAndOptions sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(sql);
    QueryEnvironment.QueryPlannerResult queryPlannerResult =
        _queryEnvironment.planQuery(sql, sqlNodeAndOptions, requestId);
    DispatchableSubPlan dispatchableSubPlan = queryPlannerResult.getQueryPlan();
    Map<String, String> requestMetadataMap = new HashMap<>();
    requestMetadataMap.put(CommonConstants.Query.Request.MetadataKeys.REQUEST_ID, String.valueOf(requestId));
    Long timeoutMsInQueryOption = QueryOptionsUtils.getTimeoutMs(sqlNodeAndOptions.getOptions());
    long timeoutMs =
        timeoutMsInQueryOption != null ? timeoutMsInQueryOption : CommonConstants.Broker.DEFAULT_BROKER_TIMEOUT_MS;
    requestMetadataMap.put(CommonConstants.Broker.Request.QueryOptionKey.TIMEOUT_MS, String.valueOf(timeoutMs));
    requestMetadataMap.put(CommonConstants.Broker.Request.QueryOptionKey.ENABLE_NULL_HANDLING, "true");
    requestMetadataMap.putAll(sqlNodeAndOptions.getOptions());

    // Putting trace testing here as extra options as it doesn't go along with the rest of the items.
    if (executionStatsAggregatorMap != null) {
      requestMetadataMap.put(CommonConstants.Broker.Request.TRACE, "true");
    }

    // Submission Stub logic are mimic {@link QueryServer}
    List<DispatchablePlanFragment> stagePlans = dispatchableSubPlan.getQueryStageList();
    List<CompletableFuture<?>> submissionStubs = new ArrayList<>();
    for (int stageId = 0; stageId < stagePlans.size(); stageId++) {
      if (stageId != 0) {
        submissionStubs.addAll(processDistributedStagePlans(dispatchableSubPlan, stageId, requestMetadataMap));
      }
      if (executionStatsAggregatorMap != null) {
        executionStatsAggregatorMap.put(stageId, new ExecutionStatsAggregator(true));
      }
    }
    try {
      CompletableFuture.allOf(submissionStubs.toArray(new CompletableFuture[0])).get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      // wrap and throw the exception here is for assert purpose on dispatch-time error
      throw new RuntimeException("Error occurred during stage submission: " + QueryException.getTruncatedStackTrace(e));
    } finally {
      // Cancel all ongoing submission
      for (CompletableFuture<?> future : submissionStubs) {
        if (!future.isDone()) {
          future.cancel(true);
        }
      }
    }
    // exception will be propagated through for assert purpose on runtime error
    return QueryDispatcher.runReducer(requestId, dispatchableSubPlan, timeoutMs, Collections.emptyMap(),
        executionStatsAggregatorMap, _mailboxService);
  }

  protected List<CompletableFuture<?>> processDistributedStagePlans(DispatchableSubPlan dispatchableSubPlan,
      int stageId, Map<String, String> requestMetadataMap) {
    Map<QueryServerInstance, List<Integer>> serverInstanceToWorkerIdMap =
        dispatchableSubPlan.getQueryStageList().get(stageId).getServerInstanceToWorkerIdMap();
    List<CompletableFuture<?>> submissionStubs = new ArrayList<>();
    for (Map.Entry<QueryServerInstance, List<Integer>> entry : serverInstanceToWorkerIdMap.entrySet()) {
      QueryServerInstance server = entry.getKey();
      for (int workerId : entry.getValue()) {
        DistributedStagePlan distributedStagePlan =
            constructDistributedStagePlan(dispatchableSubPlan, stageId, new VirtualServerAddress(server, workerId));
        submissionStubs.add(_servers.get(server).processQuery(distributedStagePlan, requestMetadataMap));
      }
    }
    return submissionStubs;
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

  protected void compareRowEquals(ResultTable resultTable, List<Object[]> expectedRows, String sql) {
    compareRowEquals(resultTable, expectedRows, sql, false);
  }

  protected void compareRowEquals(ResultTable resultTable, List<Object[]> expectedRows, String sql,
      boolean keepOutputRowsInOrder) {
    List<Object[]> resultRows = resultTable.getRows();
    int numRows = resultRows.size();
    assertEquals(numRows, expectedRows.size(), String.format("Mismatched number of results for %s. expected: %s, "
            + "actual: %s",
        sql,
        expectedRows.stream().map(Arrays::toString).collect(Collectors.joining(",\n")),
        resultRows.stream().map(Arrays::toString).collect(Collectors.joining(",\n"))));

    DataSchema dataSchema = resultTable.getDataSchema();
    resultRows.forEach(row -> canonicalizeRow(dataSchema, row));
    expectedRows.forEach(row -> canonicalizeRow(dataSchema, row));
    if (!keepOutputRowsInOrder) {
      sortRows(resultRows);
      sortRows(expectedRows);
    }
    for (int i = 0; i < numRows; i++) {
      Object[] resultRow = resultRows.get(i);
      Object[] expectedRow = expectedRows.get(i);
      assertEquals(resultRow.length, expectedRow.length,
          String.format("Unexpected row size mismatch for %s. Expected: %s, Actual: %s", sql,
              Arrays.toString(expectedRow), Arrays.toString(resultRow)));
      for (int j = 0; j < resultRow.length; j++) {
        assertTrue(typeCompatibleFuzzyEquals(dataSchema.getColumnDataType(j), resultRow[j], expectedRow[j]),
            "Not match for " + sql + " at (" + i + "," + j + ")! Expected: " + Arrays.toString(expectedRow)
                + " Actual: " + Arrays.toString(resultRow));
      }
    }
  }

  protected static void canonicalizeRow(DataSchema dataSchema, Object[] row) {
    for (int i = 0; i < row.length; i++) {
      row[i] = canonicalizeValue(dataSchema.getColumnDataType(i), row[i]);
    }
  }

  protected static Object canonicalizeValue(ColumnDataType columnDataType, Object value) {
    if (value == null) {
      return null;
    }
    switch (columnDataType) {
      case INT:
        return ((Number) value).intValue();
      case LONG:
        return ((Number) value).longValue();
      case FLOAT:
        return ((Number) value).floatValue();
      case DOUBLE:
        return ((Number) value).doubleValue();
      case BIG_DECIMAL:
        if (value instanceof String) {
          return new BigDecimal((String) value);
        }
        assertTrue(value instanceof BigDecimal, "Got unexpected value type: " + value.getClass()
            + " for BIG_DECIMAL column, expected: String or BigDecimal");
        return value;
      case BOOLEAN:
        assertTrue(value instanceof Boolean,
            "Got unexpected value type: " + value.getClass() + " for BOOLEAN column, expected: Boolean");
        return value;
      case TIMESTAMP:
        if (value instanceof String) {
          return Timestamp.valueOf((String) value);
        }
        assertTrue(value instanceof Timestamp,
            "Got unexpected value type: " + value.getClass() + " for TIMESTAMP column, expected: String or Timestamp");
        return value;
      case STRING:
        assertTrue(value instanceof String,
            "Got unexpected value type: " + value.getClass() + " for STRING column, expected: String");
        return value;
      case BYTES:
        if (value instanceof byte[]) {
          return BytesUtils.toHexString((byte[]) value);
        }
        assertTrue(value instanceof String,
            "Got unexpected value type: " + value.getClass() + " for BYTES column, expected: String or byte[]");
        return value;
      case INT_ARRAY:
        if (value instanceof JdbcArray) {
          try {
            Object[] array = (Object[]) ((JdbcArray) value).getArray();
            int[] intArray = new int[array.length];
            for (int i = 0; i < array.length; i++) {
              intArray[i] = ((Number) array[i]).intValue();
            }
            return intArray;
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        }
        assertTrue(value instanceof int[],
            "Got unexpected value type: " + value.getClass() + " for INT_ARRAY column, expected: int[] or JdbcArray");
        return value;
      case STRING_ARRAY:
        if (value instanceof List) {
          return ((List) value).toArray(new String[0]);
        }
        if (value instanceof JdbcArray) {
          try {
            Object[] array = (Object[]) ((JdbcArray) value).getArray();
            String[] stringArray = new String[array.length];
            for (int i = 0; i < array.length; i++) {
              stringArray[i] = (String) array[i];
            }
            return stringArray;
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        }
        assertTrue(value instanceof String[], "Got unexpected value type: " + value.getClass()
            + " for STRING_ARRAY column, expected: String[], List or JdbcArray");
        return value;
      default:
        throw new UnsupportedOperationException("Unsupported ColumnDataType: " + columnDataType);
    }
  }

  protected static void sortRows(List<Object[]> rows) {
    Comparator<Object> valueComparator = (v1, v2) -> {
      if (v1 == null && v2 == null) {
        return 0;
      } else if (v1 == null) {
        return -1;
      } else if (v2 == null) {
        return 1;
      }
      if (v1 instanceof Comparable) {
        return ((Comparable) v1).compareTo(v2);
      }
      if (v1 instanceof int[]) {
        return Arrays.compare((int[]) v1, (int[]) v2);
      }
      if (v1 instanceof String[]) {
        return Arrays.compare((String[]) v1, (String[]) v2);
      }
      throw new UnsupportedOperationException("Unsupported class: " + v1.getClass());
    };
    rows.sort((r1, r2) -> {
      for (int i = 0; i < r1.length; i++) {
        int cmp = valueComparator.compare(r1[i], r2[i]);
        if (cmp != 0) {
          return cmp;
        }
      }
      return 0;
    });
  }

  protected static boolean typeCompatibleFuzzyEquals(ColumnDataType columnDataType, @Nullable Object actual,
      @Nullable Object expected) {
    if (actual == null || expected == null) {
      return actual == expected;
    }

    switch (columnDataType) {
      case INT:
        return (int) actual == ((Number) expected).intValue();
      case LONG:
        return (long) actual == ((Number) expected).longValue();
      case FLOAT:
        float actualFloat = (float) actual;
        float expectedFloat = ((Number) expected).floatValue();
        if (DoubleMath.fuzzyEquals(actualFloat, expectedFloat, DOUBLE_CMP_EPSILON)) {
          return true;
        }
        float maxFloat = Math.max(Math.abs(actualFloat), Math.abs(expectedFloat));
        return DoubleMath.fuzzyEquals(actualFloat / maxFloat, expectedFloat / maxFloat, DOUBLE_CMP_EPSILON);
      case DOUBLE:
        double actualDouble = (double) actual;
        double expectedDouble = ((Number) expected).doubleValue();
        if (DoubleMath.fuzzyEquals(actualDouble, expectedDouble, DOUBLE_CMP_EPSILON)) {
          return true;
        }
        double maxDouble = Math.max(Math.abs(actualDouble), Math.abs(expectedDouble));
        return DoubleMath.fuzzyEquals(actualDouble / maxDouble, expectedDouble / maxDouble, DOUBLE_CMP_EPSILON);
      case BIG_DECIMAL:
        // Use compare to handle different scale
        return ((BigDecimal) actual).compareTo((BigDecimal) expected) == 0;
      case BOOLEAN:
      case TIMESTAMP:
      case STRING:
      case BYTES:
        return actual.equals(expected);
      case INT_ARRAY:
        return Arrays.equals((int[]) actual, (int[]) expected);
      case STRING_ARRAY:
        return Arrays.equals((String[]) actual, (String[]) expected);
      default:
        throw new UnsupportedOperationException("Unsupported ColumnDataType: " + columnDataType);
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
    Schema schema = builder.build();
    for (QueryTestCase.ColumnAndType columnAndType : columnAndTypes) {
      schema.getFieldSpecMap().get(columnAndType._name).setNullable(columnAndType._nullable);
    }
    return schema;
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
    assertNotNull(_h2Connection, "H2 Connection has not been initialized");
    return _h2Connection;
  }

  protected void setH2Connection()
      throws Exception {
    assertNull(_h2Connection);
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
      @JsonProperty("nullable")
      Boolean _nullable = null;
    }
  }
}
