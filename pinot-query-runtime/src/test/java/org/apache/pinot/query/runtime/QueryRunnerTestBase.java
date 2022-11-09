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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.QueryServerEnclosure;
import org.apache.pinot.query.QueryTestSet;
import org.apache.pinot.query.mailbox.GrpcMailboxService;
import org.apache.pinot.query.planner.QueryPlan;
import org.apache.pinot.query.planner.stage.MailboxReceiveNode;
import org.apache.pinot.query.runtime.operator.MailboxReceiveOperator;
import org.apache.pinot.query.runtime.plan.DistributedStagePlan;
import org.apache.pinot.query.service.QueryDispatcher;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.StringUtil;
import org.testng.Assert;



public class QueryRunnerTestBase extends QueryTestSet {
  protected static final Random RANDOM_REQUEST_ID_GEN = new Random();

  protected QueryEnvironment _queryEnvironment;
  protected String _reducerHostname;
  protected int _reducerGrpcPort;
  protected Map<ServerInstance, QueryServerEnclosure> _servers = new HashMap<>();
  protected GrpcMailboxService _mailboxService;

  // --------------------------------------------------------------------------
  // QUERY UTILS
  // --------------------------------------------------------------------------
  protected List<Object[]> queryRunner(String sql) {
    QueryPlan queryPlan = _queryEnvironment.planQuery(sql);
    Map<String, String> requestMetadataMap =
        ImmutableMap.of("REQUEST_ID", String.valueOf(RANDOM_REQUEST_ID_GEN.nextLong()));
    MailboxReceiveOperator mailboxReceiveOperator = null;
    for (int stageId : queryPlan.getStageMetadataMap().keySet()) {
      if (queryPlan.getQueryStageMap().get(stageId) instanceof MailboxReceiveNode) {
        MailboxReceiveNode reduceNode = (MailboxReceiveNode) queryPlan.getQueryStageMap().get(stageId);
        mailboxReceiveOperator = QueryDispatcher.createReduceStageOperator(_mailboxService,
            queryPlan.getStageMetadataMap().get(reduceNode.getSenderStageId()).getServerInstances(),
            Long.parseLong(requestMetadataMap.get("REQUEST_ID")), reduceNode.getSenderStageId(),
            reduceNode.getDataSchema(), "localhost", _reducerGrpcPort);
      } else {
        for (ServerInstance serverInstance : queryPlan.getStageMetadataMap().get(stageId).getServerInstances()) {
          DistributedStagePlan distributedStagePlan =
              QueryDispatcher.constructDistributedStagePlan(queryPlan, stageId, serverInstance);
          _servers.get(serverInstance).processQuery(distributedStagePlan, requestMetadataMap);
        }
      }
    }
    Preconditions.checkNotNull(mailboxReceiveOperator);
    return QueryDispatcher.toResultTable(QueryDispatcher.reduceMailboxReceive(mailboxReceiveOperator),
        queryPlan.getQueryResultFields(), queryPlan.getQueryStageMap().get(0).getDataSchema()).getRows();
  }

  protected List<Object[]> queryH2(String sql)
      throws Exception {
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
    Assert.assertEquals(resultRows.size(), expectedRows.size());

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
        return Float.compare((Float) l, ((Number) r).floatValue());
      } else if (l instanceof Double) {
        return Double.compare((Double) l, ((Number) r).doubleValue());
      } else if (l instanceof String) {
        return ((String) l).compareTo((String) r);
      } else if (l instanceof Boolean) {
        return ((Boolean) l).compareTo((Boolean) r);
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
    resultRows.sort(rowComp);
    expectedRows.sort(rowComp);
    for (int i = 0; i < resultRows.size(); i++) {
      Object[] resultRow = resultRows.get(i);
      Object[] expectedRow = expectedRows.get(i);
      for (int j = 0; j < resultRow.length; j++) {
        Assert.assertEquals(valueComp.compare(resultRow[j], expectedRow[j]), 0,
            "Not match at (" + i + "," + j + ")! Expected: " + expectedRow[j] + " Actual: " + resultRow[j]);
      }
    }
  }

  // --------------------------------------------------------------------------
  // TEST CASES PREP
  // --------------------------------------------------------------------------
  protected Schema constructSchema(String schemaName, List<String> dataTypes) {
    Schema.SchemaBuilder builder = new Schema.SchemaBuilder();
    for (int i = 0; i < dataTypes.size(); i++) {
      builder.addSingleValueDimension("col" + i, FieldSpec.DataType.valueOf(dataTypes.get(i)));
    }
    // ts is built in
    builder.addDateTime("ts", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:HOURS");
    builder.setSchemaName(schemaName);
    return builder.build();
  }

  protected List<GenericRow> toRow(List<Object[]> value) {
    List<GenericRow> result = new ArrayList<>(value.size());
    for (int rowId = 0; rowId < value.size(); rowId++) {
      GenericRow row = new GenericRow();
      Object[] rawRow = value.get(rowId);
      for (int colId = 0; colId < rawRow.length; colId++) {
        row.putValue("col" + colId, rawRow[colId]);
      }
      row.putValue("ts", System.currentTimeMillis());
      result.add(row);
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

  protected void addTableToH2(Schema schema, List<String> tables)
      throws SQLException {
    List<String> h2FieldNamesAndTypes = toH2FieldNamesAndTypes(schema);
    for (String tableName : tables) {
      // create table
      _h2Connection.prepareCall("DROP TABLE IF EXISTS " + tableName).execute();
      _h2Connection.prepareCall("CREATE TABLE " + tableName + " (" + StringUtil.join(",",
          h2FieldNamesAndTypes.toArray(new String[h2FieldNamesAndTypes.size()])) + ")").execute();
    }
  }

  protected void addDataToH2(Schema schema, Map<String, List<GenericRow>> rowsMap)
      throws SQLException {
    List<String> h2FieldNamesAndTypes = toH2FieldNamesAndTypes(schema);
    for (Map.Entry<String, List<GenericRow>> entry : rowsMap.entrySet()) {
      String tableName = entry.getKey();
      // remove the "_O" and "_R" suffix b/c H2 doesn't understand realtime/offline split
      if (tableName.contains("_")) {
        tableName = tableName.substring(0, tableName.length() - 2);
      }
      // insert data into table
      StringBuilder params = new StringBuilder("?");
      for (int i = 0; i < h2FieldNamesAndTypes.size() - 1; i++) {
        params.append(",?");
      }
      PreparedStatement h2Statement =
          _h2Connection.prepareStatement("INSERT INTO " + tableName + " VALUES (" + params.toString() + ")");
      for (GenericRow row : entry.getValue()) {
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
        default:
          throw new UnsupportedOperationException("Unsupported type conversion to h2 type: " + dataType);
      }
      fieldNamesAndTypes.add(fieldName + " " + fieldType);
    }
    return fieldNamesAndTypes;
  }

  public static class TestCase {
    public String _sql;
    public String _name;
    public Map<String, List<String>> _tableSchemas;
    public Map<String, List<Object[]>> _tableInputs;
  }
}
