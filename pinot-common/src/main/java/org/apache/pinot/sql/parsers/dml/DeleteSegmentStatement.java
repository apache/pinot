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
package org.apache.pinot.sql.parsers.dml;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlNode;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.config.task.AdhocTaskConfig;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DeleteSegmentStatement implements DataManipulationStatement {
  private static final Logger LOGGER = LoggerFactory.getLogger(DeleteSegmentStatement.class);
  private static final DataSchema DELETE_FROM_RESPONSE_SCHEMA =
      new DataSchema(new String[]{"tableName", "taskJobName"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.STRING});
  private static final Set<String> VALID_OPERATORS = Set.of("LIKE", "=");
  private static final String TASK_NAME = "taskName";
  private static final String TASK_TYPE = "taskType";
  private static final String DEFAULT_TASK_TYPE = "SegmentDeletionTask";
  private static final String SEGMENT_NAME = "segmentName";
  private static final String OPERATOR = "operator";

  private final String _table;
  private final Map<String, String> _queryOptions;

  public DeleteSegmentStatement(String table, Map<String, String> queryOptions) {
    _table = table;
    _queryOptions = queryOptions;
  }

  public String getTable() {
    return _table;
  }

  public Map<String, String> getQueryOptions() {
    return _queryOptions;
  }

  public static DeleteSegmentStatement parse(SqlNodeAndOptions sqlNodeAndOptions) {
    SqlNode sqlNode = sqlNodeAndOptions.getSqlNode();
    assert sqlNode instanceof SqlDelete;
    SqlDelete sqlDelete = (SqlDelete) sqlNode;
    String tableName = sqlDelete.getTargetTable().toString();
    SqlBasicCall condition = (SqlBasicCall) sqlDelete.getCondition();
    if (condition == null) {
      throw new IllegalStateException("missing WHERE clause");
    }
    List<SqlNode> operandList = condition.getOperandList();
    if (operandList.size() != 2) {
      throw new IllegalStateException("expected 2 operands. Found: " + operandList.size());
    }

    String operator = (condition.getOperator() == null) ? null : condition.getOperator().toString().toUpperCase();

    if (!VALID_OPERATORS.contains(operator)) {
      throw new IllegalStateException("unsupported operator: " + operator);
    }

    Map<String, String> optionsMap = sqlNodeAndOptions.getOptions();
    optionsMap.put(OPERATOR, operator);

    if (operandList.get(0).toString().equals("$segmentName")) {
      optionsMap.put(SEGMENT_NAME, operandList.get(1).toString());
    } else {
      throw new IllegalStateException("missing $segmentName in WHERE clause");
    }

    return new DeleteSegmentStatement(tableName, optionsMap);
  }

  @Override
  public ExecutionType getExecutionType() {
    return ExecutionType.MINION;
  }

  @Override
  public AdhocTaskConfig generateAdhocTaskConfig() {
    Map<String, String> queryOptions = this.getQueryOptions();
    String taskName = queryOptions.get(TASK_NAME);
    String taskType = queryOptions.getOrDefault(TASK_TYPE, DEFAULT_TASK_TYPE);
    return new AdhocTaskConfig(taskType, this.getTable(), taskName, queryOptions);
  }

  @Override
  public List<Object[]> execute() {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public DataSchema getResultSchema() {
    return DELETE_FROM_RESPONSE_SCHEMA;
  }
}
