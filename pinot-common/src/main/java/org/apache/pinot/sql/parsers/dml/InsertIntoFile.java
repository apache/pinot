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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.config.task.AdhocTaskConfig;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.apache.pinot.sql.parsers.parser.SqlInsertFromFile;


public class InsertIntoFile implements DataManipulationStatement {

  private static final DataSchema INSERT_FROM_FILE_RESPONSE_SCHEMA =
      new DataSchema(new String[]{"tableName", "taskJobName"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.STRING});
  private static final String TASK_NAME = "taskName";
  private static final String TASK_TYPE = "taskType";
  private static final String DEFAULT_TASK_TYPE = "SegmentGenerationAndPushTask";
  private static final String INPUT_DIR_URI = "inputDirURI";

  private final String _table;
  private final Map<String, String> _queryOptions;

  public InsertIntoFile(String table, Map<String, String> queryOptions) {
    _table = table;
    _queryOptions = queryOptions;
  }

  public String getTable() {
    return _table;
  }

  public Map<String, String> getQueryOptions() {
    return _queryOptions;
  }

  public static InsertIntoFile parse(SqlNodeAndOptions sqlNodeAndOptions) {
    SqlNode sqlNode = sqlNodeAndOptions.getSqlNode();
    assert sqlNode instanceof SqlInsertFromFile;
    SqlInsertFromFile sqlInsertFromFile = (SqlInsertFromFile) sqlNode;
    // operandList[0] : database name
    // operandList[1] : table name
    // operandList[2] : file list
    List<SqlNode> operandList = sqlInsertFromFile.getOperandList();
    // Set table
    String tableName = operandList.get(0) != null ? StringUtils.joinWith(",", operandList.get(0), operandList.get(1))
        : operandList.get(1).toString();
    // Set Options
    Map<String, String> optionsMap = sqlNodeAndOptions.getOptions();
    List<String> inputDirList = new ArrayList<>();
    ((SqlNodeList) operandList.get(2)).getList()
        .forEach(sqlNode1 -> inputDirList.add(sqlNode1.toString().replace("'", "")));
    optionsMap.put(INPUT_DIR_URI, StringUtils.join(inputDirList, ","));
    return new InsertIntoFile(tableName, optionsMap);
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
    return INSERT_FROM_FILE_RESPONSE_SCHEMA;
  }
}
