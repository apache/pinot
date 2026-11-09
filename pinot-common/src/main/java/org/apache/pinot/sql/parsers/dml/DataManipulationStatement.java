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
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.config.task.AdhocTaskConfig;


/// DML Statement
public interface DataManipulationStatement {
  /// The method to execute this Statement, e.g. MINION, HTTP or EXECUTOR. It tells which of the other methods apply
  /// to the statement, see [ExecutionType].
  /// @return
  ExecutionType getExecutionType();

  /// Generate minion task config for this statement.
  /// Only used for the [ExecutionType#MINION] ExecutionType.
  /// @return Adhoc minion task config
  AdhocTaskConfig generateAdhocTaskConfig();

  /// Execute the statement and format response to response row format.
  /// Only used for the [ExecutionType#HTTP] ExecutionType.
  /// @return Result rows
  List<Object[]> execute();

  /// Not used for the [ExecutionType#EXECUTOR] ExecutionType.
  /// @return Result schema for response
  DataSchema getResultSchema();

  /// Execution method for this SQL statement.
  enum ExecutionType {
    /// Executed in-process with [#execute()], which returns the rows of the response, described by
    /// [#getResultSchema()].
    HTTP,
    /// Executed as a minion task, configured with [#generateAdhocTaskConfig()]; the response lists the submitted
    /// tasks, described by [#getResultSchema()].
    MINION,
    /// Executed by the `SqlQueryExecutor` of the role that received the statement, through a statement-specific hook
    /// such as `SqlQueryExecutor#executeDelete`. [#generateAdhocTaskConfig()], [#execute()] and
    /// [#getResultSchema()] do not apply to such a statement and throw [UnsupportedOperationException].
    EXECUTOR
  }
}
