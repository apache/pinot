/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.client;

import java.util.concurrent.Future;


/**
 * A prepared statement, which is used to escape query parameters sent to Pinot.
 */
public class PreparedStatement {
  private final Connection _connection;
  private final String _statement;
  private final String[] _parameters;

  PreparedStatement(Connection connection, String statement) {
    _connection = connection;
    _statement = statement;

    int questionMarkCount = 0;
    int index = statement.indexOf('?');
    while (index != -1) {
      questionMarkCount++;
      index = statement.indexOf('?', index + 1);
    }

    _parameters = new String[questionMarkCount];
  }

  private String fillStatementWithParameters() {
    String statement = _statement;
    for (String parameter : _parameters) {
      statement = statement.replaceFirst("\\?", parameter);
    }
    return statement;
  }

  /**
   * Executes this prepared statement.
   *
   * @return The query results
   */
  public ResultSetGroup execute() {
    return _connection.execute(fillStatementWithParameters());
  }

  /**
   * Executes this prepared statement asynchronously.
   *
   * @return The query results
   */
  public Future<ResultSetGroup> executeAsync() {
    return _connection.executeAsync(fillStatementWithParameters());
  }

  /**
   * Replaces the given parameter by its value.
   *
   * @param parameterIndex The index of the parameter to replace
   * @param value The value of the parameter to replace
   */
  public void setString(int parameterIndex, String value) {
    _parameters[parameterIndex] = "'" + value.replace("'", "''") + "'";
  }

  /**
   * Replaces the given parameter by its value.
   *
   * @param parameterIndex The index of the parameter to replace
   * @param value The value of the parameter to replace
   */
  public void setInt(int parameterIndex, int value) {
    _parameters[parameterIndex] = String.valueOf(value);
  }

  /**
   * Replaces the given parameter by its value.
   *
   * @param parameterIndex The index of the parameter to replace
   * @param value The value of the parameter to replace
   */
  public void setLong(int parameterIndex, long value) {
    _parameters[parameterIndex] = String.valueOf(value);
  }

  /**
   * Replaces the given parameter by its value.
   *
   * @param parameterIndex The index of the parameter to replace
   * @param value The value of the parameter to replace
   */
  public void setFloat(int parameterIndex, float value) {
    _parameters[parameterIndex] = String.valueOf(value);
  }

  /**
   * Replaces the given parameter by its value.
   *
   * @param parameterIndex The index of the parameter to replace
   * @param value The value of the parameter to replace
   */
  public void setDouble(int parameterIndex, double value) {
    _parameters[parameterIndex] = String.valueOf(value);
  }
}
