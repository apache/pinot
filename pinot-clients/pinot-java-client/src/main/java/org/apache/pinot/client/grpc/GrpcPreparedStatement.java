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
package org.apache.pinot.client.grpc;

import java.io.IOException;
import java.util.Iterator;
import org.apache.pinot.common.proto.Broker;


/**
 * A prepared statement, which is used to escape query parameters sent to Pinot.
 */
public class GrpcPreparedStatement {
  private final GrpcConnection _connection;
  private final String _statement;
  private final String[] _parameters;

  public GrpcPreparedStatement(GrpcConnection connection, String query) {
    _connection = connection;
    _statement = query;
    _parameters = new String[getQuestionMarkCount(query)];
  }

  private int getQuestionMarkCount(String query) {
    int questionMarkCount = 0;
    int index = query.indexOf('?');
    while (index != -1) {
      questionMarkCount++;
      index = query.indexOf('?', index + 1);
    }
    return questionMarkCount;
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
  public GrpcResultSetGroup execute()
      throws IOException {
    return _connection.executeGrpc(fillStatementWithParameters());
  }

  /**
   * Executes this prepared statement asynchronously.
   *
   * @return The query results
   */
  public Iterator<Broker.BrokerResponse> executeWithIterator() {
    return _connection.executeWithIterator(fillStatementWithParameters());
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
