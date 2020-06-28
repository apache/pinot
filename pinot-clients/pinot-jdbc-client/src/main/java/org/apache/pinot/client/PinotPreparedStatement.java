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
package org.apache.pinot.client;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.commons.codec.binary.Hex;
import org.apache.pinot.client.base.AbstractBasePreparedStatement;


public class PinotPreparedStatement extends AbstractBasePreparedStatement {

  private static final String QUERY_FORMAT = "sql";
  private Connection _connection;
  private org.apache.pinot.client.Connection _session;
  private ResultSetGroup _resultSetGroup;
  private PreparedStatement _preparedStatement;
  private String _query;

  public PinotPreparedStatement(PinotConnection connection, String query) {
    _connection = connection;
    _session = connection.getSession();
    _query = query;
    _preparedStatement = new PreparedStatement(_session, new Request(QUERY_FORMAT, query));
  }

  @Override
  public void setNull(int parameterIndex, int sqlType)
      throws SQLException {
    _preparedStatement.setString(parameterIndex - 1, "NULL");
  }

  @Override
  public void setBoolean(int parameterIndex, boolean x)
      throws SQLException {
    _preparedStatement.setString(parameterIndex - 1, String.valueOf(x));
  }

  @Override
  public void setShort(int parameterIndex, short x)
      throws SQLException {
    _preparedStatement.setInt(parameterIndex - 1, x);
  }

  @Override
  public void setInt(int parameterIndex, int x)
      throws SQLException {
    _preparedStatement.setInt(parameterIndex - 1, x);
  }

  @Override
  public void setLong(int parameterIndex, long x)
      throws SQLException {
    _preparedStatement.setLong(parameterIndex - 1, x);
  }

  @Override
  public void setFloat(int parameterIndex, float x)
      throws SQLException {
    _preparedStatement.setFloat(parameterIndex - 1, x);
  }

  @Override
  public void setDouble(int parameterIndex, double x)
      throws SQLException {
    _preparedStatement.setDouble(parameterIndex - 1, x);
  }

  @Override
  public void setString(int parameterIndex, String x)
      throws SQLException {
    _preparedStatement.setString(parameterIndex - 1, x);
  }

  @Override
  public void setBytes(int parameterIndex, byte[] x)
      throws SQLException {
    _preparedStatement.setString(parameterIndex - 1, Hex.encodeHexString(x));
  }

  @Override
  public void clearParameters()
      throws SQLException {
    _preparedStatement = new PreparedStatement(_session, new Request(QUERY_FORMAT, _query));
  }

  @Override
  public ResultSet executeQuery()
      throws SQLException {
    try {
      _resultSetGroup = _preparedStatement.execute();

      if (_resultSetGroup.getResultSetCount() == 0) {
        return new PinotResultSet();
      }
      return new PinotResultSet(_resultSetGroup.getResultSet(0));
    } catch (PinotClientException e) {
      throw new SQLException("Failed to execute query : {}", _query, e);
    }
  }

  @Override
  public void close()
      throws SQLException {
    _preparedStatement = null;
    _connection = null;
    _session = null;
  }

  @Override
  public Connection getConnection()
      throws SQLException {
    return _connection;
  }

  @Override
  public boolean isClosed()
      throws SQLException {
    return (_connection == null);
  }
}
