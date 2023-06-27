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

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import org.apache.commons.codec.binary.Hex;
import org.apache.pinot.client.base.AbstractBasePreparedStatement;
import org.apache.pinot.client.utils.DateTimeUtils;
import org.apache.pinot.client.utils.DriverUtils;


public class PinotPreparedStatement extends AbstractBasePreparedStatement {
  private static final String LIMIT_STATEMENT = "LIMIT";

  private PinotConnection _connection;
  private org.apache.pinot.client.Connection _session;
  private ResultSetGroup _resultSetGroup;
  private PreparedStatement _preparedStatement;
  private String _query;
  private boolean _closed;
  private ResultSet _resultSet;
  private int _maxRows = Integer.MAX_VALUE;

  public PinotPreparedStatement(PinotConnection connection, String query) {
    _connection = connection;
    _session = connection.getSession();
    _closed = false;
    _query = query;
    if (!DriverUtils.queryContainsLimitStatement(_query)) {
      _query += " " + LIMIT_STATEMENT + " " + _maxRows;
    }
    _query = DriverUtils.enableQueryOptions(_query, _connection.getQueryOptions());
    _preparedStatement = new PreparedStatement(_session, _query);
  }

  @Override
  protected void validateState()
      throws SQLException {
    if (isClosed()) {
      throw new SQLException("Connection is already closed!");
    }
  }

  @Override
  public void setNull(int parameterIndex, int sqlType)
      throws SQLException {
    validateState();
    _preparedStatement.setString(parameterIndex - 1, "NULL");
  }

  @Override
  public void setBoolean(int parameterIndex, boolean x)
      throws SQLException {
    validateState();
    _preparedStatement.setString(parameterIndex - 1, String.valueOf(x));
  }

  @Override
  public void setShort(int parameterIndex, short x)
      throws SQLException {
    validateState();
    _preparedStatement.setInt(parameterIndex - 1, x);
  }

  @Override
  public void setInt(int parameterIndex, int x)
      throws SQLException {
    validateState();
    _preparedStatement.setInt(parameterIndex - 1, x);
  }

  @Override
  public void setLong(int parameterIndex, long x)
      throws SQLException {
    validateState();
    _preparedStatement.setLong(parameterIndex - 1, x);
  }

  @Override
  public void setFloat(int parameterIndex, float x)
      throws SQLException {
    validateState();
    _preparedStatement.setFloat(parameterIndex - 1, x);
  }

  @Override
  public void setDouble(int parameterIndex, double x)
      throws SQLException {
    validateState();
    _preparedStatement.setDouble(parameterIndex - 1, x);
  }

  @Override
  public void setString(int parameterIndex, String x)
      throws SQLException {
    validateState();
    _preparedStatement.setString(parameterIndex - 1, x);
  }

  @Override
  public void setBytes(int parameterIndex, byte[] x)
      throws SQLException {
    validateState();
    _preparedStatement.setString(parameterIndex - 1, Hex.encodeHexString(x));
  }

  @Override
  public void setDate(int parameterIndex, Date x)
      throws SQLException {
    _preparedStatement.setString(parameterIndex - 1, DateTimeUtils.dateToString(x));
  }

  @Override
  public void setTime(int parameterIndex, Time x)
      throws SQLException {
    validateState();
    _preparedStatement.setString(parameterIndex - 1, DateTimeUtils.timeToString(x));
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x)
      throws SQLException {
    validateState();
    _preparedStatement.setString(parameterIndex - 1, DateTimeUtils.timeStampToString(x));
  }

  @Override
  public void setBigDecimal(int parameterIndex, BigDecimal x)
      throws SQLException {
    validateState();
    _preparedStatement.setString(parameterIndex - 1, x.toString());
  }

  @Override
  public void clearParameters()
      throws SQLException {
    validateState();
    _preparedStatement = new PreparedStatement(_session, _query);
  }

  @Override
  public boolean execute()
      throws SQLException {
    _resultSet = executeQuery();
    if (_resultSet.next()) {
      _resultSet.beforeFirst();
      return true;
    } else {
      return false;
    }
  }

  @Override
  public ResultSet executeQuery(String sql)
      throws SQLException {
    validateState();
    try {
      _resultSetGroup = _session.execute(DriverUtils.enableQueryOptions(sql, _connection.getQueryOptions()));
      if (_resultSetGroup.getResultSetCount() == 0) {
        _resultSet = PinotResultSet.empty();
        return _resultSet;
      }
      _resultSet = new PinotResultSet(_resultSetGroup.getResultSet(0));
      return _resultSet;
    } catch (PinotClientException e) {
      throw new SQLException(String.format("Failed to execute query : %s", sql), e);
    }
  }

  @Override
  public ResultSet executeQuery()
      throws SQLException {
    validateState();
    try {
      _resultSetGroup = _preparedStatement.execute();

      if (_resultSetGroup.getResultSetCount() == 0) {
        _resultSet = PinotResultSet.empty();
      } else {
        _resultSet = new PinotResultSet(_resultSetGroup.getResultSet(0));
      }
      return _resultSet;
    } catch (PinotClientException e) {
      throw new SQLException("Failed to execute query : {}", _query, e);
    }
  }

  @Override
  public boolean execute(String sql)
      throws SQLException {
    _resultSet = executeQuery(sql);
    return _resultSet != null;
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys)
      throws SQLException {
    return execute(sql);
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes)
      throws SQLException {
    return execute(sql);
  }

  @Override
  public boolean execute(String sql, String[] columnNames)
      throws SQLException {
    return execute(sql);
  }

  @Override
  public ResultSet getResultSet()
      throws SQLException {
    return _resultSet;
  }

  @Override
  public void close()
      throws SQLException {
    _preparedStatement = null;
    _connection = null;
    _session = null;
    _closed = true;
  }

  @Override
  public Connection getConnection()
      throws SQLException {
    validateState();
    return _connection;
  }

  @Override
  public int getFetchSize()
      throws SQLException {
    return _maxRows;
  }

  @Override
  public void setFetchSize(int rows)
      throws SQLException {
    _maxRows = rows;
  }

  @Override
  public int getMaxRows()
      throws SQLException {
    return _maxRows;
  }

  @Override
  public void setMaxRows(int max)
      throws SQLException {
    _maxRows = max;
  }

  @Override
  public boolean isClosed()
      throws SQLException {
    return _closed;
  }
}
