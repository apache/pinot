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
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;


public class PinotStatement implements Statement {

  private final org.apache.pinot.client.Connection _connection;
  private final String _queryFormat;
  private ResultSetGroup _resultSetGroup;

  public PinotStatement(org.apache.pinot.client.Connection connection) {
    _connection = connection;
    _queryFormat = "sql";
  }

  @Override
  public ResultSet executeQuery(String sql)
      throws SQLException {
    try {
      Request request = new Request(_queryFormat, sql);
      _resultSetGroup = _connection.execute(request);
      return new PinotResultSet(_resultSetGroup.getResultSet(0));
    } catch (PinotClientException e) {
      throw new SQLException("Failed to execute query : {}", sql, e);
    }
  }

  @Override
  public int executeUpdate(String s)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void close()
      throws SQLException {

  }

  @Override
  public int getMaxFieldSize()
      throws SQLException {
    return 0;
  }

  @Override
  public void setMaxFieldSize(int i)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxRows()
      throws SQLException {
    return 0;
  }

  @Override
  public void setMaxRows(int i)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setEscapeProcessing(boolean b)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getQueryTimeout()
      throws SQLException {
    return 0;
  }

  @Override
  public void setQueryTimeout(int i)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void cancel()
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public SQLWarning getWarnings()
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void clearWarnings()
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setCursorName(String cursorName)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean execute(String sql)
      throws SQLException {
    return false;
  }

  @Override
  public ResultSet getResultSet()
      throws SQLException {
    return null;
  }

  @Override
  public int getUpdateCount()
      throws SQLException {
    return 0;
  }

  @Override
  public boolean getMoreResults()
      throws SQLException {
    return false;
  }

  @Override
  public void setFetchDirection(int i)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getFetchDirection()
      throws SQLException {
    return 0;
  }

  @Override
  public void setFetchSize(int i)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getFetchSize()
      throws SQLException {
    return 0;
  }

  @Override
  public int getResultSetConcurrency()
      throws SQLException {
    return 0;
  }

  @Override
  public int getResultSetType()
      throws SQLException {
    return 0;
  }

  @Override
  public void addBatch(String s)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void clearBatch()
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int[] executeBatch()
      throws SQLException {
    return new int[0];
  }

  @Override
  public Connection getConnection()
      throws SQLException {
    return new PinotConnection(_connection);
  }

  @Override
  public boolean getMoreResults(int i)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getGeneratedKeys()
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int executeUpdate(String s, int i)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int executeUpdate(String s, int[] ints)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int executeUpdate(String s, String[] strings)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean execute(String s, int i)
      throws SQLException {
    return false;
  }

  @Override
  public boolean execute(String s, int[] ints)
      throws SQLException {
    return false;
  }

  @Override
  public boolean execute(String s, String[] strings)
      throws SQLException {
    return false;
  }

  @Override
  public int getResultSetHoldability()
      throws SQLException {
    return 0;
  }

  @Override
  public boolean isClosed()
      throws SQLException {
    return false;
  }

  @Override
  public void setPoolable(boolean b)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isPoolable()
      throws SQLException {
    return false;
  }

  @Override
  public void closeOnCompletion()
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isCloseOnCompletion()
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public <T> T unwrap(Class<T> aClass)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isWrapperFor(Class<?> aClass)
      throws SQLException {
    return false;
  }
}
