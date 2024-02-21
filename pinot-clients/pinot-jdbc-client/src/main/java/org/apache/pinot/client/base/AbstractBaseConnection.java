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
package org.apache.pinot.client.base;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;


public abstract class AbstractBaseConnection implements Connection {
  protected abstract void validateState()
      throws SQLException;

  @Override
  public void abort(Executor executor)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void clearWarnings()
      throws SQLException {
    // no-op
  }

  @Override
  public void commit()
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Blob createBlob()
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Clob createClob()
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public NClob createNClob()
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public SQLXML createSQLXML()
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean getAutoCommit()
      throws SQLException {
    return false;
  }

  @Override
  public void setAutoCommit(boolean autoCommit)
      throws SQLException {
    // no-op
  }

  @Override
  public String getCatalog()
      throws SQLException {
    return null;
  }

  @Override
  public void setCatalog(String catalog)
      throws SQLException {
    // no-op
  }

  @Override
  public String getClientInfo(String name)
      throws SQLException {
    return null;
  }

  @Override
  public Properties getClientInfo()
      throws SQLException {
    return null;
  }

  @Override
  public void setClientInfo(Properties properties)
      throws SQLClientInfoException {
    // no-op
  }

  @Override
  public int getHoldability()
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setHoldability(int holdability)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getNetworkTimeout()
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getSchema()
      throws SQLException {
    return null;
  }

  @Override
  public void setSchema(String schema)
      throws SQLException {
    // no-op
  }

  @Override
  public int getTransactionIsolation()
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setTransactionIsolation(int level)
      throws SQLException {
    // no-op
  }

  @Override
  public Map<String, Class<?>> getTypeMap()
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public SQLWarning getWarnings()
      throws SQLException {
    return null;
  }

  @Override
  public boolean isReadOnly()
      throws SQLException {
    return false;
  }

  @Override
  public void setReadOnly(boolean readOnly)
      throws SQLException {
    // no-op
  }

  @Override
  public boolean isValid(int timeout)
      throws SQLException {
    return !isClosed();
  }

  @Override
  public boolean isWrapperFor(Class<?> iface)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String nativeSQL(String sql)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public CallableStatement prepareCall(String sql)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
      int resultSetHoldability)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public java.sql.PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public java.sql.PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
      int resultSetHoldability)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public java.sql.PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public java.sql.PreparedStatement prepareStatement(String sql, int[] columnIndexes)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void rollback()
      throws SQLException {
    // no-op
  }

  @Override
  public void rollback(Savepoint savepoint)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setClientInfo(String name, String value)
      throws SQLClientInfoException {
    // no-op
  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Savepoint setSavepoint()
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Savepoint setSavepoint(String name)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public <T> T unwrap(Class<T> iface)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }
}
