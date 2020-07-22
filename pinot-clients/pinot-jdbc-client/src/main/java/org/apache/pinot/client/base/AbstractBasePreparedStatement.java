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

import java.io.InputStream;
import java.io.Reader;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;


public abstract class AbstractBasePreparedStatement extends AbstractBaseStatement implements PreparedStatement {
  protected abstract void validateState()
      throws SQLException;

  @Override
  public ResultSet executeQuery(String sql)
      throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int executeUpdate()
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setByte(int parameterIndex, byte x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, int length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setUnicodeStream(int parameterIndex, InputStream x, int length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, int length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setObject(int parameterIndex, Object x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean execute()
      throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addBatch()
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, int length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setRef(int parameterIndex, Ref x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setBlob(int parameterIndex, Blob x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setClob(int parameterIndex, Clob x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setArray(int parameterIndex, Array x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSetMetaData getMetaData()
      throws SQLException {
    return null;
  }

  @Override
  public void setDate(int parameterIndex, Date x, Calendar cal)
      throws SQLException {
    setDate(parameterIndex, x);
  }

  @Override
  public void setTime(int parameterIndex, Time x, Calendar cal)
      throws SQLException {
    setTime(parameterIndex, x);
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
      throws SQLException {
    setTimestamp(parameterIndex, x);
  }

  @Override
  public void setNull(int parameterIndex, int sqlType, String typeName)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setURL(int parameterIndex, URL x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ParameterMetaData getParameterMetaData()
      throws SQLException {
    return null;
  }

  @Override
  public void setRowId(int parameterIndex, RowId x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setNString(int parameterIndex, String value)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setNClob(int parameterIndex, NClob value)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setClob(int parameterIndex, Reader reader, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setSQLXML(int parameterIndex, SQLXML xmlObject)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setClob(int parameterIndex, Reader reader)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setObject(int parameterIndex, Object x, SQLType targetSqlType)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public long executeLargeUpdate()
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }
}
