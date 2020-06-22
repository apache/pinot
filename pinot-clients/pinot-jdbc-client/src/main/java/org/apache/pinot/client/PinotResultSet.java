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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;


public class PinotResultSet implements ResultSet {
  private final org.apache.pinot.client.ResultSet _resultSet;
  private int _totalRows;
  private int _currentRow;
  private Map<String, Integer> _columns = new HashMap<>();

  public PinotResultSet(org.apache.pinot.client.ResultSet resultSet) {
    _resultSet = resultSet;
    _totalRows = _resultSet.getRowCount();
    _currentRow = -1;
    for (int i = 0; i < _resultSet.getColumnCount(); i++) {
      _columns.put(_resultSet.getColumnName(i), i);
    }
  }

  @Override
  public boolean next()
      throws SQLException {
    _currentRow++;
    boolean hasNext = _currentRow < _totalRows;
    return hasNext;
  }

  @Override
  public void close()
      throws SQLException {

  }

  @Override
  public boolean wasNull()
      throws SQLException {
    throw new SQLFeatureNotSupportedException(); 
  }

  @Override
  public String getString(int i)
      throws SQLException {
    return _resultSet.getString(_currentRow, i);
  }

  @Override
  public boolean getBoolean(int i)
      throws SQLException {
    return Boolean.parseBoolean(_resultSet.getString(_currentRow, i));
  }

  @Override
  public byte getByte(int i)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Pinot JDBC client does not support byte arrays");
  }

  @Override
  public short getShort(int i)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Pinot JDBC client does not support short ints");
  }

  @Override
  public int getInt(int i)
      throws SQLException {
    return _resultSet.getInt(_currentRow, i);
  }

  @Override
  public long getLong(int i)
      throws SQLException {
    return _resultSet.getLong(_currentRow, i);
  }

  @Override
  public float getFloat(int i)
      throws SQLException {
    return _resultSet.getFloat(_currentRow, i);
  }

  @Override
  public double getDouble(int i)
      throws SQLException {
    return _resultSet.getDouble(_currentRow, i);
  }

  @Override
  public BigDecimal getBigDecimal(int i, int i1)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Pinot JDBC client does not support short ints");
  }

  @Override
  public byte[] getBytes(int i)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Pinot JDBC client does not support short ints");
  }

  @Override
  public Date getDate(int i)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Pinot JDBC client does not support short ints");
  }

  @Override
  public Time getTime(int i)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Pinot JDBC client does not support short ints");
  }

  @Override
  public Timestamp getTimestamp(int i)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Pinot JDBC client does not support short ints");
  }

  @Override
  public InputStream getAsciiStream(int i)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Pinot JDBC client does not support short ints");
  }

  @Override
  public InputStream getUnicodeStream(int i)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Pinot JDBC client does not support short ints");
  }

  @Override
  public InputStream getBinaryStream(int i)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("Pinot JDBC client does not support short ints");
  }

  @Override
  public String getString(String s)
      throws SQLException {
    return getString(_columns.get(s));
  }

  @Override
  public boolean getBoolean(String s)
      throws SQLException {
    return getBoolean(_columns.get(s));
  }

  @Override
  public byte getByte(String s)
      throws SQLException {
    return 0;
  }

  @Override
  public short getShort(String s)
      throws SQLException {
    return 0;
  }

  @Override
  public int getInt(String s)
      throws SQLException {
    return getInt(_columns.get(s));
  }

  @Override
  public long getLong(String s)
      throws SQLException {
    return getLong(_columns.get(s));
  }

  @Override
  public float getFloat(String s)
      throws SQLException {
    return getFloat(_columns.get(s));
  }

  @Override
  public double getDouble(String s)
      throws SQLException {
    return getDouble(_columns.get(s));
  }

  @Override
  public BigDecimal getBigDecimal(String s, int i)
      throws SQLException {
    return null;
  }

  @Override
  public byte[] getBytes(String s)
      throws SQLException {
    return new byte[0];
  }

  @Override
  public Date getDate(String s)
      throws SQLException {
    return null;
  }

  @Override
  public Time getTime(String s)
      throws SQLException {
    return null;
  }

  @Override
  public Timestamp getTimestamp(String s)
      throws SQLException {
    return null;
  }

  @Override
  public InputStream getAsciiStream(String s)
      throws SQLException {
    return null;
  }

  @Override
  public InputStream getUnicodeStream(String s)
      throws SQLException {
    return null;
  }

  @Override
  public InputStream getBinaryStream(String s)
      throws SQLException {
    return null;
  }

  @Override
  public SQLWarning getWarnings()
      throws SQLException {
    return null;
  }

  @Override
  public void clearWarnings()
      throws SQLException {

  }

  @Override
  public String getCursorName()
      throws SQLException {
    return null;
  }

  @Override
  public ResultSetMetaData getMetaData()
      throws SQLException {
    return null;
  }

  @Override
  public Object getObject(int i)
      throws SQLException {
    return null;
  }

  @Override
  public Object getObject(String s)
      throws SQLException {
    return null;
  }

  @Override
  public int findColumn(String s)
      throws SQLException {
    return 0;
  }

  @Override
  public Reader getCharacterStream(int i)
      throws SQLException {
    return null;
  }

  @Override
  public Reader getCharacterStream(String s)
      throws SQLException {
    return null;
  }

  @Override
  public BigDecimal getBigDecimal(int i)
      throws SQLException {
    return null;
  }

  @Override
  public BigDecimal getBigDecimal(String s)
      throws SQLException {
    return null;
  }

  @Override
  public boolean isBeforeFirst()
      throws SQLException {
    return false;
  }

  @Override
  public boolean isAfterLast()
      throws SQLException {
    return false;
  }

  @Override
  public boolean isFirst()
      throws SQLException {
    return _currentRow == 0;
  }

  @Override
  public boolean isLast()
      throws SQLException {
    return _currentRow == _totalRows - 1;
  }

  @Override
  public void beforeFirst()
      throws SQLException {
    _currentRow = -1;
  }

  @Override
  public void afterLast()
      throws SQLException {
    _currentRow = _totalRows;
  }

  @Override
  public boolean first()
      throws SQLException {
    _currentRow = 0;
    return true;
  }

  @Override
  public boolean last()
      throws SQLException {
    _currentRow = _totalRows - 1;
    return true;
  }

  @Override
  public int getRow()
      throws SQLException {
    return _currentRow;
  }

  @Override
  public boolean absolute(int row)
      throws SQLException {
    if (row >= 0 && row < _totalRows) {
      _currentRow = row;
      return true;
    } else if (row < 0 && Math.abs(row) <= _totalRows) {
      _currentRow = _totalRows + row;
      return true;
    }

    return false;
  }

  @Override
  public boolean relative(int rows)
      throws SQLException {
    int nextRow = _currentRow + rows;
    if (nextRow >= 0 && nextRow < _totalRows) {
      _currentRow = nextRow;
      return true;
    }
    return false;
  }

  @Override
  public boolean previous()
      throws SQLException {
    if (!isBeforeFirst()) {
      _currentRow--;
      return true;
    }
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
  public int getType()
      throws SQLException {
    return 0;
  }

  @Override
  public int getConcurrency()
      throws SQLException {
    return 0;
  }

  @Override
  public boolean rowUpdated()
      throws SQLException {
    return false;
  }

  @Override
  public boolean rowInserted()
      throws SQLException {
    return false;
  }

  @Override
  public boolean rowDeleted()
      throws SQLException {
    return false;
  }

  @Override
  public void updateNull(int i)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBoolean(int i, boolean b)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateByte(int i, byte b)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateShort(int i, short i1)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateInt(int i, int i1)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateLong(int i, long l)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateFloat(int i, float v)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateDouble(int i, double v)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBigDecimal(int i, BigDecimal bigDecimal)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateString(int i, String s)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBytes(int i, byte[] bytes)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateDate(int i, Date date)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateTime(int i, Time time)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateTimestamp(int i, Timestamp timestamp)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(int i, InputStream inputStream, int i1)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(int i, InputStream inputStream, int i1)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(int i, Reader reader, int i1)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(int i, Object o, int i1)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(int i, Object o)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNull(String s)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBoolean(String s, boolean b)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateByte(String s, byte b)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateShort(String s, short i)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateInt(String s, int i)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateLong(String s, long l)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateFloat(String s, float v)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateDouble(String s, double v)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBigDecimal(String s, BigDecimal bigDecimal)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateString(String s, String s1)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBytes(String s, byte[] bytes)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateDate(String s, Date date)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateTime(String s, Time time)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateTimestamp(String s, Timestamp timestamp)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(String s, InputStream inputStream, int i)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(String s, InputStream inputStream, int i)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(String s, Reader reader, int i)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(String s, Object o, int i)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(String s, Object o)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void insertRow()
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRow()
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void deleteRow()
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void refreshRow()
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void cancelRowUpdates()
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void moveToInsertRow()
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void moveToCurrentRow()
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Statement getStatement()
      throws SQLException {
    return null;
  }

  @Override
  public Object getObject(int i, Map<String, Class<?>> map)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Ref getRef(int i)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Blob getBlob(int i)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Clob getClob(int i)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Array getArray(int i)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Object getObject(String s, Map<String, Class<?>> map)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Ref getRef(String s)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Blob getBlob(String s)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Clob getClob(String s)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Array getArray(String s)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Date getDate(int i, Calendar calendar)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Date getDate(String s, Calendar calendar)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Time getTime(int i, Calendar calendar)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Time getTime(String s, Calendar calendar)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Timestamp getTimestamp(int i, Calendar calendar)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Timestamp getTimestamp(String s, Calendar calendar)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public URL getURL(int i)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public URL getURL(String s)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRef(int i, Ref ref)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRef(String s, Ref ref)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(int i, Blob blob)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(String s, Blob blob)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(int i, Clob clob)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(String s, Clob clob)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateArray(int i, Array array)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateArray(String s, Array array)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public RowId getRowId(int columnIndex)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public RowId getRowId(String columnLabel)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRowId(int i, RowId rowId)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRowId(String s, RowId rowId)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getHoldability()
      throws SQLException {
    return 0;
  }

  @Override
  public boolean isClosed()
      throws SQLException {
    return false;
  }

  @Override
  public void updateNString(int i, String s)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNString(String s, String s1)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(int i, NClob nClob)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(String s, NClob nClob)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public NClob getNClob(int i)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public NClob getNClob(String s)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public SQLXML getSQLXML(int i)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public SQLXML getSQLXML(String s)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateSQLXML(int i, SQLXML sqlxml)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateSQLXML(String s, SQLXML sqlxml)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getNString(int i)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getNString(String s)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Reader getNCharacterStream(int i)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Reader getNCharacterStream(String s)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNCharacterStream(int i, Reader reader, long l)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNCharacterStream(String s, Reader reader, long l)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(int i, InputStream inputStream, long l)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(int i, InputStream inputStream, long l)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(int i, Reader reader, long l)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(String s, InputStream inputStream, long l)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(String s, InputStream inputStream, long l)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(String s, Reader reader, long l)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(int i, InputStream inputStream, long l)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(String s, InputStream inputStream, long l)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(int i, Reader reader, long l)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(String s, Reader reader, long l)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(int i, Reader reader, long l)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(String s, Reader reader, long l)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNCharacterStream(int i, Reader reader)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNCharacterStream(String s, Reader reader)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(int i, InputStream inputStream)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(int i, InputStream inputStream)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(int i, Reader reader)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(String s, InputStream inputStream)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(String s, InputStream inputStream)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(String s, Reader reader)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(int i, InputStream inputStream)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(String s, InputStream inputStream)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(int i, Reader reader)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(String s, Reader reader)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(int i, Reader reader)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(String s, Reader reader)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public <T> T getObject(int i, Class<T> aClass)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public <T> T getObject(String s, Class<T> aClass)
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
    throw new SQLFeatureNotSupportedException();
  }
}
