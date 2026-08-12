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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLType;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.pinot.client.utils.DateTimeUtils;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;


public abstract class AbstractBaseResultSet implements ResultSet {

  protected abstract void validateColumn(int columnIndex)
      throws SQLException;

  protected abstract Map<?, ?> getMap(int columnIndex)
      throws SQLException;

  protected abstract String getColumnTypeName(int columnIndex);

  protected void validateState()
      throws SQLException {
    if (isClosed()) {
      throw new SQLException("Not possible to operate on closed or empty result sets");
    }
  }

  @Override
  public void cancelRowUpdates()
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void clearWarnings()
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void deleteRow()
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Array getArray(int columnIndex)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Array getArray(String columnLabel)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public InputStream getAsciiStream(String columnLabel)
      throws SQLException {
    return getAsciiStream(findColumn(columnLabel));
  }

  @Override
  public InputStream getAsciiStream(int columnIndex)
      throws SQLException {
    String value = getString(columnIndex);
    InputStream in = new ByteArrayInputStream(value.getBytes(StandardCharsets.US_ASCII));
    return in;
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex)
      throws SQLException {
    return getBigDecimal(columnIndex, 0);
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel, int scale)
      throws SQLException {
    return getBigDecimal(findColumn(columnLabel), scale);
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale)
      throws SQLException {
    try {
      String value = this.getString(columnIndex);
      int calculatedScale = getCalculatedScale(value);
      return value == null ? null : new BigDecimal(value).setScale(calculatedScale);
    } catch (Exception e) {
      throw new SQLException("Unable to fetch BigDecimal value", e);
    }
  }

  public int getCalculatedScale(String value) {
    int index = value.indexOf(".");
    return index == -1 ? 0 : value.length() - index - 1;
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel)
      throws SQLException {
    return getBigDecimal(findColumn(columnLabel));
  }

  @Override
  public InputStream getBinaryStream(int columnIndex)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public InputStream getBinaryStream(String columnLabel)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean getBoolean(String columnLabel)
      throws SQLException {
    return getBoolean(findColumn(columnLabel));
  }

  @Override
  public boolean getBoolean(int columnIndex)
      throws SQLException {
    validateColumn(columnIndex);
    String value = getString(columnIndex);
    return value == null ? false : Boolean.parseBoolean(value);
  }

  @Override
  public Blob getBlob(int columnIndex)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Blob getBlob(String columnLabel)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public byte getByte(String columnLabel)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public byte getByte(int columnIndex)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public byte[] getBytes(String columnLabel)
      throws SQLException {
    return getBytes(findColumn(columnLabel));
  }

  @Override
  public byte[] getBytes(int columnIndex)
      throws SQLException {
    try {
      String value = getString(columnIndex);
      return value == null ? null : Hex.decodeHex(value.toCharArray());
    } catch (Exception e) {
      throw new SQLException(String.format("Unable to fetch value for column %d", columnIndex), e);
    }
  }

  @Override
  public Reader getCharacterStream(String columnLabel)
      throws SQLException {
    return getCharacterStream(findColumn(columnLabel));
  }

  @Override
  public Reader getCharacterStream(int columnIndex)
      throws SQLException {
    InputStream in = getUnicodeStream(columnIndex);
    Reader reader = new InputStreamReader(in, StandardCharsets.UTF_8);
    return reader;
  }

  @Override
  public Clob getClob(int columnIndex)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Clob getClob(String columnLabel)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getConcurrency()
      throws SQLException {
    validateState();
    return ResultSet.CONCUR_READ_ONLY;
  }

  @Override
  public String getCursorName()
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Date getDate(int columnIndex)
      throws SQLException {
    return getDate(columnIndex, Calendar.getInstance());
  }

  @Override
  public Date getDate(String columnLabel)
      throws SQLException {
    return getDate(findColumn(columnLabel), Calendar.getInstance());
  }

  @Override
  public Date getDate(String columnLabel, Calendar cal)
      throws SQLException {
    return getDate(findColumn(columnLabel), cal);
  }

  @Override
  public Date getDate(int columnIndex, Calendar cal)
      throws SQLException {
    try {
      String value = getString(columnIndex);
      return value == null ? null : DateTimeUtils.getDateFromString(value, cal);
    } catch (Exception e) {
      throw new SQLException("Unable to fetch date", e);
    }
  }

  @Override
  public double getDouble(String columnLabel)
      throws SQLException {
    return getDouble(findColumn(columnLabel));
  }

  @Override
  public double getDouble(int columnIndex)
      throws SQLException {
    validateColumn(columnIndex);
    String value = getString(columnIndex);
    return value == null ? 0.0 : Double.parseDouble(value);
  }

  @Override
  public int getFetchDirection()
      throws SQLException {
    validateState();
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public void setFetchDirection(int direction)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getFetchSize()
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setFetchSize(int rows)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public float getFloat(String columnLabel)
      throws SQLException {
    return getFloat(findColumn(columnLabel));
  }

  @Override
  public float getFloat(int columnIndex)
      throws SQLException {
    validateColumn(columnIndex);
    String value = getString(columnIndex);
    return value == null ? 0.0f : Float.parseFloat(value);
  }

  @Override
  public int getHoldability()
      throws SQLException {
    validateState();
    return ResultSet.HOLD_CURSORS_OVER_COMMIT;
  }

  @Override
  public int getInt(String columnLabel)
      throws SQLException {
    return getInt(findColumn(columnLabel));
  }

  @Override
  public int getInt(int columnIndex)
      throws SQLException {
    validateColumn(columnIndex);
    String value = getString(columnIndex);
    return value == null ? 0 : Integer.parseInt(value);
  }

  @Override
  public long getLong(String columnLabel)
      throws SQLException {
    return getLong(findColumn(columnLabel));
  }

  @Override
  public long getLong(int columnIndex)
      throws SQLException {
    validateColumn(columnIndex);
    String value = getString(columnIndex);
    return value == null ? 0 : Long.parseLong(value);
  }

  @Override
  public short getShort(String columnLabel)
      throws SQLException {
    return getShort(columnLabel);
  }

  @Override
  public short getShort(int columnIndex)
      throws SQLException {
    Integer value = getInt(columnIndex);
    return value == null ? null : value.shortValue();
  }

  @Override
  public String getString(String columnLabel)
      throws SQLException {
    return getString(findColumn(columnLabel));
  }

  @Override
  public Reader getNCharacterStream(int columnIndex)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Reader getNCharacterStream(String columnLabel)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public NClob getNClob(int columnIndex)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public NClob getNClob(String columnLabel)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getNString(int columnIndex)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getNString(String columnLabel)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Object getObject(String columnLabel)
      throws SQLException {
    return getObject(findColumn(columnLabel));
  }

  @Override
  public Object getObject(int columnIndex, Map<String, Class<?>> map)
      throws SQLException {
    return getObject(columnIndex);
  }

  @Override
  public Object getObject(String columnLabel, Map<String, Class<?>> map)
      throws SQLException {
    return getObject(findColumn(columnLabel), map);
  }

  @Override
  public <T> T getObject(String columnLabel, Class<T> type)
      throws SQLException {
    return getObject(findColumn(columnLabel), type);
  }

  @Override
  public <T> T getObject(int columnIndex, Class<T> type)
      throws SQLException {
    Object value = getObject(columnIndex);

    try {
      return type.cast(value);
    } catch (ClassCastException e) {
      throw new SQLDataException("Data type conversion is not supported from :" + value.getClass() + " to: " + type);
    }
  }

  @Override
  public Ref getRef(int columnIndex)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Ref getRef(String columnLabel)
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
  public SQLXML getSQLXML(int columnIndex)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public SQLXML getSQLXML(String columnLabel)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Statement getStatement()
      throws SQLException {
    return null;
  }

  @Override
  public Time getTime(int columnIndex)
      throws SQLException {
    return getTime(columnIndex, Calendar.getInstance());
  }

  @Override
  public Time getTime(String columnLabel)
      throws SQLException {
    return getTime(findColumn(columnLabel), Calendar.getInstance());
  }

  @Override
  public Time getTime(String columnLabel, Calendar cal)
      throws SQLException {
    return getTime(findColumn(columnLabel), cal);
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal)
      throws SQLException {
    try {
      String value = getString(columnIndex);
      return value == null ? null : DateTimeUtils.getTimeFromString(value, cal);
    } catch (Exception e) {
      throw new SQLException("Unable to fetch date", e);
    }
  }

  @Override
  public Timestamp getTimestamp(int columnIndex)
      throws SQLException {
    return getTimestamp(columnIndex, Calendar.getInstance());
  }

  @Override
  public Timestamp getTimestamp(String columnLabel)
      throws SQLException {
    return getTimestamp(findColumn(columnLabel), Calendar.getInstance());
  }

  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal)
      throws SQLException {
    return getTimestamp(findColumn(columnLabel), cal);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal)
      throws SQLException {
    try {
      String value = getString(columnIndex);
      return value == null ? null : DateTimeUtils.getTimestampFromString(value, cal);
    } catch (Exception e) {
      throw new SQLException("Unable to fetch date", e);
    }
  }

  @Override
  public int getType()
      throws SQLException {
    validateState();
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public URL getURL(String columnLabel)
      throws SQLException {
    return getURL(findColumn(columnLabel));
  }

  @Override
  public URL getURL(int columnIndex)
      throws SQLException {
    try {
      URL url = new URL(getString(columnIndex));
      return url;
    } catch (Exception e) {
      throw new SQLException("Unable to fetch URL", e);
    }
  }

  @Override
  public InputStream getUnicodeStream(String columnLabel)
      throws SQLException {
    return getUnicodeStream(findColumn(columnLabel));
  }

  @Override
  public InputStream getUnicodeStream(int columnIndex)
      throws SQLException {
    String value = getString(columnIndex);
    InputStream in = new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8));
    return in;
  }

  protected UUID getUuid(int columnIndex)
      throws SQLException {
    String value = getString(columnIndex);
    if (value == null) {
      return null;
    }
    try {
      return UUID.fromString(value);
    } catch (IllegalArgumentException e) {
      throw new SQLDataException("Error parsing UUID", e);
    }
  }

  @Override
  public SQLWarning getWarnings()
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void insertRow()
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isWrapperFor(Class<?> iface)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void moveToCurrentRow()
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void moveToInsertRow()
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void refreshRow()
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean rowDeleted()
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean rowInserted()
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean rowUpdated()
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public <T> T unwrap(Class<T> iface)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateArray(int columnIndex, Array x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateArray(String columnLabel, Array x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(int columnIndex, Blob x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(String columnLabel, Blob x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBoolean(int columnIndex, boolean x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBoolean(String columnLabel, boolean x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateByte(int columnIndex, byte x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateByte(String columnLabel, byte x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBytes(int columnIndex, byte[] x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBytes(String columnLabel, byte[] x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, int length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(int columnIndex, Clob x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(String columnLabel, Clob x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(int columnIndex, Reader reader, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(String columnLabel, Reader reader, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(int columnIndex, Reader reader)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(String columnLabel, Reader reader)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateDate(int columnIndex, Date x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateDate(String columnLabel, Date x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateDouble(int columnIndex, double x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateDouble(String columnLabel, double x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateFloat(int columnIndex, float x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateFloat(String columnLabel, float x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateInt(int columnIndex, int x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateInt(String columnLabel, int x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateLong(int columnIndex, long x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateLong(String columnLabel, long x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(int columnIndex, NClob nClob)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(String columnLabel, NClob nClob)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNString(int columnIndex, String nString)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNString(String columnLabel, String nString)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNull(int columnIndex)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNull(String columnLabel)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(int columnIndex, Object x, SQLType targetSqlType)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(String columnLabel, Object x, SQLType targetSqlType)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(int columnIndex, Object x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(String columnLabel, Object x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRef(int columnIndex, Ref x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRef(String columnLabel, Ref x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRow()
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRowId(int columnIndex, RowId x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRowId(String columnLabel, RowId x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateSQLXML(int columnIndex, SQLXML xmlObject)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateShort(int columnIndex, short x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateShort(String columnLabel, short x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateString(int columnIndex, String x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateString(String columnLabel, String x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateTime(int columnIndex, Time x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateTime(String columnLabel, Time x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateTimestamp(String columnLabel, Timestamp x)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Object getObject(int columnIndex)
      throws SQLException {

    String dataTypeName = getColumnTypeName(columnIndex);
    ColumnDataType dataType;
    try {
      dataType = ColumnDataType.valueOf(dataTypeName);
    } catch (IllegalArgumentException e) {
      throw new SQLDataException("Data type not supported for " + dataTypeName, e);
    }

    if (dataType.isArray()) {
      return getList(columnIndex, dataType);
    }
    return getScalar(columnIndex, dataType);
  }

  private Object getScalar(int columnIndex, ColumnDataType dataType)
      throws SQLException {
    switch (dataType) {
      case STRING:
      case JSON:
        return getString(columnIndex);
      case INT:
        return getInt(columnIndex);
      case LONG:
        return getLong(columnIndex);
      case FLOAT:
        return getFloat(columnIndex);
      case DOUBLE:
        return getDouble(columnIndex);
      case BIG_DECIMAL:
        return getBigDecimal(columnIndex);
      case BOOLEAN:
        return getBoolean(columnIndex);
      case BYTES:
        return getBytes(columnIndex);
      case UUID:
        return getUuid(columnIndex);
      case TIMESTAMP:
        return getTimestamp(columnIndex);
      case MAP:
        return getMap(columnIndex);
      default:
        throw new SQLDataException("Data type not supported for " + dataType);
    }
  }

  protected abstract List<?> getList(int columnIndex, ColumnDataType dataType)
      throws SQLException;

  protected List<Timestamp> toTimestampList(List<String> values)
      throws SQLException {
    if (values == null) {
      return null;
    }
    List<Timestamp> list = new ArrayList<>(values.size());
    Calendar calendar = Calendar.getInstance();
    try {
      for (String value : values) {
        list.add(value == null ? null : DateTimeUtils.getTimestampFromString(value, calendar));
      }
      return list;
    } catch (ParseException e) {
      throw new SQLDataException("Error parsing timestamp array", e);
    }
  }

  protected List<byte[]> toBytesList(List<String> values)
      throws SQLException {
    if (values == null) {
      return null;
    }
    List<byte[]> bytes = new ArrayList<>(values.size());
    try {
      for (String value : values) {
        bytes.add(value == null ? null : Hex.decodeHex(value));
      }
      return bytes;
    } catch (DecoderException e) {
      throw new SQLDataException("Error parsing bytes array", e);
    }
  }

  protected List<UUID> toUuidList(List<String> values)
      throws SQLException {
    if (values == null) {
      return null;
    }
    List<UUID> uuids = new ArrayList<>(values.size());
    try {
      for (String value : values) {
        uuids.add(value == null ? null : UUID.fromString(value));
      }
      return uuids;
    } catch (IllegalArgumentException e) {
      throw new SQLDataException("Error parsing UUID array", e);
    }
  }
}
