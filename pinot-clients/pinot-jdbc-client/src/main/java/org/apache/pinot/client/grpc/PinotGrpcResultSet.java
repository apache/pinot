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

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.commons.codec.binary.Hex;
import org.apache.pinot.client.PinotResultMetadata;
import org.apache.pinot.client.base.AbstractBaseResultSet;
import org.apache.pinot.client.utils.DateTimeUtils;
import org.apache.pinot.common.proto.Broker;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotGrpcResultSet extends AbstractBaseResultSet {
  public static final String NULL_STRING = "null";
  private static final Logger LOG = LoggerFactory.getLogger(PinotGrpcResultSet.class);

  private final Iterator<Broker.BrokerResponse> _brokerResponseIterator;
  private final int _totalColumns;
  private final Map<String, Integer> _columns = new HashMap<>();
  private final Map<Integer, String> _columnDataTypes = new HashMap<>();
  private final DataSchema _dataSchema;
  private ResultTable _currentRowBatch;
  private int _currentBatchSize;

  private int _currentBatchIndex = -1;
  private int _currentRow = 0;

  private boolean _closed;
  private boolean _wasNull = false;

  public PinotGrpcResultSet(Iterator<Broker.BrokerResponse> brokerResponseIterator)
      throws IOException {
    _brokerResponseIterator = brokerResponseIterator;
    _closed = false;
    ObjectNode metadata = GrpcUtils.extractMetadataJson(_brokerResponseIterator.next());
    _dataSchema = GrpcUtils.extractSchema(_brokerResponseIterator.next());
    _totalColumns = _dataSchema.size();
    for (int i = 0; i < _totalColumns; i++) {
      _columns.put(_dataSchema.getColumnName(i), i + 1);
      _columnDataTypes.put(i + 1, _dataSchema.getColumnDataType(i).name());
    }
  }

  public PinotGrpcResultSet() {
    _brokerResponseIterator = null;
    _currentBatchSize = 0;
    _totalColumns = 0;
    _dataSchema = null;
  }

  public static PinotGrpcResultSet empty() {
    return new PinotGrpcResultSet();
  }

  protected void validateState()
      throws SQLException {
    if (isClosed()) {
      throw new SQLException("Not possible to operate on closed or empty result sets");
    }
  }

  protected void validateColumn(int columnIndex)
      throws SQLException {
    validateState();
    _wasNull = false;
    if (columnIndex > _totalColumns) {
      throw new SQLException("Column Index should be less than " + (_totalColumns + 1) + ". Found " + columnIndex);
    }
  }

  @Override
  public boolean absolute(int row)
      throws SQLException {
    validateState();
    throw new SQLDataException("Absolute row number not supported");
  }

  @Override
  public void afterLast()
      throws SQLException {
    validateState();
    throw new SQLDataException("Absolute row number not supported");
  }

  @Override
  public void beforeFirst()
      throws SQLException {
    validateState();
    throw new SQLDataException("Absolute row number not supported");
  }

  @Override
  public void close()
      throws SQLException {
    _currentRow = -1;
    _columns.clear();
    _closed = true;
  }

  @Override
  public int findColumn(String columnLabel)
      throws SQLException {
    if (_columns.containsKey(columnLabel)) {
      return _columns.get(columnLabel);
    } else {
      throw new SQLException("Column with label " + columnLabel + " not found in ResultSet");
    }
  }

  @Override
  public ResultSetMetaData getMetaData()
      throws SQLException {
    validateState();
    return new PinotResultMetadata(_totalColumns, _columns, _columnDataTypes);
  }

  @Override
  public boolean first()
      throws SQLException {
    validateState();
    throw new SQLDataException("Absolute row number not supported");
  }

  @Override
  public InputStream getAsciiStream(int columnIndex)
      throws SQLException {
    String value = getString(columnIndex);
    InputStream in = new ByteArrayInputStream(value.getBytes(StandardCharsets.US_ASCII));
    return in;
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

  int getCalculatedScale(String value) {
    int index = value.indexOf(".");
    return index == -1 ? 0 : value.length() - index - 1;
  }

  @Override
  public boolean getBoolean(int columnIndex)
      throws SQLException {
    validateColumn(columnIndex);
    String value = getString(columnIndex);
    return value == null ? false : Boolean.parseBoolean(value);
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
  public Reader getCharacterStream(int columnIndex)
      throws SQLException {
    InputStream in = getUnicodeStream(columnIndex);
    Reader reader = new InputStreamReader(in, StandardCharsets.UTF_8);
    return reader;
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
  public double getDouble(int columnIndex)
      throws SQLException {
    validateColumn(columnIndex);
    String value = getString(columnIndex);
    return value == null ? 0.0 : Double.parseDouble(value);
  }

  @Override
  public float getFloat(int columnIndex)
      throws SQLException {
    validateColumn(columnIndex);
    String value = getString(columnIndex);
    return value == null ? 0.0f : Float.parseFloat(value);
  }

  @Override
  public int getInt(int columnIndex)
      throws SQLException {
    validateColumn(columnIndex);
    String value = getString(columnIndex);
    return value == null ? 0 : Integer.parseInt(value);
  }

  @Override
  public long getLong(int columnIndex)
      throws SQLException {
    validateColumn(columnIndex);
    String value = getString(columnIndex);
    return value == null ? 0 : Long.parseLong(value);
  }

  @Override
  public int getRow()
      throws SQLException {
    validateState();
    return _currentRow;
  }

  @Override
  public short getShort(int columnIndex)
      throws SQLException {
    Integer value = getInt(columnIndex);
    return value == null ? null : value.shortValue();
  }

  @Override
  public String getString(int columnIndex)
      throws SQLException {
    validateColumn(columnIndex);
    String val = _currentRowBatch.getRows().get(_currentBatchIndex)[columnIndex - 1].toString();
    if (checkIsNull(val)) {
      return null;
    }
    return val;
  }

  @Override
  public Object getObject(int columnIndex)
      throws SQLException {

    String dataType = _columnDataTypes.getOrDefault(columnIndex, "");

    if (dataType.isEmpty()) {
      throw new SQLDataException("Data type not supported for " + dataType);
    }

    switch (dataType) {
      case "STRING":
        return getString(columnIndex);
      case "INT":
        return getInt(columnIndex);
      case "LONG":
        return getLong(columnIndex);
      case "FLOAT":
        return getFloat(columnIndex);
      case "DOUBLE":
        return getDouble(columnIndex);
      case "BOOLEAN":
        return getBoolean(columnIndex);
      case "BYTES":
        return getBytes(columnIndex);
      default:
        throw new SQLDataException("Data type not supported for " + dataType);
    }
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
  public <T> T getObject(String columnLabel, Class<T> type)
      throws SQLException {
    return super.getObject(columnLabel, type);
  }

  private boolean checkIsNull(String val) {
    if (val == null || val.toLowerCase().contentEquals(NULL_STRING)) {
      _wasNull = true;
      return true;
    }
    return false;
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
  public InputStream getUnicodeStream(int columnIndex)
      throws SQLException {
    String value = getString(columnIndex);
    InputStream in = new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8));
    return in;
  }

  @Override
  public boolean isAfterLast()
      throws SQLException {
    validateState();
    throw new SQLDataException("Absolute row number not supported");
  }

  @Override
  public boolean isBeforeFirst()
      throws SQLException {
    validateState();
    throw new SQLDataException("Absolute row number not supported");
  }

  @Override
  public boolean isClosed()
      throws SQLException {
    return _closed;
  }

  @Override
  public boolean isFirst()
      throws SQLException {
    validateState();
    throw new SQLDataException("Absolute row number not supported");
  }

  @Override
  public boolean isLast()
      throws SQLException {
    validateState();
    throw new SQLDataException("Absolute row number not supported");
  }

  @Override
  public boolean last()
      throws SQLException {
    validateState();
    throw new SQLDataException("Absolute row number not supported");
  }

  @Override
  public boolean next()
      throws SQLException {
    validateState();

    if (_currentBatchIndex == _currentBatchSize - 1) {
      if (_brokerResponseIterator.hasNext()) {
        try {
          _currentRowBatch = GrpcUtils.extractResultTable(_brokerResponseIterator.next(), _dataSchema);
          _currentBatchIndex = 0;
          _currentBatchSize = _currentRowBatch.getRows().size();
          _currentRow++;
          return true;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      } else {
        return false;
      }
    }

    _currentBatchIndex++;
    _currentRow++;
    return _currentBatchIndex < _currentBatchSize;
  }

  @Override
  public boolean previous()
      throws SQLException {
    validateState();
    throw new SQLDataException("Absolute row number not supported");
  }

  @Override
  public boolean relative(int rows)
      throws SQLException {
    validateState();
    throw new SQLDataException("Absolute row number not supported");
  }

  @Override
  public boolean wasNull()
      throws SQLException {
    return _wasNull;
  }
}
