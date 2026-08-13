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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.ResultSetMetaData;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.client.PinotResultMetadata;
import org.apache.pinot.client.base.AbstractBaseResultSet;
import org.apache.pinot.common.proto.Broker;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotGrpcResultSet extends AbstractBaseResultSet {
  public static final String NULL_STRING = "null";
  private static final Logger LOG = LoggerFactory.getLogger(PinotGrpcResultSet.class);
  private static final ObjectReader MAP_READER =
      JsonUtils.DEFAULT_READER.forType(new TypeReference<Map<?, ?>>() { });

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
  @Nullable
  protected ColumnDataType getColumnType(int columnIndex) {
    return _dataSchema.getColumnDataType(columnIndex - 1);
  }

  @Override
  public boolean first()
      throws SQLException {
    validateState();
    throw new SQLDataException("Absolute row number not supported");
  }

  @Override
  public int getRow()
      throws SQLException {
    validateState();
    return _currentRow;
  }

  @Override
  @Nullable
  public String getString(int columnIndex)
      throws SQLException {
    Object value = getValue(columnIndex);
    String val = value == null ? null : value.toString();
    if (checkIsNull(val)) {
      return null;
    }
    return val;
  }

  @Nullable
  private Object getValue(int columnIndex)
      throws SQLException {
    validateColumn(columnIndex);
    Object value = _currentRowBatch.getRows().get(_currentBatchIndex)[columnIndex - 1];
    if (value == null) {
      _wasNull = true;
    }
    return value;
  }

  @Override
  @Nullable
  protected Map<?, ?> getMap(int columnIndex)
      throws SQLException {
    Object value = getValue(columnIndex);
    if (value == null) {
      return null;
    }
    if (!(value instanceof Map)) {
      throw new SQLDataException("Expected map value, found: " + value.getClass());
    }
    try {
      // Rebind through the same untyped Jackson reader used by PinotResultSet so numeric and nested container types
      // do not depend on the transport decoder's intermediate representation.
      return MAP_READER.readValue(JsonUtils.objectToString(value));
    } catch (IOException e) {
      throw new SQLDataException("Error parsing map", e);
    }
  }

  @Override
  @Nullable
  protected List<?> getList(int columnIndex, ColumnDataType dataType)
      throws SQLException {
    Object value = getValue(columnIndex);
    if (value == null) {
      return null;
    }
    try {
      switch (dataType) {
        case BOOLEAN_ARRAY:
          return toList((boolean[]) value);
        case INT_ARRAY:
          return toList((int[]) value);
        case LONG_ARRAY:
          return toList((long[]) value);
        case FLOAT_ARRAY:
          return toList((float[]) value);
        case DOUBLE_ARRAY:
          return toList((double[]) value);
        case BIG_DECIMAL_ARRAY:
          return toBigDecimalList((String[]) value);
        case TIMESTAMP_ARRAY:
          return toTimestampList(Arrays.asList((String[]) value));
        case STRING_ARRAY:
          return new ArrayList<>(Arrays.asList((String[]) value));
        case BYTES_ARRAY:
          return toBytesList(Arrays.asList((String[]) value));
        case UUID_ARRAY:
          return toUuidList(Arrays.asList((String[]) value));
        default:
          throw new SQLDataException("Data type is not an array: " + dataType);
      }
    } catch (ClassCastException e) {
      throw new SQLDataException("Unexpected value type for " + dataType + ": " + value.getClass(), e);
    }
  }

  private static List<Boolean> toList(boolean[] values) {
    List<Boolean> list = new ArrayList<>(values.length);
    for (boolean value : values) {
      list.add(value);
    }
    return list;
  }

  private static List<Integer> toList(int[] values) {
    List<Integer> list = new ArrayList<>(values.length);
    for (int value : values) {
      list.add(value);
    }
    return list;
  }

  private static List<Long> toList(long[] values) {
    List<Long> list = new ArrayList<>(values.length);
    for (long value : values) {
      list.add(value);
    }
    return list;
  }

  private static List<Float> toList(float[] values) {
    List<Float> list = new ArrayList<>(values.length);
    for (float value : values) {
      list.add(value);
    }
    return list;
  }

  private static List<Double> toList(double[] values) {
    List<Double> list = new ArrayList<>(values.length);
    for (double value : values) {
      list.add(value);
    }
    return list;
  }

  private static List<BigDecimal> toBigDecimalList(String[] values)
      throws SQLException {
    List<BigDecimal> list = new ArrayList<>(values.length);
    try {
      for (String value : values) {
        list.add(value == null ? null : new BigDecimal(value));
      }
      return list;
    } catch (NumberFormatException e) {
      throw new SQLDataException("Error parsing big decimal array", e);
    }
  }

  private boolean checkIsNull(String val) {
    if (val == null || val.toLowerCase().contentEquals(NULL_STRING)) {
      _wasNull = true;
      return true;
    }
    return false;
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
