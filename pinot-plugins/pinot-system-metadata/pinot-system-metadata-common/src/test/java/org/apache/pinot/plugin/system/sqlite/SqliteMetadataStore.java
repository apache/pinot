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
package org.apache.pinot.plugin.system.sqlite;

import com.google.common.base.Preconditions;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.system.SystemMetadata;
import org.apache.pinot.spi.system.SystemMetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A basic sqlite system metadata store that connects to a sqlite metadata store.
 */
public class SqliteMetadataStore implements SystemMetadataStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(SqliteMetadataStore.class);
  private static final String SQLITE_METADATA_STORE_CONN_URL = "sqlite.conn";

  private final PinotConfiguration _pinotConfiguration;

  private Connection _connection;

  public SqliteMetadataStore(PinotConfiguration pinotConfiguration) {
    _pinotConfiguration = pinotConfiguration;
  }

  @Override
  public void connect()
      throws Exception {
    _connection = DriverManager.getConnection(_pinotConfiguration.getProperty(SQLITE_METADATA_STORE_CONN_URL));
  }

  @Override
  public void shutdown()
      throws Exception {
    _connection.close();
  }

  @Override
  public boolean accepts(SystemMetadata systemMetadata) {
    try {
      Statement stat = _connection.createStatement();
      int status = stat.executeUpdate(String.format("CREATE TABLE %s(%s)", systemMetadata.getSystemMetadataName(),
          getColumnList(systemMetadata.getColumnName())));
      return status == 0;
    } catch (Exception e) {
      LOGGER.error(String.format("Unable to accept SystemMetadata %s", systemMetadata.getSystemMetadataName()), e);
      return false;
    }
  }

  @Override
  public void collect(SystemMetadata systemMetadata, long timestamp, Object[] row)
      throws Exception {
    PreparedStatement stat = _connection.prepareStatement(String.format("INSERT INTO %s VALUES(%s)",
        systemMetadata.getSystemMetadataName(), getPrepareStatementPlaceholders(systemMetadata.getColumnName())));
    executeStatement(stat, systemMetadata.getColumnDataType(), row);
  }

  @Override
  public List<Object[]> inspect(SystemMetadata systemMetadata)
      throws Exception {
    Statement stat = _connection.createStatement();
    ResultSet rs = stat.executeQuery(String.format("SELECT * FROM %s", systemMetadata.getSystemMetadataName()));
    List<Object[]> result = new ArrayList<>();
    while (rs.next()) {
      result.add(constructRow(rs, systemMetadata));
    }
    return result;
  }

  @Override
  public void flush()
      throws Exception {
    // no-op
  }

  private String getColumnList(String[] columnNames) {
    return StringUtils.join(columnNames, ", ");
  }

  private String getPrepareStatementPlaceholders(String[] columnNames) {
    int placeholderLength = columnNames.length;
    String[] placeHolders = new String[placeholderLength];
    Arrays.fill(placeHolders, "?");
    return StringUtils.join(placeHolders, ", ");
  }

  private void executeStatement(PreparedStatement stat, FieldSpec.DataType[] dataTypes, Object[] row)
      throws Exception {
    Preconditions.checkState(dataTypes.length == row.length,
        "schema and data size much match!");
    int index = 0;
    for (FieldSpec.DataType dataType : dataTypes) {
      switch (dataType) {
        case INT:
          stat.setInt(index + 1, (int) row[index]);
          index++;
          break;
        case LONG:
          stat.setLong(index + 1, (long) row[index]);
          index++;
          break;
        case FLOAT:
          stat.setFloat(index + 1, (float) row[index]);
          index++;
          break;
        case DOUBLE:
          stat.setDouble(index + 1, (double) row[index]);
          index++;
          break;
        case BOOLEAN:
          stat.setBoolean(index + 1, (boolean) row[index]);
          index++;
          break;
        case TIMESTAMP:
          stat.setTimestamp(index + 1, (Timestamp) row[index]);
          index++;
          break;
        case STRING:
          stat.setString(index + 1, (String) row[index]);
          index++;
          break;
        default:
          throw new UnsupportedOperationException("Unsupported schema type: " + dataType);
      }
    }
    stat.executeUpdate();
  }

  private Object[] constructRow(ResultSet rs, SystemMetadata systemMetadata)
      throws Exception {
    FieldSpec.DataType[] columnDataType = systemMetadata.getColumnDataType();
    String[] columnName = systemMetadata.getColumnName();
    Object[] row = new Object[columnName.length];
    for (int idx = 0; idx < columnName.length; idx++) {
      switch (columnDataType[idx]) {
        case INT:
          row[idx] = rs.getInt(columnName[idx]);
          break;
        case LONG:
          row[idx] = rs.getLong(columnName[idx]);
          break;
        case FLOAT:
          row[idx] = rs.getFloat(columnName[idx]);
          break;
        case DOUBLE:
          row[idx] = rs.getDouble(columnName[idx]);
          break;
        case BOOLEAN:
          row[idx] = rs.getBoolean(columnName[idx]);
          break;
        case TIMESTAMP:
          row[idx] = rs.getTimestamp(columnName[idx]);
          break;
        case STRING:
          row[idx] = rs.getString(columnName[idx]);
          break;
        default:
          throw new UnsupportedOperationException("Unsupported schema type: " + columnDataType[idx]);
      }
    }
    return row;
  }
}
