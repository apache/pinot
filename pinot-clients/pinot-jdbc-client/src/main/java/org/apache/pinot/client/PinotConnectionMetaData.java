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

import com.fasterxml.jackson.databind.JsonNode;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.pinot.client.base.AbstractBaseConnectionMetaData;
import org.apache.pinot.client.controller.PinotControllerTransport;
import org.apache.pinot.client.controller.response.SchemaResponse;
import org.apache.pinot.client.controller.response.TableResponse;
import org.apache.pinot.client.utils.DriverUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.client.utils.Constants.*;


public class PinotConnectionMetaData extends AbstractBaseConnectionMetaData {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotConnectionMetaData.class);

  private final PinotConnection _connection;
  private final PinotControllerTransport _controllerTransport;
  private final String _controllerURL;

  public PinotConnectionMetaData(PinotConnection connection) {
    this(connection, null, null);
  }

  public PinotConnectionMetaData(PinotConnection connection, String controllerURL,
      PinotControllerTransport controllerTransport) {
    _connection = connection;
    _controllerURL = controllerURL;
    _controllerTransport = controllerTransport;
  }

  @Override
  public String getURL()
      throws SQLException {
    return DriverUtils.getURIFromBrokers(_connection.getSession().getBrokerList());
  }

  @Override
  public String getDatabaseProductName()
      throws SQLException {
    return PRODUCT_NAME;
  }

  @Override
  public String getDatabaseProductVersion()
      throws SQLException {
    return PINOT_VERSION;
  }

  @Override
  public String getDriverName()
      throws SQLException {
    return DRIVER_NAME;
  }

  @Override
  public String getDriverVersion()
      throws SQLException {
    return DRIVER_VERSION;
  }

  @Override
  public int getDriverMajorVersion() {
    return Integer.parseInt(DRIVER_VERSION.split(".")[0]);
  }

  @Override
  public int getDriverMinorVersion() {
    return Integer.parseInt(DRIVER_VERSION.split(".")[1]);
  }

  @Override
  public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
      throws SQLException {
    try {
      PinotMeta pinotMeta = new PinotMeta();
      pinotMeta.setColumnNames(TABLE_COLUMNS);
      pinotMeta.setColumnDataTypes(TABLE_COLUMNS_DTYPES);

      TableResponse tableResponse = _controllerTransport.getAllTables(_controllerURL);
      if (tableResponse.getNumTables() == 0) {
        LOGGER.warn("No tables found in database");
      }
      for (String table : tableResponse.getAllTables()) {
        Object[] row = new Object[]{null, null, table, TABLE_TYPE, table, "", "", "", "", ""};
        pinotMeta.addRow(Arrays.asList(row));
      }

      JsonNode resultTable = JsonUtils.objectToJsonNode(pinotMeta);
      return PinotResultSet.fromResultTable(new ResultTableResultSet(resultTable));
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  @Override
  public ResultSet getSchemas()
      throws SQLException {
    return PinotResultSet.empty();
  }

  @Override
  public ResultSet getCatalogs()
      throws SQLException {
    return PinotResultSet.empty();
  }

  @Override
  public ResultSet getTableTypes()
      throws SQLException {
    PinotMeta pinotMeta = new PinotMeta();
    pinotMeta.setColumnNames(TABLE_TYPES_COLUMNS);
    pinotMeta.setColumnDataTypes(TABLE_TYPES_COLUMNS_DTYPES);

    List<Object> row = new ArrayList<>();
    row.add(TABLE_TYPE);
    pinotMeta.addRow(row);

    JsonNode resultTable = JsonUtils.objectToJsonNode(pinotMeta);
    return PinotResultSet.fromResultTable(new ResultTableResultSet(resultTable));
  }

  @Override
  public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
      throws SQLException {

    if (tableNamePattern != null && tableNamePattern.equals("%")) {
      LOGGER.warn("driver does not support pattern [{}] for table name", tableNamePattern);
      return PinotResultSet.empty();
    }

    SchemaResponse schemaResponse = _controllerTransport.getTableSchema(tableNamePattern, _controllerURL);
    PinotMeta pinotMeta = new PinotMeta();
    pinotMeta.setColumnNames(TABLE_SCHEMA_COLUMNS);
    pinotMeta.setColumnDataTypes(TABLE_SCHEMA_COLUMNS_DTYPES);

    int ordinalPosition = 1;
    if (schemaResponse.getDimensions() != null) {
      for (JsonNode columns : schemaResponse.getDimensions()) {
        appendColumnMeta(pinotMeta, tableNamePattern, ordinalPosition, columns);
        ordinalPosition++;
      }
    }

    if (schemaResponse.getMetrics() != null) {
      for (JsonNode columns : schemaResponse.getMetrics()) {
        appendColumnMeta(pinotMeta, tableNamePattern, ordinalPosition, columns);
        ordinalPosition++;
      }
    }

    if (schemaResponse.getDateTimeFieldSpecs() != null) {
      for (JsonNode columns : schemaResponse.getDateTimeFieldSpecs()) {
        appendColumnMeta(pinotMeta, tableNamePattern, ordinalPosition, columns);
        ordinalPosition++;
      }
    }

    JsonNode resultTable = JsonUtils.objectToJsonNode(pinotMeta);
    return PinotResultSet.fromResultTable(new ResultTableResultSet(resultTable));
  }

  private void appendColumnMeta(PinotMeta pinotMeta, String tableName, int ordinalPosition, JsonNode columns) {
    String columnName = columns.get("name").textValue();
    String columnDataType = columns.get("dataType").textValue();
    Integer columnsSQLDataType = DriverUtils.getSQLDataType(columnDataType);

    Object[] row = new Object[]{
        null, null, tableName, columnName, columnsSQLDataType, columnDataType, -1, -1, -1, -1, 1, null, null, -1, -1,
        -1, ordinalPosition, "NO", null, null, null, -1, "NO", "NO"
    };
    pinotMeta.addRow(Arrays.asList(row));
  }

  @Override
  public Connection getConnection()
      throws SQLException {
    return _connection;
  }
}
