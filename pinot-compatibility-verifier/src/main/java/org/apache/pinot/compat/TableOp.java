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
package org.apache.pinot.compat;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.client.admin.PinotAdminClient;
import org.apache.pinot.client.admin.PinotAdminException;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * TableOp creates, or deletes a table, or updates the table config.
 * CREATE
 *   verifies table exists, after it is created.
 * DELETE
 *   verifies that the table does not exist, after the operation.
 * UPDATE_CONFIG
 *   Updates the table config
 * UPDATE_SCHEMA
 *   Updates the schema and executes a reload on the table. Awaits reload status
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TableOp extends BaseOp {
  public enum Op {
    CREATE, DELETE, UPDATE_CONFIG, UPDATE_SCHEMA,
  }

  private String _schemaFileName;
  private Op _op;
  private String _tableConfigFileName;

  private static final Logger LOGGER = LoggerFactory.getLogger(TableOp.class);

  public TableOp() {
    super(OpType.TABLE_OP);
  }

  public String getSchemaFileName() {
    return _schemaFileName;
  }

  public void setSchemaFileName(String schemaFileName) {
    _schemaFileName = schemaFileName;
  }

  public Op getOp() {
    return _op;
  }

  public void setOp(Op op) {
    _op = op;
  }

  public String getTableConfigFileName() {
    return _tableConfigFileName;
  }

  public void setTableConfigFileName(String tableConfigFileName) {
    _tableConfigFileName = tableConfigFileName;
  }

  @Override
  boolean runOp(int generationNumber) {
    try (PinotAdminClient adminClient = createPinotAdminClient()) {
      switch (_op) {
        case CREATE:
          if (!createSchema(adminClient)) {
            return false;
          }
          return createTable(adminClient);
        case DELETE:
          return deleteTable(adminClient);
        case UPDATE_CONFIG:
          return updateTableConfig(adminClient);
        case UPDATE_SCHEMA:
          return updateSchema(adminClient);
        default:
          return true;
      }
    } catch (Exception e) {
      LOGGER.error("Failed to run table op {}", _op, e);
      return false;
    }
  }

  private boolean createSchema(PinotAdminClient adminClient) {
    try {
      String schemaJson = FileUtils.readFileToString(new File(getAbsoluteFileName(_schemaFileName)),
          Charset.defaultCharset());
      adminClient.getSchemaClient().createSchema(schemaJson, true, false);
      return true;
    } catch (IOException | PinotAdminException e) {
      LOGGER.error("Failed to create schema with file: {}", _schemaFileName, e);
      return false;
    }
  }

  private boolean createTable(PinotAdminClient adminClient) {
    try {
      adminClient.getTableClient().createTable(
          FileUtils.readFileToString(new File(getAbsoluteFileName(_tableConfigFileName)), Charset.defaultCharset()),
          null);
      return true;
    } catch (IOException | PinotAdminException e) {
      LOGGER.error("Failed to create table with file: {}", _tableConfigFileName, e);
      return false;
    }
  }

  private boolean deleteTable(PinotAdminClient adminClient) {
    try {
      TableConfig tableConfig =
          JsonUtils.fileToObject(new File(getAbsoluteFileName(_tableConfigFileName)), TableConfig.class);
      adminClient.getTableClient().deleteTable(tableConfig.getTableName());
      return true;
    } catch (IOException | PinotAdminException e) {
      LOGGER.error("Failed to delete table with file: {}", _tableConfigFileName, e);
      return false;
    }
  }

  private boolean updateTableConfig(PinotAdminClient adminClient) {
    try {
      String tableConfigJson =
          FileUtils.readFileToString(new File(getAbsoluteFileName(_tableConfigFileName)), Charset.defaultCharset());
      TableConfig tableConfig = JsonUtils.stringToObject(tableConfigJson, TableConfig.class);
      String rawTableName = TableNameBuilder.extractRawTableName(tableConfig.getTableName());
      adminClient.getTableClient().updateTableConfig(rawTableName, tableConfigJson);
      return true;
    } catch (IOException | PinotAdminException e) {
      LOGGER.error("Failed to update table config from {}", _tableConfigFileName, e);
      return false;
    }
  }

  private boolean updateSchema(PinotAdminClient adminClient) {
    try {
      String schemaJson =
          FileUtils.readFileToString(new File(getAbsoluteFileName(_schemaFileName)), Charset.defaultCharset());
      org.apache.pinot.spi.data.Schema schema =
          org.apache.pinot.spi.data.Schema.fromString(schemaJson);
      adminClient.getSchemaClient().updateSchema(schema.getSchemaName(), schemaJson);
      return true;
    } catch (IOException | PinotAdminException e) {
      LOGGER.error("Failed to update schema from {}", _schemaFileName, e);
      return false;
    }
  }
}
