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
package org.apache.pinot.compat.tests;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.controller.helix.ControllerRequestURLBuilder;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.JsonUtils;
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
    CREATE,
    DELETE,
    UPDATE_CONFIG,
    UPDATE_SCHEMA,
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
    switch (_op) {
      case CREATE:
        createSchema();
        return createTable();
      case DELETE:
        return deleteTable();
      case UPDATE_CONFIG:
        System.out.println("Updating table config to " + _tableConfigFileName);
        break;
      case UPDATE_SCHEMA:
        System.out.println("Updating schema to " + _schemaFileName);
        break;
    }
    return true;
  }

  private boolean createSchema() {
    try {
      Map<String, String> headers = new HashMap<String, String>() {
        {
          put("Content-type", "application/json");
        }
      };
      ControllerTest.sendPostRequest(
          ControllerRequestURLBuilder.baseUrl(ClusterDescriptor.CONTROLLER_URL).forSchemaCreate(),
          FileUtils.readFileToString(new File(_schemaFileName)), headers);
      return true;
    } catch (IOException e) {
      LOGGER.error("Failed to create schema with file: {}", _schemaFileName, e);
      return false;
    }
  }

  private boolean createTable() {
    try {
      ControllerTest.sendPostRequest(
          ControllerRequestURLBuilder.baseUrl(ClusterDescriptor.CONTROLLER_URL).forTableCreate(),
          FileUtils.readFileToString(new File(_tableConfigFileName)));
      return true;
    } catch (IOException e) {
      LOGGER.error("Failed to create table with file: {}", _tableConfigFileName, e);
      return false;
    }
  }

  private boolean deleteTable() {
    try {
      TableConfig tableConfig = JsonUtils.fileToObject(new File(_tableConfigFileName), TableConfig.class);
      ControllerTest.sendDeleteRequest(ControllerRequestURLBuilder.baseUrl(ClusterDescriptor.CONTROLLER_URL)
          .forTableDelete(tableConfig.getTableName()));
      return true;
    } catch (IOException e) {
      LOGGER.error("Failed to delete table with file: {}", _tableConfigFileName, e);
      return false;
    }
  }
}
