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
package org.apache.pinot.tools;

import java.io.File;
import org.apache.pinot.spi.config.table.TableType;


public class QuickstartTableRequest {

  private String _tableName;
  private TableType _tableType;
  private File _schemaFile;
  private File _tableRequestFile;
  private File _ingestionJobFile;
  private String _bootstrapTableDir;

  public QuickstartTableRequest(String bootstrapTableDir) {
    _bootstrapTableDir = bootstrapTableDir;
  }

  public QuickstartTableRequest(String tableName, File schemaFile, File tableRequest, File ingestionJobFile) {
    _tableName = tableName;
    _schemaFile = schemaFile;
    _tableRequestFile = tableRequest;
    _tableType = TableType.OFFLINE;
    _ingestionJobFile = ingestionJobFile;
  }

  public QuickstartTableRequest(String tableName, File schemaFile, File tableRequest) {
    _tableName = tableName;
    _schemaFile = schemaFile;
    _tableRequestFile = tableRequest;
    _tableType = TableType.REALTIME;
  }

  public File getSchemaFile() {
    return _schemaFile;
  }

  public void setSchemaFile(File schemaFile) {
    _schemaFile = schemaFile;
  }

  public File getTableRequestFile() {
    return _tableRequestFile;
  }

  public void setTableRequestFile(File tableRequestFile) {
    _tableRequestFile = tableRequestFile;
  }

  public File getIngestionJobFile() {
    return _ingestionJobFile;
  }

  public void setIngestionJobFile(File ingestionJobFile) {
    _ingestionJobFile = ingestionJobFile;
  }

  public TableType getTableType() {
    return _tableType;
  }

  public void setTableType(TableType tableType) {
    _tableType = tableType;
  }

  public String getTableName() {
    return _tableName;
  }

  public void setTableName(String tableName) {
    _tableName = tableName;
  }

  public String getBootstrapTableDir() {
    return _bootstrapTableDir;
  }

  public void setBootstrapTableDir(String bootstrapTableDir) {
    _bootstrapTableDir = bootstrapTableDir;
  }

  public String toString() {
    return "{ tableName = " + _tableName + ", tableType = " + _tableType + ", schemaFile = " + _schemaFile
        + ", tableRequestFile = " + _tableRequestFile + ", ingestionJobFile = " + _ingestionJobFile
        + ", bootstrapTableDir = " + _bootstrapTableDir + " }";
  }
}
