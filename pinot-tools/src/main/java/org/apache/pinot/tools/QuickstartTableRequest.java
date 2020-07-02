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
import org.apache.pinot.spi.data.readers.FileFormat;


public class QuickstartTableRequest {

  File schemaFile;
  File tableRequestFile;
  File ingestionJobFile;
  TableType tableType;
  String tableName;
  FileFormat segmentFileFormat = FileFormat.CSV;

  public QuickstartTableRequest(String tableName, File schemaFile, File tableRequest, File ingestionJobFile,
      FileFormat segmentFileFormat) {
    this.tableName = tableName;
    this.schemaFile = schemaFile;
    this.tableRequestFile = tableRequest;
    tableType = TableType.OFFLINE;
    this.segmentFileFormat = segmentFileFormat;
    this.ingestionJobFile = ingestionJobFile;
  }

  public QuickstartTableRequest(String tableName, File schemaFile, File tableRequest) {
    this.tableName = tableName;
    this.schemaFile = schemaFile;
    this.tableRequestFile = tableRequest;
    tableType = TableType.REALTIME;
  }

  public FileFormat getSegmentFileFormat() {
    return segmentFileFormat;
  }

  public void setSegmentFileFormat(FileFormat segmentFileFormat) {
    this.segmentFileFormat = segmentFileFormat;
  }

  public File getSchemaFile() {
    return schemaFile;
  }

  public void setSchemaFile(File schemaFile) {
    this.schemaFile = schemaFile;
  }

  public File getTableRequestFile() {
    return tableRequestFile;
  }

  public void setTableRequestFile(File tableRequestFile) {
    this.tableRequestFile = tableRequestFile;
  }

  public File getIngestionJobFile() {
    return ingestionJobFile;
  }

  public void setIngestionJobFile(File ingestionJobFile) {
    this.ingestionJobFile = ingestionJobFile;
  }

  public TableType getTableType() {
    return tableType;
  }

  public void setTableType(TableType tableType) {
    this.tableType = tableType;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }
}
