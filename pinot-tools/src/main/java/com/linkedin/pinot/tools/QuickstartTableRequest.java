package com.linkedin.pinot.tools;

import java.io.File;

import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;


public class QuickstartTableRequest {

  File schemaFile;
  File tableRequestFile;
  File dataDir;
  TableType tableType;
  String tableName;
  
  public QuickstartTableRequest(String tableName, File schemaFile, File tableRequest, File dataDir) {
    this.tableName = tableName;
    this.schemaFile = schemaFile;
    this.dataDir = dataDir;
    this.tableRequestFile = tableRequest;
    tableType = TableType.OFFLINE;
  }

  public QuickstartTableRequest(String tableName, File schemaFile, File tableRequest) {
    this.tableName = tableName;
    this.schemaFile = schemaFile;
    this.tableRequestFile = tableRequest;
    tableType = TableType.REALTIME;
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

  public File getDataDir() {
    return dataDir;
  }

  public void setDataDir(File dataDir) {
    this.dataDir = dataDir;
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
