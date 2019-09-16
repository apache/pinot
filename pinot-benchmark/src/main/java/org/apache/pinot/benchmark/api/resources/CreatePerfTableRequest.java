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
package org.apache.pinot.benchmark.api.resources;

import org.apache.pinot.benchmark.common.utils.PerfTableMode;


public class CreatePerfTableRequest {
  private String _rawTableName = null;
  private String _tableName = null;
  private String _tableType = null;
  private PerfTableMode _mode = null;
  private String _ldap = null;
  private String _versionId = null;
  private int _tableRetention = -1;
  private String _schema = null;
  private String _realtimeTableConfig;
  private String _offlineTableConfig;
  private String _tenantTag;
  private int _numBrokers;
  private int _numOfflineServers;
  private int _numRealtimeServers;

  public CreatePerfTableRequest() {
  }

  public CreatePerfTableRequest(String rawTableName, String tableName, String tableType, PerfTableMode mode,
      String ldap, String versionId, int tableRetention, String schema, String offlineTableConfig,
      String realtimeTableConfig, String tenantTag, int numBrokers, int numOfflineServers, int numRealtimeServers) {
    _rawTableName = rawTableName;
    _tableName = tableName;
    _tableType = tableType;
    _mode = mode;
    _ldap = ldap;
    _versionId = versionId;
    _tableRetention = tableRetention;
    _schema = schema;
    _offlineTableConfig = offlineTableConfig;
    _realtimeTableConfig = realtimeTableConfig;
    _tenantTag = tenantTag;
    _numBrokers = numBrokers;
    _numOfflineServers = numOfflineServers;
    _numRealtimeServers = numRealtimeServers;
  }

  public String getRawTableName() {
    return _rawTableName;
  }

  public void setRawTableName(String rawTableName) {
    _rawTableName = rawTableName;
  }

  public String getTableName() {
    return _tableName;
  }

  public void setTableName(String tableName) {
    _tableName = tableName;
  }

  public String getTableType() {
    return _tableType;
  }

  public void setTableType(String tableType) {
    _tableType = tableType;
  }

  public PerfTableMode getMode() {
    return _mode;
  }

  public void setMode(PerfTableMode mode) {
    _mode = mode;
  }

  public String getLdap() {
    return _ldap;
  }

  public void setLdap(String ldap) {
    _ldap = ldap;
  }

  public String getVersionId() {
    return _versionId;
  }

  public void setVersionId(String versionId) {
    _versionId = versionId;
  }

  public int getTableRetention() {
    return _tableRetention;
  }

  public void setTableRetention(int tableRetention) {
    _tableRetention = tableRetention;
  }

  public String getSchema() {
    return _schema;
  }

  public void setSchema(String schema) {
    this._schema = schema;
  }

  public String getRealtimeTableConfig() {
    return _realtimeTableConfig;
  }

  public void setRealtimeTableConfig(String realtimeTableConfig) {
    _realtimeTableConfig = realtimeTableConfig;
  }

  public String getOfflineTableConfig() {
    return _offlineTableConfig;
  }

  public void setOfflineTableConfig(String offlineTableConfig) {
    _offlineTableConfig = offlineTableConfig;
  }

  public String getTenantTag() {
    return _tenantTag;
  }

  public void setTenantTag(String tenantTag) {
    _tenantTag = tenantTag;
  }

  public int getNumBrokers() {
    return _numBrokers;
  }

  public void setNumBrokers(int numBrokers) {
    _numBrokers = numBrokers;
  }

  public int getNumOfflineServers() {
    return _numOfflineServers;
  }

  public void setNumOfflineServers(int numOfflineServers) {
    _numOfflineServers = numOfflineServers;
  }

  public int getNumRealtimeServers() {
    return _numRealtimeServers;
  }

  public void setNumRealtimeServers(int numRealtimeServers) {
    _numRealtimeServers = numRealtimeServers;
  }
}
