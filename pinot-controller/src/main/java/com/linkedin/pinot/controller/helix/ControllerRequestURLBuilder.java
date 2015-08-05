/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.controller.helix;

import org.apache.commons.lang.StringUtils;

import com.linkedin.pinot.common.utils.StringUtil;


/**
 * Sep 30, 2014
 */

public class ControllerRequestURLBuilder {
  private final String _baseUrl;

  private ControllerRequestURLBuilder(String baseUrl) {
    _baseUrl = baseUrl;
  }

  public static ControllerRequestURLBuilder baseUrl(String baseUrl) {
    return new ControllerRequestURLBuilder(baseUrl);
  }

  public String forResourceCreate() {
    return StringUtil.join("/", StringUtils.chomp(_baseUrl, "/"), "dataresources");
  }

  public String forResourceDelete(String resourceName) {
    return StringUtil.join("/", StringUtils.chomp(_baseUrl, "/"), "dataresources", resourceName);
  }

  public String forResourceGet(String resourceName) {
    return StringUtil.join("/", StringUtils.chomp(_baseUrl, "/"), "dataresources", resourceName);
  }

  public String forDataFileUpload() {
    return StringUtil.join("/", StringUtils.chomp(_baseUrl, "/"), "segments");
  }

  public String forInstanceCreate() {
    return StringUtil.join("/", StringUtils.chomp(_baseUrl, "/"), "instances/");
  }

  public String forInstanceDisable(String instanceName) {
    return StringUtil.join("/", StringUtils.chomp(_baseUrl, "/"), "instances", instanceName, "?state=disable");
  }

  public String forInstanceEnable(String instanceName) {
    return StringUtil.join("/", StringUtils.chomp(_baseUrl, "/"), "instances", instanceName, "?state=enable");
  }

  public String forInstanceBulkCreate() {
    return StringUtil.join("/", StringUtils.chomp(_baseUrl, "/"), "instances", "bulkAdd");
  }

  // V2 API started
  public String forTenantCreate() {
    return StringUtil.join("/", StringUtils.chomp(_baseUrl, "/"), "tenants/");
  }

  public String forBrokerTenantCreate() {
    return StringUtil.join("/", StringUtils.chomp(_baseUrl, "/"), "tenants?type=broker");
  }

  public String forServerTenantCreate() {
    return StringUtil.join("/", StringUtils.chomp(_baseUrl, "/"), "tenants?type=server");
  }

  public String forTenantCreate(String tenantName) {
    return StringUtil.join("/", StringUtils.chomp(_baseUrl, "/"), "tenants", tenantName, "instances");
  }

  public String forServerTenantCreate(String tenantName) {
    return StringUtil.join("/", StringUtils.chomp(_baseUrl, "/"), "tenants", tenantName, "instances?type=server");
  }

  public String forBrokerTenantCreate(String tenantName) {
    return StringUtil.join("/", StringUtils.chomp(_baseUrl, "/"), "tenants", tenantName, "instances?type=broker");
  }

  public String forTenantGet() {
    return StringUtil.join("/", StringUtils.chomp(_baseUrl, "/"), "tenants");
  }

  public String forTenantGet(String tenantName) {
    return StringUtil.join("/", StringUtils.chomp(_baseUrl, "/"), "tenants", tenantName);
  }

  public String forBrokerTenantGet(String tenantName) {
    return StringUtil.join("/", StringUtils.chomp(_baseUrl, "/"), "tenants", tenantName, "?type=broker");
  }

  public String forServerTenantGet(String tenantName) {
    return StringUtil.join("/", StringUtils.chomp(_baseUrl, "/"), "tenants", tenantName, "?type=server");
  }

  public String forBrokerTenantDelete(String tenantName) {
    return StringUtil.join("/", StringUtils.chomp(_baseUrl, "/"), "tenants", tenantName, "?type=broker");
  }

  public String forServerTenantDelete(String tenantName) {
    return StringUtil.join("/", StringUtils.chomp(_baseUrl, "/"), "tenants", tenantName, "?type=server");
  }

  public String forTableCreate() {
    return StringUtil.join("/", StringUtils.chomp(_baseUrl, "/"), "tables");
  }

  public String forTableGetConfig(String tableName) {
    return StringUtil.join("/", StringUtils.chomp(_baseUrl, "/"), "tables", tableName, "configs");
  }

  public String forTableGetServerInstances(String tableName) {
    return StringUtil.join("/", StringUtils.chomp(_baseUrl, "/"), "tables", tableName, "instances?type=server");
  }

  public String forTableGetBrokerInstances(String tableName) {
    return StringUtil.join("/", StringUtils.chomp(_baseUrl, "/"), "tables", tableName, "instances?type=broker");
  }

  public String forTableGet(String tableName) {
    return StringUtil.join("/", StringUtils.chomp(_baseUrl, "/"), "tables", tableName, "instances");
  }

  public String forTableDelete(String tableName) {
    return StringUtil.join("/", StringUtils.chomp(_baseUrl, "/"), "tables", tableName);
  }

  public static void main(String[] args) {
    System.out.println(ControllerRequestURLBuilder.baseUrl("localhost:8089").forResourceCreate());
    System.out.println(ControllerRequestURLBuilder.baseUrl("localhost:8089").forInstanceCreate());
  }

}
