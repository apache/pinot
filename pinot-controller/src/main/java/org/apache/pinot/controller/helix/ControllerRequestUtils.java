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
package org.apache.pinot.controller.helix;

import java.io.IOException;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.pinot.common.utils.http.HttpUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.tenant.Tenant;
import org.apache.pinot.spi.config.tenant.TenantRole;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;


public class ControllerRequestUtils {
  private final String _clusterName;
  private final ControllerRequestURLBuilder _controllerRequestURLBuilder;

  private ControllerRequestUtils(String clusterName, ControllerRequestURLBuilder controllerRequestURLBuilder) {
    _clusterName = clusterName;
    _controllerRequestURLBuilder = controllerRequestURLBuilder;
  }

  public static ControllerRequestUtils baseUrl(String clusterName, String baseUrl) {
    return new ControllerRequestUtils(clusterName, ControllerRequestURLBuilder.baseUrl(baseUrl));
  }

  /**
   * Add a schema to the controller.
   */
  public void addSchema(Schema schema)
      throws IOException {
    String url = _controllerRequestURLBuilder.forSchemaCreate();
    PostMethod postMethod = HttpUtils.sendMultipartPostRequest(url, schema.toSingleLineJsonString());
    if (postMethod.getStatusCode() != 200) {
      throw new IOException("Exception when add schema! " + postMethod.getStatusText());
    }
  }

  public Schema getSchema(String schemaName)
      throws IOException {
    String url = _controllerRequestURLBuilder.forSchemaGet(schemaName);
    String resp = HttpUtils.sendGetRequest(url);
    return JsonUtils.stringToObject(resp, Schema.class);
  }

  public void addTableConfig(TableConfig tableConfig)
      throws IOException {
    HttpUtils.sendPostRequest(_controllerRequestURLBuilder.forTableCreate(), tableConfig.toJsonString());
  }

  protected String getBrokerTenantRequestPayload(String tenantName, int numBrokers) {
    return new Tenant(TenantRole.BROKER, tenantName, numBrokers, 0, 0).toJsonString();
  }

  public void createBrokerTenant(String tenantName, int numBrokers)
      throws IOException {
    HttpUtils.sendPostRequest(_controllerRequestURLBuilder.forTenantCreate(),
        getBrokerTenantRequestPayload(tenantName, numBrokers));
  }

  public void updateBrokerTenant(String tenantName, int numBrokers)
      throws IOException {
    HttpUtils.sendPutRequest(_controllerRequestURLBuilder.forTenantCreate(),
        getBrokerTenantRequestPayload(tenantName, numBrokers));
  }

  protected String getServerTenantRequestPayload(String tenantName, int numOfflineServers,
      int numRealtimeServers) {
    return new Tenant(TenantRole.SERVER, tenantName, numOfflineServers + numRealtimeServers, numOfflineServers,
        numRealtimeServers).toJsonString();
  }

  public void createServerTenant(String tenantName, int numOfflineServers, int numRealtimeServers)
      throws IOException {
    HttpUtils.sendPostRequest(_controllerRequestURLBuilder.forTenantCreate(),
        getServerTenantRequestPayload(tenantName, numOfflineServers, numRealtimeServers));
  }

  public String getClusterName() {
    return _clusterName;
  }

  public ControllerRequestURLBuilder getControllerRequestURLBuilder() {
    return _controllerRequestURLBuilder;
  }
}
