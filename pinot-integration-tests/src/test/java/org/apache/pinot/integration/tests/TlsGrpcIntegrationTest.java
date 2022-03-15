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
package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import org.apache.pinot.client.Request;
import org.apache.pinot.client.ResultSetGroup;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.Assert;


// TLS based test, but use gRPC request handler between Broker and Server
public class TlsGrpcIntegrationTest extends TlsIntegrationTest {

  //TODO: after grpcBrokerRequestHandler can handle select count(*), delete this method
  @Override
  protected long getCurrentCountStarResult()
      throws Exception {
    return getPinotConnection().prepareStatement(new Request("sql", "SELECT * FROM " + getTableName())).execute()
        .getExecutionStats().getTotalDocs();
  }

  @Override
  protected PinotConfiguration getDefaultBrokerConfiguration() {
    Map<String, Object> prop = super.getDefaultBrokerConfiguration().toMap();

    prop.put(CommonConstants.Broker.BROKER_REQUEST_HANDLER_TYPE,
        CommonConstants.Broker.GRPC_BROKER_REQUEST_HANDLER_TYPE);

    prop.put("pinot.broker.nettytls.enabled", "true");

    return new PinotConfiguration(prop);
  }

  @Override
  protected PinotConfiguration getDefaultServerConfiguration() {
    Map<String, Object> prop = super.getDefaultServerConfiguration().toMap();
    prop.put("pinot.server.grpctls.enabled", "true");
    prop.put("pinot.server.grpctls.keystore.path", _tlsStorePKCS12);
    prop.put("pinot.server.grpctls.keystore.password", PASSWORD);
    prop.put("pinot.server.grpctls.keystore.type", PKCS_12);
    prop.put("pinot.server.grpctls.truststore.path", _tlsStorePKCS12);
    prop.put("pinot.server.grpctls.truststore.password", PASSWORD);
    prop.put("pinot.server.grpctls.truststore.type", PKCS_12);
    prop.put("pinot.server.grpctls.client.auth.enabled", "true");
    prop.put("pinot.server.grpc.port", "8090");
    prop.put("pinot.server.grpc.enable", "true");
    return new PinotConfiguration(prop);
  }

  //TODO: after grpcBrokerRequestHandler can handle select count(*), delete this method
  @Override
  protected String getCountQueryString() {
    return "SELECT * FROM " + getTableName();
  }

  @Override
  protected void verifyRows(JsonNode resultTable) {
    Assert.assertTrue(resultTable.get("rows").size() >= 10);
  }

  @Override
  protected void verifyRows(ResultSetGroup resultSetGroup) {
    Assert.assertTrue(resultSetGroup.getResultSet(0).getRowCount() >= 10);
  }
}
