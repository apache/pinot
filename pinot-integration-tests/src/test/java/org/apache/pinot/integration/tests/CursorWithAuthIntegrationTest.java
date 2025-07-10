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

import java.net.URL;
import java.util.List;
import java.util.Map;
import org.apache.hc.core5.http.Header;
import org.apache.pinot.common.auth.UrlAuthProvider;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.apache.pinot.integration.tests.BasicAuthTestUtils.AUTH_HEADER;
import static org.apache.pinot.integration.tests.BasicAuthTestUtils.AUTH_HEADER_BASIC;


@Test
public class CursorWithAuthIntegrationTest extends CursorIntegrationTest {
  final static String AUTH_PROVIDER_CLASS = UrlAuthProvider.class.getCanonicalName();
  final static URL AUTH_URL = CursorWithAuthIntegrationTest.class.getResource("/url-auth-token.txt");
  final static String AUTH_PREFIX = "Basic";

  protected Object[][] getPageSizesAndQueryEngine() {
    return new Object[][]{
        {false, 1000},
        {true, 1000}
    };
  }

  @Override
  protected void overrideControllerConf(Map<String, Object> properties) {
    BasicAuthTestUtils.addControllerConfiguration(properties);
    properties.put("controller.segment.fetcher.auth.provider.class", AUTH_PROVIDER_CLASS);
    properties.put("controller.segment.fetcher.auth.url", AUTH_URL);
    properties.put("controller.segment.fetcher.auth.prefix", AUTH_PREFIX);
    properties.put(ControllerConf.CONTROLLER_BROKER_AUTH_PREFIX + ".provider.class", AUTH_PROVIDER_CLASS);
    properties.put(ControllerConf.CONTROLLER_BROKER_AUTH_PREFIX + ".url", AUTH_URL);
    properties.put(ControllerConf.CONTROLLER_BROKER_AUTH_PREFIX + ".prefix", AUTH_PREFIX);
    properties.put(CommonConstants.CursorConfigs.RESPONSE_STORE_CLEANER_FREQUENCY_PERIOD, "5m");
  }

  @Override
  protected void overrideBrokerConf(PinotConfiguration configuration) {
    super.overrideBrokerConf(configuration);
    BasicAuthTestUtils.addBrokerConfiguration(configuration);
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    BasicAuthTestUtils.addServerConfiguration(serverConf);
    serverConf.setProperty("pinot.server.segment.fetcher.auth.provider.class", AUTH_PROVIDER_CLASS);
    serverConf.setProperty("pinot.server.segment.fetcher.auth.url", AUTH_URL);
    serverConf.setProperty("pinot.server.segment.fetcher.auth.prefix", AUTH_PREFIX);
    serverConf.setProperty("pinot.server.segment.uploader.auth.provider.class", AUTH_PROVIDER_CLASS);
    serverConf.setProperty("pinot.server.segment.uploader.auth.url", AUTH_URL);
    serverConf.setProperty("pinot.server.segment.uploader.auth.prefix", AUTH_PREFIX);
    serverConf.setProperty("pinot.server.instance.auth.provider.class", AUTH_PROVIDER_CLASS);
    serverConf.setProperty("pinot.server.instance.auth.url", AUTH_URL);
    serverConf.setProperty("pinot.server.instance.auth.prefix", AUTH_PREFIX);
  }

  @Override
  protected Map<String, String> getHeaders() {
    return BasicAuthTestUtils.AUTH_HEADER;
  }

  @Override
  protected Map<String, String> getControllerRequestClientHeaders() {
    return AUTH_HEADER;
  }

  @Override
  protected Map<String, String> getPinotClientTransportHeaders() {
    return AUTH_HEADER;
  }

  @Override
  protected List<Header> getSegmentUploadAuthHeaders() {
    return List.of(AUTH_HEADER_BASIC);
  }
}
