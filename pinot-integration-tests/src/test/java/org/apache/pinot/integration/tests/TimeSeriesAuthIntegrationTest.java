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

import java.util.List;
import java.util.Map;
import org.apache.hc.core5.http.Header;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.integration.tests.BasicAuthTestUtils.AUTH_HEADER;
import static org.apache.pinot.integration.tests.BasicAuthTestUtils.AUTH_HEADER_BASIC;


public class TimeSeriesAuthIntegrationTest extends TimeSeriesIntegrationTest {
  protected static final Logger LOGGER = LoggerFactory.getLogger(TimeSeriesAuthIntegrationTest.class);

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
  protected void overrideControllerConf(Map<String, Object> properties) {
    super.overrideControllerConf(properties);
    BasicAuthTestUtils.addControllerConfiguration(properties);
  }

  @Override
  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    super.overrideBrokerConf(brokerConf);
    BasicAuthTestUtils.addBrokerConfiguration(brokerConf);
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    super.overrideServerConf(serverConf);
    BasicAuthTestUtils.addServerConfiguration(serverConf);
  }

  @Override
  protected List<Header> getSegmentUploadAuthHeaders() {
    return List.of(AUTH_HEADER_BASIC);
  }
}
