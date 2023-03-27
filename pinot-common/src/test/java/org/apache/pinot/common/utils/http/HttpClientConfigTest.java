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
package org.apache.pinot.common.utils.http;

import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class HttpClientConfigTest {

  @Test
  public void testNewBuilder() {
    String testConfigPrefix = "pinot.test.";
    PinotConfiguration pinotConfiguration = new PinotConfiguration();
    pinotConfiguration.setProperty(testConfigPrefix + HttpClientConfig.MAX_CONNS_CONFIG_NAME, "123");
    pinotConfiguration.setProperty(testConfigPrefix + HttpClientConfig.MAX_CONNS_PER_ROUTE_CONFIG_NAME, "11");
    HttpClientConfig httpClientConfig = HttpClientConfig.newBuilder(pinotConfiguration, testConfigPrefix).build();
    Assert.assertEquals(123, httpClientConfig.getMaxConns());
    Assert.assertEquals(11, httpClientConfig.getMaxConnsPerRoute());
  }
}