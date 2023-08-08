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


public class HttpClientConfigTest {

  @Test
  public void testNewBuilder() {
    // Ensure config values are picked up by the builder.
    PinotConfiguration pinotConfiguration = new PinotConfiguration();
    pinotConfiguration.setProperty(HttpClientConfig.MAX_CONNS_CONFIG_NAME, "123");
    pinotConfiguration.setProperty(HttpClientConfig.MAX_CONNS_PER_ROUTE_CONFIG_NAME, "11");
    pinotConfiguration.setProperty(HttpClientConfig.DISABLE_DEFAULT_USER_AGENT_CONFIG_NAME, "true");
    HttpClientConfig httpClientConfig = HttpClientConfig.newBuilder(pinotConfiguration).build();
    Assert.assertEquals(123, httpClientConfig.getMaxConnTotal());
    Assert.assertEquals(11, httpClientConfig.getMaxConnPerRoute());
    Assert.assertTrue(httpClientConfig.isDisableDefaultUserAgent());

    // Ensure default builder uses negative values
    HttpClientConfig defaultConfig = HttpClientConfig.newBuilder(new PinotConfiguration()).build();
    Assert.assertTrue(defaultConfig.getMaxConnTotal() < 0, "default value should be < 0");
    Assert.assertTrue(defaultConfig.getMaxConnPerRoute() < 0, "default value should be < 0");
    Assert.assertFalse(defaultConfig.isDisableDefaultUserAgent(), "Default user agent should be enabled by default");
  }
}
