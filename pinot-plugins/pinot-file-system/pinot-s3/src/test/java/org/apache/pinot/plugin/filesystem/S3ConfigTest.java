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
package org.apache.pinot.plugin.filesystem;

import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;


public class S3ConfigTest {
  @Test
  public void testGetHttpClientBuilder() {
    PinotConfiguration pinotConfig = new PinotConfiguration();
    S3Config cfg = new S3Config(pinotConfig);
    Assert.assertNull(cfg.getHttpClientBuilder());

    pinotConfig.setProperty("httpclient.maxConnections", 100);
    pinotConfig.setProperty("httpclient.socketTimeout", "PT1M10S");
    pinotConfig.setProperty("httpclient.connectionTimeout", "PT2M20S");
    pinotConfig.setProperty("httpclient.connectionTimeToLive", "3m30s");
    pinotConfig.setProperty("httpclient.connectionAcquisitionTimeout", "4m40s");
    cfg = new S3Config(pinotConfig);
    Assert.assertNotNull(cfg.getHttpClientBuilder());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseDuration() {
    Assert.assertEquals(S3Config.parseDuration("P1DT2H30S"), S3Config.parseDuration("1d2h30s"));
    S3Config.parseDuration("10");
  }
}
