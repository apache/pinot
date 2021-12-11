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
package org.apache.pinot.spi.environmentprovider;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PinotEnvironmentProviderFactoryTest {

  @Test
  public void testCustomPinotEnvironmentProviderFactory() {
    Map<String, Object> properties = new HashMap<>();
    properties.put("class.test", PinotEnvironmentProviderFactoryTest.TestEnvironmentProvider.class.getName());
    properties.put("test.maxRetry", "3");
    properties.put("test.connectionTimeoutMillis", "100");
    properties.put("test.requestTimeoutMillis", "100");
    PinotEnvironmentProviderFactory.init(new PinotConfiguration(properties));

    PinotEnvironmentProvider testPinotEnvironment = PinotEnvironmentProviderFactory.getEnvironmentProvider("test");
    Assert.assertTrue(testPinotEnvironment instanceof PinotEnvironmentProviderFactoryTest.TestEnvironmentProvider);
    Assert.assertEquals(
        ((PinotEnvironmentProviderFactoryTest.TestEnvironmentProvider) testPinotEnvironment).getInitCalled(), 1);
    Assert.assertEquals(
        ((PinotEnvironmentProviderFactoryTest.TestEnvironmentProvider) testPinotEnvironment).getConfiguration()
            .getProperty("maxRetry"), "3");
    Assert.assertEquals(
        ((PinotEnvironmentProviderFactoryTest.TestEnvironmentProvider) testPinotEnvironment).getConfiguration()
            .getProperty("connectionTimeoutMillis"), "100");
    Assert.assertEquals(
        ((PinotEnvironmentProviderFactoryTest.TestEnvironmentProvider) testPinotEnvironment).getConfiguration()
            .getProperty("requestTimeoutMillis"), "100");
  }

  public static class TestEnvironmentProvider implements PinotEnvironmentProvider {
    public int _initCalled = 0;
    private PinotConfiguration _configuration;

    public int getInitCalled() {
      return _initCalled;
    }

    @Override
    public void init(PinotConfiguration configuration) {
      _configuration = configuration;
      _initCalled++;
    }

    public PinotConfiguration getConfiguration() {
      return _configuration;
    }
  }
}
