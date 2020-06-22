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
package org.apache.pinot.spi.env;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;


public class PinotConfigurationTest {
  @Test
  public void assertPropertyPriorities() throws IOException {
    Map<String, Object> cliArguments = new HashMap<>();
    Map<String, String> mockedEnvironmentVariables = new HashMap<>();

    String configFile2 = File.createTempFile("pinot-configuration-test-2", ".properties").getAbsolutePath();
    String configFile3 = File.createTempFile("pinot-configuration-test-3", ".properties").getAbsolutePath();

    cliArguments.put("controller.host", "cli-argument-controller-host");
    cliArguments.put("config.paths", "classpath:/pinot-configuration-1.properties");
    mockedEnvironmentVariables.put("PINOT_CONTROLLER_HOST", "env-var-controller-host");
    mockedEnvironmentVariables.put("PINOT_CONTROLLER_PORT", "env-var-controller-port");
    mockedEnvironmentVariables.put("PINOT_RELAXEDPROPERTY_TEST", "true");
    mockedEnvironmentVariables.put("PINOT_CONFIG_PATHS", configFile2 + "," + configFile3);

    copyClasspathResource("/pinot-configuration-2.properties", configFile2);
    copyClasspathResource("/pinot-configuration-3.properties", configFile3);

    PinotConfiguration configuration = new PinotConfiguration(cliArguments, mockedEnvironmentVariables);

    // Tests that cli arguments have the highest priority.
    Assert.assertEquals(configuration.getProperty("controller.host"), "cli-argument-controller-host");

    // Tests that environment variable have priority overs config.paths properties.
    Assert.assertEquals(configuration.getProperty("controller.port"), "env-var-controller-port");

    // Tests that config.paths properties provided through cli arguments are prioritized.
    Assert.assertEquals(configuration.getProperty("controller.cluster-name"), "config-path-1-cluster-name");

    // Tests that config.paths properties provided through environment variables are available.
    Assert.assertEquals(configuration.getProperty("controller.timeout"), "config-path-2-timeout");

    // Tests a priority of a property available in both config files of a config.paths array. 
    Assert.assertEquals(configuration.getProperty("controller.config-paths-multi-value-test-1"),
        "config-path-2-config-paths-multi-value-test-1");

    // Tests properties provided through the last config file of a config.paths array. 
    Assert.assertEquals(configuration.getProperty("controller.config-paths-multi-value-test-2"),
        "config-path-3-config-paths-multi-value-test-2");

    // Tests relaxed binding on environment variables
    Assert.assertEquals(configuration.getProperty("relaxed-property.test"), "true");
  }

  private void copyClasspathResource(String classpathResource, String target) throws IOException {
    try (InputStream inputStream = PinotConfigurationTest.class.getResourceAsStream(classpathResource)) {
      Files.copy(inputStream, Paths.get(target), StandardCopyOption.REPLACE_EXISTING);
    }
  }
}
