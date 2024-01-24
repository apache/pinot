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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.pinot.spi.ingestion.batch.spec.PinotFSSpec;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PinotConfigurationTest {

  @Test
  public void assertBaseOperations() {
    Map<String, Object> typedProperties = new HashMap<>();

    typedProperties.put("config.boolean", "true");
    typedProperties.put("config.double", "10.0");
    typedProperties.put("config.int", "100");
    typedProperties.put("config.long", "10000000");
    typedProperties.put("config.string", "val");
    typedProperties.put("config.list", "val1,val2,val3");

    PinotConfiguration pinotConfiguration = new PinotConfiguration(typedProperties);

    Assert.assertEquals(pinotConfiguration.getProperty("config.boolean", false), true);
    Assert.assertEquals(pinotConfiguration.getProperty("config.boolean", Boolean.class), Boolean.TRUE);
    Assert.assertEquals(pinotConfiguration.getProperty("config.boolean-missing", false), false);
    Assert.assertEquals(pinotConfiguration.getProperty("config.double", 0d), 10.0d);
    Assert.assertEquals(pinotConfiguration.getProperty("config.double-missing", 20d), 20d);
    Assert.assertEquals(pinotConfiguration.getProperty("config.int", 0), 100);
    Assert.assertEquals(pinotConfiguration.getProperty("config.int-missing", 200), 200);
    Assert.assertEquals(pinotConfiguration.getProperty("config.long", 0L), 10000000L);
    Assert.assertEquals(pinotConfiguration.getProperty("config.long-missing", 20000000L), 20000000L);
    Assert.assertEquals(pinotConfiguration.getProperty("config.string", "missing-val"), "val");
    Assert.assertEquals(pinotConfiguration.getProperty("config.string-missing", "missing-val"), "missing-val");

    // Asserts array properties can be read as string and array.
    Assert.assertEquals(pinotConfiguration.getProperty("config.list"), "val1,val2,val3");
    List<String> listValues = pinotConfiguration.getProperty("config.list", Arrays.asList());
    Assert.assertEquals(listValues.size(), 3);
    Assert.assertTrue(listValues.contains("val1"));
    Assert.assertTrue(listValues.contains("val2"));
    Assert.assertTrue(listValues.contains("val3"));
    Assert.assertEquals(pinotConfiguration.getProperty("config.list-missing", Arrays.asList()), Arrays.asList());

    Assert.assertFalse(pinotConfiguration.containsKey("missing-property"));
    Assert.assertNull(pinotConfiguration.getProperty("missing-property"));
  }

  @Test
  public void assertGetKeys() {
    Map<String, Object> properties = new HashMap<>();
    properties.put("property.1.key", "val1");
    properties.put("property.2.key", "val1");
    properties.put("property.3.key", "val1");
    properties.put("property.4.key", "val1");

    PinotConfiguration pinotConfiguration = new PinotConfiguration(properties, new HashMap<>());

    List<String> keys = pinotConfiguration.getKeys();
    Assert.assertTrue(keys.contains("property.1.key"));
    Assert.assertTrue(keys.contains("property.2.key"));
    Assert.assertTrue(keys.contains("property.3.key"));
    Assert.assertTrue(keys.contains("property.4.key"));
    Assert.assertEquals(keys.size(), 4);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void assertUnsupportedTypeBehavior() {
    Map<String, Object> properties = new HashMap<>();
    properties.put("property.invalid-type", "true");

    PinotConfiguration pinotConfiguration = new PinotConfiguration(properties);

    pinotConfiguration.getProperty("property.invalid-type", Math.class);
  }

  @Test
  public void assertPropertyOverride() {
    PinotConfiguration pinotConfiguration = new PinotConfiguration();
    pinotConfiguration.setProperty("property.override", "overriden-value");

    pinotConfiguration.containsKey("property.override");
    Assert.assertEquals(pinotConfiguration.getProperty("property.override"), "overriden-value");

    pinotConfiguration = new PinotConfiguration(pinotConfiguration.toMap()).clone().subset("property");

    pinotConfiguration.addProperty("override", "overriden-value-2");

    Assert.assertEquals(pinotConfiguration.getProperty("override"), "overriden-value,overriden-value-2");

    Object object = new Object();
    pinotConfiguration.setProperty("raw-property", object);
    Assert.assertEquals(pinotConfiguration.getRawProperty("raw-property"), object);
  }

  @Test
  public void assertPropertyPriorities()
      throws IOException {
    Map<String, Object> baseProperties = new HashMap<>();
    Map<String, String> mockedEnvironmentVariables = new HashMap<>();

    String configFile2 = File.createTempFile("pinot-configuration-test-2", ".properties").getAbsolutePath();
    String configFile3 = File.createTempFile("pinot-configuration-test-3", ".properties").getAbsolutePath();

    baseProperties.put("controller.host", "cli-argument-controller-host");
    baseProperties.put("config.paths", "classpath:/pinot-configuration-1.properties");
    mockedEnvironmentVariables.put("PINOT_ENV_CONTROLLER_HOST", "env-var-controller-host");
    mockedEnvironmentVariables.put("PINOT_ENV_CONTROLLER_PORT", "env-var-controller-port");
    mockedEnvironmentVariables.put("PINOT_ENV_RELAXEDPROPERTY_TEST", "true");
    mockedEnvironmentVariables.put("PINOT_ENV_CONFIG_PATHS", configFile2 + "," + configFile3);

    // Legacy prefix
    mockedEnvironmentVariables.put("PINOT_LEGACY_PROP", "legacy");

    copyClasspathResource("/pinot-configuration-2.properties", configFile2);
    copyClasspathResource("/pinot-configuration-3.properties", configFile3);

    PinotConfiguration configuration = new PinotConfiguration(baseProperties, mockedEnvironmentVariables);

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

    // Legacy prefix test
    Assert.assertEquals(configuration.getProperty("legacy.prop"), "legacy");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void assertInvalidConfigPathBehavior() {
    Map<String, Object> baseProperties = new HashMap<>();

    baseProperties.put("config.paths", "invalid-path.properties");

    new PinotConfiguration(baseProperties);
  }

  @Test
  public void assertPropertiesFromBaseConfiguration()
      throws ConfigurationException {
    PropertiesConfiguration propertiesConfiguration = CommonsConfigurationUtils.fromPath(
        PropertiesConfiguration.class.getClassLoader().getResource("pinot-configuration-1.properties").getFile(), true,
        true);

    PinotConfiguration config = new PinotConfiguration(propertiesConfiguration);

    Assert.assertEquals(config.getProperty("pinot.server.storage.factory.class.s3"),
        "org.apache.pinot.plugin.filesystem.S3PinotFS");
    Assert.assertEquals(config.getProperty("pinot.server.segment.fetcher.protocols"), "file,http,s3");
  }

  @Test
  public void assertPropertiesFromFSSpec() {
    Map<String, String> configs = new HashMap<>();
    configs.put("config.property.1", "val1");
    configs.put("config.property.2", "val2");
    configs.put("config.property.3", "val3");

    PinotFSSpec pinotFSSpec = new PinotFSSpec();
    pinotFSSpec.setConfigs(configs);

    PinotConfiguration pinotConfiguration = new PinotConfiguration(pinotFSSpec);

    Assert.assertEquals(pinotConfiguration.getProperty("config.property.1"), "val1");
    Assert.assertEquals(pinotConfiguration.getProperty("config.property.2"), "val2");
    Assert.assertEquals(pinotConfiguration.getProperty("config.property.3"), "val3");

    // Asserts no error occurs when no configuration is provided in the spec.
    new PinotConfiguration(new PinotFSSpec());
  }

  @Test
  public void assertPropertyInterpolation() {
    Map<String, Object> configs = new HashMap<>();
    configs.put("config.property.1", "${sys:PINOT_CONFIGURATION_TEST_VAR}");
    PinotConfiguration pinotConfiguration = new PinotConfiguration(configs);

    try {
      // Env var is not defined, thus interpolation doesn't happen
      Assert.assertEquals(pinotConfiguration.getProperty("config.property.1"), "${sys:PINOT_CONFIGURATION_TEST_VAR}");

      // String value
      System.setProperty("PINOT_CONFIGURATION_TEST_VAR", "val1");
      Assert.assertEquals(pinotConfiguration.getProperty("config.property.1"), "val1");
      Assert.assertEquals(pinotConfiguration.getProperty("config.property.1", "defaultVal"), "val1");
      Map<String, Object> properties = pinotConfiguration.toMap();
      Assert.assertEquals(properties.get("config.property.1"), "val1");

      // Boolean value
      System.setProperty("PINOT_CONFIGURATION_TEST_VAR", "true");
      Assert.assertTrue(pinotConfiguration.getProperty("config.property.1", false));
      properties = pinotConfiguration.toMap();
      Assert.assertEquals(properties.get("config.property.1"), "true");
      Assert.assertTrue(new PinotConfiguration(properties).getProperty("config.property.1", false));

      // Number value
      System.setProperty("PINOT_CONFIGURATION_TEST_VAR", "10");
      Assert.assertEquals(pinotConfiguration.getProperty("config.property.1", 0), 10);
      properties = pinotConfiguration.toMap();
      Assert.assertEquals(properties.get("config.property.1"), "10");
      Assert.assertEquals(new PinotConfiguration(properties).getProperty("config.property.1", 0), 10);

      // String array, with elements specified as env vars separately.
      System.setProperty("PINOT_CONFIGURATION_TEST_VAR", "a");
      System.setProperty("PINOT_CONFIGURATION_TEST_VAR2", "b");
      configs.put("config.property.1", "${sys:PINOT_CONFIGURATION_TEST_VAR},${sys:PINOT_CONFIGURATION_TEST_VAR2}");
      pinotConfiguration = new PinotConfiguration(configs);
      Assert.assertEquals(pinotConfiguration.getProperty("config.property.1", Arrays.asList()),
          Arrays.asList("a", "b"));
      Assert.assertEquals(pinotConfiguration.getProperty("config.property.1", "val1"), "a,b");
      Assert.assertEquals(pinotConfiguration.getProperty("config.property.1"), "a,b");
      properties = pinotConfiguration.toMap();
      Assert.assertEquals(properties.get("config.property.1"), "a,b");
      Assert.assertEquals(new PinotConfiguration(properties).getProperty("config.property.1", Arrays.asList()),
          Arrays.asList("a", "b"));
      Assert.assertEquals(new PinotConfiguration(properties).getProperty("config.property.1", "val1"), "a,b");
      Assert.assertEquals(new PinotConfiguration(properties).getProperty("config.property.1"), "a,b");
    } finally {
      System.clearProperty("PINOT_CONFIGURATION_TEST_VAR");
      System.clearProperty("PINOT_CONFIGURATION_TEST_VAR2");
    }
  }

  private void copyClasspathResource(String classpathResource, String target)
      throws IOException {
    try (InputStream inputStream = PinotConfigurationTest.class.getResourceAsStream(classpathResource)) {
      Files.copy(inputStream, Paths.get(target), StandardCopyOption.REPLACE_EXISTING);
    }
  }
}
