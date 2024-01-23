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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.configuration2.CompositeConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.convert.LegacyListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.pinot.spi.ingestion.batch.spec.PinotFSSpec;
import org.apache.pinot.spi.utils.Obfuscator;


/**
 * <p>
 * Provides a configuration abstraction for Pinot to decouple services from configuration sources and frameworks.
 * </p>
 * <p>
 * Pinot services may retreived configurations from PinotConfiguration independently from any source of configuration.
 * {@link PinotConfiguration} currently supports configuration loaded from the following sources :
 *
 * <ul>
 * <li>Apache Commons {@link Configuration} (see {@link #PinotConfiguration(Configuration)})</li>
 * <li>Generic key-value properties provided with a {@link Map} (see
 * {@link PinotConfiguration#PinotConfiguration(Map)}</li>
 * <li>Environment variables (see {@link PinotConfiguration#PinotConfiguration(Map, Map)}</li>
 * <li>{@link PinotFSSpec} (see {@link PinotConfiguration#PinotConfiguration(PinotFSSpec)}</li>
 * </ul>
 * </p>
 * <p>
 * These different sources will all merged in an underlying {@link CompositeConfiguration} for which all
 * configuration keys will have
 * been sanitized.
 * Through this mechanism, properties can be configured and retrieved with kebab case, camel case, snake case and
 * environment variable
 * conventions.
 * </p>
 * <table>
 * <tr>
 * <th>Property</th>
 * <th>Note</th>
 * </tr>
 * <tr>
 * <td>module.sub-module.alerts.email-address</td>
 * <td>Kebab case, which is recommended for use in .properties and .yml files.</td>
 * </tr>
 * <tr>
 * <td>module.subModule.alerts.emailAddress</td>
 * <td>Standard camel case syntax.</td>
 * </tr>
 * <tr>
 * <td>controller.sub_module.alerts.email_address</td>
 * <td>Snake case notation, which is an alternative format for use in .properties and .yml files.</td>
 * </tr>
 * <tr>
 * <td>PINOT_MODULE_SUBMODULE_ALERTS_EMAILADDRESS</td>
 * <td>Upper case format, which is recommended when using system environment variables.</td>
 * </tr>
 * </table>
 *
 */
public class PinotConfiguration {
  public static final String CONFIG_PATHS_KEY = "config.paths";
  public static final String ENV_PREFIX = "PINOT_CONF_";

  private final CompositeConfiguration _configuration;

  /**
   * Creates an empty instance.
   */
  public PinotConfiguration() {
    this(new HashMap<>());
  }

  /**
   * Builds a new instance out of an existing Apache Commons {@link Configuration} instance. Will apply property key
   * sanitization to
   * enable relaxed binding.
   * Properties from the base configuration will be copied and won't be linked.
   *
   * @param baseConfiguration an existing {@link Configuration} for which all properties will be duplicated and
   *                          sanitized for relaxed
   *                          binding.
   */
  public PinotConfiguration(Configuration baseConfiguration) {
    _configuration = new CompositeConfiguration(computeConfigurationsFromSources(baseConfiguration, new HashMap<>()));
  }

  /**
   * Creates a new instance with existing properties provided through a map.
   *
   * @param baseProperties to provide programmatically through a {@link Map}.
   */
  public PinotConfiguration(Map<String, Object> baseProperties) {
    this(baseProperties, new SystemEnvironment().getEnvironmentVariables());
  }

  /**
   * Creates a new instance with existing properties provided through a map as well as environment variables.
   * Base properties will have priority offers properties defined in environment variables. Keys will be
   * sanitized for relaxed binding. See {@link PinotConfiguration} for further details.
   *
   * @param baseProperties with highest precedences (e.g. CLI arguments)
   * @param environmentVariables as a {@link Map}.
   */
  public PinotConfiguration(Map<String, Object> baseProperties, Map<String, String> environmentVariables) {
    _configuration = new CompositeConfiguration(computeConfigurationsFromSources(baseProperties, environmentVariables));
  }

  /**
   * Helper constructor to create an instance from configurations found in a PinotFSSpec instance.
   * Intended for use by Job Runners
   *
   * @param pinotFSSpec
   */
  public PinotConfiguration(PinotFSSpec pinotFSSpec) {
    this(Optional.ofNullable(pinotFSSpec.getConfigs()).map(configs -> configs.entrySet().stream()
            .collect(Collectors.<Entry<String, String>, String, Object>toMap(Entry::getKey, Entry::getValue)))
        .orElseGet(HashMap::new));
  }

  private static List<Configuration> computeConfigurationsFromSources(Configuration baseConfiguration,
      Map<String, String> environmentVariables) {
    return computeConfigurationsFromSources(relaxConfigurationKeys(baseConfiguration), environmentVariables);
  }

  private static List<Configuration> computeConfigurationsFromSources(Map<String, Object> baseProperties,
      Map<String, String> environmentVariables) {
    Map<String, Object> relaxedBaseProperties = relaxProperties(baseProperties);
    Map<String, String> relaxedEnvVariables = relaxEnvironmentVariables(environmentVariables);

    Stream<Configuration> propertiesFromConfigPaths =
        Stream.of(Optional.ofNullable(relaxedBaseProperties.get(CONFIG_PATHS_KEY)).map(Object::toString),
                Optional.ofNullable(relaxedEnvVariables.get(CONFIG_PATHS_KEY))).filter(Optional::isPresent)
            .map(Optional::get).flatMap(configPaths -> Arrays.stream(configPaths.split(",")))
            .map(PinotConfiguration::loadProperties);

    return Stream.concat(Stream.of(relaxedBaseProperties, relaxedEnvVariables).map(e -> {
      MapConfiguration mapConfiguration = new MapConfiguration(e);
      mapConfiguration.setListDelimiterHandler(new LegacyListDelimiterHandler(','));
      return mapConfiguration;
    }), propertiesFromConfigPaths).collect(Collectors.toList());
  }

  private static String getProperty(String name, Configuration configuration) {
    return Optional.of(configuration.getStringArray(relaxPropertyName(name)))

        .filter(values -> values.length > 0)

        .map(Arrays::stream)

        .map(stream -> stream.collect(Collectors.joining(",")))

        .orElse(null);
  }

  private static Configuration loadProperties(String configPath) {
    try {
      PropertiesConfiguration propertiesConfiguration;
      if (configPath.startsWith("classpath:")) {
        propertiesConfiguration = CommonsConfigurationUtils.fromInputStream(
            PinotConfiguration.class.getResourceAsStream(configPath.substring("classpath:".length())), true, true);
      } else {
        propertiesConfiguration = CommonsConfigurationUtils.fromPath(configPath, true, true);
      }
      return propertiesConfiguration;
    } catch (ConfigurationException e) {
      throw new IllegalArgumentException("Could not read properties from " + configPath, e);
    }
  }

  private static Map<String, Object> relaxConfigurationKeys(Configuration configuration) {
    return CommonsConfigurationUtils.getKeysStream(configuration).distinct()

        .collect(Collectors.toMap(PinotConfiguration::relaxPropertyName, configuration::getProperty));
  }

  private static Map<String, String> relaxEnvironmentVariables(Map<String, String> environmentVariables) {
    return environmentVariables.entrySet().stream().filter(entry -> entry.getKey().startsWith(ENV_PREFIX))
        .collect(Collectors.toMap(PinotConfiguration::relaxEnvVarName, Entry::getValue));
  }

  private static String relaxEnvVarName(Entry<String, String> envVarEntry) {
    return envVarEntry.getKey().substring(ENV_PREFIX.length()).replace("_", ".").toLowerCase();
  }

  private static Map<String, Object> relaxProperties(Map<String, Object> properties) {
    return properties.entrySet().stream()
        .collect(Collectors.toMap(PinotConfiguration::relaxPropertyName, Entry::getValue));
  }

  private static String relaxPropertyName(Entry<String, Object> propertyEntry) {
    return relaxPropertyName(propertyEntry.getKey());
  }

  private static String relaxPropertyName(String propertyName) {
    return propertyName.replace("-", "").replace("_", "").toLowerCase();
  }

  /**
   * Overwrites a property value in memory.
   *
   * @param name of the property to append in memory. Applies relaxed binding on the property name.
   * @param value to overwrite in memory
   *
   * @deprecated Configurations should be immutable. Prefer creating a new {@link #PinotConfiguration} with base
   * properties to overwrite
   * properties.
   */
  public void addProperty(String name, Object value) {
    _configuration.addProperty(relaxPropertyName(name), value);
  }

  /**
   * Creates a new instance of {@link PinotConfiguration} by closing the underlying {@link CompositeConfiguration}.
   * Useful to mutate configurations {@link PinotConfiguration} without impacting the original configurations.
   *
   *  @return a new {@link PinotConfiguration} instance with a cloned {@link CompositeConfiguration}.
   */
  public PinotConfiguration clone() {
    return new PinotConfiguration(_configuration);
  }

  /**
   * Check if the configuration contains the specified key after being sanitized.
   * @param key to sanitized and lookup
   * @return true if key exists.
   */
  public boolean containsKey(String key) {
    return _configuration.containsKey(relaxPropertyName(key));
  }

  /**
   * @return all the sanitized keys defined in the underlying {@link CompositeConfiguration}.
   */
  public List<String> getKeys() {
    return CommonsConfigurationUtils.getKeys(_configuration);
  }

  /**
   * Retrieves a String value with the given property name. See {@link PinotConfiguration} for supported key naming
   * conventions.
   *
   * If multiple properties exists with the same key, all values will be joined as a comma separated String.
   *
   * @param name of the property to retrieve a value. Property name will be sanitized.
   * @return the property String value. Null if missing.
   */
  public String getProperty(String name) {
    return getProperty(name, _configuration);
  }

  /**
   * Retrieves a boolean value with the given property name. See {@link PinotConfiguration} for supported key naming
   * conventions.
   *
   * @param name of the property to retrieve a value. Property name will be sanitized.
   * @return the property boolean value. Fallback to default value if missing.
   */
  public boolean getProperty(String name, boolean defaultValue) {
    return getProperty(name, defaultValue, Boolean.class);
  }

  /**
   * Retrieves a typed value with the given property name. See {@link PinotConfiguration} for supported key naming
   * conventions.
   *
   * @param name of the property to retrieve a value. Property name will be sanitized.
   * @param returnType a class reference of the value type.
   * @return the typed configuration value. Null if missing.
   */
  public <T> T getProperty(String name, Class<T> returnType) {
    return getProperty(name, null, returnType);
  }

  /**
   * Retrieves a double value with the given property name. See {@link PinotConfiguration} for supported key naming
   * conventions.
   *
   * @param name of the property to retrieve a value. Property name will be sanitized.
   * @return the property double value. Fallback to default value if missing.
   */
  public double getProperty(String name, double defaultValue) {
    return getProperty(name, defaultValue, Double.class);
  }

  /**
   * Retrieves a integer value with the given property name. See {@link PinotConfiguration} for supported key naming
   * conventions.
   *
   * @param name of the property to retrieve a value. Property name will be sanitized.
   * @return the property integer value. Fallback to default value if missing.
   */
  public int getProperty(String name, int defaultValue) {
    return getProperty(name, defaultValue, Integer.class);
  }

  /**
   * Retrieves a list of String values with the given property name. See {@link PinotConfiguration} for supported key
   * naming conventions.
   *
   * @param name of the property to retrieve a list of values. Property name will be sanitized.
   * @return the property String value. Fallback to the provided default values if no property is found.
   */
  public List<String> getProperty(String name, List<String> defaultValues) {
    return Optional.of(
            Arrays.stream(_configuration.getStringArray(relaxPropertyName(name))).collect(Collectors.toList()))
        .filter(list -> !list.isEmpty()).orElse(defaultValues);
  }

  /**
   * Retrieves a long value with the given property name. See {@link PinotConfiguration} for supported key naming
   * conventions.
   *
   * @param name of the property to retrieve a value. Property name will be sanitized.
   * @return the property long value. Fallback to default value if missing.
   */
  public long getProperty(String name, long defaultValue) {
    return getProperty(name, defaultValue, Long.class);
  }

  /**
   * Retrieves a String value with the given property name. See {@link PinotConfiguration} for supported key naming
   * conventions.
   *
   * @param name of the property to retrieve a value. Property name will be sanitized.
   * @return the property String value. Fallback to default value if missing.
   */
  public String getProperty(String name, String defaultValue) {
    String relaxedPropertyName = relaxPropertyName(name);
    if (!_configuration.containsKey(relaxedPropertyName)) {
      return defaultValue;
    }
    // Method below calls getStringArray() which can interpolate multi values properly.
    return getProperty(name, _configuration);
  }

  private <T> T getProperty(String name, T defaultValue, Class<T> returnType) {
    String relaxedPropertyName = relaxPropertyName(name);
    if (!_configuration.containsKey(relaxedPropertyName)) {
      return defaultValue;
    }
    Object rawProperty = _configuration.getProperty(relaxedPropertyName);
    if (CommonsConfigurationUtils.needInterpolate(rawProperty)) {
      return CommonsConfigurationUtils.interpolate(_configuration, relaxedPropertyName, defaultValue, returnType);
    }
    return CommonsConfigurationUtils.convert(rawProperty, returnType);
  }

  /**
   * Returns the raw object stored within the underlying {@link CompositeConfiguration}.
   *
   * See {@link PinotConfiguration} for supported key naming conventions.
   *
   * @param name of the property to retrieve a raw object value. Property name will be sanitized.
   * @return the object referenced. Null if key is not found.
   */
  public Object getRawProperty(String name) {
    return getRawProperty(name, null);
  }

  /**
   * Returns the raw object stored within the underlying {@link CompositeConfiguration}.
   *
   * See {@link PinotConfiguration} for supported key naming conventions.
   *
   * @param name of the property to retrieve a raw object value. Property name will be sanitized.
   * @return the object referenced. Fallback to provided default value if key is not found.
   */
  public Object getRawProperty(String name, Object defaultValue) {
    String relaxedPropertyName = relaxPropertyName(name);
    if (!_configuration.containsKey(relaxedPropertyName)) {
      return defaultValue;
    }

    return _configuration.getProperty(relaxedPropertyName);
  }

  /**
   * Overwrites a property value in memory.
   *
   * @param name of the property to overwrite in memory. Applies relaxed binding on the property name.
   * @param value to overwrite in memory
   */
  public void setProperty(String name, Object value) {
    _configuration.setProperty(relaxPropertyName(name), value);
  }

  /**
   * Delete a property value in memory.
   *
   * @param name of the property to remove in memory. Applies relaxed binding on the property name.
   */
  public void clearProperty(String name) {
    _configuration.clearProperty(relaxPropertyName(name));
  }

  /**
   * <p>
   * Creates a copy of the configuration only containing properties matching a property prefix.
   * Changes made on the source {@link PinotConfiguration} will not be available in the subset instance
   * since properties are copied.
   * </p>
   *
   * <p>
   * The prefix is removed from the keys in the subset. See {@link Configuration#subset(String)} for further details.
   * </p>
   *
   * @param prefix of the properties to copy. Applies relaxed binding on the properties prefix.
   * @return a new {@link PinotConfiguration} instance with
   */
  public PinotConfiguration subset(String prefix) {
    return new PinotConfiguration(_configuration.subset(relaxPropertyName(prefix)));
  }

  /**
   * @return a key-value {@link Map} found in the underlying {@link CompositeConfiguration}
   */
  public Map<String, Object> toMap() {
    return CommonsConfigurationUtils.toMap(_configuration);
  }

  @Override
  public String toString() {
    return new Obfuscator().toJsonString(toMap());
  }

  public boolean isEmpty() {
    return _configuration.isEmpty();
  }
}
