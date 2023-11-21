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
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.io.FileHandler;
import org.apache.commons.lang3.StringUtils;


/**
 * Provide utility functions to manipulate Apache Commons {@link Configuration} instances.
 */
public class CommonsConfigurationUtils {
  private static final Character DEFAULT_LIST_DELIMITER = ',';
  private CommonsConfigurationUtils() {
  }

  public static PropertiesConfiguration fromFile(File file, boolean setJupIOFactory) {
    try {
      return loadFromFile(file, false, false, setJupIOFactory);
    } catch (ConfigurationException e) {
      throw new RuntimeException(e);
    }
  }
  public static PropertiesConfiguration fromFile(File file) {
    try {
      return loadFromFile(file, false, false, false);
    } catch (ConfigurationException e) {
      throw new RuntimeException(e);
    }
  }

  public static PropertiesConfiguration fromInputStream(InputStream stream) {
    try {
      return loadFromInputStream(stream, false, false);
    } catch (ConfigurationException e) {
      throw new RuntimeException(e);
    }
  }

  public static PropertiesConfiguration loadFromPath(String path) throws ConfigurationException {
    return loadFromPath(path, false, false);
  }

  public static PropertiesConfiguration loadFromPath(String path, boolean setIOFactory, boolean setDefaultDelimiter)
      throws ConfigurationException {
    PropertiesConfiguration config = createPropertiesConfiguration(setIOFactory, setDefaultDelimiter, false);
    FileHandler fileHandler = new FileHandler(config);
    fileHandler.load(path);
    return config;
  }


  public static PropertiesConfiguration loadFromInputStream(InputStream stream) throws ConfigurationException {
    return loadFromInputStream(stream, false, false);
  }

  public static PropertiesConfiguration loadFromInputStream(InputStream stream, boolean setIOFactory,
      boolean setDefaultDelimiter) throws ConfigurationException {
    PropertiesConfiguration config = createPropertiesConfiguration(setIOFactory, setDefaultDelimiter, false);
    FileHandler fileHandler = new FileHandler(config);
    fileHandler.load(stream);
    return config;
  }

  public static PropertiesConfiguration loadFromFile(File file) throws ConfigurationException {
    return loadFromFile(file, false, false, false);
  }

  public static PropertiesConfiguration loadFromFile(File file, boolean setIOFactory,
      boolean setDefaultDelimiter, boolean setJupIOFactory) throws ConfigurationException {
    PropertiesConfiguration config = createPropertiesConfiguration(setIOFactory, setDefaultDelimiter, setJupIOFactory);
    FileHandler fileHandler = new FileHandler(config);
    // check if file exits, load the properties otherwise set the file.
    if (file.exists()) {
      fileHandler.load(file);
    } else {
      fileHandler.setFile(file);
    }
    return config;
  }

  public static void saveToExistingFile(PropertiesConfiguration propertiesConfiguration, File file) {
    try {
      FileHandler fileHandler = new FileHandler(propertiesConfiguration);
      fileHandler.setFile(file);
      fileHandler.save();
    } catch (ConfigurationException e) {
      throw new RuntimeException(e);
    }
  }

  public static void saveToFile(PropertiesConfiguration propertiesConfiguration, File file) {
    try {
      FileHandler fileHandler = new FileHandler(propertiesConfiguration);
      fileHandler.setFile(file);
      fileHandler.save();
    } catch (ConfigurationException e) {
      throw new RuntimeException(e);
    }
  }

  private static Iterable<String> getIterable(Iterator<String> keys) {
    return () -> keys;
  }

  public static Stream<String> getKeysStream(Configuration configuration) {
    return StreamSupport.stream(getIterable(configuration.getKeys()).spliterator(), false);
  }


  public static List<String> getKeys(Configuration configuration) {
    return getKeysStream(configuration).collect(Collectors.toList());
  }

  public static Map<String, Object> toMap(Configuration configuration) {
    return getKeysStream(configuration).collect(Collectors.toMap(key -> key, key -> mapValue(key, configuration)));
  }

  private static Object mapValue(String key, Configuration configuration) {
    // For multi-value config, convert it to a single comma connected string value.
    // For single-value config, return its raw property, unless it needs interpolation.
    return Optional.of(configuration.getStringArray(key)).filter(values -> values.length > 1)
        .<Object>map(values -> Arrays.stream(values).collect(Collectors.joining(","))).orElseGet(() -> {
          Object rawProperty = configuration.getProperty(key);
          if (!needInterpolate(rawProperty)) {
            return rawProperty;
          }
          // The string value is converted to the requested type when accessing it via PinotConfiguration.
          return configuration.getString(key);
        });
  }

  public static boolean needInterpolate(Object rawProperty) {
    if (rawProperty instanceof String) {
      String strProperty = (String) rawProperty;
      // e.g. if config value is '${sys:FOO}', it's replaced by value of system property FOO; If config value is
      // '${env:FOO}', it's replaced by value of env var FOO.
      return StringUtils.isNotEmpty(strProperty) && strProperty.startsWith("${") && strProperty.endsWith("}");
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  public static <T> T interpolate(Configuration configuration, String key, T defaultValue, Class<T> returnType) {
    // Different from the generic getProperty() method, those type specific getters do config interpolation.
    if (Integer.class.equals(returnType)) {
      return (T) configuration.getInteger(key, (Integer) defaultValue);
    } else if (Boolean.class.equals(returnType)) {
      return (T) configuration.getBoolean(key, (Boolean) defaultValue);
    } else if (Long.class.equals(returnType)) {
      return (T) configuration.getLong(key, (Long) defaultValue);
    } else if (Double.class.equals(returnType)) {
      return (T) configuration.getDouble(key, (Double) defaultValue);
    } else {
      throw new IllegalArgumentException(returnType + " is not a supported type for conversion.");
    }
  }

  @SuppressWarnings("unchecked")
  public static <T> T convert(Object value, Class<T> returnType) {
    if (Integer.class.equals(returnType)) {
      return (T) Integer.valueOf(value.toString());
    } else if (Boolean.class.equals(returnType)) {
      return (T) Boolean.valueOf(value.toString());
    } else if (Long.class.equals(returnType)) {
      return (T) Long.valueOf(value.toString());
    } else if (Double.class.equals(returnType)) {
      return (T) Double.valueOf(value.toString());
    } else {
      throw new IllegalArgumentException(returnType + " is not a supported type for conversion.");
    }
  }

  /**
   * Replaces the special character in the given property value.
   * - Leading/trailing space is prefixed/suffixed with "\0"
   * - Comma is replaces with "\0\0"
   *
   * Note:
   * - '\0' is not allowed in string values, so we can use it as the replaced character
   * - Escaping comma with backslash doesn't work when comma is preceded by a backslash
   */
  public static String replaceSpecialCharacterInPropertyValue(String value) {
    if (value.isEmpty()) {
      return value;
    }
    if (value.charAt(0) == ' ') {
      value = "\0" + value;
    }
    if (value.charAt(value.length() - 1) == ' ') {
      value = value + "\0";
    }
    return value.replace(",", "\0\0");
  }

  /**
   * Recovers the special character in the given property value that is previous replaced by
   * {@link #replaceSpecialCharacterInPropertyValue(String)}.
   */
  public static String recoverSpecialCharacterInPropertyValue(String value) {
    if (value.isEmpty()) {
      return value;
    }
    if (value.startsWith("\0 ")) {
      value = value.substring(1);
    }
    if (value.endsWith(" \0")) {
      value = value.substring(0, value.length() - 1);
    }
    return value.replace("\0\0", ",");
  }

  private static PropertiesConfiguration createPropertiesConfiguration(boolean setIOFactory,
      boolean setDefaultDelimiter, boolean setJupIOFactory) {
    PropertiesConfiguration config = new PropertiesConfiguration();

    // setting IO Reader Factory
    if (setIOFactory) {
      config.setIOFactory(new ConfigFilePropertyReaderFactory());
    } else if (setJupIOFactory) {
      config.setIOFactory(new PropertiesConfiguration.JupIOFactory(true));
    }

    // setting DEFAULT_LIST_DELIMITER
    if (setDefaultDelimiter) {
      CommonsConfigurationUtils.setDefaultListDelimiterHandler(config);
    }
    return config;
  }

  private static void setListDelimiterHandler(PropertiesConfiguration configuration, Character delimiter) {
    configuration.setListDelimiterHandler(new DefaultListDelimiterHandler(delimiter));
  }

  private static void setDefaultListDelimiterHandler(PropertiesConfiguration configuration) {
    setListDelimiterHandler(configuration, DEFAULT_LIST_DELIMITER);
  }
}
