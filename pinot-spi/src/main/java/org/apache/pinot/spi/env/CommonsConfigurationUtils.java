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
import java.io.FileOutputStream;
import java.io.IOException;
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
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.io.FileHandler;
import org.apache.commons.lang3.StringUtils;


/**
 * Provide utility functions to manipulate Apache Commons {@link Configuration} instances.
 */
public class CommonsConfigurationUtils {
  private CommonsConfigurationUtils() {
  }

  /**
   * Instantiate a {@link PropertiesConfiguration} from a {@link File}.
   * @param file containing properties
   * @return a {@link PropertiesConfiguration} instance. Empty if file does not exist.
   */
  public static PropertiesConfiguration fromFile(File file) {
    try {
      Configurations configs = new Configurations();
      return configs.properties(file);
    } catch (ConfigurationException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Instantiate a {@link PropertiesConfiguration} from an inputstream.
   * @param inputStream containing properties
   * @return a {@link PropertiesConfiguration} instance.
   */
  public static PropertiesConfiguration fromInputStream(InputStream inputStream) {
    try {
      PropertiesConfiguration propertiesConfiguration = new PropertiesConfiguration();
      FileHandler handler = new FileHandler(propertiesConfiguration);
      handler.load(inputStream);
      return propertiesConfiguration;
    } catch (ConfigurationException e) {
      throw new RuntimeException(e);
    }
  }

  public static void saveToFile(PropertiesConfiguration propertiesConfiguration, File file) {
    // Commons Configuration 1.10 does not support file path containing '%'.
    // Explicitly providing the output stream for save bypasses the problem.
    FileHandler handler = new FileHandler(propertiesConfiguration);
    try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
      handler.save(fileOutputStream);
    } catch (ConfigurationException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static Iterable<String> getIterable(Iterator<String> keys) {
    return () -> keys;
  }

  /**
   * Provides a stream of all the keys found in a {@link Configuration}.
   * @param configuration to iterate on keys
   * @return a stream of keys
   */
  public static Stream<String> getKeysStream(Configuration configuration) {
    return StreamSupport.stream(getIterable(configuration.getKeys()).spliterator(), false);
  }

  /**
   * Provides a list of all the keys found in a {@link Configuration}.
   * @param configuration to iterate on keys
   * @return a list of keys
   */
  public static List<String> getKeys(Configuration configuration) {
    return getKeysStream(configuration).collect(Collectors.toList());
  }

  /**
   * @return a key-value {@link Map} found in the provided {@link Configuration}
   */
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
}
