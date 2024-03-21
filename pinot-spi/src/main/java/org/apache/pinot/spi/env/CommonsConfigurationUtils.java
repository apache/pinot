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

import com.google.common.base.Preconditions;
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
import javax.annotation.Nullable;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.convert.LegacyListDelimiterHandler;
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

  /**
   * Instantiate a {@link PropertiesConfiguration} from a {@link File}.
   * @param file containing properties
   * @return a {@link PropertiesConfiguration} instance. Empty if file does not exist.
   */
  public static PropertiesConfiguration fromFile(File file)
      throws ConfigurationException {
    return fromFile(file, false, true);
  }

  /**
   * Instantiate a {@link PropertiesConfiguration} from an {@link InputStream}.
   * @param stream containing properties
   * @return a {@link PropertiesConfiguration} instance.
   */
  public static PropertiesConfiguration fromInputStream(InputStream stream)
      throws ConfigurationException {
    return fromInputStream(stream, false, true);
  }

  /**
   * Instantiate a {@link PropertiesConfiguration} from an {@link String}.
   * @param path representing the path of file
   * @return a {@link PropertiesConfiguration} instance.
   */
  public static PropertiesConfiguration fromPath(String path)
      throws ConfigurationException {
    return fromPath(path, false, true);
  }

  /**
   * Instantiate a {@link PropertiesConfiguration} from an {@link String}.
   * @param path representing the path of file
   * @param setIOFactory representing to set the IOFactory or not
   * @param setDefaultDelimiter representing to set the default list delimiter.
   * @return a {@link PropertiesConfiguration} instance.
   */
  public static PropertiesConfiguration fromPath(String path, boolean setIOFactory, boolean setDefaultDelimiter)
      throws ConfigurationException {
    PropertiesConfiguration config = createPropertiesConfiguration(setIOFactory, setDefaultDelimiter);
    FileHandler fileHandler = new FileHandler(config);
    fileHandler.load(path);
    return config;
  }

  /**
   * Instantiate a {@link PropertiesConfiguration} from an {@link InputStream}.
   * @param stream containing properties
   * @param setIOFactory representing to set the IOFactory or not
   * @param setDefaultDelimiter representing to set the default list delimiter.
   * @return a {@link PropertiesConfiguration} instance.
   */
  public static PropertiesConfiguration fromInputStream(InputStream stream, boolean setIOFactory,
      boolean setDefaultDelimiter)
      throws ConfigurationException {
    PropertiesConfiguration config = createPropertiesConfiguration(setIOFactory, setDefaultDelimiter);
    FileHandler fileHandler = new FileHandler(config);
    fileHandler.load(stream);
    return config;
  }

  /**
   * Instantiate a {@link PropertiesConfiguration} from a {@link File}.
   * @param file containing properties
   * @param setIOFactory representing to set the IOFactory or not
   * @param setDefaultDelimiter representing to set the default list delimiter.
   * @return a {@link PropertiesConfiguration} instance.
   */
  public static PropertiesConfiguration fromFile(File file, boolean setIOFactory, boolean setDefaultDelimiter)
      throws ConfigurationException {
    PropertiesConfiguration config = createPropertiesConfiguration(setIOFactory, setDefaultDelimiter);
    FileHandler fileHandler = new FileHandler(config);
    // check if file exists, load the properties otherwise set the file.
    if (file.exists()) {
      fileHandler.load(file);
    } else {
      fileHandler.setFile(file);
    }
    checkForDuplicateKeys(config);
    return config;
  }

  private static void checkForDuplicateKeys(PropertiesConfiguration config) throws ConfigurationException {
    Iterator<String> keys = config.getKeys();
    while (keys.hasNext()) {
      String key = keys.next();
      Object value = config.getProperty(key);
      if (!(value instanceof String)) {
        throw new ConfigurationException(String.format("duplicate key found in properties configuration file %s", key));
      }
    }
  }

  /**
   * Save the propertiesConfiguration content into the provided file.
   * @param propertiesConfiguration a {@link PropertiesConfiguration} instance.
   * @param file a {@link File} instance.
   */
  public static void saveToFile(PropertiesConfiguration propertiesConfiguration, File file) {
    Preconditions.checkNotNull(file, "File object can not be null for saving configurations");
    FileHandler fileHandler = new FileHandler(propertiesConfiguration);
    fileHandler.setFile(file);
    try {
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
   * - Comma is replaced with "\0\0"
   * Returns {@code null} when the given value contains surrogate characters because it is not supported by
   * {@link PropertiesConfiguration}.
   *
   * Note:
   * - '\0' is not allowed in string values, so we can use it as the replaced character
   * - Escaping comma with backslash doesn't work when comma is preceded by a backslash
   */
  @Nullable
  public static String replaceSpecialCharacterInPropertyValue(String value) {
    int length = value.length();
    if (length == 0) {
      return value;
    }
    boolean containsDelimiter = false;
    for (int i = 0; i < length; i++) {
      char c = value.charAt(i);
      if (Character.isSurrogate(c)) {
        return null;
      }
      if (c == DEFAULT_LIST_DELIMITER) {
        containsDelimiter = true;
      }
    }
    if (containsDelimiter) {
      value = value.replace(",", "\0\0");
    }
    if (value.charAt(0) == ' ') {
      value = "\0" + value;
    }
    if (value.charAt(value.length() - 1) == ' ') {
      value = value + "\0";
    }
    return value;
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
      boolean setDefaultDelimiter) {
    PropertiesConfiguration config = new PropertiesConfiguration();

    // setting IO Reader Factory
    if (setIOFactory) {
      config.setIOFactory(new ConfigFilePropertyReaderFactory());
    }

    // setting DEFAULT_LIST_DELIMITER
    if (setDefaultDelimiter) {
      config.setListDelimiterHandler(new LegacyListDelimiterHandler(DEFAULT_LIST_DELIMITER));
    }

    return config;
  }
}
