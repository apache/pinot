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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
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
  // value to be used to set the global separator for versioned properties configuration.
  // the value is same of PropertiesConfiguration `DEFAULT_SEPARATOR` constant.
  public static final String VERSIONED_CONFIG_SEPARATOR = " = ";
  private static final Character DEFAULT_LIST_DELIMITER = ',';
  public static final String VERSION_HEADER_IDENTIFIER = "version";

  // usage: default header version of all configurations.
  // if properties configuration doesn't contain header version, it will be considered as 1
  public static final String DEFAULT_PROPERTIES_CONFIGURATION_HEADER_VERSION = "1";

  // usage: used in reading segment metadata or other properties configurations with 'VersionedIOFactory' IO Factory.
  // version signifies that segment metadata or other properties configurations contains keys with no special character.
  public static final String PROPERTIES_CONFIGURATION_HEADER_VERSION_2 = "2";

  private CommonsConfigurationUtils() {
  }

  /**
   * Instantiate a {@link PropertiesConfiguration} from an {@link String}.
   * @param path representing the path of file
   * @return a {@link PropertiesConfiguration} instance.
   */
  public static PropertiesConfiguration fromPath(String path)
      throws ConfigurationException {
    return fromPath(path, true, null);
  }

  /**
   * Instantiate a {@link PropertiesConfiguration} from an {@link String}.
   * @param path representing the path of file
   * @param setDefaultDelimiter representing to set the default list delimiter.
   * @param ioFactoryKind representing to set IOFactory. It can be null.
   * @return a {@link PropertiesConfiguration} instance.
   */
  public static PropertiesConfiguration fromPath(@Nullable String path, boolean setDefaultDelimiter,
      @Nullable PropertyIOFactoryKind ioFactoryKind)
      throws ConfigurationException {
    PropertiesConfiguration config = createPropertiesConfiguration(setDefaultDelimiter, ioFactoryKind);
    // if provided path is non-empty, load the existing properties from provided file path
    if (StringUtils.isNotEmpty(path)) {
      FileHandler fileHandler = new FileHandler(config);
      fileHandler.load(path);
    }
    return config;
  }

  /**
   * Instantiate a {@link PropertiesConfiguration} from an {@link InputStream}.
   * @param stream containing properties
   * @return a {@link PropertiesConfiguration} instance.
   */
  public static PropertiesConfiguration fromInputStream(InputStream stream)
      throws ConfigurationException {
    return fromInputStream(stream, true, PropertyIOFactoryKind.DefaultIOFactory);
  }

  /**
   * Instantiate a {@link PropertiesConfiguration} from an {@link InputStream}.
   * @param stream containing properties
   * @param setDefaultDelimiter representing to set the default list delimiter.
   * @param ioFactoryKind representing to set IOFactory. It can be null.
   * @return a {@link PropertiesConfiguration} instance.
   */
  public static PropertiesConfiguration fromInputStream(@Nullable InputStream stream,
      boolean setDefaultDelimiter, @Nullable PropertyIOFactoryKind ioFactoryKind)
      throws ConfigurationException {
    PropertiesConfiguration config = createPropertiesConfiguration(setDefaultDelimiter, ioFactoryKind);
    // if provided stream is not null, load the existing properties from provided input stream.
    if (stream != null) {
      FileHandler fileHandler = new FileHandler(config);
      fileHandler.load(stream);
    }
    return config;
  }

  /**
   * Instantiate a Segment Metadata {@link PropertiesConfiguration} from a {@link File}.
   * @param file containing properties
   * @param setDefaultDelimiter representing to set the default list delimiter.
   * @return a {@link PropertiesConfiguration} instance.
   */
  public static PropertiesConfiguration getSegmentMetadataFromFile(File file, boolean setDefaultDelimiter)
      throws ConfigurationException {
    PropertyIOFactoryKind ioFactoryKind = PropertyIOFactoryKind.DefaultIOFactory;

    // if segment metadata contains version header with value '2', set VersionedIOFactory as IO factory.
    if (PROPERTIES_CONFIGURATION_HEADER_VERSION_2.equals(getConfigurationHeaderVersion(file))) {
      ioFactoryKind = PropertyIOFactoryKind.VersionedIOFactory;
    }

    return fromFile(file, setDefaultDelimiter, ioFactoryKind);
  }

  /**
   * Instantiate a {@link PropertiesConfiguration} from a {@link File}.
   * @param file containing properties
   * @return a {@link PropertiesConfiguration} instance. Empty if file does not exist.
   */
  public static PropertiesConfiguration fromFile(File file)
      throws ConfigurationException {
    return fromFile(file, true, PropertyIOFactoryKind.DefaultIOFactory);
  }

  /**
   * Instantiate a {@link PropertiesConfiguration} from a {@link File}.
   * @param file containing properties
   * @param setDefaultDelimiter representing to set the default list delimiter.
   * @param ioFactoryKind representing to set IOFactory. It can be null.
   * @return a {@link PropertiesConfiguration} instance.
   */
  public static PropertiesConfiguration fromFile(@Nullable File file,
      boolean setDefaultDelimiter, @Nullable PropertyIOFactoryKind ioFactoryKind)
      throws ConfigurationException {
    PropertiesConfiguration config = createPropertiesConfiguration(setDefaultDelimiter, ioFactoryKind);
    // check if file exists, load the existing properties.
    if (file != null && file.exists()) {
      FileHandler fileHandler = new FileHandler(config);
      fileHandler.load(file);
    }
    return config;
  }

  /**
   * save the segment metadata configuration content into the provided file based on the version header.
   * @param propertiesConfiguration a {@link PropertiesConfiguration} instance.
   * @param file a {@link File} instance.
   * @param versionHeader a Nullable {@link String} instance.
   */
  public static void saveSegmentMetadataToFile(PropertiesConfiguration propertiesConfiguration, File file,
      @Nullable String versionHeader) {
    if (StringUtils.isNotEmpty(versionHeader)) {
      String header = getVersionHeaderString(versionHeader);
      propertiesConfiguration.setHeader(header);

      // checks whether the provided versionHeader equals to VersionedIOFactory kind.
      // if true, set IO factory as VersionedIOFactory
      if (PROPERTIES_CONFIGURATION_HEADER_VERSION_2.equals(versionHeader)) {
        // set versioned IOFactory
        propertiesConfiguration.setIOFactory(PropertyIOFactoryKind.VersionedIOFactory.getInstance());
        // setting the global separator makes sure the configurations gets written with ' = ' separator.
        // global separator overrides the separator set at the key level as well.
        propertiesConfiguration.getLayout().setGlobalSeparator(VERSIONED_CONFIG_SEPARATOR);
      }
    }
    saveToFile(propertiesConfiguration, file);
  }

  /**
   * Save the propertiesConfiguration content into the provided file.
   * @param propertiesConfiguration a {@link PropertiesConfiguration} instance.
   * @param file a {@link File} instance.
   */
  public static void saveToFile(PropertiesConfiguration propertiesConfiguration, File file) {
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

  /**
   * creates the instance of the {@link org.apache.commons.configuration2.PropertiesConfiguration}
   * with custom IO factory based on kind {@link org.apache.commons.configuration2.PropertiesConfiguration.IOFactory}
   * and legacy list delimiter {@link org.apache.commons.configuration2.convert.LegacyListDelimiterHandler}
   *
   * @param setDefaultDelimiter sets the default list delimiter.
   * @param ioFactoryKind IOFactory kind, can be null.
   * @return PropertiesConfiguration
   */
  private static PropertiesConfiguration createPropertiesConfiguration(boolean setDefaultDelimiter,
     @Nullable PropertyIOFactoryKind ioFactoryKind) {
    PropertiesConfiguration config = new PropertiesConfiguration();

    // setting IO Reader Factory of the configuration.
    if (ioFactoryKind != null) {
      config.setIOFactory(ioFactoryKind.getInstance());
    }

    // setting the DEFAULT_LIST_DELIMITER
    if (setDefaultDelimiter) {
      config.setListDelimiterHandler(new LegacyListDelimiterHandler(DEFAULT_LIST_DELIMITER));
    }

    return config;
  }

  /**
   * checks whether the configuration file first line is version header or not.
   * @param file configuration file
   * @return String
   * @throws ConfigurationException exception.
   */
  private static String getConfigurationHeaderVersion(File file)
      throws ConfigurationException {
    String versionValue = DEFAULT_PROPERTIES_CONFIGURATION_HEADER_VERSION;
    if (file.exists()) {
      try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
        String fileFirstLine = reader.readLine();
        // header version is written as a comment and start with '# '
        String versionHeaderCommentPrefix = "# " + VERSION_HEADER_IDENTIFIER;
        // check whether the file has the version header or not
        if (StringUtils.startsWith(fileFirstLine, versionHeaderCommentPrefix)) {
          String[] headerKeyValue = StringUtils.splitByWholeSeparator(fileFirstLine, VERSIONED_CONFIG_SEPARATOR, 2);
          if (headerKeyValue.length == 2) {
            versionValue = headerKeyValue[1];
          }
        }
      } catch (IOException exception) {
        throw new ConfigurationException(
            "Error occurred while reading configuration file " + file.getName(), exception);
      }
    }
    return versionValue;
  }

  // Returns the version header string based on the version header value provided.
  // The return statement follow the pattern 'version = <value>'
  static String getVersionHeaderString(String versionHeaderValue) {
    return VERSION_HEADER_IDENTIFIER + VERSIONED_CONFIG_SEPARATOR + versionHeaderValue;
  }
}
