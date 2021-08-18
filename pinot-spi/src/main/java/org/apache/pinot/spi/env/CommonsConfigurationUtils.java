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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;


/**
 * Provide utility functions to manipulate Apache Commons {@link Configuration} instances.
 *
 */
public abstract class CommonsConfigurationUtils {
  /**
   * Instantiate a {@link PropertiesConfiguration} from a {@link File}.
   * @param file containing properties
   * @return a {@link PropertiesConfiguration} instance. Empty if file does not exist.
   */
  public static PropertiesConfiguration fromFile(File file) {
    try {
      PropertiesConfiguration propertiesConfiguration = new PropertiesConfiguration();

      // Commons Configuration 1.10 does not support file path containing '%'. 
      // Explicitly providing the input stream on load bypasses the problem. 
      propertiesConfiguration.setFile(file);
      if (file.exists()) {
        propertiesConfiguration.load(new FileInputStream(file));
      }

      return propertiesConfiguration;
    } catch (ConfigurationException | FileNotFoundException e) {
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
      propertiesConfiguration.load(inputStream);
      return propertiesConfiguration;
    } catch (ConfigurationException e) {
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
    return Optional.of(configuration.getStringArray(key)).filter(values -> values.length > 1).<Object>map(
        values -> Arrays.stream(values).collect(Collectors.joining(","))).orElseGet(() -> configuration.getProperty(key));
  }
}
