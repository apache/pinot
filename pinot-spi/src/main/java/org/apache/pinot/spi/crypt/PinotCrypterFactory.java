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
package org.apache.pinot.spi.crypt;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.plugin.PluginManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This factory instantiates the PinotCrypter, which participates in encrypting and decrypting files.
 */
public class PinotCrypterFactory {
  public static final Logger LOGGER = LoggerFactory.getLogger(PinotCrypterFactory.class);
  // Map of lower case simple class name to PinotCrypter object
  private static Map<String, PinotCrypter> _crypterMap = new HashMap<>();
  private static final String NOOP_PINOT_CRYPTER = "nooppinotcrypter";
  private static final String CLASS = "class";

  // Prevent factory from being instantiated
  private PinotCrypterFactory() {

  }

  /**
   * Initializes map of crypter classes at startup time. Will initialize map with lower case simple class names.
   * @param config
   * Sample configuration:
   * class.nooppinotcrypter = org.apache.pinot.core.crypt.NoOpPinotCrypter
   * nooppinotcrypter.keyMap = sample_key
   */
  public static void init(PinotConfiguration config) {
    // Get schemes and their respective classes
    PinotConfiguration schemesConfig = config.subset(CLASS);
    List<String> schemes = schemesConfig.getKeys();
    if (!schemes.isEmpty()) {
      LOGGER.info("Did not find any crypter classes in the configuration");
    }
    for (String scheme : schemes) {
      String className = schemesConfig.getProperty(scheme);
      
      LOGGER.info("Got crypter class name {}, full crypter path {}, starting to initialize", scheme, className);

      try {
        PinotCrypter pinotCrypter = PluginManager.get().createInstance(className);
        pinotCrypter.init(config.subset(scheme));
        LOGGER.info("Initializing PinotCrypter for scheme {}, classname {}", scheme, className);
        _crypterMap.put(scheme.toLowerCase(), pinotCrypter);
      } catch (Exception e) {
        LOGGER.error("Could not instantiate crypter for class {}", className, e);
        throw new RuntimeException(e);
      }
    }

    if (!_crypterMap.containsKey(NOOP_PINOT_CRYPTER)) {
      LOGGER.info("NoOpPinotCrypter not configured, adding as default");
      _crypterMap.put(NOOP_PINOT_CRYPTER, new NoOpPinotCrypter());
    }
  }

  public static PinotCrypter create(String crypterClassName) {
    PinotCrypter pinotCrypter = _crypterMap.get(crypterClassName.toLowerCase());
    if (pinotCrypter == null) {
      throw new RuntimeException("Pinot crypter not configured for class name: " + crypterClassName);
    }
    return pinotCrypter;
  }
}
