/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.crypt;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.commons.configuration.Configuration;
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
   * class.nooppinotcrypter = com.linkedin.pinot.core.crypt.NoOpPinotCrypter
   * nooppinotcrypter.keyMap = sample_key
   */
  public static void init(Configuration config) {
    // Get schemes and their respective classes
    Iterator<String> keys = config.subset(CLASS).getKeys();
    if (!keys.hasNext()) {
      LOGGER.info("Did not find any crypter classes in the configuration");
    }
    while (keys.hasNext()) {
      String key = keys.next();
      String className = (String) config.getProperty(CLASS + "." + key);
      LOGGER.info("Got crypter class name {}, full crypter path {}, starting to initialize", key, className);

      try {
        PinotCrypter pinotCrypter = (PinotCrypter) Class.forName(className).newInstance();
        pinotCrypter.init(config.subset(key));

        LOGGER.info("Initializing PinotCrypter for scheme {}, classname {}", key, className);
        _crypterMap.put(key.toLowerCase(), pinotCrypter);
      } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
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
