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
package com.linkedin.pinot.common.segment.crypt;

import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This factory instantiates the PinotCrypter, which participates in encrypting and decrypting files.
 */
public class PinotCrypterFactory {
  private static final String CRYPTER_CLASS_CONFIG = "class";
  private static String _crypterClass;
  public static final Logger LOGGER = LoggerFactory.getLogger(PinotCrypterFactory.class);

  // Prevent factory from being instantiated
  private PinotCrypterFactory() {

  }
  public static void init(Configuration config) {
    _crypterClass = config.getString(CRYPTER_CLASS_CONFIG);
  }

  public static PinotCrypter create() {
    String crypterClassName = _crypterClass;
    try {
      LOGGER.info("Instantiating PinotCrypter class {}", crypterClassName);
      PinotCrypter pinotCrypter =  (PinotCrypter) Class.forName(crypterClassName).newInstance();
      return pinotCrypter;
    } catch (Exception e) {
      return new DefaultPinotCrypter();
    }
  }
}
