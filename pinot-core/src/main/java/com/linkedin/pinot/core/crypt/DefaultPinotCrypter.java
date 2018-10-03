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

import java.io.File;
import java.io.IOException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class is the default implementation for the PinotCrypter. It simply copies the file to the given location.
 */
public class DefaultPinotCrypter implements PinotCrypter {
  public static final Logger LOGGER = LoggerFactory.getLogger(DefaultPinotCrypter.class);

  @Override
  public void init(Configuration config) {

  }

  @Override
  public void encrypt(File decryptedFile, File encryptedFile) {
    try {
      FileUtils.copyFile(decryptedFile, encryptedFile);
    } catch (IOException e) {
      LOGGER.warn("Could not encrypt file");
      FileUtils.deleteQuietly(encryptedFile);
    }
  }

  @Override
  public void decrypt(File encryptedFile, File decryptedFile) {
    try {
      FileUtils.copyFile(encryptedFile, decryptedFile);
    } catch (IOException e) {
      LOGGER.warn("Could not decrypt file");
      FileUtils.deleteQuietly(decryptedFile);
    }
  }
}
