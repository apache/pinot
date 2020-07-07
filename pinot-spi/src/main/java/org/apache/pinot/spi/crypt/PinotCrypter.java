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

import java.io.File;

import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * The PinotCrypter will encrypt and decrypt segments when they are downloaded. This class is especially useful in cases
 * where segments cannot be stored unencrypted in storage.
 */
public interface PinotCrypter {

  /**
   * Initializes a crypter with any configurations it might need.
   * @param config
   */
  void init(PinotConfiguration config);

  /**
   * Encrypts the file into the file location provided. The implementation should clean up file after any failures.
   * @param decryptedFile
   * @param encryptedFile
   */
  void encrypt(File decryptedFile, File encryptedFile);

  /**
   * Decrypts file into file location provided. The implementation should clean up file after any failures.
   * @param encryptedFile
   * @param decryptedFile
   */
  void decrypt(File encryptedFile, File decryptedFile);
}
