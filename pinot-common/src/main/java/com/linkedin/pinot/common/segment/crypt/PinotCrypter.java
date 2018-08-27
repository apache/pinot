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

import java.net.URI;


/**
 * The PinotCrypter will encrypt and decrypt segments when they are downloaded. This class is especially useful in cases
 * where segments cannot be stored unencrypted in storage.
 */
public interface PinotCrypter {
  /**
   * Encrypts data.
   * @param uri location of data to be encrypted
   * @Exception throws an Exception if encrypt fails
   */
  void encrypt(URI uri) throws Exception;

  /**
   * Decrypts data.
   * @param uri location of data to be decrypted
   * @Exception throws an Exception if decrypt fails
   */
  void decrypt(URI uri) throws Exception;
}
