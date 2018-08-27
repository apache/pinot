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

import java.io.File;
import java.io.IOException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;


/**
 * The default crypter class assumes that the file is not encrypted
 */
public class DefaultPinotCrypter implements PinotCrypter {
  @Override
  public void init(Configuration configs) {

  }

  @Override
  public void encrypt(File srcFile, File dstFile) throws IOException {
    FileUtils.copyFile(srcFile, dstFile);
  }

  @Override
  public void decrypt(File srcFile, File dstFile) throws IOException {
    FileUtils.copyFile(srcFile, dstFile);
  }
}
