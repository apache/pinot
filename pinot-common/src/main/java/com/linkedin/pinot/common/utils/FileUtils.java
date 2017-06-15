/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.common.utils;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Nov 12, 2014
 */

public class FileUtils {

  public static Logger LOGGER = LoggerFactory.getLogger(FileUtils.class);

  private static final Random RANDOM = new Random();

  public static String getRandomFileName() {
    return StringUtil.join("-", "tmp", String.valueOf(System.currentTimeMillis()), Long.toString(RANDOM.nextLong()));
  }

  /**
   * Deletes the destination file if it exists then calls org.apache.commons moveFile.
   * @param srcFile
   * @param destFile
   */
  public static void moveFileWithOverwrite(File srcFile, File destFile) throws IOException {
    if (destFile.exists()) {
      org.apache.commons.io.FileUtils.deleteQuietly(destFile);
    }
    org.apache.commons.io.FileUtils.moveFile(srcFile, destFile);
  }
}
