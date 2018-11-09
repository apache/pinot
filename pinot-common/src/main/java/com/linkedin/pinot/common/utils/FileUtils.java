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
package com.linkedin.pinot.common.utils;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Random;


public class FileUtils {
  private FileUtils() {
  }

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

  /**
   * Transfers bytes from the source file to the destination file. This method can handle transfer size larger than 2G.
   *
   * @param src Source file channel
   * @param position Position in source file
   * @param count Number of bytes to transfer
   * @param dest Destination file channel
   * @throws IOException
   */
  public static void transferBytes(FileChannel src, long position, long count, FileChannel dest) throws IOException {
    long numBytesTransferred;
    while ((numBytesTransferred = src.transferTo(position, count, dest)) < count) {
      position += numBytesTransferred;
      count -= numBytesTransferred;
    }
  }
}
