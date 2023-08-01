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
package org.apache.pinot.common.utils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Arrays;


public class FileUtils {
  private FileUtils() {
  }

  /**
   * Deletes the destination file if it exists then calls org.apache.commons moveFile.
   * @param srcFile
   * @param destFile
   */
  public static void moveFileWithOverwrite(File srcFile, File destFile)
      throws IOException {
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
  public static void transferBytes(FileChannel src, long position, long count, FileChannel dest)
      throws IOException {
    long numBytesTransferred;
    while ((numBytesTransferred = src.transferTo(position, count, dest)) < count) {
      position += numBytesTransferred;
      count -= numBytesTransferred;
    }
  }

  /**
   * Close a collection of {@link Closeable} resources
   * This is a utility method to help release multiple {@link Closeable}
   * resources in a safe manner without leaking.
   * As an example if we have a list of Closeable resources,
   * then the following code is prone to leaking 1 or more
   * subsequent resources if an exception is thrown while
   * closing one of them.
   *
   * for (closeable_resource : resources) {
   *   closeable_resource.close()
   * }
   *
   * The helper methods provided here do this safely
   * while keeping track of exception(s) raised during
   * close() of each resource and still continuing to close
   * subsequent resources.
   * @param closeables collection of resources to close
   * @throws IOException
   */
  public static void close(Iterable<? extends Closeable> closeables)
      throws IOException {
    IOException topLevelException = null;

    for (Closeable closeable : closeables) {
      try {
        if (closeable != null) {
          closeable.close();
        }
      } catch (IOException e) {
        if (topLevelException == null) {
          topLevelException = e;
        } else if (e != topLevelException) {
          topLevelException.addSuppressed(e);
        }
      }
    }

    if (topLevelException != null) {
      throw topLevelException;
    }
  }

  /**
   * Another version of {@link FileUtils#close(Iterable)} which allows
   * to pass variable number of closeable resources when the caller
   * doesn't already have them in a collection.
   * @param closeables one or more resources to close
   * @throws IOException
   */
  public static void close(Closeable... closeables)
      throws IOException {
    close(Arrays.asList(closeables));
  }

  /**
   * Concatenates the folderDir and filename and validates that the resulting file path is still within the folderDir.
   * @param folderDir the parent directory
   * @param filename the filename to concatenate to the parent directory
   * @param msg the error message if the resulting file path is not within the parent directory
   * @param args the error message arguments
   * @return File object representing the concatenated file path
   * @throws IllegalArgumentException if the resulting file path is not within the parent directory
   * @throws IOException if the resulting file path is invalid
   */
  public static File concatAndValidateFile(File folderDir, String filename, String msg, Object... args)
      throws IllegalArgumentException, IOException {
    File filePath = new File(folderDir, filename);
    if (!filePath.getCanonicalPath().startsWith(folderDir.getCanonicalPath() + File.separator)) {
      throw new IllegalArgumentException(String.format(msg, args));
    }

    return filePath;
  }
}
