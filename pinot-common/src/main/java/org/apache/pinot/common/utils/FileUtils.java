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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;


public class FileUtils {
  private FileUtils() {
  }

  /// Deletes the destination file if it exists then calls org.apache.commons moveFile.
  /// @param srcFile
  /// @param destFile
  public static void moveFileWithOverwrite(File srcFile, File destFile)
      throws IOException {
    if (destFile.exists()) {
      org.apache.commons.io.FileUtils.deleteQuietly(destFile);
    }
    org.apache.commons.io.FileUtils.moveFile(srcFile, destFile);
  }

  /// Transfers bytes from the source file to the destination file. This method can handle transfer size larger than 2G.
  ///
  /// @param src Source file channel
  /// @param position Position in source file
  /// @param count Number of bytes to transfer
  /// @param dest Destination file channel
  /// @throws IOException
  public static void transferBytes(FileChannel src, long position, long count, FileChannel dest)
      throws IOException {
    long numBytesTransferred;
    while ((numBytesTransferred = src.transferTo(position, count, dest)) < count) {
      position += numBytesTransferred;
      count -= numBytesTransferred;
    }
  }

  /// Close a collection of [Closeable] resources
  /// This is a utility method to help release multiple [Closeable]
  /// resources in a safe manner without leaking.
  /// As an example if we have a list of Closeable resources,
  /// then the following code is prone to leaking 1 or more
  /// subsequent resources if an exception is thrown while
  /// closing one of them.
  ///
  /// for (closeable_resource : resources) {
  ///   closeable_resource.close()
  /// }
  ///
  /// The helper methods provided here do this safely
  /// while keeping track of exception(s) raised during
  /// close() of each resource and still continuing to close
  /// subsequent resources.
  /// @param closeables collection of resources to close
  /// @throws IOException
  public static void close(Iterable<? extends Closeable> closeables)
      throws IOException {
    IOException topLevelException = null;

    for (Closeable closeable : closeables) {
      try {
        if (closeable != null) {
          closeable.close();
        }
      } catch (IOException e) {
        topLevelException = ExceptionUtils.suppress(e, topLevelException);
      }
    }

    if (topLevelException != null) {
      throw topLevelException;
    }
  }

  /// Another version of [FileUtils#close(Iterable)] which allows
  /// to pass variable number of closeable resources when the caller
  /// doesn't already have them in a collection.
  /// @param closeables one or more resources to close
  /// @throws IOException
  public static void close(Closeable... closeables)
      throws IOException {
    close(Arrays.asList(closeables));
  }

  /// Forces each channel's dirty pages to durable storage before closing it. Without this, a write can sit
  /// unconfirmed in the page cache, and a fault before the OS's own lazy writeback runs can leave bad bytes on
  /// disk with no error. Every channel is attempted even if an earlier one fails to sync or close, mirroring
  /// the leak-avoidance behavior of [FileUtils#close].
  ///
  /// @param channels one or more writable file channels to sync and close
  /// @throws IOException the first exception encountered while syncing or closing, with the rest suppressed
  public static void syncAndClose(FileChannel... channels)
      throws IOException {
    IOException topLevelException = null;

    for (FileChannel channel : channels) {
      if (channel == null) {
        continue;
      }
      try {
        if (channel.isOpen()) {
          channel.force(true);
        }
      } catch (IOException e) {
        if (topLevelException == null) {
          topLevelException = e;
        } else {
          topLevelException.addSuppressed(e);
        }
      }
      try {
        channel.close();
      } catch (IOException e) {
        if (topLevelException == null) {
          topLevelException = e;
        } else {
          topLevelException.addSuppressed(e);
        }
      }
    }

    if (topLevelException != null) {
      throw topLevelException;
    }
  }

  /// Forces dirty pages in each writable memory-mapped buffer to durable storage.
  ///
  /// FileChannel.force() does not guarantee that changes made through a memory-mapped buffer are persisted, so callers
  /// that write through mappings must force the mappings separately before closing the channel.
  /// @param buffers one or more writable memory-mapped buffers to force
  /// @throws IOException if forcing any buffer fails
  public static void forceMappedBuffers(MappedByteBuffer... buffers)
      throws IOException {
    IOException topLevelException = null;

    for (MappedByteBuffer buffer : buffers) {
      if (buffer == null) {
        continue;
      }
      try {
        buffer.force();
      } catch (RuntimeException e) {
        IOException exception = new IOException("Failed to force memory-mapped buffer", e);
        if (topLevelException == null) {
          topLevelException = exception;
        } else {
          topLevelException.addSuppressed(exception);
        }
      }
    }

    if (topLevelException != null) {
      throw topLevelException;
    }
  }

  /// Concatenates the folderDir and filename and validates that the resulting file path is still within the folderDir.
  /// @param folderDir the parent directory
  /// @param filename the filename to concatenate to the parent directory
  /// @param msg the error message if the resulting file path is not within the parent directory
  /// @param args the error message arguments
  /// @return File object representing the concatenated file path
  /// @throws IllegalArgumentException if the resulting file path is not within the parent directory
  /// @throws IOException if the resulting file path is invalid
  public static File concatAndValidateFile(File folderDir, String filename, String msg, Object... args)
      throws IllegalArgumentException, IOException {
    File filePath = new File(folderDir, filename);
    if (!filePath.getCanonicalPath().startsWith(folderDir.getCanonicalPath() + File.separator)) {
      throw new IllegalArgumentException(String.format(msg, args));
    }

    return filePath;
  }

  public static void ensureDirectoryExists(Path path)
      throws IllegalArgumentException {
    if (!Files.exists(path)) {
      try {
        Files.createDirectories(path);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /// Returns the total size in bytes of the regular files under `dir`, or `0` if `dir` does not exist. Symbolic links
  /// are not followed.
  ///
  /// Use this for a directory that another thread may be writing to while it is measured. It walks with the `File`
  /// API, whose `listFiles()` and `length()` report a concurrently deleted entry as `null` / `0` instead of throwing,
  /// so a delete racing with the walk cannot fail the whole computation. The trade-off is that an unreadable directory
  /// is also counted as `0`. For a directory nothing else is writing to, prefer
  /// `org.apache.commons.io.FileUtils#sizeOfDirectory` so that a genuine I/O error surfaces.
  public static long sizeOfDirectory(File dir) {
    long size = 0;
    File[] files = dir.listFiles();
    if (files != null) {
      for (File file : files) {
        if (Files.isSymbolicLink(file.toPath())) {
          continue;
        }
        size += file.isDirectory() ? sizeOfDirectory(file) : file.length();
      }
    }
    return size;
  }
}
