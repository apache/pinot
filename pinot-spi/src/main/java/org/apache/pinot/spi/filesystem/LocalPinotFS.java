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
package org.apache.pinot.spi.filesystem;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * Implementation of PinotFS for a local filesystem. Methods in this class may throw a SecurityException at runtime
 * if access to the file is denied.
 */
public class LocalPinotFS extends BasePinotFS {

  public static final String BACKUP = ".backup";
  public static final String TMP = ".tmp";

  @Override
  public void init(PinotConfiguration configuration) {
  }

  @Override
  public boolean mkdir(URI uri)
      throws IOException {
    FileUtils.forceMkdir(toFile(uri));
    return true;
  }

  @Override
  public boolean delete(URI segmentUri, boolean forceDelete)
      throws IOException {
    File file = toFile(segmentUri);
    if (file.isDirectory()) {
      // Returns false if directory isn't empty
      if (listFiles(segmentUri, false).length > 0 && !forceDelete) {
        return false;
      }
      // Throws an IOException if it is unable to delete
      FileUtils.deleteDirectory(file);
    } else {
      // Returns false if delete fails
      return FileUtils.deleteQuietly(file);
    }
    return true;
  }

  @Override
  public boolean doMove(URI srcUri, URI dstUri)
      throws IOException {
    File srcFile = toFile(srcUri);
    File dstFile = toFile(dstUri);
    if (srcFile.isDirectory()) {
      FileUtils.moveDirectory(srcFile, dstFile);
    } else {
      FileUtils.moveFile(srcFile, dstFile);
    }
    return true;
  }

  @Override
  public boolean copyDir(URI srcUri, URI dstUri)
      throws IOException {
    copy(toFile(srcUri), toFile(dstUri), true);
    return true;
  }

  @Override
  public boolean exists(URI fileUri) {
    return toFile(fileUri).exists();
  }

  @Override
  public long length(URI fileUri) {
    File file = toFile(fileUri);
    if (file.isDirectory()) {
      throw new IllegalArgumentException("File is directory");
    }
    return FileUtils.sizeOf(file);
  }

  @Override
  public String[] listFiles(URI fileUri, boolean recursive)
      throws IOException {
    File file = toFile(fileUri);
    if (!recursive) {
      return Arrays.stream(file.list()).map(s -> new File(file, s)).map(File::getAbsolutePath).toArray(String[]::new);
    } else {
      try (Stream<Path> pathStream = Files.walk(Paths.get(fileUri))) {
        return pathStream.filter(s -> !s.equals(file.toPath())).map(Path::toString).toArray(String[]::new);
      }
    }
  }

  @Override
  public List<FileMetadata> listFilesWithMetadata(URI fileUri, boolean recursive)
      throws IOException {
    File file = toFile(fileUri);
    if (!recursive) {
      return Arrays.stream(file.list()).map(s -> getFileMetadata(new File(file, s))).collect(Collectors.toList());
    } else {
      try (Stream<Path> pathStream = Files.walk(Paths.get(fileUri))) {
        return pathStream.filter(s -> !s.equals(file.toPath()))
            .map(p -> getFileMetadata(p.toFile()))
            .collect(Collectors.toList());
      }
    }
  }

  private static FileMetadata getFileMetadata(File file) {
    return new FileMetadata.Builder().setFilePath(file.getAbsolutePath())
        .setLastModifiedTime(file.lastModified())
        .setLength(file.length())
        .setIsDirectory(file.isDirectory())
        .build();
  }

  @Override
  public void copyToLocalFile(URI srcUri, File dstFile)
      throws Exception {
    copy(toFile(srcUri), dstFile, false);
  }

  @Override
  public void copyFromLocalFile(File srcFile, URI dstUri)
      throws Exception {
    copy(srcFile, toFile(dstUri), false);
  }

  @Override
  public void copyFromLocalDir(File srcFile, URI dstUri)
      throws Exception {
    if (!srcFile.isDirectory()) {
      throw new IllegalArgumentException(srcFile.getAbsolutePath() + " is not a directory");
    }
    copy(srcFile, toFile(dstUri), true);
  }

  @Override
  public boolean isDirectory(URI uri) {
    return toFile(uri).isDirectory();
  }

  @Override
  public long lastModified(URI uri) {
    return toFile(uri).lastModified();
  }

  @Override
  public boolean touch(URI uri)
      throws IOException {
    File file = toFile(uri);
    if (!file.exists()) {
      return file.createNewFile();
    }
    return file.setLastModified(System.currentTimeMillis());
  }

  @Override
  public InputStream open(URI uri)
      throws IOException {
    return new BufferedInputStream(new FileInputStream(toFile(uri)));
  }

  private static File toFile(URI uri) {
    // NOTE: Do not use new File(uri) because scheme might not exist and it does not decode '+' to ' '
    //       Do not use uri.getPath() because it does not decode '+' to ' '
    return new File(URLDecoder.decode(uri.getRawPath(), StandardCharsets.UTF_8));
  }

  private static void copy(File srcFile, File dstFile, boolean recursive)
      throws IOException {

    // Create a temporary file in the same directory as dstFile
    File tmpFile = new File(dstFile.getParent(), dstFile.getName() + TMP);
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss.SSS");
    sdf.setTimeZone(TimeZone.getTimeZone("UTC")); // Set time zone to UTC
    File backupFile = new File(
        dstFile.getParent(),
        dstFile.getName() + BACKUP + "_" + sdf.format(new Date())
    );

    try {
      // Step 1: Copy the file or directory into the temporary file
      if (srcFile.isDirectory()) {
        if (recursive) {
          FileUtils.copyDirectory(srcFile, tmpFile);
        } else {
          throw new IOException(
              srcFile.getAbsolutePath() + " is a directory and recursive copy is not enabled.");
        }
      } else {
        FileUtils.copyFile(srcFile, tmpFile);
      }

      if (dstFile.exists() && !dstFile.renameTo(backupFile)) {
        throw new IOException("Failed to rename destination file to backup file.");
      }

      // Step 2: Rename the temporary file to the destination file
      if (!tmpFile.renameTo(dstFile)) {
        throw new IOException("Failed to rename temporary file to destination file.");
      }
      FileUtils.deleteQuietly(backupFile);
    } catch (Exception e) {
      // Cleanup the temporary file in case of errors
      // intentionally not deleting the backup file if anything goes wrong
      FileUtils.deleteQuietly(tmpFile);
      throw new LocalPinotFSException(e);
    }
  }

  public static class LocalPinotFSException extends IOException {
    LocalPinotFSException(Throwable e) {
      super(e);
    }
  }
}
