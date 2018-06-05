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
package com.linkedin.pinot.filesystem;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.Collection;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;


/**
 * Implementation of PinotFS for a local filesystem. Methods in this class may throw a SecurityException at runtime
 * if access to the file is denied.
 */
public class LocalPinotFS extends PinotFS {

  public LocalPinotFS() {
  }

  @Override
  public void init(Configuration configuration) {
  }

  @Override
  public boolean delete(URI segmentUri) throws IOException {
    File file = new File(segmentUri);
    if (file.isDirectory()) {
      // Throws an IOException if it is unable to delete
      FileUtils.deleteDirectory(file);
    } else {
      // Returns false if delete fails
      return FileUtils.deleteQuietly(file);
    }
    return true;
  }

  @Override
  public boolean move(URI srcUri, URI dstUri) throws IOException {
    File srcFile = new File(srcUri);
    File dstFile = new File(dstUri);
    if (dstFile.exists()) {
      FileUtils.deleteQuietly(dstFile);
    }
    if (!srcFile.isDirectory()) {
      dstFile.getParentFile().mkdirs();
      FileUtils.moveFile(srcFile, dstFile);
    }
    if (srcFile.isDirectory()) {
      Files.move(srcFile.toPath(), dstFile.toPath());
    }
    return true;
  }

  @Override
  public boolean copy(URI srcUri, URI dstUri) throws IOException {
    File srcFile = new File(srcUri);
    File dstFile = new File(dstUri);
    if (srcFile.isDirectory()) {
      // Throws Exception on failure
      FileUtils.copyDirectory(srcFile, dstFile);
    } else {
      // Will create parent directories, throws Exception on failure
      FileUtils.copyFile(srcFile, dstFile);
    }
    return true;
  }

  @Override
  public boolean exists(URI segmentUri) throws IOException {
    File file = new File(segmentUri);
    return file.exists();
  }

  @Override
  public long length(URI segmentUri) throws IOException {
    File file = new File(segmentUri);
    if (file.isDirectory()) {
      throw new IllegalArgumentException("File is directory");
    }
    return FileUtils.sizeOf(file);
  }

  @Override
  public String[] listFiles(URI segmentUri) throws IOException {
    File file = new File(segmentUri);
    Collection<File> files = FileUtils.listFiles(file, null, true);
    return files.stream().map(File::getPath).toArray(String[]::new);
  }

  @Override
  public void copyToLocalFile(URI srcUri, URI dstUri) throws IOException {
    copy(srcUri, dstUri);
  }

  @Override
  public void copyFromLocalFile(URI srcUri, URI dstUri) throws IOException {
    copy(srcUri, dstUri);
  }
}