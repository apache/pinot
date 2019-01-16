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
package org.apache.pinot.filesystem;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of PinotFS for a local filesystem. Methods in this class may throw a SecurityException at runtime
 * if access to the file is denied.
 */
public class LocalPinotFS extends PinotFS {
  private static final Logger LOGGER = LoggerFactory.getLogger(LocalPinotFS.class);
  private static final String DEFAULT_ENCODING = "UTF-8";

  public LocalPinotFS() {
  }

  @Override
  public void init(Configuration configuration) {
  }

  @Override
  public boolean mkdir(URI uri) throws IOException {
    FileUtils.forceMkdir(new File(decodeURI(uri.getRawPath())));
    return true;
  }

  @Override
  public boolean delete(URI segmentUri, boolean forceDelete) throws IOException {
    File file = new File(decodeURI(segmentUri.getRawPath()));
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
  public boolean move(URI srcUri, URI dstUri, boolean overwrite) throws IOException {
    File srcFile = new File(decodeURI(srcUri.getRawPath()));
    File dstFile = new File(decodeURI(dstUri.getRawPath()));
    if (dstFile.exists()) {
      if (overwrite) {
        FileUtils.deleteQuietly(dstFile);
      } else {
        // dst file exists, returning
        return false;
      }
    } else {
      // ensure the dst path exists
      FileUtils.forceMkdir(dstFile.getParentFile());
    }

    Files.move(srcFile.toPath(), dstFile.toPath());

    return true;
  }

  @Override
  public boolean copy(URI srcUri, URI dstUri) throws IOException {
    File srcFile = new File(decodeURI(srcUri.getRawPath()));
    File dstFile = new File(decodeURI(dstUri.getRawPath()));
    if (dstFile.exists()) {
      FileUtils.deleteQuietly(dstFile);
    }
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
  public boolean exists(URI fileUri) throws IOException {
    File file = new File(decodeURI(fileUri.getRawPath()));
    return file.exists();
  }

  @Override
  public long length(URI fileUri) throws IOException {
    File file = new File(decodeURI(fileUri.getRawPath()));
    if (file.isDirectory()) {
      throw new IllegalArgumentException("File is directory");
    }
    return FileUtils.sizeOf(file);
  }

  @Override
  public String[] listFiles(URI fileUri, boolean recursive) throws IOException {
    File file = new File(decodeURI(fileUri.getRawPath()));
    if (!recursive) {
      return Arrays.stream(file.list())
          .map(s -> new File(file, s))
          .map(File::getAbsolutePath).toArray(String[]::new);
    } else {
       return Files.walk(Paths.get(fileUri)).
           filter(s -> !s.equals(file.toPath()))
           .map(Path::toString).toArray(String[]::new);
    }
  }

  @Override
  public void copyToLocalFile(URI srcUri, File dstFile) throws Exception {
    copy(srcUri, new URI(encodeURI(dstFile.getAbsolutePath())));
  }

  @Override
  public void copyFromLocalFile(File srcFile, URI dstUri) throws Exception {
    copy(new URI(encodeURI(srcFile.getAbsolutePath())), dstUri);
  }

  @Override
  public boolean isDirectory(URI uri) {
    File file = new File(decodeURI(uri.getRawPath()));
    return file.isDirectory();
  }

  @Override
  public long lastModified(URI uri) {
    File file = new File(decodeURI(uri.getRawPath()));
    return file.lastModified();
  }

  @Override
  public boolean touch(URI uri) throws IOException {
    File file = new File(decodeURI(uri.getRawPath()));
    if (!exists(uri)) {
      return file.createNewFile();
    }
    return file.setLastModified(System.currentTimeMillis());
  }

  private String encodeURI(String uri) {
    String encodedStr;
    try {
      encodedStr = URLEncoder.encode(uri, DEFAULT_ENCODING);
    } catch (UnsupportedEncodingException e) {
      LOGGER.warn("Could not encode uri {}", uri);
      throw new RuntimeException(e);
    }
    return encodedStr;
  }

  private String decodeURI(String uri) {
    String decodedStr;
    try {
      decodedStr = URLDecoder.decode(uri, DEFAULT_ENCODING);
    } catch (UnsupportedEncodingException e) {
      LOGGER.warn("Could not decode uri {}", uri);
      throw new RuntimeException(e);
    }
    return decodedStr;
  }
}
