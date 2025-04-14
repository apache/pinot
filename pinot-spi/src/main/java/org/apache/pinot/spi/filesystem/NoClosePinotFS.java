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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import org.apache.pinot.spi.env.PinotConfiguration;


public class NoClosePinotFS implements PinotFS {
  protected final PinotFS _delegate;

  protected NoClosePinotFS(PinotFS delegate) {
    _delegate = delegate;
  }

  @Override
  public void init(PinotConfiguration config) {
    _delegate.init(config);
  }

  @Override
  public boolean mkdir(URI uri)
      throws IOException {
    return _delegate.mkdir(uri);
  }

  @Override
  public boolean delete(URI segmentUri, boolean forceDelete)
      throws IOException {
    return _delegate.delete(segmentUri, forceDelete);
  }

  @Override
  public boolean deleteBatch(List<URI> segmentUris, boolean forceDelete)
      throws IOException {
    return _delegate.deleteBatch(segmentUris, forceDelete);
  }

  @Override
  public boolean move(URI srcUri, URI dstUri, boolean overwrite)
      throws IOException {
    return _delegate.move(srcUri, dstUri, overwrite);
  }

  @Override
  public boolean copy(URI srcUri, URI dstUri)
      throws IOException {
    return _delegate.copy(srcUri, dstUri);
  }

  @Override
  public boolean copyDir(URI srcUri, URI dstUri)
      throws IOException {
    return _delegate.copyDir(srcUri, dstUri);
  }

  @Override
  public boolean exists(URI fileUri)
      throws IOException {
    return _delegate.exists(fileUri);
  }

  @Override
  public long length(URI fileUri)
      throws IOException {
    return _delegate.length(fileUri);
  }

  @Override
  public String[] listFiles(URI fileUri, boolean recursive)
      throws IOException {
    return _delegate.listFiles(fileUri, recursive);
  }

  @Override
  public List<FileMetadata> listFilesWithMetadata(URI fileUri, boolean recursive)
      throws IOException {
    return _delegate.listFilesWithMetadata(fileUri, recursive);
  }

  @Override
  public void copyToLocalFile(URI srcUri, File dstFile)
      throws Exception {
    _delegate.copyToLocalFile(srcUri, dstFile);
  }

  @Override
  public void copyFromLocalDir(File srcFile, URI dstUri)
      throws Exception {
    _delegate.copyFromLocalDir(srcFile, dstUri);
  }

  @Override
  public void copyFromLocalFile(File srcFile, URI dstUri)
      throws Exception {
    _delegate.copyFromLocalFile(srcFile, dstUri);
  }

  @Override
  public boolean isDirectory(URI uri)
      throws IOException {
    return _delegate.isDirectory(uri);
  }

  @Override
  public long lastModified(URI uri)
      throws IOException {
    return _delegate.lastModified(uri);
  }

  @Override
  public boolean touch(URI uri)
      throws IOException {
    return _delegate.touch(uri);
  }

  @Override
  public InputStream open(URI uri)
      throws IOException {
    return _delegate.open(uri);
  }
}
