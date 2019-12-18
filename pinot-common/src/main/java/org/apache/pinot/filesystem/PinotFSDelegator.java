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
import java.io.InputStream;
import java.net.URI;
import org.apache.commons.configuration.Configuration;
import org.apache.pinot.common.utils.retry.RetryPolicies;
import org.apache.pinot.common.utils.retry.RetryPolicy;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.common.utils.CommonConstants.SegmentOperations.RETRY;
import static org.apache.pinot.common.utils.CommonConstants.SegmentOperations.RETRY_DEFAULT;
import static org.apache.pinot.common.utils.CommonConstants.SegmentOperations.RETRY_WAITIME_MS;
import static org.apache.pinot.common.utils.CommonConstants.SegmentOperations.RETRY_WAITIME_MS_DEFAULT;


public class PinotFSDelegator extends PinotFS {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotFSDelegator.class);

  private PinotFS _pinotFS;

  private int _retryCount = RETRY_DEFAULT;
  private int _retryWaitMs = RETRY_WAITIME_MS_DEFAULT;

  PinotFSDelegator(PinotFS pinotFS) {

    _pinotFS = pinotFS;
  }

  @Override
  public void init(Configuration config) {
    _retryCount = config.getInt(RETRY, _retryCount);
    _retryWaitMs = config.getInt(RETRY_WAITIME_MS, _retryWaitMs);
    _pinotFS.init(config);
  }

  @Override
  public boolean mkdir(URI uri)
      throws IOException {
    return _pinotFS.mkdir(uri);
  }

  @Override
  public boolean delete(URI segmentUri, boolean forceDelete)
      throws IOException {
    return _pinotFS.delete(segmentUri, forceDelete);
  }

  @Override
  public boolean doMove(URI srcUri, URI dstUri)
      throws IOException {
    return _pinotFS.doMove(srcUri, dstUri);
  }

  @Override
  public boolean copy(URI srcUri, URI dstUri)
      throws IOException {
    return _pinotFS.copy(srcUri, dstUri);
  }

  @Override
  public boolean exists(URI fileUri)
      throws IOException {
    return _pinotFS.exists(fileUri);
  }

  @Override
  public long length(URI fileUri)
      throws IOException {
    return _pinotFS.length(fileUri);
  }

  @Override
  public String[] listFiles(URI fileUri, boolean recursive)
      throws IOException {
    return _pinotFS.listFiles(fileUri, recursive);
  }

  @Override
  public void copyToLocalFile(URI srcUri, File dstFile)
      throws Exception {
    RetryPolicy fixedDelayRetryPolicy = RetryPolicies.fixedDelayRetryPolicy(_retryCount, _retryWaitMs);
    fixedDelayRetryPolicy.attempt(() -> {
      try {
        long startMs = System.currentTimeMillis();
        _pinotFS.copyToLocalFile(srcUri, dstFile);
        LOGGER.debug("copied {} to {},  size {}, took {} ms", srcUri, dstFile,
            dstFile.length(), System.currentTimeMillis() - startMs);

        return true;
      } catch (Exception e) {
        return false;
      }
    });
  }

  @Override
  public void copyFromLocalFile(File srcFile, URI dstUri)
      throws Exception {
    _pinotFS.copyFromLocalFile(srcFile, dstUri);
  }

  @Override
  public boolean isDirectory(URI uri)
      throws IOException {
    return _pinotFS.isDirectory(uri);
  }

  @Override
  public long lastModified(URI uri)
      throws IOException {
    return _pinotFS.lastModified(uri);
  }

  @Override
  public boolean touch(URI uri)
      throws IOException {
    return _pinotFS.touch(uri);
  }

  @Override
  public InputStream open(URI uri)
      throws IOException {
    return _pinotFS.open(uri);
  }
}
