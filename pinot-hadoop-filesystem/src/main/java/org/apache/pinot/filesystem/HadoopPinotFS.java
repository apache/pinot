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

import com.google.common.base.Strings;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.pinot.common.utils.retry.RetryPolicies;
import org.apache.pinot.common.utils.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.common.utils.CommonConstants.SegmentOperations.HadoopSegmentOperations.HADOOP_CONF_PATH;
import static org.apache.pinot.common.utils.CommonConstants.SegmentOperations.HadoopSegmentOperations.KEYTAB;
import static org.apache.pinot.common.utils.CommonConstants.SegmentOperations.HadoopSegmentOperations.PRINCIPAL;
import static org.apache.pinot.common.utils.CommonConstants.SegmentOperations.RETRY;
import static org.apache.pinot.common.utils.CommonConstants.SegmentOperations.RETRY_DEFAULT;
import static org.apache.pinot.common.utils.CommonConstants.SegmentOperations.RETRY_WAITIME_MS;
import static org.apache.pinot.common.utils.CommonConstants.SegmentOperations.RETRY_WAITIME_MS_DEFAULT;


/**
 * Implementation of PinotFS for the Hadoop Filesystem
 */
public class HadoopPinotFS extends PinotFS {
  private static final Logger LOGGER = LoggerFactory.getLogger(HadoopPinotFS.class);
  private org.apache.hadoop.fs.FileSystem _hadoopFS = null;
  private int _retryCount = RETRY_DEFAULT;
  private int _retryWaitMs = RETRY_WAITIME_MS_DEFAULT;
  private org.apache.hadoop.conf.Configuration _hadoopConf;

  public HadoopPinotFS() {

  }

  @Override
  public void init(Configuration config) {
    try {
      _retryCount = config.getInt(RETRY, _retryCount);
      _retryWaitMs = config.getInt(RETRY_WAITIME_MS, _retryWaitMs);
      _hadoopConf = getConf(config.getString(HADOOP_CONF_PATH));
      authenticate(_hadoopConf, config);
      _hadoopFS = org.apache.hadoop.fs.FileSystem.get(_hadoopConf);
      LOGGER.info("successfully initialized HadoopPinotFS");
    } catch (IOException e) {
      throw new RuntimeException("Could not initialize HadoopPinotFS", e);
    }
  }

  @Override
  public boolean mkdir(URI uri) throws IOException {
    return _hadoopFS.mkdirs(new Path(uri));
  }

  @Override
  public boolean delete(URI segmentUri, boolean forceDelete) throws IOException {
    // Returns false if we are moving a directory and that directory is not empty
    if (isDirectory(segmentUri) && listFiles(segmentUri, false).length > 0 && !forceDelete) {
      return false;
    }
    return _hadoopFS.delete(new Path(segmentUri), true);
  }

  @Override
  public boolean move(URI srcUri, URI dstUri, boolean overwrite) throws IOException {
    if (exists(dstUri) && !overwrite) {
      return false;
    }
    return _hadoopFS.rename(new Path(srcUri), new Path(dstUri));
  }

  /**
   * Note that this method copies within a cluster. If you want to copy outside the cluster, you will
   * need to create a new configuration and filesystem. Keeps files if copy/move is partial.
   */
  @Override
  public boolean copy(URI srcUri, URI dstUri) throws IOException {
    Path source = new Path(srcUri);
    Path target = new Path(dstUri);
    RemoteIterator<LocatedFileStatus> sourceFiles = _hadoopFS.listFiles(source, true);
    if (sourceFiles != null) {
      while (sourceFiles.hasNext()) {
        boolean succeeded =
            FileUtil.copy(_hadoopFS, sourceFiles.next().getPath(), _hadoopFS, target, true, _hadoopConf);
        if (!succeeded) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public boolean exists(URI fileUri) throws IOException {
    return _hadoopFS.exists(new Path(fileUri));
  }

  @Override
  public long length(URI fileUri) throws IOException {
    return _hadoopFS.getLength(new Path(fileUri));
  }

  @Override
  public String[] listFiles(URI fileUri, boolean recursive) throws IOException {
    ArrayList<String> filePathStrings = new ArrayList<>();
    Path path = new Path(fileUri);
    if (_hadoopFS.exists(path)) {
      RemoteIterator<LocatedFileStatus> fileListItr = _hadoopFS.listFiles(path, recursive);
      while (fileListItr != null && fileListItr.hasNext()) {
        LocatedFileStatus file = fileListItr.next();
        filePathStrings.add(file.getPath().toUri().toString());
      }
    } else {
      throw new IllegalArgumentException("segmentUri is not valid");
    }
    String[] retArray = new String[filePathStrings.size()];
    filePathStrings.toArray(retArray);
    return retArray;
  }

  @Override
  public void copyToLocalFile(URI srcUri, File dstFile) throws Exception {
    LOGGER.debug("starting to fetch segment from hdfs");
    final String dstFilePath = dstFile.getAbsolutePath();
    try {
      final Path remoteFile = new Path(srcUri);
      final Path localFile = new Path(dstFile.toURI());

      RetryPolicy fixedDelayRetryPolicy = RetryPolicies.fixedDelayRetryPolicy(_retryCount, _retryWaitMs);
      fixedDelayRetryPolicy.attempt(() -> {
        try {
          if (_hadoopFS == null) {
            throw new RuntimeException("_hadoopFS client is not initialized when trying to copy files");
          }
          long startMs = System.currentTimeMillis();
          _hadoopFS.copyToLocalFile(remoteFile, localFile);
          LOGGER.debug("copied {} from hdfs to {} in local for size {}, take {} ms", srcUri, dstFilePath,
              dstFile.length(), System.currentTimeMillis() - startMs);
          return true;
        } catch (IOException e) {
          LOGGER.warn("failed to fetch segment {} from hdfs, might retry", srcUri, e);
          return false;
        }
      });
    } catch (Exception e) {
      LOGGER.error("failed to fetch {} from hdfs to local {}", srcUri, dstFilePath, e);
      throw e;
    }
  }

  @Override
  public void copyFromLocalFile(File srcFile, URI dstUri) throws Exception {
    _hadoopFS.copyFromLocalFile(new Path(srcFile.toURI()), new Path(dstUri));
  }

  @Override
  public boolean isDirectory(URI uri) {
    FileStatus fileStatus = new FileStatus();
    fileStatus.setPath(new Path(uri));
    return fileStatus.isDirectory();
  }

  @Override
  public long lastModified(URI uri) {
    try {
      return _hadoopFS.getFileStatus(new Path(uri)).getModificationTime();
    } catch (IOException e) {
      LOGGER.error("Could not get file status for {}", uri);
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean touch(URI uri) throws IOException {
    Path path = new Path(uri);
    if (!exists(uri)) {
      FSDataOutputStream fos = _hadoopFS.create(path);
      fos.close();
    } else {
      _hadoopFS.setTimes(path, System.currentTimeMillis(), -1);
    }
    return true;
  }

  private void authenticate(org.apache.hadoop.conf.Configuration hadoopConf,
      org.apache.commons.configuration.Configuration configs) {
    String principal = configs.getString(PRINCIPAL);
    String keytab = configs.getString(KEYTAB);
    if (!Strings.isNullOrEmpty(principal) && !Strings.isNullOrEmpty(keytab)) {
      UserGroupInformation.setConfiguration(hadoopConf);
      if (UserGroupInformation.isSecurityEnabled()) {
        try {
          if (!UserGroupInformation.getCurrentUser().hasKerberosCredentials() || !UserGroupInformation.getCurrentUser()
              .getUserName()
              .equals(principal)) {
            LOGGER.info("Trying to authenticate user [%s] with keytab [%s]..", principal, keytab);
            UserGroupInformation.loginUserFromKeytab(principal, keytab);
          }
        } catch (IOException e) {
          throw new RuntimeException(
              String.format("Failed to authenticate user principal [%s] with keytab [%s]", principal, keytab), e);
        }
      }
    }
  }

  private org.apache.hadoop.conf.Configuration getConf(String hadoopConfPath) {
    org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
    if (Strings.isNullOrEmpty(hadoopConfPath)) {
      LOGGER.warn("no hadoop conf path is provided, will rely on default config");
    } else {
      hadoopConf.addResource(new Path(hadoopConfPath, "core-site.xml"));
      hadoopConf.addResource(new Path(hadoopConfPath, "hdfs-site.xml"));
    }
    return hadoopConf;
  }
}
