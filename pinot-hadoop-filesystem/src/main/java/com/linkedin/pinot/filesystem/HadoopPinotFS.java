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
package com.linkedin.pinot.filesystem;

import com.google.common.base.Strings;
import com.linkedin.pinot.common.utils.retry.RetryPolicies;
import com.linkedin.pinot.common.utils.retry.RetryPolicy;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.pinot.common.utils.CommonConstants.SegmentOperations.HadoopSegmentOperations.*;
import static com.linkedin.pinot.common.utils.CommonConstants.SegmentOperations.*;


/**
 * Implementation of PinotFS for the Hadoop Filesystem
 */
public class HadoopPinotFS extends PinotFS {
  private static final Logger LOGGER = LoggerFactory.getLogger(HadoopPinotFS.class);
  private org.apache.hadoop.fs.FileSystem hadoopFS = null;
  private int retryCount = RETRY_DEFAULT;
  private int retryWaitMs = RETRY_WAITIME_MS_DEFAULT;
  private org.apache.hadoop.conf.Configuration hadoopConf;

  public HadoopPinotFS() {

  }

  @Override
  public void init(Configuration config) {
    try {
      retryCount = config.getInt(RETRY, retryCount);
      retryWaitMs = config.getInt(RETRY_WAITIME_MS, retryWaitMs);
      hadoopConf = getConf(config.getString(HADOOP_CONF_PATH));
      authenticate(hadoopConf, config);
      hadoopFS = org.apache.hadoop.fs.FileSystem.get(hadoopConf);
      LOGGER.info("successfully initialized HadoopPinotFS");
    } catch (IOException e) {
      throw new RuntimeException("Could not initialize HadoopPinotFS", e);
    }
  }

  @Override
  public boolean delete(URI segmentUri) throws IOException {
    return hadoopFS.delete(new Path(segmentUri), true);
  }

  @Override
  public boolean move(URI srcUri, URI dstUri) throws IOException {
    return hadoopFS.rename(new Path(srcUri), new Path(dstUri));
  }

  /**
   * Note that this method copies within a cluster. If you want to copy outside the cluster, you will
   * need to create a new configuration and filesystem. Keeps files if copy/move is partial.
   */
  @Override
  public boolean copy(URI srcUri, URI dstUri) throws IOException {
    Path source = new Path(srcUri);
    Path target = new Path(dstUri);
    RemoteIterator<LocatedFileStatus> sourceFiles = hadoopFS.listFiles(source, true);
    if (sourceFiles != null) {
      while (sourceFiles.hasNext()) {
        boolean succeeded = FileUtil.copy(hadoopFS, sourceFiles.next().getPath(), hadoopFS, target, true, hadoopConf);
        if (!succeeded) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public boolean exists(URI fileUri) throws IOException {
    return hadoopFS.exists(new Path(fileUri));
  }

  @Override
  public long length(URI fileUri) throws IOException {
    return hadoopFS.getLength(new Path(fileUri));
  }

  @Override
  public String[] listFiles(URI fileUri) throws IOException {
    ArrayList<String> filePathStrings = new ArrayList<>();
    Path path = new Path(fileUri);
    if (hadoopFS.exists(path)) {
      RemoteIterator<LocatedFileStatus> fileListItr = hadoopFS.listFiles(path, true);
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
  public void copyToLocalFile(URI srcUri, URI dstUri) throws Exception {
    LOGGER.debug("starting to fetch segment from hdfs");
    final String tempFilePath = dstUri.getPath();
    try {
      final Path remoteFile = new Path(srcUri);
      final Path localFile = new Path(dstUri);

      RetryPolicy fixDelayRetryPolicy = RetryPolicies.fixedDelayRetryPolicy(retryCount, retryWaitMs);
      fixDelayRetryPolicy.attempt(() -> {
        try {
          if (hadoopFS == null) {
            throw new RuntimeException("hadoopFS client is not initialized when trying to copy files");
          }
          long startMs = System.currentTimeMillis();
          hadoopFS.copyToLocalFile(remoteFile, localFile);
          LOGGER.debug("copied {} from hdfs to {} in local for size {}, take {} ms", srcUri.getPath(), tempFilePath,
              new File(dstUri).length(), System.currentTimeMillis() - startMs);
          return true;
        } catch (IOException ex) {
          LOGGER.warn(String.format("failed to fetch segment %s from hdfs, might retry", srcUri.getPath()), ex);
          return false;
        }
      });
    } catch (Exception ex) {
      LOGGER.error(String.format("failed to fetch %s from hdfs to local %s", srcUri.getPath(), tempFilePath), ex);
      throw ex;
    }
  }

  @Override
  public void copyFromLocalFile(URI srcUri, URI dstUri) throws IOException {
    hadoopFS.copyFromLocalFile(new Path(srcUri), new Path(dstUri));
  }

  @Override
  public boolean canMoveBetweenLocations(URI srcUri, URI dstUri) {
    return srcUri.getScheme().equals("hdfs")
        && dstUri.getScheme().equals("hdfs")
        && srcUri.getHost().equals(dstUri.getHost());
  }

  private void authenticate(org.apache.hadoop.conf.Configuration hadoopConf, org.apache.commons.configuration.Configuration configs) {
    String principal = configs.getString(PRINCIPAL);
    String keytab = configs.getString(KEYTAB);
    if (!Strings.isNullOrEmpty(principal) && !Strings.isNullOrEmpty(keytab)) {
      UserGroupInformation.setConfiguration(hadoopConf);
      if (UserGroupInformation.isSecurityEnabled()) {
        try {
          if (!UserGroupInformation.getCurrentUser().hasKerberosCredentials()
              || !UserGroupInformation.getCurrentUser().getUserName().equals(principal)) {
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
