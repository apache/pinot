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
package com.linkedin.pinot.common.segment.fetcher;

import com.google.common.base.Strings;
import com.linkedin.pinot.common.utils.retry.RetryPolicies;
import com.linkedin.pinot.common.utils.retry.RetryPolicy;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.pinot.common.utils.CommonConstants.SegmentOperations.HadoopSegmentOperations.*;
import static com.linkedin.pinot.common.utils.CommonConstants.SegmentOperations.*;


public class HdfsSegmentFetcher implements SegmentFetcher {

  private static final Logger LOGGER = LoggerFactory.getLogger(HdfsSegmentFetcher.class);
  private FileSystem _hadoopFS = null;
  private int _retryCount = RETRY_DEFAULT;
  private int _retryWaitMs = RETRY_WAITIME_MS_DEFAULT;

  @Override
  public void init(org.apache.commons.configuration.Configuration configs) {
    try {
      _retryCount = configs.getInt(RETRY, _retryCount);
      _retryWaitMs = configs.getInt(RETRY_WAITIME_MS, _retryWaitMs);
      Configuration hadoopConf = getConf(configs.getString(HADOOP_CONF_PATH));
      authenticate(hadoopConf, configs);
      _hadoopFS = FileSystem.get(hadoopConf);
      LOGGER.info("successfully initialized hdfs segment fetcher");
    } catch (Exception e) {
      LOGGER.error("failed to initialized the hdfs segment fetcher", e);
    }
  }

  private Configuration getConf(String hadoopConfPath) {
    Configuration hadoopConf = new Configuration();
    if (Strings.isNullOrEmpty(hadoopConfPath)) {
      LOGGER.warn("no hadoop conf path is provided, will rely on default config");
    } else {
      hadoopConf.addResource(new Path(hadoopConfPath, "core-site.xml"));
      hadoopConf.addResource(new Path(hadoopConfPath, "hdfs-site.xml"));
    }
    return hadoopConf;
  }

  private void authenticate(Configuration hadoopConf, org.apache.commons.configuration.Configuration configs) {
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

  @Override
  public void fetchSegmentToLocal(final String uri, final File tempFile) throws Exception {
    LOGGER.debug("starting to fetch segment from hdfs");
    final String dstFilePath = tempFile.getAbsolutePath();
    try {
      final Path remoteFile = new Path(uri);
      final Path localFile = new Path(tempFile.toURI());

      RetryPolicy fixedDelayRetryPolicy = RetryPolicies.fixedDelayRetryPolicy(_retryCount, _retryWaitMs);
      fixedDelayRetryPolicy.attempt(() -> {
        try {
          if (_hadoopFS == null) {
            throw new RuntimeException("_hadoopFS client is not initialized when trying to copy files");
          }
          long startMs = System.currentTimeMillis();
          _hadoopFS.copyToLocalFile(remoteFile, localFile);
          LOGGER.debug("copied {} from hdfs to {} in local for size {}, take {} ms", uri, dstFilePath,
              tempFile.length(), System.currentTimeMillis() - startMs);
          return true;
        } catch (IOException e) {
          LOGGER.warn("failed to fetch segment {} from hdfs, might retry", uri, e);
          return false;
        }
      });
    } catch (Exception e) {
      LOGGER.error("failed to fetch {} from hdfs to local {}", uri, dstFilePath, e);
      throw e;
    }
  }

  @Override
  public Set<String> getProtectedConfigKeys() {
    return Collections.emptySet();
  }
}
