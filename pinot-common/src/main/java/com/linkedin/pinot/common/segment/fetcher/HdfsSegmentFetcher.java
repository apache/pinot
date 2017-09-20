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
package com.linkedin.pinot.common.segment.fetcher;

import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * Created by jamesshao on 9/14/17.
 */
public class HdfsSegmentFetcher implements SegmentFetcher {

  private static final String PRINCIPLE = "hadoop.kerberos.principle";
  private static final String KEYTAB = "hadoop.kerberos.keytab";
  private static final String HADOOP_CONF_PATH = "hadoop.conf.path";
  private static final Logger LOGGER = LoggerFactory.getLogger(HdfsSegmentFetcher.class);
  private FileSystem fs = null;

  @Override
  public void init(Map<String, String> configs) {
    String hadoopConfPath = configs.get(HADOOP_CONF_PATH);
    Configuration hadoopConf = getConf(hadoopConfPath);
    authenticate(hadoopConf, configs);
    try {
      fs = FileSystem.get(hadoopConf);
      LOGGER.debug("successfully initialized hdfs segment fetcher");
    } catch (IOException e) {
      LOGGER.error("failed to initialized the hdfs", e);
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

  private void authenticate(Configuration hadoopConf, Map<String, String> configs) {
    String principal = configs.get(PRINCIPLE);
    String keytab = configs.get(KEYTAB);
    if (!Strings.isNullOrEmpty(principal) && !Strings.isNullOrEmpty(keytab)) {
      UserGroupInformation.setConfiguration(hadoopConf);
      if (UserGroupInformation.isSecurityEnabled()) {
        try {
          if (!UserGroupInformation.getCurrentUser().hasKerberosCredentials()
              || !UserGroupInformation.getCurrentUser().getUserName().equals(principal)) {
            LOGGER.info("Trying to authenticate user [%s] with keytab [%s]..", principal, keytab);
            UserGroupInformation.loginUserFromKeytab(principal, keytab);
          }
        }
        catch (IOException e) {
          throw new RuntimeException(String.format("Failed to authenticate user principal [%s] with keytab [%s]",
                  principal, keytab), e);
        }
      }
    }
  }

  @Override
  public void fetchSegmentToLocal(String uri, File tempFile) throws Exception {
    LOGGER.debug("starting to fetch segment from hdfs");
    try {
      if (fs == null) {
        LOGGER.error("uninitialized fs for fetching data from hdfs");
        throw new RuntimeException("failed to get hdfs client");
      }
      
      fs.copyToLocalFile(new Path(uri), new Path(tempFile.toURI()));
      LOGGER.debug("copied {} from hdfs to {} in local for size {}", uri, tempFile.getAbsolutePath(), tempFile.length());
    } catch(Exception ex) {
      LOGGER.error(String.format("failed to fetch %s from hdfs", uri), ex);
      throw ex;
    }
  }

}
