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

  private static final String PRINCIPLE = "kerberos.principle";
  private static final String KEYTAB = "kerberos.keytab";
  private static final Logger LOGGER = LoggerFactory.getLogger(HdfsSegmentFetcher.class);
  private FileSystem fs = null;

  @Override
  public void init(Map<String, String> configs) {
    Configuration hadoopConf = new Configuration();
    authenticate(hadoopConf, configs);
    try {
      fs = FileSystem.get(new Configuration());
    } catch (IOException e) {
      LOGGER.error("failed to initialized the hdfs", e);
    }
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
      fs.copyToLocalFile(new Path(uri), new Path(tempFile.toURI()));
      LOGGER.info("copied {} from hdfs to {} in local for size %d", uri, tempFile.getAbsolutePath(), tempFile.length());
  }

}
