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
package com.linkedin.pinot.hadoop.job;

import com.linkedin.pinot.common.utils.FileUploadDownloadClient;
import java.io.InputStream;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentTarPushJob extends Configured {

  private String _segmentPath;
  private String[] _hosts;
  private int _port;

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentTarPushJob.class);

  public SegmentTarPushJob(String name, Properties properties) {
    super(new Configuration());
    _segmentPath = properties.getProperty("path.to.output") + "/";
    _hosts = properties.getProperty("push.to.hosts").split(",");
    _port = Integer.parseInt(properties.getProperty("push.to.port"));
  }

  public void run() throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path path = new Path(_segmentPath);
    FileStatus[] fileStatusArr = fs.globStatus(path);
    for (FileStatus fileStatus : fileStatusArr) {
      if (fileStatus.isDirectory()) {
        pushDir(fs, fileStatus.getPath());
      } else {
        pushOneTarFile(fs, fileStatus.getPath());
      }
    }
  }

  public void pushDir(FileSystem fs, Path path) throws Exception {
    LOGGER.info("******** Now uploading segments tar from dir: {}", path);
    FileStatus[] fileStatusArr = fs.listStatus(new Path(path.toString() + "/"));
    for (FileStatus fileStatus : fileStatusArr) {
      if (fileStatus.isDirectory()) {
        pushDir(fs, fileStatus.getPath());
      } else {
        pushOneTarFile(fs, fileStatus.getPath());
      }
    }
  }

  public void pushOneTarFile(FileSystem fs, Path path) throws Exception {
    String fileName = path.getName();
    if (!fileName.endsWith(".tar.gz")) {
      return;
    }
    try (FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient()) {
      for (String host : _hosts) {
        try (InputStream inputStream = fs.open(path)) {
          fileName = fileName.split(".tar.gz")[0];
          LOGGER.info("******** Uploading file: {} to Host: {} and Port: {} *******", fileName, host, _port);
          int responseCode =
              fileUploadDownloadClient.uploadSegment(FileUploadDownloadClient.getUploadSegmentHttpURI(host, _port),
                  fileName, inputStream);
          LOGGER.info("Response code: {}", responseCode);
        } catch (Exception e) {
          LOGGER.error("******** Error Uploading file: {} to Host: {} and Port: {}  *******", fileName, host, _port);
          LOGGER.error("Caught exception during upload", e);
          throw e;
        }
      }
    }
  }
}
