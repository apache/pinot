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
package org.apache.pinot.common.utils.webhdfs;

import java.io.File;
import java.io.IOException;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.FileRequestEntity;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WebHdfsV1Client {

  private static final Logger LOGGER = LoggerFactory.getLogger(WebHdfsV1Client.class);
  private static final String LOCATION = "Location";
  private static final String DEFAULT_PROTOCOL = CommonConstants.HTTP_PROTOCOL;
  private static final boolean DEFAULT_OVERWRITE = true;
  private static final int DEFAULT_PERMISSION = 755;

  //Web hdfs upload path template has 6 parameters: protocol/host/port/hdfs_path/overwrite/permission
  private static final String WEB_HDFS_UPLOAD_PATH_TEMPLATE = "%s://%s:%s/webhdfs/v1%s?op=CREATE&overwrite=%s&permission=%s";
  //Web hdfs download path template has 4 parameters: protocol/host/port/hdfs_path
  private static final String WEB_HDFS_DOWNLOAD_PATH_TEMPLATE = "%s://%s:%s/webhdfs/v1%s?op=OPEN";

  private final String _protocol;
  private final String _host;
  private final int _port;
  private final boolean _overwrite;
  private final int _permission;

  private final HttpClient _httpClient;

  public WebHdfsV1Client(String host, int port) {
    this(host, port, DEFAULT_PROTOCOL, DEFAULT_OVERWRITE, DEFAULT_PERMISSION);
  }

  public WebHdfsV1Client(String host, int port, String protocol, boolean overwrite, int permission) {
    _host = host;
    _port = port;
    _protocol = protocol;
    _overwrite = overwrite;
    _permission = permission;
    _httpClient = new HttpClient();
  }

  // This method is based on:
  // https://hadoop.apache.org/docs/r1.0.4/webhdfs.html#CREATE
  public synchronized boolean uploadSegment(String webHdfsPath, String localFilePath) {
    // Step 1: Submit a HTTP PUT request without automatically following
    // redirects and without sending the file data.
    String firstPutReqString = String.format(WEB_HDFS_UPLOAD_PATH_TEMPLATE, _protocol, _host, _port, webHdfsPath, _overwrite, _permission);
    HttpMethod firstPutReq = new PutMethod(firstPutReqString);
    try {
      LOGGER.info("Trying to send request: {}.", firstPutReqString);
      int firstResponseCode = _httpClient.executeMethod(firstPutReq);
      if (firstResponseCode != 307) {
        LOGGER.error(String.format("Failed to execute the first PUT request to upload segment to webhdfs: %s. "
                + "Expected response code 307, but get %s. Response body: %s", firstPutReqString, firstResponseCode,
            firstPutReq.getResponseBodyAsString()));
        return false;
      }
    } catch (Exception e) {
      LOGGER.error(String.format("Failed to execute the first request to upload segment to webhdfs: %s.", firstPutReqString), e);
      return false;
    } finally {
      firstPutReq.releaseConnection();
    }
    // Step 2: Submit another HTTP PUT request using the URL in the Location
    // header with the file data to be written.
    String redirectedReqString = firstPutReq.getResponseHeader(LOCATION).getValue();
    PutMethod redirectedReq = new PutMethod(redirectedReqString);
    File localFile = new File(localFilePath);
    RequestEntity requestEntity = new FileRequestEntity(localFile, "application/binary");
    redirectedReq.setRequestEntity(requestEntity);

    try {
      LOGGER.info("Trying to send request: {}.", redirectedReqString);
      int redirectedResponseCode = _httpClient.executeMethod(redirectedReq);
      if (redirectedResponseCode != 201) {
        LOGGER.error(String.format("Failed to execute the redirected PUT request to upload segment to webhdfs: %s. "
                + "Expected response code 201, but get %s. Response: %s", redirectedReqString, redirectedResponseCode,
            redirectedReq.getResponseBodyAsString()));
      }
      return true;
    } catch (IOException e) {
      LOGGER.error(String.format("Failed to execute the redirected request to upload segment to webhdfs: %s.", redirectedReqString), e);
      return false;
    } finally {
      redirectedReq.releaseConnection();
    }
  }

  public String getDownloadUriPath(String webHdfsPath) {
    return String.format(WEB_HDFS_DOWNLOAD_PATH_TEMPLATE, _protocol, _host, _port, webHdfsPath);
  }
}
