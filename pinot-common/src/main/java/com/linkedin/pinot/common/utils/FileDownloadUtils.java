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
package com.linkedin.pinot.common.utils;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.exception.HttpErrorStatusException;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;


public class FileDownloadUtils {
  private FileDownloadUtils() {
  }

  private static final CloseableHttpClient HTTP_CLIENT = HttpClientBuilder.create().build();

  /**
   * Download a file with uri.
   * @param fileUri Uri of the file.
   * @param dest File destination.
   * @return Response status code
   * @throws Exception
   */
  public static int downloadFile(String fileUri, File dest) throws Exception {
    HttpUriRequest request = RequestBuilder.get(fileUri).setVersion(HttpVersion.HTTP_1_1).build();
    try (CloseableHttpResponse response = HTTP_CLIENT.execute(request)) {
      int statusCode = response.getStatusLine().getStatusCode();
      if (statusCode >= 400) {
        throw new HttpErrorStatusException(getErrorMessage(fileUri, response), statusCode);
      }

      HttpEntity entity = response.getEntity();
      long contentLength = entity.getContentLength();
      try (InputStream inputStream = response.getEntity().getContent();
          OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(dest))) {
        IOUtils.copyLarge(inputStream, outputStream);
      }
      long fileLength = dest.length();
      Preconditions.checkState(fileLength == contentLength,
          String.format("File length: %d does not match content length: %d", fileLength, contentLength));

      return statusCode;
    }
  }

  private static String getErrorMessage(String fileUri, CloseableHttpResponse response) {
    String controllerHost = null;
    String controllerVersion = null;
    if (response.containsHeader(CommonConstants.Controller.HOST_HTTP_HEADER)) {
      controllerHost = response.getFirstHeader(CommonConstants.Controller.HOST_HTTP_HEADER).getValue();
      controllerVersion = response.getFirstHeader(CommonConstants.Controller.VERSION_HTTP_HEADER).getValue();
    }
    StatusLine statusLine = response.getStatusLine();
    String errorMessage = String.format("Got error status code: %d with reason: %s while downloading file with uri: %s",
        statusLine.getStatusCode(), statusLine.getReasonPhrase(), fileUri);
    if (controllerHost != null) {
      errorMessage =
          String.format("%s from controller: %s, version: %s", errorMessage, controllerHost, controllerVersion);
    }
    return errorMessage;
  }
}
