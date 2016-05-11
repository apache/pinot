/*******************************************************************************
 * © [2013] LinkedIn Corp. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.linkedin.pinot.common.utils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpVersion;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.EntityEnclosingMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.methods.multipart.FilePart;
import org.apache.commons.httpclient.methods.multipart.MultipartRequestEntity;
import org.apache.commons.httpclient.methods.multipart.Part;
import org.apache.commons.httpclient.methods.multipart.PartSource;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.io.IOUtils;
import org.apache.http.entity.ContentType;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.Utils;

public class FileUploadUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileUploadUtils.class);
  private static final String SEGMENTS_PATH = "segments";
  public static final String UPLOAD_TYPE = "UPLOAD_TYPE";
  public static final String DOWNLOAD_URI = "DOWNLOAD_URI";
  private static final MultiThreadedHttpConnectionManager CONNECTION_MANAGER =
      new MultiThreadedHttpConnectionManager();
  private static final HttpClient FILE_UPLOAD_HTTP_CLIENT = new HttpClient(CONNECTION_MANAGER);

  {
    FILE_UPLOAD_HTTP_CLIENT.getParams().setParameter("http.protocol.version", HttpVersion.HTTP_1_1);
    FILE_UPLOAD_HTTP_CLIENT.getParams().setSoTimeout(3600 * 1000); // One hour
  }

  public enum SendFileMethod {
    POST {
      public EntityEnclosingMethod forUri(String uri) {
        return new PostMethod(uri);
      }
    },
    PUT {
      public EntityEnclosingMethod forUri(String uri) {
        return new PutMethod(uri);
      }
    };

    public abstract EntityEnclosingMethod forUri(String uri);
  }

  public static int sendFile(final String host, final String port, final String path, final String fileName,
      final InputStream inputStream, final long lengthInBytes, SendFileMethod httpMethod) {
    EntityEnclosingMethod method = null;
    try {
      method = httpMethod.forUri("http://" + host + ":" + port + "/" + path);
      Part[] parts = {
          new FilePart(fileName, new PartSource() {
            @Override
            public long getLength() {
              return lengthInBytes;
            }

            @Override
            public String getFileName() {
              return "fileName";
            }

            @Override
            public InputStream createInputStream() throws IOException {
              return new BufferedInputStream(inputStream);
            }
          })
      };
      method.setRequestEntity(new MultipartRequestEntity(parts, new HttpMethodParams()));
      FILE_UPLOAD_HTTP_CLIENT.executeMethod(method);
      if (method.getStatusCode() >= 400) {
        String errorString = "POST Status Code: " + method.getStatusCode() + "\n";
        if (method.getResponseHeader("Error") != null) {
          errorString += "ServletException: " + method.getResponseHeader("Error").getValue();
        }
        throw new HttpException(errorString);
      }
      return method.getStatusCode();
    } catch (Exception e) {
      LOGGER.error("Caught exception while sending file: {}", fileName, e);
      Utils.rethrowException(e);
      throw new AssertionError("Should not reach this");
    } finally {
      if (method != null) {
        method.releaseConnection();
      }
    }
  }

  public static int sendSegmentFile(final String host, final String port, final String fileName,
      final InputStream inputStream, final long lengthInBytes) {
    return sendFile(host, port, SEGMENTS_PATH, fileName, inputStream, lengthInBytes, SendFileMethod.POST);
  }


  public static int sendSegmentUri(final String host, final String port, final String uri) {
    SendFileMethod httpMethod = SendFileMethod.POST;
    EntityEnclosingMethod method = null;
    try {
      method = httpMethod.forUri("http://" + host + ":" + port + "/" + SEGMENTS_PATH);
      method.setRequestHeader(UPLOAD_TYPE, FileUploadType.URI.toString());
      method.setRequestHeader(DOWNLOAD_URI, uri);
      FILE_UPLOAD_HTTP_CLIENT.executeMethod(method);
      if (method.getStatusCode() >= 400) {
        String errorString = "POST Status Code: " + method.getStatusCode() + "\n";
        if (method.getResponseHeader("Error") != null) {
          errorString += "ServletException: " + method.getResponseHeader("Error").getValue();
        }
        throw new HttpException(errorString);
      }
      return method.getStatusCode();
    } catch (Exception e) {
      LOGGER.error("Caught exception while sending uri: {}", uri, e);
      Utils.rethrowException(e);
      throw new AssertionError("Should not reach this");
    } finally {
      if (method != null) {
        method.releaseConnection();
      }
    }
  }

  public static int sendSegmentJson(final String host, final String port, final JSONObject segmentJson) {
    PostMethod postMethod = null;
    try {
      RequestEntity requestEntity = new StringRequestEntity(
          segmentJson.toString(),
          ContentType.APPLICATION_JSON.getMimeType(),
          ContentType.APPLICATION_JSON.getCharset().name());
      postMethod = new PostMethod("http://" + host + ":" + port + "/" + SEGMENTS_PATH);
      postMethod.setRequestEntity(requestEntity);
      postMethod.setRequestHeader(UPLOAD_TYPE, FileUploadType.JSON.toString());
      int statusCode = FILE_UPLOAD_HTTP_CLIENT.executeMethod(postMethod);
      if (statusCode >= 400) {
        String errorString = "POST Status Code: " + statusCode + "\n";
        if (postMethod.getResponseHeader("Error") != null) {
          errorString += "ServletException: " + postMethod.getResponseHeader("Error").getValue();
        }
        throw new HttpException(errorString);
      }
      return statusCode;
    } catch (Exception e) {
      LOGGER.error("Caught exception while sending json: {}", segmentJson.toString(), e);
      Utils.rethrowException(e);
      throw new AssertionError("Should not reach this");
    } finally {
      if (postMethod != null) {
        postMethod.releaseConnection();
      }
    }
  }

  public static long getFile(String url, File file) throws Exception {
    GetMethod httpget = null;
    try {
      httpget = new GetMethod(url);
      int responseCode = FILE_UPLOAD_HTTP_CLIENT.executeMethod(httpget);
      if (responseCode >= 400) {
        long contentLength = httpget.getResponseContentLength();
        if (contentLength > 0) {
          InputStream responseBodyAsStream = httpget.getResponseBodyAsStream();
          // don't read more than 1000 bytes
          byte[] buffer = new byte[(int) Math.min(contentLength, 1000)];
          responseBodyAsStream.read(buffer);
          LOGGER.error("Error response from url:{} \n {}", url, new String(buffer));
        }
        throw new RuntimeException(
            "Received error response from server while downloading file. url:" + url
                + " response code:" + responseCode);
      } else {
        long ret = httpget.getResponseContentLength();
        BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(file));
        IOUtils.copyLarge(httpget.getResponseBodyAsStream(), output);
        IOUtils.closeQuietly(output);
        return ret;
      }
    } catch (Exception ex) {
      LOGGER.error("Caught exception", ex);
      throw ex;
    } finally {
      if (httpget != null) {
        httpget.releaseConnection();
      }
    }
  }

  public enum FileUploadType {
    URI,
    JSON,
    TAR; // Default value

    public FileUploadType valueOf(Object o) {
      if (o != null) {
        String ostring = o.toString();
        for (FileUploadType u: FileUploadType.values()) {
          if (ostring.equalsIgnoreCase(u.toString())) {
            return u;
          }
        }
      }
      return getDefaultUploadType();
    }

    public static FileUploadType getDefaultUploadType() {
      return TAR;
    }
  }
}
