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
package com.linkedin.pinot.server.utils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.servlet.ServletException;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpVersion;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.multipart.FilePart;
import org.apache.commons.httpclient.methods.multipart.MultipartRequestEntity;
import org.apache.commons.httpclient.methods.multipart.Part;
import org.apache.commons.httpclient.methods.multipart.PartSource;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.io.IOUtils;


/**
 * To upload via command line use
 * curl -F segmentId2=@./part-r-0001.tar.gz localhost:8089/files/
 * 
 */
public class FileUploadUtils {
  public static void sendFile(final String host, final String port, final String fileName,
      final InputStream inputStream, final long lengthInBytes) {
    HttpClient client = new HttpClient();
    try {

      client.getParams().setParameter("http.protocol.version", HttpVersion.HTTP_1_1);
      PostMethod post = new PostMethod("http://" + host + ":" + port + "/files/");
      Part[] parts = { new FilePart(fileName, new PartSource() {
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
      }) };
      post.setRequestEntity(new MultipartRequestEntity(parts, new HttpMethodParams()));

      client.executeMethod(post);
      if (post.getStatusCode() >= 400) {
        String errorString = "POST Status Code: " + post.getStatusCode() + "\n";
        if (post.getResponseHeader("Error") != null) {
          errorString += "ServletException: " + post.getResponseHeader("Error").getValue();
        }
        throw new ServletException(errorString);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {

    }
  }

  public static String listFiles(String host, String port) {
    try {
      HttpClient httpClient = new HttpClient();
      GetMethod httpget = new GetMethod("http://" + host + ":" + port + "/files/");
      httpClient.executeMethod(httpget);
      return IOUtils.toString(httpget.getResponseBodyAsStream());
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public static String getStringResponse(String url) {
    try {
      HttpClient httpClient = new HttpClient();
      GetMethod httpget = new GetMethod(url);
      httpClient.executeMethod(httpget);
      InputStream responseBodyAsStream = httpget.getResponseBodyAsStream();
      String ret = IOUtils.toString(responseBodyAsStream);
      IOUtils.closeQuietly(responseBodyAsStream);
      return ret;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public static long getFile(String host, String port, String remoteFileName, File file) {
    try {
      HttpClient httpClient = new HttpClient();
      GetMethod httpget = new GetMethod("http://" + host + ":" + port + "/files/" + remoteFileName);
      httpClient.executeMethod(httpget);
      long ret = httpget.getResponseContentLength();
      if (file.getParentFile() != null && !file.getParentFile().exists()) {
        file.getParentFile().mkdirs();
      }
      BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(file));
      IOUtils.copyLarge(httpget.getResponseBodyAsStream(), output);
      IOUtils.closeQuietly(output);

      return ret;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public static long getFile(String url, File file) {
    try {
      HttpClient httpClient = new HttpClient();
      GetMethod httpget = new GetMethod(url);
      httpClient.executeMethod(httpget);
      long ret = httpget.getResponseContentLength();
      BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(file));
      IOUtils.copyLarge(httpget.getResponseBodyAsStream(), output);
      IOUtils.closeQuietly(output);

      return ret;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public static int deleteFile(final String host, final String port, final String fileName) throws HttpException,
      IOException {
    String url = "http://" + host + ":" + port + "/files/" + fileName;
    HttpClient httpClient = new HttpClient();
    httpClient.getParams().setParameter("http.protocol.version", HttpVersion.HTTP_1_1);
    HttpMethod method = new DeleteMethod(url);
    return httpClient.executeMethod(method);
  }

  public static int deleteFile(final String url) throws HttpException, IOException {
    HttpClient httpClient = new HttpClient();
    httpClient.getParams().setParameter("http.protocol.version", HttpVersion.HTTP_1_1);
    HttpMethod method = new DeleteMethod(url);
    int responseCode = httpClient.executeMethod(method);
    if (responseCode >= 400) {
      String errorString = "Response Code: " + responseCode + "\n";
      throw new HttpException(errorString);
    }
    return responseCode;
  }

  public static void main(String[] args) throws Exception {
    /*
     * ControllerHttpHolder jettyServerHolder = new ControllerHttpHolder();
     * jettyServerHolder.setPort(8088);
     */
    /*
     * String directory = "/tmp/fileUpload";
     * FileUtils.deleteDirectory(new File(directory));
     * new File(directory).mkdirs();
     */
    /*
     * jettyServerHolder.setDirectoryPath(directory);
     * jettyServerHolder.start();
     */
    String path = "/tmp/ba-index/node1/shard0/part-r-0001.tar.gz";
    FileUploadUtils.sendFile("localhost", "8089", "part-r-000016", new FileInputStream(path), new File(path).length());
    String stringResponse = FileUploadUtils.listFiles("localhost", "8089");
    System.out.println(stringResponse);
  }
}
