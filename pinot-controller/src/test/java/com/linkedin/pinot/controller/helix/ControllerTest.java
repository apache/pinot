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
package com.linkedin.pinot.controller.helix;

import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.ControllerStarter;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.multipart.MultipartRequestEntity;
import org.apache.commons.httpclient.methods.multipart.Part;
import org.apache.commons.httpclient.methods.multipart.StringPart;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.testng.Assert;


/**
 * Base class for controller tests.
 */
public abstract class ControllerTest {
  public static final String LOCAL_HOST = "localhost";

  private static final int DEFAULT_CONTROLLER_PORT = 8998;
  private static final String DEFAULT_DATA_DIR =
      FileUtils.getTempDirectoryPath() + File.separator + "test-controller-" + System.currentTimeMillis();

  protected int _controllerPort;
  protected String _controllerBaseApiUrl;
  protected ControllerRequestURLBuilder _controllerRequestURLBuilder;
  protected String _controllerDataDir;

  protected ZkClient _zkClient;
  protected ControllerStarter _controllerStarter;
  protected PinotHelixResourceManager _helixResourceManager;
  protected HelixManager _helixManager;
  protected HelixAdmin _helixAdmin;
  protected ZkHelixPropertyStore<ZNRecord> _propertyStore;

  private ZkStarter.ZookeeperInstance _zookeeperInstance;

  protected String getHelixClusterName() {
    return getClass().getSimpleName();
  }

  protected void startZk() {
    _zookeeperInstance = ZkStarter.startLocalZkServer();
  }

  protected void startZk(int port) {
    _zookeeperInstance = ZkStarter.startLocalZkServer(port);
  }

  protected void stopZk() {
    try {
      ZkStarter.stopLocalZkServer(_zookeeperInstance);
    } catch (Exception e) {
      // Swallow exceptions
    }
  }

  public static ControllerConf getDefaultControllerConfiguration() {
    ControllerConf config = new ControllerConf();
    config.setControllerHost(LOCAL_HOST);
    config.setControllerPort(Integer.toString(DEFAULT_CONTROLLER_PORT));
    config.setDataDir(DEFAULT_DATA_DIR);
    config.setZkStr(ZkStarter.DEFAULT_ZK_STR);
    return config;
  }

  protected void startController() {
    startController(getDefaultControllerConfiguration());
  }

  protected void startController(ControllerConf config) {
    Assert.assertNotNull(config);
    Assert.assertNull(_controllerStarter);

    _controllerPort = Integer.valueOf(config.getControllerPort());
    _controllerBaseApiUrl = "http://localhost:" + _controllerPort;
    _controllerRequestURLBuilder = ControllerRequestURLBuilder.baseUrl(_controllerBaseApiUrl);
    _controllerDataDir = config.getDataDir();

    String helixClusterName = getHelixClusterName();
    config.setHelixClusterName(helixClusterName);

    String zkStr = config.getZkStr();
    _zkClient = new ZkClient(zkStr);
    if (_zkClient.exists("/" + helixClusterName)) {
      _zkClient.deleteRecursive("/" + helixClusterName);
    }

    _controllerStarter = new ControllerStarter(config);
    _controllerStarter.start();

    _helixResourceManager = _controllerStarter.getHelixResourceManager();
    _helixManager = _helixResourceManager.getHelixZkManager();
    _helixAdmin = _helixResourceManager.getHelixAdmin();
    _propertyStore = _helixResourceManager.getPropertyStore();
  }

  protected void stopController() {
    Assert.assertNotNull(_controllerStarter);

    _controllerStarter.stop();
    _controllerStarter = null;
    FileUtils.deleteQuietly(new File(_controllerDataDir));
    _zkClient.close();
  }

  public static String sendGetRequest(String urlString) throws IOException {
    return constructResponse(new URL(urlString).openStream());
  }

  public static String sendPostRequest(String urlString, String payload) throws IOException {
    HttpURLConnection httpConnection = (HttpURLConnection) new URL(urlString).openConnection();
    httpConnection.setRequestMethod("POST");

    if (payload != null && !payload.isEmpty()) {
      httpConnection.setDoOutput(true);
      try (BufferedWriter writer = new BufferedWriter(
          new OutputStreamWriter(httpConnection.getOutputStream(), "UTF-8"))) {
        writer.write(payload, 0, payload.length());
        writer.flush();
      }
    }

    return constructResponse(httpConnection.getInputStream());
  }

  public static String sendPutRequest(String urlString, String payload) throws IOException {
    HttpURLConnection httpConnection = (HttpURLConnection) new URL(urlString).openConnection();
    httpConnection.setDoOutput(true);
    httpConnection.setRequestMethod("PUT");

    try (
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(httpConnection.getOutputStream(), "UTF-8"))) {
      writer.write(payload);
      writer.flush();
    }

    return constructResponse(httpConnection.getInputStream());
  }

  public static String sendDeleteRequest(String urlString) throws IOException {
    HttpURLConnection httpConnection = (HttpURLConnection) new URL(urlString).openConnection();
    httpConnection.setRequestMethod("DELETE");
    httpConnection.connect();

    return constructResponse(httpConnection.getInputStream());
  }

  private static String constructResponse(InputStream inputStream) throws IOException {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"))) {
      StringBuilder responseBuilder = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        responseBuilder.append(line);
      }
      return responseBuilder.toString();
    }
  }

  public static PostMethod sendMultipartPostRequest(String url, String body) throws IOException {
    HttpClient httpClient = new HttpClient();
    PostMethod postMethod = new PostMethod(url);
    // our handlers ignore key...so we can put anything here
    Part[] parts = {new StringPart("body", body)};
    postMethod.setRequestEntity(new MultipartRequestEntity(parts, postMethod.getParams()));
    httpClient.executeMethod(postMethod);
    return postMethod;
  }

  public static PutMethod sendMultipartPutRequest(String url, String body) throws IOException {
    HttpClient httpClient = new HttpClient();
    PutMethod putMethod = new PutMethod(url);
    // our handlers ignore key...so we can put anything here
    Part[] parts = {new StringPart("body", body)};
    putMethod.setRequestEntity(new MultipartRequestEntity(parts, putMethod.getParams()));
    httpClient.executeMethod(putMethod);
    return putMethod;
  }
}
