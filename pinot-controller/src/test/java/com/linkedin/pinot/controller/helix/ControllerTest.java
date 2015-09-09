/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;

import com.linkedin.pinot.core.trace.TraceContext;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.ControllerStarter;


/**
 * Base class for controller tests.
 *
 */
public abstract class ControllerTest {
  protected static boolean isTraceEnabled;
  private static final Logger LOGGER = LoggerFactory.getLogger(ControllerTest.class);
  protected String CONTROLLER_BASE_API_URL = "http://localhost:" + ControllerTestUtils.DEFAULT_CONTROLLER_API_PORT;
  protected String BROKER_BASE_API_URL = "http://localhost:18099";
  protected final String CONTROLLER_INSTANCE_NAME = "localhost_11984";
  protected ZkClient _zkClient;
  protected ControllerStarter _controllerStarter;
  protected HelixAdmin _helixAdmin;
  protected ZkHelixPropertyStore<ZNRecord> _propertyStore;
  protected HelixManager _helixZkManager;

  public JSONObject postQuery(String query, String brokerBaseApiUrl, boolean usePql2Compiler) throws Exception {
    final JSONObject json = new JSONObject();
    json.put("pql", query);
    json.put("trace", isTraceEnabled);

    if (usePql2Compiler) {
      json.put("dialect", "pql2");
    } else {
      json.put("dialect", "bql");
    }

    final long start = System.currentTimeMillis();
    final URLConnection conn = new URL(brokerBaseApiUrl + "/query").openConnection();
    conn.setDoOutput(true);
    final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(conn.getOutputStream(), "UTF-8"));
    final String reqStr = json.toString();
    System.out.println("reqStr = " + reqStr);

    writer.write(reqStr, 0, reqStr.length());
    writer.flush();
    final BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));

    final StringBuilder sb = new StringBuilder();
    String line = null;
    while ((line = reader.readLine()) != null) {
      sb.append(line);
    }

    final long stop = System.currentTimeMillis();

    LOGGER.info(" Time take for Request : " + query + " in ms:" + (stop - start));

    final String res = sb.toString();
    System.out.println("res = " + res);
    final JSONObject ret = new JSONObject(res);
    ret.put("totalTime", (stop - start));

    return ret;
  }

  public JSONObject postQuery(String query) throws Exception {
    return postQuery(query, BROKER_BASE_API_URL, true);
  }

  protected void startZk() {
    ZkStarter.startLocalZkServer();
  }

  protected void stopZk() {
    ZkStarter.stopLocalZkServer();
  }

  /**
   * Starts a controller instance.
   */
  protected void startController() {
    assert _controllerStarter == null;
    ControllerConf config = ControllerTestUtils.getDefaultControllerConfiguration();

    _zkClient = new ZkClient(ZkStarter.DEFAULT_ZK_STR);
    if (_zkClient.exists("/" + getHelixClusterName())) {
      _zkClient.deleteRecursive("/" + getHelixClusterName());
    }
    _controllerStarter = ControllerTestUtils.startController(getHelixClusterName(), ZkStarter.DEFAULT_ZK_STR, config);
    _helixAdmin = _controllerStarter.getHelixResourceManager().getHelixAdmin();
    _propertyStore = _controllerStarter.getHelixResourceManager().getPropertyStore();
  }

  /**
   * Stops an already started controller
   */
  protected void stopController() {
    assert _controllerStarter != null;
    ControllerTestUtils.stopController(_controllerStarter);
    _controllerStarter = null;
    _zkClient.close();
  }

  public static String sendDeleteRequest(String urlString) throws IOException {
    final long start = System.currentTimeMillis();

    final URL url = new URL(urlString);
    final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setDoOutput(true);
    conn.setRequestMethod("DELETE");
    conn.connect();

    final BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));

    final StringBuilder sb = new StringBuilder();
    String line = null;
    while ((line = reader.readLine()) != null) {
      sb.append(line);
    }

    final long stop = System.currentTimeMillis();

    LOGGER.info(" Time take for Request : " + urlString + " in ms:" + (stop - start));

    return sb.toString();
  }

  public static String sendPutRequest(String urlString, String payload) throws IOException {
    LOGGER.info("Sending PUT to " + urlString + " with payload " + payload);
    final long start = System.currentTimeMillis();
    final URL url = new URL(urlString);
    final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setDoOutput(true);
    conn.setRequestMethod("PUT");
    final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(conn.getOutputStream(), "UTF-8"));

    writer.write(payload, 0, payload.length());
    writer.flush();

    final BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
    final StringBuilder sb = new StringBuilder();
    String line = null;
    while ((line = reader.readLine()) != null) {
      sb.append(line);
    }

    final long stop = System.currentTimeMillis();

    LOGGER.info(" Time take for Request : " + urlString + " in ms:" + (stop - start));

    return sb.toString();
  }

  public static String sendPostRequest(String urlString, String payload) throws UnsupportedEncodingException,
      IOException, JSONException {
    LOGGER.info("Sending POST to " + urlString + " with payload " + payload);
    final long start = System.currentTimeMillis();
    final URL url = new URL(urlString);
    final URLConnection conn = url.openConnection();
    conn.setDoOutput(true);
    final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(conn.getOutputStream(), "UTF-8"));

    writer.write(payload, 0, payload.length());
    writer.flush();
    final BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));

    final StringBuilder sb = new StringBuilder();
    String line = null;
    while ((line = reader.readLine()) != null) {
      sb.append(line);
    }

    final long stop = System.currentTimeMillis();

    LOGGER.info(" Time take for Request : " + payload + " in ms:" + (stop - start));

    return sb.toString();
  }

  public static String sendGetRequest(String urlString) throws UnsupportedEncodingException, IOException, JSONException {
    BufferedReader reader = null;
    final URL url = new URL(urlString);
    reader = new BufferedReader(new InputStreamReader(url.openStream(), "UTF-8"));
    final StringBuilder queryResp = new StringBuilder();
    for (String respLine; (respLine = reader.readLine()) != null;) {
      queryResp.append(respLine);
    }
    return queryResp.toString();
  }

  protected abstract String getHelixClusterName();
}
