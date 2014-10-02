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

import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.ControllerStarter;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Sep 29, 2014
 */

public class ControllerSentinelTest {
  private static final String FAILURE_STATUS = "failure";
  private static final String SUCCESS_STATUS = "success";

  private static final Logger logger = Logger.getLogger(ControllerSentinelTest.class);

  private static final String ZK_STR = "localhost:2181";
  private static final String DATA_DIR = "/tmp";
  private static final String CONTROLLER_INSTANCE_NAME = "localhost_11984";
  private static final String CONTROLLER_API_PORT = "8099";
  private static final String CONTROLLER_BASE_API_URL = StringUtil.join(":", "http://localhost", CONTROLLER_API_PORT);
  private static final String HELIX_CLUSTER_NAME = "pinotHelixTestCluster";

  private static ZkClient zkClient = new ZkClient(ZK_STR);

  private static ControllerStarter starter;

  @BeforeClass
  public void setup() throws InterruptedException, JSONException, UnsupportedEncodingException, IOException {
    final ControllerConf conf = new ControllerConf();
    conf.setControllerHost(CONTROLLER_INSTANCE_NAME);
    conf.setControllerPort(CONTROLLER_API_PORT);
    conf.setDataDir(DATA_DIR);
    conf.setZkStr(ZK_STR);
    conf.setHelixClusterName(HELIX_CLUSTER_NAME);

    if (zkClient.exists("/" + HELIX_CLUSTER_NAME)) {
      zkClient.deleteRecursive("/" + HELIX_CLUSTER_NAME);
    }

    starter = new ControllerStarter(conf);
    starter.start();

    for (int i = 0; i < 20; i++) {
      final JSONObject payload =
          ControllerRequestBuilderUtil.buildInstanceCreateRequestJSON("localhost", String.valueOf(i), PinotHelixResourceManager.UNTAGGED);
      final String res =
          sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forInstanceCreate(), payload.toString());
      final JSONObject resJSON = new JSONObject(res);
      Assert.assertEquals(SUCCESS_STATUS, resJSON.getString("status"));
    }
  }

  @AfterClass
  public void tearDown() {
    starter.stop();
  }

  @Test
  public void testAddAlreadyAddedInstance() throws JSONException, UnsupportedEncodingException, IOException {
    final JSONObject payload =
        ControllerRequestBuilderUtil.buildInstanceCreateRequestJSON("localhost", String.valueOf(0), PinotHelixResourceManager.UNTAGGED);
    final String res =
        sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forInstanceCreate(), payload.toString());
    final JSONObject resJSON = new JSONObject(res);
    Assert.assertEquals(FAILURE_STATUS, resJSON.getString("status"));
    logger.info(resJSON.toString(1));
  }

  @Test
  public void testCreateResource() throws JSONException, UnsupportedEncodingException, IOException {
    final JSONObject payload = ControllerRequestBuilderUtil.buildCreateResourceJSON("testCreateResource", 2, 2);
    final String res =
        sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forResourceCreate(), payload.toString());
    System.out.println(res);
  }

  @Test
  public void testUpdateResource() throws JSONException, UnsupportedEncodingException, IOException {
    final JSONObject payload = ControllerRequestBuilderUtil.buildCreateResourceJSON("testUpdateResource", 2, 2);
    final String res =
        sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forResourceCreate(), payload.toString());
    System.out.println(res);
  }

  @Test
  public void testDeleteResource() throws JSONException, UnsupportedEncodingException, IOException {
    final JSONObject payload = ControllerRequestBuilderUtil.buildCreateResourceJSON("testDeleteResource", 2, 2);
    final String res =
        sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forResourceCreate(), payload.toString());
    System.out.println(res);
  }

  @Test
  public void testGetResource() throws JSONException, UnsupportedEncodingException, IOException {
    final JSONObject payload = ControllerRequestBuilderUtil.buildCreateResourceJSON("testGetResource", 2, 2);
    final String res =
        sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forResourceCreate(), payload.toString());
    final String getResponse =
        senGetRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forResourceGet("testGetResource"));

    System.out.println(res);
    final JSONObject getResJSON = new JSONObject(getResponse);
    Assert.assertEquals("testGetResource", getResJSON.getString("resourceName"));
  }

  public static String sendDeleteReques(String urlString) throws IOException {
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

    logger.info(" Time take for Request : " + urlString + " in ms:" + (stop - start));

    return sb.toString();
  }

  public static String sendPutRequest(String urlString, String payload) throws IOException {
    final long start = System.currentTimeMillis();
    final URL url = new URL(urlString);
    final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setDoOutput(true);
    conn.setRequestMethod("PUT");
    final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(conn.getOutputStream(), "UTF-8"));
    final String reqStr = payload.toString();

    writer.write(reqStr, 0, reqStr.length());
    writer.flush();

    final BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
    final StringBuilder sb = new StringBuilder();
    String line = null;
    while ((line = reader.readLine()) != null) {
      sb.append(line);
    }

    final long stop = System.currentTimeMillis();

    logger.info(" Time take for Request : " + urlString + " in ms:" + (stop - start));

    return sb.toString();
  }

  public static String sendPostRequest(String urlString, String payload) throws UnsupportedEncodingException, IOException, JSONException {
    final long start = System.currentTimeMillis();
    final URL url = new URL(urlString);
    final URLConnection conn = url.openConnection();
    conn.setDoOutput(true);
    final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(conn.getOutputStream(), "UTF-8"));
    final String reqStr = payload.toString();

    writer.write(reqStr, 0, reqStr.length());
    writer.flush();
    final BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));

    final StringBuilder sb = new StringBuilder();
    String line = null;
    while ((line = reader.readLine()) != null) {
      sb.append(line);
    }

    final long stop = System.currentTimeMillis();

    logger.info(" Time take for Request : " + payload.toString() + " in ms:" + (stop - start));

    final String res = sb.toString();

    return res;
  }

  public static String senGetRequest(String urlString) throws UnsupportedEncodingException, IOException, JSONException {
    BufferedReader reader = null;
    final URL url = new URL(urlString);
    reader = new BufferedReader(new InputStreamReader(url.openStream(), "UTF-8"));
    final StringBuilder queryResp = new StringBuilder();
    for (String respLine; (respLine = reader.readLine()) != null;) {
      queryResp.append(respLine);
    }
    return queryResp.toString();
  }

}
