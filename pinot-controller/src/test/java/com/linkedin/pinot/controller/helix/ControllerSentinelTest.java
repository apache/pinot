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

import org.apache.helix.manager.zk.ZkClient;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.controller.helix.core.HelixHelper;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Sep 29, 2014
 */

public class ControllerSentinelTest extends ControllerTest {
  private static final String FAILURE_STATUS = "failure";
  private static final String SUCCESS_STATUS = "success";

  private static final Logger logger = Logger.getLogger(ControllerSentinelTest.class);

  private static final String HELIX_CLUSTER_NAME = "ControllerSentinelTest";
  static final ZkClient _zkClient = new ZkClient("localhost:2181");

  @BeforeClass
  public void setup() throws Exception {
    startController();

    ControllerRequestBuilderUtil.addFakeBrokerInstancesToAutoJoinHelixCluster(HELIX_CLUSTER_NAME, ZK_STR, 20);
    ControllerRequestBuilderUtil.addFakeDataInstancesToAutoJoinHelixCluster(HELIX_CLUSTER_NAME, ZK_STR, 20);
  }

  @AfterClass
  public void tearDown() {
    stopController();
    if (_zkClient.exists("/ControllerSentinelTest")) {
      _zkClient.deleteRecursive("/ControllerSentinelTest");
    }
    _zkClient.close();
  }

  @Test
  public void testAddAlreadyAddedInstance() throws JSONException, UnsupportedEncodingException, IOException {
    for (int i = 0; i < 20; i++) {
      final JSONObject payload =
          ControllerRequestBuilderUtil.buildInstanceCreateRequestJSON("localhost", String.valueOf(i),
              CommonConstants.Helix.UNTAGGED_SERVER_INSTANCE);
      final String res =
          sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forInstanceCreate(),
              payload.toString());
      final JSONObject resJSON = new JSONObject(res);
      Assert.assertEquals(SUCCESS_STATUS, resJSON.getString("status"));
    }
  }

  @Test
  public void testCreateResource() throws JSONException, UnsupportedEncodingException, IOException {
    final JSONObject payload = ControllerRequestBuilderUtil.buildCreateResourceJSON("mirror", 2, 2);
    final String res =
        sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forResourceCreate(),
            payload.toString());
    System.out.println(res);
    Assert.assertEquals(SUCCESS_STATUS, new JSONObject(res).getString("status"));
    System.out.println(res);
  }

  @Test
  public void testUpdateResource() throws JSONException, UnsupportedEncodingException, IOException {
    final JSONObject payload = ControllerRequestBuilderUtil.buildCreateResourceJSON("testUpdateResource", 2, 2);
    final String res =
        sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forResourceCreate(),
            payload.toString());
    System.out.println(res);
  }

  @Test
  public void testClusterExpansionResource() throws JSONException, UnsupportedEncodingException, IOException {
    String tag = "testExpansionResource";
    JSONObject payload = ControllerRequestBuilderUtil.buildCreateResourceJSON(tag, 2, 2);
    String res =
        sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forResourceCreate(),
            payload.toString());
    System.out.println(res);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "testExpansionResource_O").size(), 2);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_" + tag).size(), 1);
    Assert
        .assertEquals(
            Integer.parseInt(HelixHelper.getResourceConfigsFor(HELIX_CLUSTER_NAME, "testExpansionResource_O", _helixAdmin).get(
                "numberOfCopies")), 2);
    Assert.assertEquals(Integer.parseInt(
        HelixHelper.getResourceConfigsFor(HELIX_CLUSTER_NAME, "testExpansionResource_O", _helixAdmin).get("numberOfDataInstances")), 2);
    Assert.assertEquals(Integer.parseInt(
        HelixHelper.getResourceConfigsFor(HELIX_CLUSTER_NAME, "testExpansionResource_O", _helixAdmin).get("numberOfBrokerInstances")), 1);

    // Update DataResource
    payload = ControllerRequestBuilderUtil.buildUpdateDataResourceJSON("testExpansionResource", 4, 3);
    res = sendPutRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forResourceCreate(),
        payload.toString());
    System.out.println(res);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "testExpansionResource_O").size(), 4);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_" + tag).size(), 1);
    Assert.assertEquals(Integer.parseInt(
        HelixHelper.getResourceConfigsFor(HELIX_CLUSTER_NAME, "testExpansionResource_O", _helixAdmin).get("numberOfCopies")),
        3);
    Assert.assertEquals(Integer.parseInt(
        HelixHelper.getResourceConfigsFor(HELIX_CLUSTER_NAME, "testExpansionResource_O", _helixAdmin).get("numberOfDataInstances")), 4);
    Assert.assertEquals(Integer.parseInt(
        HelixHelper.getResourceConfigsFor(HELIX_CLUSTER_NAME, "testExpansionResource_O", _helixAdmin).get("numberOfBrokerInstances")), 1);

    //Update DataResource
    payload = ControllerRequestBuilderUtil.buildUpdateDataResourceJSON("testExpansionResource", 6, 5);
    res = sendPutRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forResourceCreate(),
        payload.toString());
    System.out.println(res);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "testExpansionResource_O").size(), 6);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_" + tag).size(), 1);
    Assert.assertEquals(Integer.parseInt(
        HelixHelper.getResourceConfigsFor(HELIX_CLUSTER_NAME, "testExpansionResource_O", _helixAdmin).get("numberOfCopies")),
        5);
    Assert.assertEquals(Integer.parseInt(
        HelixHelper.getResourceConfigsFor(HELIX_CLUSTER_NAME, "testExpansionResource_O", _helixAdmin).get("numberOfDataInstances")), 6);
    Assert.assertEquals(Integer.parseInt(
        HelixHelper.getResourceConfigsFor(HELIX_CLUSTER_NAME, "testExpansionResource_O", _helixAdmin).get("numberOfBrokerInstances")), 1);

    // Update BrokerResource
    payload = ControllerRequestBuilderUtil.buildUpdateBrokerResourceJSON("testExpansionResource", 2);
    res = sendPutRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forResourceCreate(),
        payload.toString());
    System.out.println(res);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "testExpansionResource_O").size(), 6);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_" + tag).size(), 2);
    Assert.assertEquals(Integer.parseInt(
        HelixHelper.getResourceConfigsFor(HELIX_CLUSTER_NAME, "testExpansionResource_O", _helixAdmin).get("numberOfCopies")),
        5);
    Assert.assertEquals(Integer.parseInt(
        HelixHelper.getResourceConfigsFor(HELIX_CLUSTER_NAME, "testExpansionResource_O", _helixAdmin).get("numberOfDataInstances")), 6);
    Assert.assertEquals(Integer.parseInt(
        HelixHelper.getResourceConfigsFor(HELIX_CLUSTER_NAME, "testExpansionResource_O", _helixAdmin).get("numberOfBrokerInstances")), 2);

    // Update BrokerResource
    payload = ControllerRequestBuilderUtil.buildUpdateBrokerResourceJSON("testExpansionResource", 4);
    res = sendPutRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forResourceCreate(),
        payload.toString());
    System.out.println(res);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "testExpansionResource_O").size(), 6);
    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "broker_" + tag).size(), 4);
    Assert.assertEquals(Integer.parseInt(
        HelixHelper.getResourceConfigsFor(HELIX_CLUSTER_NAME, "testExpansionResource_O", _helixAdmin).get("numberOfCopies")),
        5);
    Assert.assertEquals(Integer.parseInt(
        HelixHelper.getResourceConfigsFor(HELIX_CLUSTER_NAME, "testExpansionResource_O", _helixAdmin).get("numberOfDataInstances")), 6);
    Assert.assertEquals(Integer.parseInt(
        HelixHelper.getResourceConfigsFor(HELIX_CLUSTER_NAME, "testExpansionResource_O", _helixAdmin).get("numberOfBrokerInstances")), 4);
  }

  @Test
  public void testDeleteResource() throws JSONException, UnsupportedEncodingException, IOException {
    final JSONObject payload = ControllerRequestBuilderUtil.buildCreateResourceJSON("testDeleteResource", 2, 2);
    final String res =
        sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forResourceCreate(),
            payload.toString());
    Assert.assertEquals(SUCCESS_STATUS, new JSONObject(res).getString("status"));
    final String deleteRes =
        sendDeleteReques(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forResourceDelete(
            "testDeleteResource_O"));
    final JSONObject resJSON = new JSONObject(deleteRes);
    Assert.assertEquals(SUCCESS_STATUS, resJSON.getString("status"));
  }

  @Test
  public void testGetResource() throws JSONException, UnsupportedEncodingException, IOException {
    final JSONObject payload = ControllerRequestBuilderUtil.buildCreateResourceJSON("testGetResource", 2, 2);
    final String res =
        sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forResourceCreate(),
            payload.toString());
    System.out.println("**************");
    System.out.println(res);
    System.out.println("**************");
    final String getResponse =
        senGetRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forResourceGet("testGetResource_O"));
    System.out.println("**************");
    System.out.println(getResponse);
    System.out.println("**************");
    final JSONObject getResJSON = new JSONObject(getResponse);
    Assert.assertEquals("testGetResource", getResJSON.getString(CommonConstants.Helix.DataSource.RESOURCE_NAME));
    final String getAllResponse =
        senGetRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forResourceGet(null));
    System.out.println("**************");
    System.out.println(getAllResponse);
    System.out.println("**************");
    Assert.assertEquals(getAllResponse.contains("testGetResource_O"), true);
    Assert.assertEquals(getAllResponse.contains(CommonConstants.Helix.BROKER_RESOURCE_INSTANCE), true);
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

  public static String sendPostRequest(String urlString, String payload) throws UnsupportedEncodingException,
      IOException, JSONException {
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

  public static void main(String[] args) throws Exception {
    final JSONObject payload = ControllerRequestBuilderUtil.buildCreateResourceJSON("mirror", 1, 1);
    final String res =
        sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forResourceCreate(),
            payload.toString());
    System.out.println(res);
    Assert.assertEquals(SUCCESS_STATUS, new JSONObject(res).getString("status"));
    System.out.println(res);
  }

  @Override
  protected String getHelixClusterName() {
    return HELIX_CLUSTER_NAME;
  }
}
