package com.linkedin.pinot.controller.restlet.resources;

import com.linkedin.pinot.controller.helix.ControllerRequestURLBuilder;
import com.linkedin.pinot.controller.helix.ControllerTest;
import java.io.IOException;
import org.json.JSONObject;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import static org.testng.Assert.*;


/**
 * Tests for the instances Restlet.
 */
public class PinotInstanceRestletResourceTest extends ControllerTest {
  @BeforeClass
  public void setUp() {
    startZk();
    startController();
  }

  @Test
  public void testInstanceListingAndCreation() throws Exception {
    ControllerRequestURLBuilder urlBuilder = ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL);

    // Check that there are no instances
    JSONObject instanceList =
        new JSONObject(sendGetRequest(urlBuilder.forInstanceList()));
    assertEquals(instanceList.getJSONArray("instances").length(), 0,
        "Expected empty instance list at beginning of test");

    // Create untagged broker and server instances
    JSONObject brokerInstance = new JSONObject("{\"host\":\"1.2.3.4\", \"type\":\"broker\", \"port\":\"1234\"}");
    sendPostRequest(urlBuilder.forInstanceCreate(), brokerInstance.toString());

    JSONObject serverInstance = new JSONObject("{\"host\":\"1.2.3.4\", \"type\":\"server\", \"port\":\"2345\"}");
    sendPostRequest(urlBuilder.forInstanceCreate(), serverInstance.toString());

    // Check that there are two instances
    instanceList = new JSONObject(sendGetRequest(urlBuilder.forInstanceList()));
    assertEquals(instanceList.getJSONArray("instances").length(), 2,
        "Expected two instances after creation of untagged instances");

    // Create tagged broker and server instances
    brokerInstance.put("tag", "someTag");
    brokerInstance.put("host", "2.3.4.5");
    sendPostRequest(urlBuilder.forInstanceCreate(), brokerInstance.toString());

    serverInstance.put("tag", "someTag");
    serverInstance.put("host", "2.3.4.5");
    sendPostRequest(urlBuilder.forInstanceCreate(), serverInstance.toString());

    // Check that there are four instances
    instanceList = new JSONObject(sendGetRequest(urlBuilder.forInstanceList()));
    assertEquals(instanceList.getJSONArray("instances").length(), 4,
        "Expected two instances after creation of tagged instances");

    // Create duplicate broker and server instances (both calls should fail)
    try {
      sendPostRequest(urlBuilder.forInstanceCreate(), brokerInstance.toString());
      fail("Duplicate broker instance creation did not fail");
    } catch (IOException e) {
      // Expected
    }

    try {
      sendPostRequest(urlBuilder.forInstanceCreate(), serverInstance.toString());
      fail("Duplicate server instance creation did not fail");
    } catch (IOException e) {
      // Expected
    }

    // Check that there are four instances
    instanceList = new JSONObject(sendGetRequest(urlBuilder.forInstanceList()));
    assertEquals(instanceList.getJSONArray("instances").length(), 4,
        "Expected two instances after creation of duplicate instances");
  }
}
