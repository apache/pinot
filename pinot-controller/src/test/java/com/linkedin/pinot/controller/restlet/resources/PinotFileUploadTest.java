package com.linkedin.pinot.controller.restlet.resources;

import com.linkedin.pinot.controller.helix.ControllerRequestBuilderUtil;
import com.linkedin.pinot.controller.helix.ControllerRequestURLBuilder;
import com.linkedin.pinot.controller.helix.ControllerTest;
import org.json.JSONObject;
import org.restlet.Client;
import org.restlet.Request;
import org.restlet.Response;
import org.restlet.data.MediaType;
import org.restlet.data.Method;
import org.restlet.data.Protocol;
import org.restlet.data.Status;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Tests for the file upload restlet.
 *
 * @author jfim
 */
public class PinotFileUploadTest extends ControllerTest {
  @Test
  public void testUploadBogusData() {
    Client client = new Client(Protocol.HTTP);
    Request request = new Request(Method.POST, ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forDataFileUpload());
    request.setEntity("blah", MediaType.MULTIPART_ALL);
    Response response = client.handle(request);

    Assert.assertEquals(response.getStatus(), Status.CLIENT_ERROR_UNPROCESSABLE_ENTITY);
  }

  @BeforeClass
  public void setUp() throws Exception {
    startController();

    ControllerRequestBuilderUtil.addFakeBrokerInstancesToAutoJoinHelixCluster(getHelixClusterName(), ZK_STR, 5);
    ControllerRequestBuilderUtil.addFakeDataInstancesToAutoJoinHelixCluster(getHelixClusterName(), ZK_STR, 5);

    final JSONObject payload = ControllerRequestBuilderUtil.buildCreateResourceJSON("mirror", 2, 2);
    sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forResourceCreate(),
        payload.toString(), MediaType.APPLICATION_JSON, Status.SUCCESS_OK);
  }

  private String sendPostRequest(String url, String payload, MediaType mediaType, Status expectedStatus) {
    Client client = new Client(Protocol.HTTP);
    Request request = new Request(Method.POST, url);
    request.setEntity(payload, mediaType);
    Response response = client.handle(request);

    Assert.assertEquals(response.getStatus(), expectedStatus);
    return response.getEntityAsText();
  }

  @AfterClass
  public void tearDown() throws Exception {
    stopController();
  }

  @Override
  protected String getHelixClusterName() {
    return "PinotFileUploadTest";
  }
}
