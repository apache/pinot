package org.apache.pinot.controller.api;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.controller.ControllerTestUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class ZookeeperResourceTest {

  @BeforeClass
  public void setUp() throws Exception {
    ControllerTestUtils.setupClusterAndValidate();
  }

  @Test
  public void testZkPutDataWithLargePayload() {
    String url = ControllerTestUtils.getControllerRequestURLBuilder().forZkPut();
    String path = "/zookeeper";
    String _data = "{\"id\" : \"QuickStartCluster\"," + "  \"simpleFields\" : {\n"
        + "    \"allowParticipantAutoJoin\" : \"true\",\n" + "    \"default.hyperloglog.log2m\" : \"8\",\n"
        + "    \"enable.case.insensitive\" : \"true\",\n"
        + "    \"pinot.broker.enable.query.limit.override\" : \"false\"\n" + "  },\n" + "  \"mapFields\" : { },\n"
        + "  \"listFields\" : { }\n" + "}";
    String data = "";
    int expectedVersion = -1;
    int accessOption = 1;

    // CASE 1: Send data in query params form using HTTP PUT
    try {
      String params = "path=" + URLEncoder.encode(path, StandardCharsets.UTF_8) +
          "&data=" + URLEncoder.encode(data, StandardCharsets.UTF_8) +
          "&expectedVersion=" + expectedVersion +
          "&accessOption=" + accessOption;

      String result = ControllerTestUtils.sendPutRequest(url + "?" + params);
      Assert.assertTrue(result.toLowerCase().contains("successfully updated"));
    } catch (IOException e) {
      // Should not get here
      Assert.fail("Update should be done successfully");
    }

    String lorem = "Loremipsumdolorsitametconsecteturadipisicingelitseddoeiusmod"
        + "temporincididuntutlaboreetdoloremagnaaliquaUtenimadminimveniam"
        + "quisnostrudexercitationullamcolaborisnisiutaliquipexeacommodo"
        + "consequatDuisauteiruredolorinreprehenderitinvoluptatevelitesse"
        + "cillumdoloreeufugiatnullapariaturExcepteursintoccaecatcupidatatnon"
        + "proidentsuntinculpaquiofficiadeseruntmollitanimidestlaborum";
    // make the content even more larger
    for (int i = 0; i < 5; i++) {
      lorem += lorem;
    }

    String largeConfig = "{\n" + "  \"id\" : \"QuickStartCluster\",\n" + "  \"simpleFields\" : {\n"
        + "    \"allowParticipantAutoJoin\" : \"true\",\n" + "    \"default.hyperloglog.log2m\" : \"8\",\n"
        + "    \"enable.case.insensitive\" : \"true\",\n"
        + "    \"pinot.broker.enable.query.limit.override\" : \"false\"\n" + "  },\n" + "  \"mapFields\" : { },\n"
        + "  \"lorem\" : " + "\"" + lorem + "\"\n" + "}";
    String largeData = URLEncoder.encode(largeConfig, StandardCharsets.UTF_8);

    // CASE 2: Fail when sending large data in query params
    boolean isSuccessful = false;
    try {
      String params = "path=" + URLEncoder.encode(path, StandardCharsets.UTF_8) +
          "&data=" + URLEncoder.encode(largeData, StandardCharsets.UTF_8) +
          "&expectedVersion=" + expectedVersion +
          "&accessOption=" + accessOption;
      ControllerTestUtils.sendPutRequest(url + "?" + params);

      isSuccessful = true;
      Assert.fail("Should not get here, large payload");
    } catch (IOException e) {
      Assert.assertFalse(isSuccessful);
    }

    // CASE 3: Send large content data should return success
    try {
      ObjectNode jsonPayload = JsonUtils.newObjectNode();
      jsonPayload.put("path", path);
      jsonPayload.put("data", largeConfig);
      jsonPayload.put("expectedVersion", expectedVersion);
      jsonPayload.put("accessOption", accessOption);

      Map<String, String> headers = new HashMap<>();
      headers.put("Content-Type", "application/json");

      String result = ControllerTestUtils.sendPutRequest(url, headers, jsonPayload.toString());
      Assert.assertTrue(result.toLowerCase().contains("successfully updated"));
    } catch (IOException e) {
      // Should not get here
      Assert.fail("request should be executed successfully");
    }
  }
}
