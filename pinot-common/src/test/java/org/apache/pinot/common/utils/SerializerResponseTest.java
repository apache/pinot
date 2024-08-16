package org.apache.pinot.common.utils;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.pinot.common.restlet.resources.ResourceUtils;
import org.apache.pinot.common.restlet.resources.SegmentsReloadCheckResponse;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class SerializerResponseTest {

  @Test
  public void testSerialization() {
    // Given
    boolean needReload = true;
    String instanceId = "instance123";
    SegmentsReloadCheckResponse response = new SegmentsReloadCheckResponse(needReload, instanceId);
    String responseString = ResourceUtils.convertToJsonString(response);

    assertNotNull(responseString);
    assertEquals("{\n" + "  \"needReload\" : true,\n" + "  \"instanceId\" : \"instance123\"\n" + "}", responseString);
  }

  @Test
  public void testDeserialization()
      throws Exception {
    // Given
    String json = "{\"needReload\":true,\"instanceId\":\"instance123\"}";

    // When
    JsonNode jsonNode = JsonUtils.stringToJsonNode(json);

    // Then
    assertNotNull(jsonNode);
    assertTrue(jsonNode.get("needReload").asBoolean());
    assertEquals("instance123", jsonNode.get("instanceId").asText());
  }
}
