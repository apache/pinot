package org.apache.pinot.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pinot.client.controller.PinotControllerTransport;
import org.apache.pinot.client.controller.response.ControllerTenantBrokerResponse;


public class DummyPinotControllerTransport extends PinotControllerTransport {

  @Override
  public ControllerTenantBrokerResponse getBrokersFromController(String controllerAddress, String tenant) {
    try {
      String jsonString = "[\"dummy\"]";
      ObjectMapper objectMapper = new ObjectMapper();
      JsonNode dummyBrokerJsonResponse = objectMapper.readTree(jsonString);
      return ControllerTenantBrokerResponse.fromJson(dummyBrokerJsonResponse);
    } catch (Exception e) {

    }
    return ControllerTenantBrokerResponse.empty();
  }
}
