package org.apache.pinot.controller.api.resources;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.InputStream;
import java.util.Map;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class PinotTableRestletResourceTest {

  @Test
  public void testTweakRealtimeTableConfig() throws Exception {
    try (InputStream inputStream = Thread.currentThread().getContextClassLoader()
        .getResourceAsStream("table/table_config_with_instance_assignment.json")) {
      ObjectNode tableConfig = (ObjectNode) JsonUtils.inputStreamToJsonNode(inputStream);

      String brokerTenant = "testBroker";
      String serverTenant = "testServer";
      PinotTableRestletResource.tweakRealtimeTableConfig(tableConfig, brokerTenant, serverTenant,
          Map.of("rtaShared2_REALTIME", "testServer_REALTIME"));

      assertEquals(tableConfig.get("tenants").get("broker").asText(), brokerTenant);
      assertEquals(tableConfig.get("tenants").get("server").asText(), serverTenant);
      assertEquals(tableConfig.path("instanceAssignmentConfigMap").path("CONSUMING").path("tagPoolConfig").path("tag")
          .asText(), serverTenant + "_REALTIME");
    }
  }
}
