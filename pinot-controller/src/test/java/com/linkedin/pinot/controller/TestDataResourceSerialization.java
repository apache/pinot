package com.linkedin.pinot.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.controller.api.pojos.DataResource;
import com.linkedin.pinot.controller.helix.ControllerRequestBuilderUtil;
import org.json.JSONObject;
import org.testng.annotations.Test;


/**
 * Regression test for DataResource serialization.
 *
 * @author jfim
 */
public class TestDataResourceSerialization {
  @Test
  public void testDataResourceSerialization() throws Exception {
    JSONObject dataResourceJson = ControllerRequestBuilderUtil.createOfflineClusterBrokerResourceUpdateConfig(1, "test")
        .toJSON();
    ObjectMapper mapper = new ObjectMapper();
    DataResource resource = mapper.readValue(dataResourceJson.toString().getBytes(), DataResource.class);
  }
}
