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
package com.linkedin.pinot.controller;

import org.json.JSONObject;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.pinot.controller.api.pojos.DataResource;
import com.linkedin.pinot.controller.helix.ControllerRequestBuilderUtil;


/**
 * Regression test for DataResource serialization.
 *
 * @author jfim
 */
public class DataResourceSerializationTest {
  @Test
  public void testDataResourceSerialization() throws Exception {
    JSONObject dataResourceJson = ControllerRequestBuilderUtil.createOfflineClusterBrokerResourceUpdateConfig(1, "test")
        .toJSON();
    ObjectMapper mapper = new ObjectMapper();
    DataResource resource = mapper.readValue(dataResourceJson.toString().getBytes(), DataResource.class);
  }
}
