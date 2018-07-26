/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.common.config;

import com.linkedin.pinot.common.utils.TenantRole;
import java.io.IOException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TenantTest {

  @Test
  public void testDeserializeFromJson()
      throws JSONException, IOException {
    JSONObject json = new JSONObject();
    json.put("tenantRole", "SERVER");
    json.put("tenantName", "newTenant");
    json.put("numberOfInstances", 10);
    json.put("offlineInstances", 5);
    json.put("realtimeInstances", 5);
    json.put("keyIDontKnow", "blahblahblah");

    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonNode = mapper.readTree(json.toString());
    Tenant tenant = mapper.readValue(jsonNode, Tenant.class);
    Assert.assertEquals(5, tenant.getOfflineInstances());
    Assert.assertEquals(10, tenant.getNumberOfInstances());
    Assert.assertEquals("newTenant", tenant.getTenantName());
    Assert.assertEquals(TenantRole.SERVER, tenant.getTenantRole());
  }

}
