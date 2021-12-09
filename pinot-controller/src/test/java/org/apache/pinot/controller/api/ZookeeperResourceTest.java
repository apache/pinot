/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.controller.api;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.controller.ControllerTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class ZookeeperResourceTest {

  @BeforeClass
  public void setUp()
      throws Exception {
    ControllerTestUtils.setupClusterAndValidate();
  }

  @Test
  public void testZkPutData()
      throws IOException {
    String url = ControllerTestUtils.getControllerRequestURLBuilder().forZkPut();
    String path = "/zookeeper";
    int expectedVersion = -1;
    int accessOption = 1;
    String data = "{\"id\" : \"QuickStartCluster\"," + "  \"data\" : { }\n" + "}";

    // CASE 1: Send data in query params form using HTTP PUT
    String params =
        "path=" + path + "&data=" + URIUtils.encode(data) + "&expectedVersion=" + expectedVersion + "&accessOption="
            + accessOption;
    String result = ControllerTestUtils.sendPutRequest(url + "?" + params);
    Assert.assertTrue(result.toLowerCase().contains("successfully updated"));

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

    String largeConfig = "{\n" + "  \"id\" : \"QuickStartCluster\",\n" + "  \"data\" : " + "\"" + lorem + "\"\n" + "}";

    // CASE 2: Fail when sending large data in query params
    try {
      params = "path=" + path + "&data=" + URIUtils.encode(largeConfig) + "&expectedVersion=" + expectedVersion
          + "&accessOption=" + accessOption;
      ControllerTestUtils.sendPutRequest(url + "?" + params);
      Assert.fail("Should not get here, large payload");
    } catch (IOException e) {
      // Expected
    }

    // CASE 3: Send large content data should return success
    params = "path=" + path + "&expectedVersion=" + expectedVersion + "&accessOption=" + accessOption;
    Map<String, String> headers = new HashMap<>();
    headers.put("Content-Type", "application/json");
    result = ControllerTestUtils.sendPutRequest(url + "?" + params, headers, largeConfig);
    Assert.assertTrue(result.toLowerCase().contains("successfully updated"));
  }
}
