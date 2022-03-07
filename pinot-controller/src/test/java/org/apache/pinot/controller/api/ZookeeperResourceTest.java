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

import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.controller.ControllerTestUtils;
import org.apache.pinot.spi.utils.JsonUtils;
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
    String urlPut = ControllerTestUtils.getControllerRequestURLBuilder().forZkPut();
    String path = "/zookeeper";
    int expectedVersion = -1;
    int accessOption = 1;
    String data = "{\"id\" : \"QuickStartCluster\"," + "  \"data\" : { }\n" + "}";

    // CASE 1: Send data in query params form using HTTP PUT
    String path1 = path + "/testCase1";
    String params =
        "path=" + path1 + "&data=" + URIUtils.encode(data) + "&expectedVersion=" + expectedVersion + "&accessOption="
            + accessOption;
    String result = ControllerTestUtils.sendPutRequest(urlPut + "?" + params);
    Assert.assertTrue(result.toLowerCase().contains("successfully updated"));

    // validate zk/get results in correct data
    String urlGet = ControllerTestUtils.getControllerRequestURLBuilder().forZkGet(path1);
    result = ControllerTestUtils.sendGetRequest(urlGet);

    ZNRecordSerializer znRecordSerializer = new ZNRecordSerializer();
    Object deserialize = znRecordSerializer.deserialize(result.getBytes(StandardCharsets.UTF_8));
    Assert.assertTrue(deserialize instanceof ZNRecord);
    ZNRecord znRecord = (ZNRecord) deserialize;
    Assert.assertEquals(znRecord.getId(), "QuickStartCluster");

    // validate zk/getChildren in parent path
    urlGet = ControllerTestUtils.getControllerRequestURLBuilder().forZkGetChildren(path);
    result = ControllerTestUtils.sendGetRequest(urlGet);

    List<String> recordList = JsonUtils.stringToObject(result, new TypeReference<List<String>>() { });
    Assert.assertEquals(recordList.size(), 1);
    Assert.assertEquals(znRecordSerializer.deserialize(recordList.get(0).getBytes(StandardCharsets.UTF_8)), znRecord);

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
    String path2 = path + "/testCase2";
    try {
      params = "path=" + path2 + "&data=" + URIUtils.encode(largeConfig) + "&expectedVersion=" + expectedVersion
          + "&accessOption=" + accessOption;
      ControllerTestUtils.sendPutRequest(urlPut + "?" + params);
      Assert.fail("Should not get here, large payload");
    } catch (IOException e) {
      // Expected
    }

    // CASE 3: Send large content data should return success
    params = "path=" + path2 + "&expectedVersion=" + expectedVersion + "&accessOption=" + accessOption;
    Map<String, String> headers = new HashMap<>();
    headers.put("Content-Type", "application/json");
    result = ControllerTestUtils.sendPutRequest(urlPut + "?" + params, headers, largeConfig);
    Assert.assertTrue(result.toLowerCase().contains("successfully updated"));

    // validate that zk/getChildren return 2 items.
    urlGet = ControllerTestUtils.getControllerRequestURLBuilder().forZkGetChildren(path);
    result = ControllerTestUtils.sendGetRequest(urlGet);

    recordList = JsonUtils.stringToObject(result, new TypeReference<List<String>>() { });
    Assert.assertEquals(recordList.size(), 2);
    for (String record : recordList) {
      Assert.assertTrue(znRecordSerializer.deserialize(record.getBytes(StandardCharsets.UTF_8)) instanceof ZNRecord);
    }
  }
}
