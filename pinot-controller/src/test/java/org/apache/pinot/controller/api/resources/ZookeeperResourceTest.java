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
package org.apache.pinot.controller.api.resources;

import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.controller.helix.ControllerTest;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class ZookeeperResourceTest {
  private static final ControllerTest TEST_INSTANCE = ControllerTest.getInstance();

  @BeforeClass
  public void setUp()
      throws Exception {
    TEST_INSTANCE.setupSharedStateAndValidate();
  }

  @Test
  public void testZookeeperDataEndpoints()
      throws Exception {
    String urlPut = TEST_INSTANCE.getControllerRequestURLBuilder().forZkPut();
    String path = "/zookeeper";
    int expectedVersion = -1;
    int accessOption = 1;
    String data = "{\"id\" : \"QuickStartCluster\","
        + "\"simpleFields\" : { \"key\" : \"value\" }, "
        + "\"mapFields\" : { },  "
        + "\"listFields\" : { } }";

    // CASE 1: Send data in query params form using HTTP PUT
    String path1 = path + "/testCase1";
    String params =
        "path=" + path1 + "&data=" + URIUtils.encode(data) + "&expectedVersion=" + expectedVersion + "&accessOption="
            + accessOption;
    String result = ControllerTest.sendPutRequest(urlPut + "?" + params);
    Assert.assertTrue(result.toLowerCase().contains("successfully updated"));

    // validate zk/get results in correct data
    String urlGet = TEST_INSTANCE.getControllerRequestURLBuilder().forZkGet(path1);
    result = ControllerTest.sendGetRequest(urlGet);

    ZNRecord znRecord = ZookeeperResource.MAPPER.readValue(result.getBytes(StandardCharsets.UTF_8), ZNRecord.class);
    Assert.assertEquals(znRecord.getId(), "QuickStartCluster");
    Assert.assertEquals(znRecord.getSimpleField("key"), "value");

    // validate zk/getChildren in parent path
    urlGet = TEST_INSTANCE.getControllerRequestURLBuilder().forZkGetChildren(path);
    result = ControllerTest.sendGetRequest(urlGet);

    List<ZNRecord> recordList1 = ZookeeperResource.MAPPER.readValue(result.getBytes(StandardCharsets.UTF_8),
        new TypeReference<List<ZNRecord>>() { });
    Assert.assertEquals(recordList1.size(), 1);
    Assert.assertEquals(recordList1.get(0), znRecord);

    String lorem = "Loremipsumdolorsitametconsecteturadipisicingelitseddoeiusmod"
        + "temporincididuntutlaboreetdoloremagnaaliquaUtenimadminimveniam"
        + "quisnostrudexercitationullamcolaborisnisiutaliquipexeacommodo"
        + "consequatDuisauteiruredolorinreprehenderitinvoluptatevelitesse"
        + "cillumdoloreeufugiatnullapariaturExcepteursintoccaecatcupidatatnon"
        + "proidentsuntinculpaquiofficiadeseruntmollitanimidestlaborum";

    // make the content even more larger
    for (int i = 0; i < 10; i++) {
      lorem += lorem;
    }
    String largeData = "{\"id\" : \"QuickStartCluster\","
        + "\"simpleFields\" : { \"key\" : \"" + lorem + "\" }, "
        + "\"mapFields\" : { },  "
        + "\"listFields\" : { } }";

    // CASE 2: Fail when sending large data in query params
    String path2 = path + "/testCase2";
    try {
      params = "path=" + path2 + "&data=" + URIUtils.encode(largeData) + "&expectedVersion=" + expectedVersion
          + "&accessOption=" + accessOption;
      ControllerTest.sendPutRequest(urlPut + "?" + params);
      Assert.fail("Should not get here, large payload");
    } catch (IOException e) {
      // Expected
    }

    // CASE 3: Send large content data should return success
    params = "path=" + path2 + "&expectedVersion=" + expectedVersion + "&accessOption=" + accessOption;
    result = ControllerTest.sendPutRequest(urlPut + "?" + params, largeData);
    Assert.assertTrue(result.toLowerCase().contains("successfully updated"));

    // validate that zk/getChildren return 2 items.
    urlGet = TEST_INSTANCE.getControllerRequestURLBuilder().forZkGetChildren(path);
    result = ControllerTest.sendGetRequest(urlGet);

    List<ZNRecord> recordList3 = ZookeeperResource.MAPPER.readValue(result.getBytes(StandardCharsets.UTF_8),
        new TypeReference<List<ZNRecord>>() { });
    Assert.assertEquals(recordList3.size(), 2);

    // CASE 4: put all children back into a different path
    String path4 = path + "/testCase4";
    params = "path=" + path4 + "&expectedVersion=" + expectedVersion + "&accessOption=" + accessOption;

    // validate that zk/putChildren will insert all correctly to another path
    urlPut = TEST_INSTANCE.getControllerRequestURLBuilder().forZkPutChildren(path);
    String encodedChildrenData = ZookeeperResource.MAPPER.writeValueAsString(recordList3);
    result = ControllerTest.sendPutRequest(urlPut + "?" + params, encodedChildrenData);

    // validate that zk/getChildren from new path should result in the same recordList
    urlGet = TEST_INSTANCE.getControllerRequestURLBuilder().forZkGetChildren(path);
    result = ControllerTest.sendGetRequest(urlGet);

    List<ZNRecord> recordList4 = ZookeeperResource.MAPPER.readValue(result.getBytes(StandardCharsets.UTF_8),
        new TypeReference<List<ZNRecord>>() { });
    Assert.assertEquals(recordList4.get(0), recordList3.get(0));
    Assert.assertEquals(recordList4.get(1), recordList3.get(1));
  }
}
