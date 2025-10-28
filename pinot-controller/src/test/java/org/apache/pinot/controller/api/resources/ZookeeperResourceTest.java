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
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.client.admin.PinotAdminClient;
import org.apache.pinot.controller.helix.ControllerTest;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.expectThrows;


public class ZookeeperResourceTest extends ControllerTest {
  @BeforeClass
  public void setUp()
      throws Exception {
    setupSharedStateAndValidate();
  }

  @Test
  public void testZookeeperDataEndpoints()
      throws Exception {
    PinotAdminClient adminClient = getOrCreateAdminClient();
    String path = "/zookeeper";
    int expectedVersion = -1;
    int accessOption = 1;
    String data = "{\"id\" : \"QuickStartCluster\","
        + "\"simpleFields\" : { \"key\" : \"value\" }, "
        + "\"mapFields\" : { },  "
        + "\"listFields\" : { } }";

    // CASE 1: Send data in query params form using HTTP PUT
    String path1 = path + "/testCase1";
    String result =
        adminClient.getZookeeperClient().putDataWithQueryParam(path1, data, expectedVersion, accessOption);
    Assert.assertTrue(result.toLowerCase().contains("successfully updated"));

    // validate zk/get results in correct data
    result = adminClient.getZookeeperClient().getData(path1);

    ZNRecord znRecord = ZookeeperResource.MAPPER.readValue(result.getBytes(StandardCharsets.UTF_8), ZNRecord.class);
    Assert.assertEquals(znRecord.getId(), "QuickStartCluster");
    Assert.assertEquals(znRecord.getSimpleField("key"), "value");

    // validate zk/getChildren in parent path
    result = adminClient.getZookeeperClient().getChildren(path);

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
    for (int i = 0; i < 5; i++) {
      lorem += lorem;
    }
    String largeData = "{\"id\" : \"QuickStartCluster\","
        + "\"simpleFields\" : { \"key\" : \"" + lorem + "\" }, "
        + "\"mapFields\" : { },  "
        + "\"listFields\" : { } }";

    // CASE 2: Fail when sending large data in query params
    String path2 = path + "/testCase2";
    expectThrows(RuntimeException.class,
        () -> adminClient.getZookeeperClient().putDataWithQueryParam(path2, largeData, expectedVersion, accessOption));

    // CASE 3: Send large content data should return success
    result = adminClient.getZookeeperClient().putData(path2, largeData, expectedVersion, accessOption);
    Assert.assertTrue(result.toLowerCase().contains("successfully updated"));

    // validate that zk/getChildren return 2 items.
    result = adminClient.getZookeeperClient().getChildren(path);

    List<ZNRecord> recordList3 = ZookeeperResource.MAPPER.readValue(result.getBytes(StandardCharsets.UTF_8),
        new TypeReference<List<ZNRecord>>() { });
    Assert.assertEquals(recordList3.size(), 2);

    // CASE 4: put all children back into a different path
    String path4 = path + "/testCase4";

    // validate that zk/putChildren will insert all correctly to another path
    String encodedChildrenData = ZookeeperResource.MAPPER.writeValueAsString(recordList3);
    result = adminClient.getZookeeperClient().putChildren(path4, encodedChildrenData, expectedVersion, accessOption);

    // validate that zk/getChildren from new path should result in the same recordList
    result = adminClient.getZookeeperClient().getChildren(path);

    List<ZNRecord> recordList4 = ZookeeperResource.MAPPER.readValue(result.getBytes(StandardCharsets.UTF_8),
        new TypeReference<List<ZNRecord>>() { });
    Assert.assertEquals(recordList4.get(0), recordList3.get(0));
    Assert.assertEquals(recordList4.get(1), recordList3.get(1));
  }
}
