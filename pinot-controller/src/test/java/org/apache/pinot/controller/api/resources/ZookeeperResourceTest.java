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
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.helix.AccessOption;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.client.admin.PinotAdminClient;
import org.apache.pinot.controller.api.access.AccessControl;
import org.apache.pinot.controller.api.access.BasicAuthAccessControlFactory;
import org.apache.pinot.controller.api.access.ZkBasicAuthAccessControlFactory;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.TargetType;
import org.apache.zookeeper.data.Stat;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.expectThrows;


public class ZookeeperResourceTest extends ControllerTest {
  @Test
  public void testZookeeperDataEndpoints()
      throws Exception {
    setupSharedStateAndValidate();
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
    expectThrows(Exception.class,
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

  @Test
  public void testUserMetadataWriteAuthorization()
      throws Exception {
    ZookeeperResource resource = new ZookeeperResource();
    PinotHelixResourceManager manager = Mockito.mock(PinotHelixResourceManager.class);
    AccessControl accessControl = Mockito.mock(AccessControl.class);
    resource._pinotHelixResourceManager = manager;
    resource._accessControlFactory = Mockito.mock(ZkBasicAuthAccessControlFactory.class);
    resource._httpHeaders = Mockito.mock(HttpHeaders.class);
    Mockito.when(resource._accessControlFactory.create()).thenReturn(accessControl);

    String configRoot = PropertyPathBuilder.propertyStore("testCluster") + "/CONFIGS";
    String userPath = configRoot + "/USER/fixture";
    Mockito.when(manager.getZKStat(configRoot)).thenReturn(new Stat());
    Mockito.when(manager.getZKChildNames(configRoot)).thenReturn(List.of("USER"));
    String record = ZookeeperResource.MAPPER.writeValueAsString(new ZNRecord("fixture"));
    String batch = ZookeeperResource.MAPPER.writeValueAsString(List.of(new ZNRecord("other"), new ZNRecord("USER")));
    assertForbidden(() -> resource.createNode(userPath, -1, AccessOption.PERSISTENT, record));
    assertForbidden(() -> resource.putData(userPath, null, -1, AccessOption.PERSISTENT, record));
    assertForbidden(() -> resource.putData("/peers/testCluster/HELIX_PROPERTYSTORE/CONFIGS/USER/fixture", null, -1,
        AccessOption.PERSISTENT, record));
    assertForbidden(() -> resource.putChildren(configRoot, null, -1, AccessOption.PERSISTENT, batch));
    assertForbidden(() -> resource.delete(configRoot));
    Mockito.verifyNoMoreInteractions(Mockito.ignoreStubs(manager));

    String topLevel = "/fixture";
    Mockito.when(manager.getZKStat(topLevel)).thenReturn(new Stat());
    Mockito.when(manager.getZKChildNames(topLevel)).thenReturn(List.of("child"));
    Mockito.when(manager.deleteZKLeaf(topLevel + "/child")).thenReturn(true);
    Mockito.when(manager.deleteZKLeaf(topLevel)).thenReturn(true);
    resource.delete(topLevel);
    InOrder deletes = Mockito.inOrder(manager);
    deletes.verify(manager).deleteZKLeaf(topLevel + "/child");
    deletes.verify(manager).deleteZKLeaf(topLevel);
    Mockito.verify(manager, Mockito.never()).deleteZKPath(topLevel);

    Mockito.when(accessControl.hasAccess(resource._httpHeaders, TargetType.CLUSTER, null, Actions.Cluster.UPDATE_USER))
        .thenReturn(true);
    Mockito.when(manager.deleteZKPath(configRoot)).thenReturn(true);
    resource.delete(configRoot);
    Mockito.verify(manager).deleteZKPath(configRoot);
    resource._accessControlFactory = Mockito.mock(BasicAuthAccessControlFactory.class);
    resource.delete(configRoot);
    Mockito.verify(manager, Mockito.times(2)).deleteZKPath(configRoot);
  }

  private static void assertForbidden(Runnable operation) {
    ControllerApplicationException exception = expectThrows(ControllerApplicationException.class, operation::run);
    Assert.assertEquals(exception.getResponse().getStatus(), Response.Status.FORBIDDEN.getStatusCode());
  }
}
