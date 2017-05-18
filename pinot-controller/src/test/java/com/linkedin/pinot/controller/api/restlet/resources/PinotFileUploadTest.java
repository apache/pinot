/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.controller.api.restlet.resources;

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.controller.helix.ControllerRequestBuilderUtil;
import com.linkedin.pinot.controller.helix.ControllerRequestURLBuilder;
import com.linkedin.pinot.controller.helix.ControllerTest;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import org.restlet.Client;
import org.restlet.Request;
import org.restlet.Response;
import org.restlet.data.MediaType;
import org.restlet.data.Method;
import org.restlet.data.Protocol;
import org.restlet.data.Status;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Tests for the file upload restlet.
 *
 */
public class PinotFileUploadTest extends ControllerTest {

  private final static String TABLE_NAME = "testTable";

  private static final String HELIX_CLUSTER_NAME = "PinotFileUploadTest";

  private PinotHelixResourceManager _pinotHelixResourceManager;

  @Test
  public void testUploadBogusData() {
    Client client = new Client(Protocol.HTTP);
    Request request =
        new Request(Method.POST, ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL).forDataFileUpload());
    request.setEntity("blah", MediaType.MULTIPART_ALL);
    Response response = client.handle(request);

    Assert.assertEquals(response.getStatus(), Status.SERVER_ERROR_INTERNAL);
  }

  @BeforeClass
  public void setUp() throws Exception {
    startZk();
    startController();
    _pinotHelixResourceManager = _controllerStarter.getHelixResourceManager();
    ControllerRequestBuilderUtil.addFakeBrokerInstancesToAutoJoinHelixCluster(getHelixClusterName(),
        ZkStarter.DEFAULT_ZK_STR, 5, true);
    ControllerRequestBuilderUtil.addFakeDataInstancesToAutoJoinHelixCluster(getHelixClusterName(),
        ZkStarter.DEFAULT_ZK_STR, 5, true);

    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(HELIX_CLUSTER_NAME, "DefaultTenant_BROKER").size(), 5);

    // Adding table
    String OfflineTableConfigJson =
        ControllerRequestBuilderUtil.buildCreateOfflineTableJSON(TABLE_NAME, null, null, 2, "RandomAssignmentStrategy")
            .toString();
    TableConfig offlineTableConfig = TableConfig.init(OfflineTableConfigJson);
    _pinotHelixResourceManager.addTable(offlineTableConfig);
  }

  @AfterClass
  public void tearDown() throws Exception {
    stopController();
    stopZk();
  }

  @Override
  protected String getHelixClusterName() {
    return HELIX_CLUSTER_NAME;
  }
}
