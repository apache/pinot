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
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.controller.helix.ControllerRequestBuilderUtil;
import com.linkedin.pinot.controller.helix.ControllerTest;
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
  private static final String TABLE_NAME = "testTable";

  @Test
  public void testUploadBogusData() {
    Client client = new Client(Protocol.HTTP);
    Request request = new Request(Method.POST, _controllerRequestURLBuilder.forDataFileUpload());
    request.setEntity("blah", MediaType.MULTIPART_ALL);
    Response response = client.handle(request);

    Assert.assertEquals(response.getStatus(), Status.SERVER_ERROR_INTERNAL);
  }

  @BeforeClass
  public void setUp() throws Exception {
    startZk();
    startController();
    ControllerRequestBuilderUtil.addFakeBrokerInstancesToAutoJoinHelixCluster(getHelixClusterName(),
        ZkStarter.DEFAULT_ZK_STR, 5, true);
    ControllerRequestBuilderUtil.addFakeDataInstancesToAutoJoinHelixCluster(getHelixClusterName(),
        ZkStarter.DEFAULT_ZK_STR, 5, true);

    Assert.assertEquals(_helixAdmin.getInstancesInClusterWithTag(getHelixClusterName(), "DefaultTenant_BROKER").size(),
        5);

    // Adding table
    TableConfig tableConfig = new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE).setTableName(TABLE_NAME)
        .setSegmentAssignmentStrategy("RandomAssignmentStrategy")
        .setNumReplicas(2)
        .build();
    _controllerStarter.getHelixResourceManager().addTable(tableConfig);
  }

  @AfterClass
  public void tearDown() throws Exception {
    stopController();
    stopZk();
  }
}
