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
package com.linkedin.pinot.controller.api.resources;

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.controller.helix.ControllerRequestBuilderUtil;
import com.linkedin.pinot.controller.helix.ControllerTest;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
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
  public void testUploadBogusData() throws Exception {
    org.apache.http.client.HttpClient httpClient = new DefaultHttpClient();
    HttpPost httpPost = new HttpPost(_controllerRequestURLBuilder.forDataFileUpload());
    HttpEntity entity = new StringEntity("blah");
    httpPost.setEntity(entity);
    HttpResponse response = httpClient.execute(httpPost);
    int statusCode = response.getStatusLine().getStatusCode();

    Assert.assertTrue(statusCode >= 400 && statusCode < 500, "Status code = " + statusCode);
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
    _helixResourceManager.addTable(tableConfig);
  }

  @AfterClass
  public void tearDown() throws Exception {
    stopController();
    stopZk();
  }
}
