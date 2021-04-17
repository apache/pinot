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
package org.apache.pinot.server.api;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.data.manager.TableDataManager;
import org.apache.pinot.core.transport.ListenerConfig;
import org.apache.pinot.core.transport.TlsConfig;
import org.apache.pinot.server.api.access.AccessControl;
import org.apache.pinot.server.api.access.AccessControlFactory;
import org.apache.pinot.server.starter.ServerInstance;
import org.apache.pinot.server.starter.helix.AdminApiApplication;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.NetUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;


public class AccessControlTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "AccessControlTest");
  protected static final String TABLE_NAME = "testTable";

  private final Map<String, TableDataManager> _tableDataManagerMap = new HashMap<>();
  private AdminApiApplication _adminApiApplication;
  protected WebTarget _webTarget;

  @BeforeClass
  public void setUp() throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);
    Assert.assertTrue(INDEX_DIR.mkdirs());

    // Mock the instance data manager
    // Mock the server instance
    ServerInstance serverInstance = mock(ServerInstance.class);

    _adminApiApplication = new AdminApiApplication(serverInstance, new DenyAllAccessFactory());
    _adminApiApplication.start(Collections.singletonList(new ListenerConfig(CommonConstants.HTTP_PROTOCOL, "0.0.0.0",
        CommonConstants.Server.DEFAULT_ADMIN_API_PORT, CommonConstants.HTTP_PROTOCOL, new TlsConfig())));

    _webTarget = ClientBuilder.newClient().target(
        String.format("http://%s:%d", NetUtils.getHostAddress(), CommonConstants.Server.DEFAULT_ADMIN_API_PORT));
  }

  @AfterClass
  public void tearDown() {
    _adminApiApplication.stop();
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @Test
  public void testAccessDenied() {
    String segmentPath = "/segments/testTable_REALTIME/segment_name";
    // Download any segment will be denied.
    Response response = _webTarget.path(segmentPath).request().get(Response.class);
    Assert.assertNotEquals(response.getStatus(), Response.Status.FORBIDDEN);
  }

  public static class DenyAllAccessFactory implements AccessControlFactory {
    private static final AccessControl DENY_ALL_ACCESS = (httpHeaders, tableName) -> false;

    @Override
    public AccessControl create() {
      return DENY_ALL_ACCESS;
    }
  }
}
