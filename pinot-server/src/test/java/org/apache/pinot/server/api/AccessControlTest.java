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

import io.netty.channel.ChannelHandlerContext;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.auth.BasicAuthTokenUtils;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.transport.HttpServerThreadPoolConfig;
import org.apache.pinot.core.transport.ListenerConfig;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.utils.ServerReloadJobStatusCache;
import org.apache.pinot.server.access.AccessControl;
import org.apache.pinot.server.access.BasicAuthAccessFactory;
import org.apache.pinot.server.access.GrpcRequesterIdentity;
import org.apache.pinot.server.access.HttpRequesterIdentity;
import org.apache.pinot.server.starter.ServerInstance;
import org.apache.pinot.spi.auth.server.RequesterIdentity;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.NetUtils;
import org.glassfish.jersey.internal.MapPropertiesDelegate;
import org.glassfish.jersey.server.ContainerRequest;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class AccessControlTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "AccessControlTest");
  private static final String TABLE_NAME = "testTable";
  private static final String REALTIME_TABLE_NAME = TABLE_NAME + "_REALTIME";
  private static final String ADMIN_TOKEN = BasicAuthTokenUtils.toBasicAuthToken("admin123", "verysecret");
  private static final String ADMIN_TOKEN_WITHOUT_PADDING = ADMIN_TOKEN.replace("=", "");
  private static final String DATA_TOKEN = BasicAuthTokenUtils.toBasicAuthToken("user456", "kindasecret");
  private static final String OTHER_TABLE_TOKEN = BasicAuthTokenUtils.toBasicAuthToken("user789", "othersecret");
  private static final String INVALID_TOKEN = BasicAuthTokenUtils.toBasicAuthToken("unknown", "wrongpassword");

  private AdminApiApplication _adminApiApplication;
  private WebTarget _webTarget;
  private AccessControl _accessControl;
  private String _instanceId;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);
    Assert.assertTrue(INDEX_DIR.mkdirs());

    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    TableDataManager tableDataManager = mock(TableDataManager.class);
    when(instanceDataManager.getTableDataManager(REALTIME_TABLE_NAME)).thenReturn(tableDataManager);

    ServerInstance serverInstance = mock(ServerInstance.class);
    when(serverInstance.getServerMetrics()).thenReturn(mock(ServerMetrics.class));
    when(serverInstance.getHelixManager()).thenReturn(mock(HelixManager.class));
    when(serverInstance.getInstanceDataManager()).thenReturn(instanceDataManager);

    PinotConfiguration serverConf = new PinotConfiguration();
    String hostname = serverConf.getProperty(CommonConstants.Helix.KEY_OF_SERVER_NETTY_HOST,
        serverConf.getProperty(CommonConstants.Helix.SET_INSTANCE_ID_TO_HOSTNAME_KEY, false)
            ? NetUtils.getHostnameOrAddress() : NetUtils.getHostAddress());
    int port = serverConf.getProperty(CommonConstants.Helix.KEY_OF_SERVER_NETTY_PORT,
        CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT);
    _instanceId = CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE + hostname + "_" + port;
    serverConf.setProperty(CommonConstants.Server.CONFIG_OF_INSTANCE_ID, _instanceId);

    BasicAuthAccessFactory basicAuthAccessFactory = new BasicAuthAccessFactory();
    basicAuthAccessFactory.init(new PinotConfiguration(Map.of(
        "principals", "admin123,user456,user789",
        "principals.admin123.password", "verysecret",
        "principals.admin123.permissions", "admin",
        "principals.user456.password", "kindasecret",
        "principals.user456.tables", TABLE_NAME,
        "principals.user456.permissions", "read",
        "principals.user789.password", "othersecret",
        "principals.user789.tables", "otherTable")));
    _accessControl = basicAuthAccessFactory.create();

    ServiceStatus.ServiceStatusCallback serviceStatusCallback = mock(ServiceStatus.ServiceStatusCallback.class);
    when(serviceStatusCallback.getServiceStatus()).thenReturn(ServiceStatus.Status.GOOD);
    when(serviceStatusCallback.getStatusDescription()).thenReturn("Ready");
    ServiceStatus.setServiceStatusCallback(_instanceId, serviceStatusCallback);

    _adminApiApplication = new AdminApiApplication(serverInstance, basicAuthAccessFactory,
        mock(ServerReloadJobStatusCache.class),
        serverConf);

    int adminApiApplicationPort = getAvailablePort();
    _adminApiApplication.start(List.of(
        new ListenerConfig(CommonConstants.HTTP_PROTOCOL, "0.0.0.0", adminApiApplicationPort,
            CommonConstants.HTTP_PROTOCOL, new TlsConfig(), HttpServerThreadPoolConfig.defaultInstance())));

    _webTarget = ClientBuilder.newClient().target(
        String.format("http://%s:%d", NetUtils.getHostAddress(), adminApiApplicationPort));
  }

  @AfterClass
  public void tearDown() {
    _adminApiApplication.stop();
    ServiceStatus.removeServiceStatusCallback(_instanceId);
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @Test
  public void testAdministrativeHttpAccess() {
    assertGetStatus("/appconfigs", null, Response.Status.UNAUTHORIZED);
    assertGetStatus("/appconfigs", INVALID_TOKEN, Response.Status.UNAUTHORIZED);
    assertGetStatus("/appconfigs", DATA_TOKEN, Response.Status.FORBIDDEN);
    assertGetStatus("/appconfigs", OTHER_TABLE_TOKEN, Response.Status.FORBIDDEN);
    assertGetStatus("/appconfigs", ADMIN_TOKEN, Response.Status.OK);
  }

  @Test
  public void testStaticSwaggerHttpAccess() {
    for (String path : List.of("/api/index.html", "/swaggerui-dist/index.html")) {
      assertGetStatus(path, null, Response.Status.UNAUTHORIZED);
      assertGetStatus(path, INVALID_TOKEN, Response.Status.UNAUTHORIZED);
      assertGetStatus(path, DATA_TOKEN, Response.Status.FORBIDDEN);
      assertGetStatus(path, ADMIN_TOKEN, Response.Status.OK);
      // An authorized response must not populate Grizzly's listener-wide file cache and bypass later checks.
      assertGetStatus(path, null, Response.Status.UNAUTHORIZED);
    }
  }

  @Test
  public void testOnlyHealthAndReadinessRoutesArePublic() {
    assertGetStatus("/health", null, Response.Status.OK);
    assertGetStatus("/health/liveness", null, Response.Status.OK);
    assertGetStatus("/health/readiness", null, Response.Status.OK);
    assertHeadStatus("/health", null, Response.Status.UNAUTHORIZED);

    for (String path : List.of("/uptime", "/start-time")) {
      assertGetStatus(path, null, Response.Status.UNAUTHORIZED);
      assertGetStatus(path, DATA_TOKEN, Response.Status.FORBIDDEN);
      assertGetStatus(path, ADMIN_TOKEN, Response.Status.OK);
    }
  }

  @Test
  public void testSegmentDataAccessStillUsesTablePermission() {
    for (String path : List.of(
        "/segments/testTable_REALTIME/segment_name",
        "/segments/testTable_REALTIME/segment_name/validDocIdsBitmap")) {
      // The authorized table principal reaches the resource and receives its missing-segment result.
      assertGetStatus(path, DATA_TOKEN, Response.Status.NOT_FOUND);
      // A valid principal without access to this table is rejected by the existing table-level check.
      assertGetStatus(path, OTHER_TABLE_TOKEN, Response.Status.FORBIDDEN);
    }
  }

  @Test
  public void testGrpcBasicAuth() {
    testBasicAuth(new GrpcRequesterIdentity(Map.of("authorization", ADMIN_TOKEN)), true);
    testBasicAuth(new GrpcRequesterIdentity(Map.of("authorization", DATA_TOKEN)), false);
    testBasicAuth(new GrpcRequesterIdentity(Map.of("authorization", ADMIN_TOKEN_WITHOUT_PADDING)), true);
  }

  @Test
  public void testHttpBasicAuth() {
    HttpHeaders headers = new ContainerRequest(null, null, null, null, new MapPropertiesDelegate());
    headers.getRequestHeaders().put("authorization", List.of(ADMIN_TOKEN));
    testBasicAuth(new HttpRequesterIdentity(headers), true);
    headers.getRequestHeaders().put("authorization", List.of(DATA_TOKEN));
    testBasicAuth(new HttpRequesterIdentity(headers), false);
    headers.getRequestHeaders().put("authorization", List.of(OTHER_TABLE_TOKEN));
    HttpRequesterIdentity tableOnlyIdentity = new HttpRequesterIdentity(headers);
    Assert.assertFalse(_accessControl.authorizeAdminAccess(tableOnlyIdentity).hasAccess());
    Assert.assertTrue(_accessControl.hasDataAccess(tableOnlyIdentity, "otherTable"));
    Assert.assertFalse(_accessControl.hasDataAccess(tableOnlyIdentity, TABLE_NAME));
    headers.getRequestHeaders().put("authorization", List.of(ADMIN_TOKEN_WITHOUT_PADDING));
    testBasicAuth(new HttpRequesterIdentity(headers), true);

    HttpHeaders missingHeaders = new ContainerRequest(null, null, null, null, new MapPropertiesDelegate());
    Assert.expectThrows(NotAuthorizedException.class,
        () -> _accessControl.authorizeAdminAccess(new HttpRequesterIdentity(missingHeaders)));
    headers.getRequestHeaders().put("authorization", List.of(INVALID_TOKEN));
    Assert.expectThrows(NotAuthorizedException.class,
        () -> _accessControl.authorizeAdminAccess(new HttpRequesterIdentity(headers)));
  }

  @Test
  public void testHttpIdentityPreservesHeadersAndUsesCaseInsensitiveLookup() {
    HttpHeaders headers = new ContainerRequest(null, null, null, null, new MapPropertiesDelegate());
    headers.getRequestHeaders().put(HttpHeaders.AUTHORIZATION, List.of(ADMIN_TOKEN));
    headers.getRequestHeaders().put("X-Custom", List.of("value"));

    HttpRequesterIdentity identity = new HttpRequesterIdentity(headers);

    Assert.assertEquals(identity.getHttpHeaders().keySet(), Set.of(HttpHeaders.AUTHORIZATION, "X-Custom"));
    Assert.assertEquals(identity.getHeaderValues("authorization"), List.of(ADMIN_TOKEN));
  }

  @Test
  public void testLegacyAccessControlDefaultsToAdminDenied() {
    AccessControl legacyAccessControl = new AccessControl() {
      @Override
      public boolean isAuthorizedChannel(ChannelHandlerContext channelHandlerContext) {
        return true;
      }

      @Override
      public boolean hasDataAccess(RequesterIdentity requesterIdentity, String tableName) {
        return true;
      }
    };

    Assert.assertFalse(legacyAccessControl.authorizeAdminAccess(new GrpcRequesterIdentity(Map.of())).hasAccess());
  }

  private void testBasicAuth(RequesterIdentity requesterIdentity, boolean isAdmin) {
    Assert.assertTrue(_accessControl.hasDataAccess(requesterIdentity, TABLE_NAME));
    Assert.assertTrue(_accessControl.hasDataAccess(requesterIdentity, TABLE_NAME + "_OFFLINE"));
    Assert.assertTrue(_accessControl.hasDataAccess(requesterIdentity, REALTIME_TABLE_NAME));
    if (isAdmin) {
      Assert.assertTrue(_accessControl.authorizeAdminAccess(requesterIdentity).hasAccess());
      Assert.assertTrue(_accessControl.hasDataAccess(requesterIdentity, "myTable"));
    } else {
      Assert.assertFalse(_accessControl.authorizeAdminAccess(requesterIdentity).hasAccess());
      Assert.assertFalse(_accessControl.hasDataAccess(requesterIdentity, "myTable"));
    }
  }

  private void assertGetStatus(String path, String authorization, Response.Status expectedStatus) {
    try (Response response = authorization == null
        ? _webTarget.path(path).request().get(Response.class)
        : _webTarget.path(path).request().header(HttpHeaders.AUTHORIZATION, authorization).get(Response.class)) {
      Assert.assertEquals(response.getStatus(), expectedStatus.getStatusCode(), path);
      if (expectedStatus == Response.Status.UNAUTHORIZED) {
        Assert.assertEquals(response.getHeaderString(HttpHeaders.WWW_AUTHENTICATE), "Basic", path);
      }
    }
  }

  private void assertHeadStatus(String path, String authorization, Response.Status expectedStatus) {
    try (Response response = authorization == null
        ? _webTarget.path(path).request().head()
        : _webTarget.path(path).request().header(HttpHeaders.AUTHORIZATION, authorization).head()) {
      Assert.assertEquals(response.getStatus(), expectedStatus.getStatusCode(), path);
    }
  }

  public static int getAvailablePort() {
    try {
      try (ServerSocket socket = new ServerSocket(0)) {
        return socket.getLocalPort();
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to find an available port to use", e);
    }
  }
}
