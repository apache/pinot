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

import com.google.common.collect.ImmutableMap;
import io.netty.channel.ChannelHandlerContext;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.Collections;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.auth.BasicAuthUtils;
import org.apache.pinot.core.transport.HttpServerThreadPoolConfig;
import org.apache.pinot.core.transport.ListenerConfig;
import org.apache.pinot.server.access.AccessControl;
import org.apache.pinot.server.access.AccessControlFactory;
import org.apache.pinot.server.access.BasicAuthAccessFactory;
import org.apache.pinot.server.access.GrpcRequesterIdentity;
import org.apache.pinot.server.access.HttpRequesterIdentity;
import org.apache.pinot.server.access.RequesterIdentity;
import org.apache.pinot.server.starter.ServerInstance;
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
  protected static final String TABLE_NAME = "testTable";

  private AdminApiApplication _adminApiApplication;
  protected WebTarget _webTarget;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);
    Assert.assertTrue(INDEX_DIR.mkdirs());

    // Mock the instance data manager
    // Mock the server instance
    ServerInstance serverInstance = mock(ServerInstance.class);
    when(serverInstance.getServerMetrics()).thenReturn(mock(ServerMetrics.class));
    when(serverInstance.getHelixManager()).thenReturn(mock(HelixManager.class));

    PinotConfiguration serverConf = new PinotConfiguration();
    String hostname = serverConf.getProperty(CommonConstants.Helix.KEY_OF_SERVER_NETTY_HOST,
        serverConf.getProperty(CommonConstants.Helix.SET_INSTANCE_ID_TO_HOSTNAME_KEY, false)
            ? NetUtils.getHostnameOrAddress() : NetUtils.getHostAddress());
    int port = serverConf.getProperty(CommonConstants.Helix.KEY_OF_SERVER_NETTY_PORT,
        CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT);
    serverConf.setProperty(CommonConstants.Server.CONFIG_OF_INSTANCE_ID,
        CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE + hostname + "_" + port);
    _adminApiApplication = new AdminApiApplication(serverInstance, new DenyAllAccessFactory(), serverConf);

    int adminApiApplicationPort = getAvailablePort();
    _adminApiApplication.start(Collections.singletonList(
        new ListenerConfig(CommonConstants.HTTP_PROTOCOL, "0.0.0.0", adminApiApplicationPort,
            CommonConstants.HTTP_PROTOCOL, new TlsConfig(), HttpServerThreadPoolConfig.defaultInstance())));

    _webTarget = ClientBuilder.newClient().target(
        String.format("http://%s:%d", NetUtils.getHostAddress(), adminApiApplicationPort));
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
    private static final AccessControl DENY_ALL_ACCESS = new AccessControl() {
      @Override
      public boolean isAuthorizedChannel(ChannelHandlerContext channelHandlerContext) {
        return false;
      }

      @Override
      public boolean hasDataAccess(RequesterIdentity requesterIdentity, String tableName) {
        return false;
      }
    };

    @Override
    public AccessControl create() {
      return DENY_ALL_ACCESS;
    }
  }

  @Test
  public void testGrpcBasicAuth() {
    testBasicAuth(new GrpcRequesterIdentity(
        ImmutableMap.of("authorization", BasicAuthUtils.toBasicAuthToken("admin123", "verysecret"))), true);
    testBasicAuth(new GrpcRequesterIdentity(
        ImmutableMap.of("authorization", BasicAuthUtils.toBasicAuthToken("user456", "kindasecret"))), false);

    testBasicAuth(new GrpcRequesterIdentity(
        ImmutableMap.of("authorization", "Basic YWRtaW4xMjM6dmVyeXNlY3JldA")), true);
    testBasicAuth(new GrpcRequesterIdentity(
        ImmutableMap.of("authorization", "Basic dXNlcjQ1NjpraW5kYXNlY3JldA==")), false);
  }

  @Test
  public void testHttpBasicAuth() {
    HttpHeaders headers = new ContainerRequest(null, null, null, null, new MapPropertiesDelegate());
    headers.getRequestHeaders()
        .put("authorization", Arrays.asList(BasicAuthUtils.toBasicAuthToken("admin123", "verysecret")));
    testBasicAuth(new HttpRequesterIdentity(headers), true);
    headers.getRequestHeaders()
        .put("authorization", Arrays.asList(BasicAuthUtils.toBasicAuthToken("user456", "kindasecret")));
    testBasicAuth(new HttpRequesterIdentity(headers), false);
    headers.getRequestHeaders().put("authorization", Arrays.asList("Basic YWRtaW4xMjM6dmVyeXNlY3JldA"));
    testBasicAuth(new HttpRequesterIdentity(headers), true);
    headers.getRequestHeaders().put("authorization", Arrays.asList("Basic dXNlcjQ1NjpraW5kYXNlY3JldA=="));
    testBasicAuth(new HttpRequesterIdentity(headers), false);
  }

  public void testBasicAuth(RequesterIdentity requesterIdentity, boolean isAdmin) {
    final BasicAuthAccessFactory basicAuthAccessFactory = new BasicAuthAccessFactory();
    PinotConfiguration config = new PinotConfiguration(ImmutableMap.of("principals", "admin123,user456",
        "principals.admin123.password", "verysecret", "principals.user456.password", "kindasecret",
        "principals.user456.tables", "stuff,lessImportantStuff"));
    basicAuthAccessFactory.init(config);
    final AccessControl accessControl = basicAuthAccessFactory.create();
    Assert.assertTrue(accessControl.hasDataAccess(requesterIdentity, "stuff"));
    Assert.assertTrue(accessControl.hasDataAccess(requesterIdentity, "stuff_OFFLINE"));
    Assert.assertTrue(accessControl.hasDataAccess(requesterIdentity, "stuff_REALTIME"));
    Assert.assertTrue(accessControl.hasDataAccess(requesterIdentity, "lessImportantStuff"));
    Assert.assertTrue(accessControl.hasDataAccess(requesterIdentity, "lessImportantStuff_OFFLINE"));
    Assert.assertTrue(accessControl.hasDataAccess(requesterIdentity, "lessImportantStuff_REALTIME"));
    if (isAdmin) {
      Assert.assertTrue(accessControl.hasDataAccess(requesterIdentity, "myTable"));
      Assert.assertTrue(accessControl.hasDataAccess(requesterIdentity, "myTable_OFFLINE"));
      Assert.assertTrue(accessControl.hasDataAccess(requesterIdentity, "myTable_REALTIME"));
    } else {
      Assert.assertFalse(accessControl.hasDataAccess(requesterIdentity, "myTable"));
      Assert.assertFalse(accessControl.hasDataAccess(requesterIdentity, "myTable_OFFLINE"));
      Assert.assertFalse(accessControl.hasDataAccess(requesterIdentity, "myTable_REALTIME"));
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
