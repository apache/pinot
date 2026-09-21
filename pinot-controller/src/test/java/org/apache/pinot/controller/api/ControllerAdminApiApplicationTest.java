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

import java.io.File;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Providers;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.resources.ControllerFilePathProvider;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.glassfish.jersey.media.multipart.MultiPartProperties;
import org.glassfish.jersey.server.ApplicationHandler;
import org.glassfish.jersey.server.ResourceConfig;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class ControllerAdminApiApplicationTest {
  private static final File DATA_DIR = new File(FileUtils.getTempDirectory(), "ControllerAdminApiApplicationTest");
  private static final File LOCAL_TEMP_DIR = new File(DATA_DIR, "localTemp");

  @BeforeMethod
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(DATA_DIR);
    PinotFSFactory.init(new PinotConfiguration());

    ControllerConf controllerConf = new ControllerConf();
    controllerConf.setControllerHost("localhost");
    controllerConf.setControllerPort("12345");
    controllerConf.setDataDir(DATA_DIR.getPath());
    controllerConf.setLocalTempDir(LOCAL_TEMP_DIR.getPath());
    ControllerFilePathProvider.init(controllerConf);
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(DATA_DIR);
  }

  /// Jersey spills large multipart parts to disk and abandons them when a request fails to parse, so they must land
  /// somewhere the controller clears on restart rather than in java.io.tmpdir.
  @Test
  public void testMultiPartTempDirResolvesToControllerTempDir() {
    MultiPartProperties properties =
        new ControllerAdminApiApplication.MultiPartTempDirResolver().getContext(getClass());

    assertNotNull(properties);
    assertEquals(properties.getTempDir(),
        ControllerFilePathProvider.getInstance().getMultiPartTempDir().getAbsolutePath());
    assertEquals(properties.getTempDir(), new File(LOCAL_TEMP_DIR, "multipartTemp").getAbsolutePath());
  }

  /// Registering the resolver is only useful if Jersey actually finds it: the multipart reader looks it up through
  /// Providers, not from the application's property bag.
  @Test
  public void testJerseyResolvesTheRegisteredMultiPartProperties() {
    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.register(new ControllerAdminApiApplication.MultiPartTempDirResolver());
    ApplicationHandler handler = new ApplicationHandler(resourceConfig);

    Providers providers = handler.getInjectionManager().getInstance(Providers.class);
    assertNotNull(providers);

    // This is the exact lookup MultiPartReaderClientSide performs when it builds its MIMEConfig
    ContextResolver<MultiPartProperties> resolver =
        providers.getContextResolver(MultiPartProperties.class, MediaType.WILDCARD_TYPE);
    assertNotNull(resolver, "Jersey did not resolve the registered MultiPartProperties ContextResolver");
    assertEquals(resolver.getContext(MultiPartProperties.class).getTempDir(),
        ControllerFilePathProvider.getInstance().getMultiPartTempDir().getAbsolutePath());
  }

  /// Guards the registration itself: without it the two tests above still pass while Jersey quietly keeps spilling
  /// parts into java.io.tmpdir.
  @Test
  public void testAdminApplicationRegistersTheMultiPartTempDirResolver() {
    ControllerConf controllerConf = new ControllerConf();
    controllerConf.setControllerHost("localhost");
    controllerConf.setControllerPort("12345");
    controllerConf.setDataDir(DATA_DIR.getPath());
    controllerConf.setLocalTempDir(LOCAL_TEMP_DIR.getPath());

    ControllerAdminApiApplication application = new ControllerAdminApiApplication(controllerConf);

    assertTrue(
        application.getInstances().stream()
            .anyMatch(instance -> instance instanceof ControllerAdminApiApplication.MultiPartTempDirResolver),
        "The admin application must register a MultiPartProperties resolver, otherwise Jersey buffers multipart "
            + "uploads into java.io.tmpdir where orphaned parts are never reclaimed");
  }
}
