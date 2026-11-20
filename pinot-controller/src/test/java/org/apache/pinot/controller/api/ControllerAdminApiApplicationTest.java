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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.resources.ControllerFilePathProvider;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.glassfish.jersey.internal.MapPropertiesDelegate;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.MultiPartProperties;
import org.glassfish.jersey.server.ApplicationHandler;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.ContainerResponse;
import org.glassfish.jersey.server.ResourceConfig;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class ControllerAdminApiApplicationTest {
  private static final File DATA_DIR = new File(FileUtils.getTempDirectory(), "ControllerAdminApiApplicationTest");
  private static final File LOCAL_TEMP_DIR = new File(DATA_DIR, "localTemp");
  private static final String BOUNDARY = "PinotMultiPartTempDirTestBoundary";

  /// Comfortably past Jersey's default buffer threshold (`ReaderWriter.BUFFER_SIZE`, 8 KB), so mimepull is forced to
  /// spill the part to disk rather than keeping it in memory. A segment tar is of course far larger still.
  private static final int PART_SIZE_BYTES = 256 * 1024;

  @BeforeMethod
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(DATA_DIR);
    PinotFSFactory.init(new PinotConfiguration());
    ControllerFilePathProvider.init(newControllerConf());
    MultiPartProbeResource.reset();
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(DATA_DIR);
  }

  private static ControllerConf newControllerConf() {
    ControllerConf controllerConf = new ControllerConf();
    controllerConf.setControllerHost("localhost");
    controllerConf.setControllerPort("12345");
    controllerConf.setDataDir(DATA_DIR.getPath());
    controllerConf.setLocalTempDir(LOCAL_TEMP_DIR.getPath());
    return controllerConf;
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

  /// The load-bearing test: drives a real multipart request through a real `ApplicationHandler` with
  /// `MultiPartFeature` registered, and asserts the part was actually spilled into the controller's directory.
  ///
  /// Asserting that `Providers` merely hands back the resolver would not prove much — Jersey's multipart reader is
  /// what has to find it, and it does so once, in its own constructor. This exercises registration, the `Providers`
  /// lookup, the resulting `MIMEConfig`, and mimepull's `createTempFile` in one go.
  @Test
  public void testJerseySpillsMultiPartBodiesIntoControllerTempDir()
      throws Exception {
    File multiPartTempDir = ControllerFilePathProvider.getInstance().getMultiPartTempDir();

    ContainerResponse response = postMultiPart(newHandler());

    assertEquals(response.getStatus(), 200);
    assertTrue(MultiPartProbeResource.getObservedSpillFiles().stream().anyMatch(name -> name.startsWith("MIME")),
        "Jersey did not spill the multipart body into " + multiPartTempDir + ", it saw: "
            + MultiPartProbeResource.getObservedSpillFiles());
  }

  /// The resolver runs once at startup and mimepull holds that path for the life of the controller, so a directory
  /// that disappears underneath a running controller would otherwise fail every upload until a restart.
  @Test
  public void testMultiPartUploadSurvivesTempDirDeletion()
      throws Exception {
    ApplicationHandler handler = newHandler();
    File multiPartTempDir = ControllerFilePathProvider.getInstance().getMultiPartTempDir();

    // Stand in for a tmp sweeper, or an operator clearing controller.local.temp.dir, removing it mid-flight
    FileUtils.deleteDirectory(multiPartTempDir);
    assertFalse(multiPartTempDir.exists());

    ContainerResponse response = postMultiPart(handler);

    assertEquals(response.getStatus(), 200, "Upload failed after the multipart temporary directory was deleted");
    assertTrue(MultiPartProbeResource.getObservedSpillFiles().stream().anyMatch(name -> name.startsWith("MIME")),
        "The multipart temporary directory was not re-created, spill files seen: "
            + MultiPartProbeResource.getObservedSpillFiles());
  }

  /// Guards the registration itself: without it the tests above still pass while Jersey quietly keeps spilling parts
  /// into java.io.tmpdir.
  @Test
  public void testAdminApplicationRegistersTheMultiPartProviders() {
    ControllerAdminApiApplication application = new ControllerAdminApiApplication(newControllerConf());

    assertTrue(
        application.getInstances().stream()
            .anyMatch(instance -> instance instanceof ControllerAdminApiApplication.MultiPartTempDirResolver),
        "The admin application must register a MultiPartProperties resolver, otherwise Jersey buffers multipart "
            + "uploads into java.io.tmpdir where orphaned parts are never reclaimed");
    assertTrue(
        application.getInstances().stream()
            .anyMatch(instance -> instance instanceof ControllerAdminApiApplication.MultiPartTempDirGuard),
        "The admin application must register the multipart temporary directory guard, otherwise a directory removed "
            + "at runtime fails every upload until the controller restarts");
  }

  private static ApplicationHandler newHandler() {
    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.register(MultiPartFeature.class);
    resourceConfig.register(new ControllerAdminApiApplication.MultiPartTempDirResolver());
    resourceConfig.register(new ControllerAdminApiApplication.MultiPartTempDirGuard());
    resourceConfig.register(MultiPartProbeResource.class);
    return new ApplicationHandler(resourceConfig);
  }

  private static ContainerResponse postMultiPart(ApplicationHandler handler)
      throws Exception {
    ContainerRequest request =
        new ContainerRequest(URI.create("http://localhost/"), URI.create("http://localhost/probe"), "POST", null,
            new MapPropertiesDelegate(), handler.getConfiguration());
    request.getHeaders().add(HttpHeaders.CONTENT_TYPE, MediaType.MULTIPART_FORM_DATA + "; boundary=" + BOUNDARY);
    request.setEntityStream(new ByteArrayInputStream(multiPartBody()));
    return handler.apply(request).get();
  }

  private static byte[] multiPartBody()
      throws Exception {
    byte[] payload = new byte[PART_SIZE_BYTES];
    Arrays.fill(payload, (byte) 'x');

    ByteArrayOutputStream body = new ByteArrayOutputStream();
    body.write(("--" + BOUNDARY + "\r\n"
        + "Content-Disposition: form-data; name=\"segment\"; filename=\"segment.tar.gz\"\r\n"
        + "Content-Type: application/octet-stream\r\n\r\n").getBytes(StandardCharsets.UTF_8));
    body.write(payload);
    body.write(("\r\n--" + BOUNDARY + "--\r\n").getBytes(StandardCharsets.UTF_8));
    return body.toByteArray();
  }

  /// Reports what is sitting in the multipart temporary directory while the request is still in flight, which is the
  /// only window in which the spilled part is observable — `CloseableService` deletes it once the request ends.
  @Path("/")
  public static class MultiPartProbeResource {
    private static volatile List<String> _observedSpillFiles = List.of();

    static void reset() {
      _observedSpillFiles = List.of();
    }

    static List<String> getObservedSpillFiles() {
      return _observedSpillFiles;
    }

    @POST
    @Path("probe")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.TEXT_PLAIN)
    public String probe(FormDataMultiPart multiPart) {
      String[] children = ControllerFilePathProvider.getInstance().getMultiPartTempDir().list();
      _observedSpillFiles = children == null ? List.of() : List.of(children);
      return String.valueOf(multiPart.getBodyParts().size());
    }
  }
}
