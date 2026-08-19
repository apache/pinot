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
package org.apache.pinot.server.starter;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.operator.transform.function.AdditionTransformFunction;
import org.apache.pinot.core.operator.transform.function.BaseTransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunctionFactory;
import org.apache.pinot.server.conf.ServerConf;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Verifies the [ServerInstance] transform function initialization path: a [TransformFunction] service provider on
/// the classpath registers through `ServiceLoader` discovery without `pinot.server.transforms`, while the legacy
/// `pinot.server.transforms` configuration keeps working.
///
/// The service descriptor is generated into a temporary directory exposed through an isolated [URLClassLoader]
/// (installed as the thread context classloader), so no `META-INF/services` fixture leaks into the test classpath.
public class ServerInstanceTransformFunctionTest {
  private static final String SERVICE_DESCRIPTOR_PATH = "META-INF/services/" + TransformFunction.class.getName();
  private static final List<Path> TEMP_DIRS = new ArrayList<>();
  private static final List<URLClassLoader> DESCRIPTOR_CLASS_LOADERS = new ArrayList<>();

  @AfterMethod
  public void resetRegistry() {
    // Drop any registration made by the previous test method
    TransformFunctionFactory.init(Set.of());
  }

  @AfterClass
  public void tearDownDescriptorFixtures()
      throws IOException {
    for (URLClassLoader descriptorClassLoader : DESCRIPTOR_CLASS_LOADERS) {
      descriptorClassLoader.close();
    }
    for (Path tempDir : TEMP_DIRS) {
      FileUtils.deleteQuietly(tempDir.toFile());
    }
  }

  @Test
  public void testServiceProvidedTransformRegisteredWithoutConfig()
      throws Exception {
    ServerConf serverConf = new ServerConf(new PinotConfiguration());
    assertTrue(serverConf.getTransformFunctions().isEmpty(), "pinot.server.transforms must not be set");

    Thread currentThread = Thread.currentThread();
    ClassLoader originalClassLoader = currentThread.getContextClassLoader();
    currentThread.setContextClassLoader(
        createDescriptorClassLoader(ServiceProvidedTransformFunction.class.getName()));
    try {
      ServerInstance.initTransformFunctions(serverConf);
    } finally {
      currentThread.setContextClassLoader(originalClassLoader);
    }

    assertSame(TransformFunctionFactory.getAllFunctions().get("serviceprovidedtransform"),
        ServiceProvidedTransformFunction.class);
    // Built-ins remain registered
    assertSame(TransformFunctionFactory.getAllFunctions().get("add"), AdditionTransformFunction.class);
  }

  @Test
  public void testLegacyTransformFunctionConfig() {
    ServerConf serverConf = new ServerConf(new PinotConfiguration(
        Map.of(CommonConstants.Server.CONFIG_OF_TRANSFORM_FUNCTIONS, ConfiguredTransformFunction.class.getName())));
    ServerInstance.initTransformFunctions(serverConf);
    assertSame(TransformFunctionFactory.getAllFunctions().get("configuredtransform"),
        ConfiguredTransformFunction.class);
  }

  @Test
  public void testMissingConfiguredTransformFunctionClassFails() {
    ServerConf serverConf = new ServerConf(new PinotConfiguration(
        Map.of(CommonConstants.Server.CONFIG_OF_TRANSFORM_FUNCTIONS, "com.example.DoesNotExist")));
    Map<String, Class<? extends TransformFunction>> registryBefore = TransformFunctionFactory.getAllFunctions();
    RuntimeException exception =
        expectThrows(RuntimeException.class, () -> ServerInstance.initTransformFunctions(serverConf));
    assertTrue(exception.getMessage().contains("com.example.DoesNotExist"), exception.getMessage());
    assertTrue(exception.getCause() instanceof ClassNotFoundException,
        String.valueOf(exception.getCause()));
    // The failed init must leave the previously published registry snapshot untouched
    assertSame(TransformFunctionFactory.getAllFunctions(), registryBefore);
  }

  private static ClassLoader createDescriptorClassLoader(String... descriptorLines)
      throws IOException {
    Path tempDir = Files.createTempDirectory("server-transform-function-descriptor");
    TEMP_DIRS.add(tempDir);
    Path descriptorFile = tempDir.resolve(SERVICE_DESCRIPTOR_PATH);
    Files.createDirectories(descriptorFile.getParent());
    Files.write(descriptorFile, List.of(descriptorLines), StandardCharsets.UTF_8);
    URLClassLoader descriptorClassLoader = new URLClassLoader(new URL[]{tempDir.toUri().toURL()},
        ServerInstanceTransformFunctionTest.class.getClassLoader());
    DESCRIPTOR_CLASS_LOADERS.add(descriptorClassLoader);
    return descriptorClassLoader;
  }

  /// Provider registered through the generated service descriptor.
  public static class ServiceProvidedTransformFunction extends BaseTransformFunction {
    @Override
    public String getName() {
      return "Service_Provided_Transform";
    }

    @Override
    public TransformResultMetadata getResultMetadata() {
      return INT_SV_NO_DICTIONARY_METADATA;
    }
  }

  /// Provider registered through the legacy `pinot.server.transforms` configuration.
  public static class ConfiguredTransformFunction extends BaseTransformFunction {
    @Override
    public String getName() {
      return "Configured_Transform";
    }

    @Override
    public TransformResultMetadata getResultMetadata() {
      return INT_SV_NO_DICTIONARY_METADATA;
    }
  }
}
