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
package org.apache.pinot.spi.plugin;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.concurrent.Callable;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Tests for [PluginManager#loadServiceProviders(Class)]: enumeration across the thread context classloader and
/// plugin classloaders, de-duplication by provider class name, source descriptions, and fail-fast wrapping of
/// [ServiceConfigurationError].
///
/// Service descriptors are generated into temporary directories exposed through isolated [URLClassLoader]s
/// (installed as the thread context classloader) or plugin realms of a fresh [PluginManager] instance, so no
/// `META-INF/services` fixture leaks into the test classpath.
public class PluginManagerServiceProviderTest {
  private static final String SERVICE_DESCRIPTOR_PATH = "META-INF/services/" + TestDiscoveryService.class.getName();
  private static final List<Path> TEMP_DIRS = new ArrayList<>();
  private static final List<URLClassLoader> DESCRIPTOR_CLASS_LOADERS = new ArrayList<>();

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
  public void testContextClassLoaderDiscovery()
      throws Exception {
    PluginManager pluginManager = new PluginManager();
    List<PluginManager.ServiceProvider<TestDiscoveryService>> providers =
        withContextClassLoader(createDescriptorClassLoader(ImplA.class.getName()),
            () -> pluginManager.loadServiceProviders(TestDiscoveryService.class));
    assertEquals(providers.size(), 1);
    assertEquals(providers.get(0).getProvider().getClass(), ImplA.class);
    assertTrue(providers.get(0).getSource().contains("thread context classloader"), providers.get(0).getSource());
  }

  @Test
  public void testPluginClassLoaderDiscovery()
      throws Exception {
    PluginManager pluginManager = new PluginManager();
    loadDescriptorOnlyPlugin(pluginManager, "spi-service-provider-test-plugin", ImplB.class.getName());
    List<PluginManager.ServiceProvider<TestDiscoveryService>> providers =
        pluginManager.loadServiceProviders(TestDiscoveryService.class);
    assertEquals(providers.size(), 1);
    assertEquals(providers.get(0).getProvider().getClass(), ImplB.class);
    assertTrue(providers.get(0).getSource().contains("plugin classloader"), providers.get(0).getSource());
  }

  @Test
  public void testDeduplicationAcrossOverlappingClassLoaders()
      throws Exception {
    // The same provider class is listed both in the context classloader descriptor and in the plugin realm
    // descriptor; the first sighting (context classloader) wins and no duplicate is returned
    PluginManager pluginManager = new PluginManager();
    loadDescriptorOnlyPlugin(pluginManager, "spi-service-provider-dedup-plugin", ImplA.class.getName());
    List<PluginManager.ServiceProvider<TestDiscoveryService>> providers =
        withContextClassLoader(createDescriptorClassLoader(ImplA.class.getName()),
            () -> pluginManager.loadServiceProviders(TestDiscoveryService.class));
    assertEquals(providers.size(), 1);
    assertEquals(providers.get(0).getProvider().getClass(), ImplA.class);
    assertTrue(providers.get(0).getSource().contains("thread context classloader"), providers.get(0).getSource());
  }

  @Test
  public void testDiscoveryOrderContextClassLoaderFirst()
      throws Exception {
    PluginManager pluginManager = new PluginManager();
    loadDescriptorOnlyPlugin(pluginManager, "spi-service-provider-order-plugin", ImplB.class.getName());
    List<PluginManager.ServiceProvider<TestDiscoveryService>> providers =
        withContextClassLoader(createDescriptorClassLoader(ImplA.class.getName()),
            () -> pluginManager.loadServiceProviders(TestDiscoveryService.class));
    assertEquals(providers.size(), 2);
    assertEquals(providers.get(0).getProvider().getClass(), ImplA.class);
    assertEquals(providers.get(1).getProvider().getClass(), ImplB.class);
  }

  @Test
  public void testVersionSkewedDuplicateKeepsFirstDiscoveredCopy()
      throws Exception {
    // The context classloader defines its own copy of ImplA (child-first) while the plugin realm resolves the
    // test-classpath copy through parent delegation: same class name, different Class objects. The first sighting
    // (context classloader) must win and the skewed duplicate must be skipped with a WARN
    PluginManager pluginManager = new PluginManager();
    loadDescriptorOnlyPlugin(pluginManager, "spi-service-provider-skew-plugin", ImplA.class.getName());
    List<PluginManager.ServiceProvider<TestDiscoveryService>> providers =
        withContextClassLoader(createChildFirstDescriptorClassLoader(ImplA.class.getName()),
            () -> pluginManager.loadServiceProviders(TestDiscoveryService.class));
    assertEquals(providers.size(), 1);
    Class<?> providerClass = providers.get(0).getProvider().getClass();
    assertEquals(providerClass.getName(), ImplA.class.getName());
    assertNotSame(providerClass, ImplA.class);
    assertTrue(providers.get(0).getSource().contains("thread context classloader"), providers.get(0).getSource());
  }

  @Test
  public void testMalformedDescriptorFailsWithSourceAndCause()
      throws Exception {
    PluginManager pluginManager = new PluginManager();
    IllegalStateException exception = expectThrows(IllegalStateException.class,
        () -> withContextClassLoader(createDescriptorClassLoader("com.example.DoesNotExist"),
            () -> pluginManager.loadServiceProviders(TestDiscoveryService.class)));
    assertTrue(exception.getMessage().contains(TestDiscoveryService.class.getName()), exception.getMessage());
    assertTrue(exception.getMessage().contains("thread context classloader"), exception.getMessage());
    assertTrue(exception.getCause() instanceof ServiceConfigurationError, String.valueOf(exception.getCause()));
  }

  @Test
  public void testProviderConstructorFailureFailsWithCause()
      throws Exception {
    PluginManager pluginManager = new PluginManager();
    IllegalStateException exception = expectThrows(IllegalStateException.class,
        () -> withContextClassLoader(createDescriptorClassLoader(ThrowingImpl.class.getName()),
            () -> pluginManager.loadServiceProviders(TestDiscoveryService.class)));
    assertTrue(exception.getCause() instanceof ServiceConfigurationError, String.valueOf(exception.getCause()));
  }

  @Test
  public void testNoDescriptorsYieldsEmptyList() {
    PluginManager pluginManager = new PluginManager();
    assertTrue(pluginManager.loadServiceProviders(TestDiscoveryService.class).isEmpty());
  }

  /// Registers a new-style plugin realm holding only a service descriptor. The realm delegates to the pinot realm
  /// for classes (`parent.realmId=pinot`), but the descriptor itself is only visible through the plugin
  /// classloader.
  private static void loadDescriptorOnlyPlugin(PluginManager pluginManager, String pluginName,
      String providerClassName)
      throws IOException {
    File pluginDir = Files.createTempDirectory(pluginName).toFile();
    TEMP_DIRS.add(pluginDir.toPath());
    Files.writeString(new File(pluginDir, "pinot-plugin.properties").toPath(), "parent.realmId=pinot\n");
    try (JarOutputStream jarOutputStream = new JarOutputStream(
        new FileOutputStream(new File(pluginDir, "service-descriptor.jar")))) {
      jarOutputStream.putNextEntry(new JarEntry(SERVICE_DESCRIPTOR_PATH));
      jarOutputStream.write((providerClassName + "\n").getBytes(StandardCharsets.UTF_8));
      jarOutputStream.closeEntry();
    }
    pluginManager.load(pluginName, pluginDir);
  }

  private static ClassLoader createDescriptorClassLoader(String... descriptorLines)
      throws IOException {
    Path tempDir = Files.createTempDirectory("spi-service-provider-descriptor");
    TEMP_DIRS.add(tempDir);
    Path descriptorFile = tempDir.resolve(SERVICE_DESCRIPTOR_PATH);
    Files.createDirectories(descriptorFile.getParent());
    Files.write(descriptorFile, List.of(descriptorLines), StandardCharsets.UTF_8);
    URLClassLoader descriptorClassLoader = new URLClassLoader(new URL[]{tempDir.toUri().toURL()},
        PluginManagerServiceProviderTest.class.getClassLoader());
    DESCRIPTOR_CLASS_LOADERS.add(descriptorClassLoader);
    return descriptorClassLoader;
  }

  /// Like [#createDescriptorClassLoader(String...)], but the returned loader additionally defines its own copy of
  /// the provider class from the test-classpath bytes instead of delegating to the parent, simulating a
  /// classloader shipping its own (version-skewed) copy of the class.
  private static ClassLoader createChildFirstDescriptorClassLoader(String providerClassName)
      throws IOException {
    Path tempDir = Files.createTempDirectory("spi-service-provider-descriptor");
    TEMP_DIRS.add(tempDir);
    Path descriptorFile = tempDir.resolve(SERVICE_DESCRIPTOR_PATH);
    Files.createDirectories(descriptorFile.getParent());
    Files.write(descriptorFile, List.of(providerClassName), StandardCharsets.UTF_8);
    URLClassLoader descriptorClassLoader = new ChildFirstClassLoader(new URL[]{tempDir.toUri().toURL()},
        PluginManagerServiceProviderTest.class.getClassLoader(), providerClassName);
    DESCRIPTOR_CLASS_LOADERS.add(descriptorClassLoader);
    return descriptorClassLoader;
  }

  /// Classloader that defines its own copy of one class from the parent's class-file bytes instead of delegating,
  /// producing a Class object with the same name but a different defining classloader. Everything else (including
  /// the service interface) is delegated to the parent so the copy remains a valid provider.
  private static class ChildFirstClassLoader extends URLClassLoader {
    private final String _childFirstClassName;

    ChildFirstClassLoader(URL[] urls, ClassLoader parent, String childFirstClassName) {
      super(urls, parent);
      _childFirstClassName = childFirstClassName;
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve)
        throws ClassNotFoundException {
      if (!name.equals(_childFirstClassName)) {
        return super.loadClass(name, resolve);
      }
      synchronized (getClassLoadingLock(name)) {
        Class<?> loaded = findLoadedClass(name);
        if (loaded == null) {
          try (InputStream inputStream = getParent().getResourceAsStream(name.replace('.', '/') + ".class")) {
            byte[] classBytes = inputStream.readAllBytes();
            loaded = defineClass(name, classBytes, 0, classBytes.length);
          } catch (IOException e) {
            throw new ClassNotFoundException(name, e);
          }
        }
        if (resolve) {
          resolveClass(loaded);
        }
        return loaded;
      }
    }
  }

  private static <T> T withContextClassLoader(ClassLoader classLoader, Callable<T> callable)
      throws Exception {
    Thread currentThread = Thread.currentThread();
    ClassLoader originalClassLoader = currentThread.getContextClassLoader();
    currentThread.setContextClassLoader(classLoader);
    try {
      return callable.call();
    } finally {
      currentThread.setContextClassLoader(originalClassLoader);
    }
  }

  /// Service type used only by this test; no descriptor for it exists on the test classpath.
  public interface TestDiscoveryService {
  }

  public static class ImplA implements TestDiscoveryService {
  }

  public static class ImplB implements TestDiscoveryService {
  }

  public static class ThrowingImpl implements TestDiscoveryService {
    public ThrowingImpl() {
      throw new IllegalStateException("Intentional constructor failure");
    }
  }
}
