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
package org.apache.pinot.core.operator.transform.function;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.spi.plugin.ChildFirstClassLoader;
import org.apache.pinot.spi.plugin.PluginManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Tests for [TransformFunctionFactory], with a focus on `ServiceLoader` discovery of external
/// [TransformFunction] implementations.
///
/// Service descriptors are generated into temporary directories and exposed through isolated
/// [URLClassLoader]s (installed as the thread context classloader) or through a [PluginManager]
/// plugin realm, so no `META-INF/services` fixture leaks into the test classpath of this or any
/// downstream module.
public class TransformFunctionFactoryTest extends BaseTransformFunctionTest {
  private static final String SERVICE_DESCRIPTOR_PATH = "META-INF/services/" + TransformFunction.class.getName();
  private static final String PLUGIN_NAME = "transform-function-factory-test-plugin";
  private static final List<Path> TEMP_DIRS = new ArrayList<>();
  private static final List<URLClassLoader> DESCRIPTOR_CLASS_LOADERS = new ArrayList<>();

  private Map<String, Class<? extends TransformFunction>> _builtInFunctions;

  @BeforeClass
  public void setUpServiceDiscoveryFixtures()
      throws Exception {
    // Captured before any init() call, so this is exactly the built-in registry
    _builtInFunctions = Map.copyOf(TransformFunctionFactory.getAllFunctions());

    // Register a new-style plugin realm holding only a service descriptor for
    // PluginServiceTransformFunction. The realm delegates to the pinot realm for classes
    // (parent.realmId=pinot), but the descriptor itself is only visible through the plugin
    // classloader, so discovering the function proves the plugin classloaders are enumerated.
    File pluginDir = Files.createTempDirectory(PLUGIN_NAME).toFile();
    TEMP_DIRS.add(pluginDir.toPath());
    Files.writeString(new File(pluginDir, "pinot-plugin.properties").toPath(), "parent.realmId=pinot\n");
    try (JarOutputStream jarOutputStream = new JarOutputStream(
        new FileOutputStream(new File(pluginDir, "transform-functions.jar")))) {
      jarOutputStream.putNextEntry(new JarEntry(SERVICE_DESCRIPTOR_PATH));
      jarOutputStream.write((PluginServiceTransformFunction.class.getName() + "\n").getBytes(StandardCharsets.UTF_8));
      jarOutputStream.closeEntry();
    }
    PluginManager.get().load(PLUGIN_NAME, pluginDir);
  }

  @AfterMethod
  public void resetRegistry() {
    // Re-initializing without any context descriptor restores the built-ins (plus the plugin realm
    // function) and drops any registration or override made by the previous test method
    TransformFunctionFactory.init(Set.of());
  }

  @AfterClass
  public void tearDownServiceDiscoveryFixtures()
      throws IOException {
    // Closing is safe: registered fixture classes were loaded by the parent (test) classloader, the isolated
    // loaders only served descriptor resources
    for (URLClassLoader descriptorClassLoader : DESCRIPTOR_CLASS_LOADERS) {
      descriptorClassLoader.close();
    }
    for (Path tempDir : TEMP_DIRS) {
      FileUtils.deleteQuietly(tempDir.toFile());
    }
  }

  @Test
  public void testApplicationClasspathDiscovery()
      throws Exception {
    initWithContextClassLoader(createDescriptorClassLoader(DiscoveredServiceTransformFunction.class.getName()),
        Set.of());
    assertSame(TransformFunctionFactory.getAllFunctions().get("discoveredservicetransform"),
        DiscoveredServiceTransformFunction.class);
  }

  @Test
  public void testDiscoveredFunctionResolutionAndEvaluation()
      throws Exception {
    initWithContextClassLoader(createDescriptorClassLoader(DiscoveredServiceTransformFunction.class.getName()),
        Set.of());
    ExpressionContext expression =
        RequestContextUtils.getExpression(String.format("discovered_service_transform(%s)", INT_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    assertTrue(transformFunction instanceof DiscoveredServiceTransformFunction);
    int[] expectedValues = new int[NUM_ROWS];
    for (int i = 0; i < NUM_ROWS; i++) {
      expectedValues[i] = _intSVValues[i] + 1;
    }
    testTransformFunction(transformFunction, expectedValues);
  }

  @Test
  public void testCanonicalization()
      throws Exception {
    initWithContextClassLoader(createDescriptorClassLoader(DiscoveredServiceTransformFunction.class.getName()),
        Set.of());
    // getName() returns "Discovered_Service_Transform"; the registry key is the canonical form
    assertTrue(TransformFunctionFactory.getAllFunctions().containsKey("discoveredservicetransform"));
    // Any case/underscore variant of the name resolves through the factory
    for (String variant : new String[]{
        "DISCOVERED_SERVICE_TRANSFORM", "discoveredServiceTransform", "Discovered_Service_Transform"
    }) {
      ExpressionContext expression =
          RequestContextUtils.getExpression(String.format("%s(%s)", variant, INT_SV_COLUMN));
      assertTrue(TransformFunctionFactory.get(expression, _dataSourceMap) instanceof DiscoveredServiceTransformFunction,
          "Failed to resolve variant: " + variant);
    }
  }

  @Test
  public void testPluginClassLoaderDiscovery() {
    TransformFunctionFactory.init(Set.of());
    assertSame(TransformFunctionFactory.getAllFunctions().get("pluginrealmservicetransform"),
        PluginServiceTransformFunction.class);
  }

  @Test
  public void testOverlappingClassLoaderDeduplication()
      throws Exception {
    // The same implementation class is visible both through the application classpath descriptor
    // (context classloader) and through the plugin realm descriptor; it must be registered once
    // without raising a collision
    initWithContextClassLoader(createDescriptorClassLoader(PluginServiceTransformFunction.class.getName()), Set.of());
    assertSame(TransformFunctionFactory.getAllFunctions().get("pluginrealmservicetransform"),
        PluginServiceTransformFunction.class);
  }

  @Test
  public void testRepeatedInitIdempotent()
      throws Exception {
    ClassLoader descriptorClassLoader =
        createDescriptorClassLoader(DiscoveredServiceTransformFunction.class.getName());
    initWithContextClassLoader(descriptorClassLoader, Set.of());
    Map<String, Class<? extends TransformFunction>> firstSnapshot = TransformFunctionFactory.getAllFunctions();
    initWithContextClassLoader(descriptorClassLoader, Set.of());
    Map<String, Class<? extends TransformFunction>> secondSnapshot = TransformFunctionFactory.getAllFunctions();
    assertEquals(secondSnapshot, firstSnapshot);
    assertSame(secondSnapshot.get("discoveredservicetransform"), DiscoveredServiceTransformFunction.class);
  }

  @Test
  public void testCollisionBetweenDiscoveredProviders()
      throws Exception {
    IllegalStateException exception = expectThrows(IllegalStateException.class,
        () -> initWithContextClassLoader(
            createDescriptorClassLoader(DuplicateNameServiceTransformFunctionA.class.getName(),
                DuplicateNameServiceTransformFunctionB.class.getName()), Set.of()));
    assertTrue(exception.getMessage().contains("duplicateservicename"), exception.getMessage());
    assertTrue(exception.getMessage().contains(DuplicateNameServiceTransformFunctionA.class.getName()),
        exception.getMessage());
    assertTrue(exception.getMessage().contains(DuplicateNameServiceTransformFunctionB.class.getName()),
        exception.getMessage());
    assertTrue(exception.getMessage().contains("classloader"), exception.getMessage());
    // The failed init must not have published a partial registry
    assertNull(TransformFunctionFactory.getAllFunctions().get("duplicateservicename"));
    assertSame(TransformFunctionFactory.getAllFunctions().get("add"), AdditionTransformFunction.class);
  }

  @Test
  public void testCollisionWithBuiltIn()
      throws Exception {
    IllegalStateException exception = expectThrows(IllegalStateException.class,
        () -> initWithContextClassLoader(
            createDescriptorClassLoader(BuiltInNameServiceTransformFunction.class.getName()), Set.of()));
    assertTrue(exception.getMessage().contains("add"), exception.getMessage());
    assertTrue(exception.getMessage().contains("built-in"), exception.getMessage());
    assertTrue(exception.getMessage().contains(BuiltInNameServiceTransformFunction.class.getName()),
        exception.getMessage());
    assertTrue(exception.getMessage().contains(AdditionTransformFunction.class.getName()), exception.getMessage());
    // The built-in registration is untouched
    assertSame(TransformFunctionFactory.getAllFunctions().get("add"), AdditionTransformFunction.class);
  }

  @Test
  public void testMalformedDescriptorMissingClass()
      throws Exception {
    IllegalStateException exception = expectThrows(IllegalStateException.class,
        () -> initWithContextClassLoader(createDescriptorClassLoader("com.example.DoesNotExist"), Set.of()));
    assertTrue(exception.getCause() instanceof ServiceConfigurationError, String.valueOf(exception.getCause()));
  }

  @Test
  public void testMalformedDescriptorInvalidSyntax()
      throws Exception {
    IllegalStateException exception = expectThrows(IllegalStateException.class,
        () -> initWithContextClassLoader(createDescriptorClassLoader("not a valid class name"), Set.of()));
    assertTrue(exception.getCause() instanceof ServiceConfigurationError, String.valueOf(exception.getCause()));
  }

  @Test
  public void testDescriptorClassNotATransformFunction()
      throws Exception {
    IllegalStateException exception = expectThrows(IllegalStateException.class,
        () -> initWithContextClassLoader(createDescriptorClassLoader(NotATransformFunction.class.getName()),
            Set.of()));
    assertTrue(exception.getCause() instanceof ServiceConfigurationError, String.valueOf(exception.getCause()));
  }

  @Test
  public void testProviderConstructorFailure()
      throws Exception {
    IllegalStateException exception = expectThrows(IllegalStateException.class,
        () -> initWithContextClassLoader(
            createDescriptorClassLoader(ThrowingConstructorTransformFunction.class.getName()), Set.of()));
    // The original cause must be preserved through the ServiceConfigurationError
    Throwable cause = exception.getCause();
    assertTrue(cause instanceof ServiceConfigurationError, String.valueOf(cause));
    boolean foundOriginalCause = false;
    for (Throwable t = cause; t != null; t = t.getCause()) {
      if ("Intentional constructor failure".equals(t.getMessage())) {
        foundOriginalCause = true;
        break;
      }
    }
    assertTrue(foundOriginalCause, "Original constructor failure not preserved in cause chain");
  }

  @Test
  public void testProviderInaccessibleConstructor()
      throws Exception {
    IllegalStateException exception = expectThrows(IllegalStateException.class,
        () -> initWithContextClassLoader(
            createDescriptorClassLoader(PrivateConstructorTransformFunction.class.getName()), Set.of()));
    assertTrue(exception.getCause() instanceof ServiceConfigurationError, String.valueOf(exception.getCause()));
  }

  @Test
  public void testNullFunctionName()
      throws Exception {
    IllegalStateException exception = expectThrows(IllegalStateException.class,
        () -> initWithContextClassLoader(createDescriptorClassLoader(NullNameServiceTransformFunction.class.getName()),
            Set.of()));
    assertTrue(exception.getMessage().contains(NullNameServiceTransformFunction.class.getName()),
        exception.getMessage());
    assertTrue(exception.getMessage().contains("null"), exception.getMessage());
  }

  @Test
  public void testBlankFunctionName()
      throws Exception {
    IllegalStateException exception = expectThrows(IllegalStateException.class,
        () -> initWithContextClassLoader(createDescriptorClassLoader(BlankNameServiceTransformFunction.class.getName()),
            Set.of()));
    assertTrue(exception.getMessage().contains(BlankNameServiceTransformFunction.class.getName()),
        exception.getMessage());
    assertTrue(exception.getMessage().contains("blank"), exception.getMessage());
  }

  @Test
  public void testBuiltInsRemainAvailable()
      throws Exception {
    initWithContextClassLoader(createDescriptorClassLoader(DiscoveredServiceTransformFunction.class.getName()),
        Set.of());
    Map<String, Class<? extends TransformFunction>> registry = TransformFunctionFactory.getAllFunctions();
    for (Map.Entry<String, Class<? extends TransformFunction>> builtIn : _builtInFunctions.entrySet()) {
      assertSame(registry.get(builtIn.getKey()), builtIn.getValue(),
          "Built-in function lost after discovery: " + builtIn.getKey());
    }
    // Built-ins remain resolvable and evaluable through the factory
    ExpressionContext expression =
        RequestContextUtils.getExpression(String.format("add(%s,%s)", INT_SV_COLUMN, INT_SV_COLUMN));
    assertTrue(TransformFunctionFactory.get(expression, _dataSourceMap) instanceof AdditionTransformFunction);
  }

  @Test
  public void testLegacyExplicitRegistrationAndOverride() {
    // Legacy pinot.server.transforms style registration: no service descriptor involved
    TransformFunctionFactory.init(Set.of(asTransformFunctionClass(LegacyConfiguredTransformFunction.class)));
    assertSame(TransformFunctionFactory.getAllFunctions().get("legacyconfiguredtransform"),
        LegacyConfiguredTransformFunction.class);

    // Explicitly configured classes keep their historical ability to override built-ins
    TransformFunctionFactory.init(Set.of(asTransformFunctionClass(AddOverrideTransformFunction.class)));
    assertSame(TransformFunctionFactory.getAllFunctions().get("add"), AddOverrideTransformFunction.class);

    // Re-initializing without the explicit class restores the built-in
    TransformFunctionFactory.init(Set.of());
    assertSame(TransformFunctionFactory.getAllFunctions().get("add"), AdditionTransformFunction.class);
  }

  @Test
  public void testExplicitConfigurationOverridesDiscovered()
      throws Exception {
    // The explicitly configured class claims the same canonical name as the discovered provider and
    // is applied after discovery, so it must win without raising a collision
    initWithContextClassLoader(createDescriptorClassLoader(DiscoveredServiceTransformFunction.class.getName()),
        Set.of(asTransformFunctionClass(ExplicitOverrideTransformFunction.class)));
    assertSame(TransformFunctionFactory.getAllFunctions().get("discoveredservicetransform"),
        ExplicitOverrideTransformFunction.class);
  }

  @Test
  public void testExplicitlyConfiguredClassExemptFromDiscoveryCollision()
      throws Exception {
    // The same class is visible through a service descriptor AND explicitly configured to override a built-in.
    // Discovery must skip it (instead of failing on the built-in collision) so the explicit registration keeps its
    // historical override semantics
    initWithContextClassLoader(createDescriptorClassLoader(AddOverrideTransformFunction.class.getName()),
        Set.of(asTransformFunctionClass(AddOverrideTransformFunction.class)));
    assertSame(TransformFunctionFactory.getAllFunctions().get("add"), AddOverrideTransformFunction.class);
  }

  @Test
  public void testAbstractProviderClass()
      throws Exception {
    IllegalStateException exception = expectThrows(IllegalStateException.class,
        () -> initWithContextClassLoader(
            createDescriptorClassLoader(AbstractServiceTransformFunction.class.getName()), Set.of()));
    assertTrue(exception.getCause() instanceof ServiceConfigurationError, String.valueOf(exception.getCause()));
  }

  @Test
  public void testScalarFunctionNameShadowingAllowed()
      throws Exception {
    // Precondition: "reverse" is an existing scalar function
    assertTrue(FunctionRegistry.contains(FunctionRegistry.canonicalize("reverse")));
    // Shadowing a scalar function name is allowed (WARN only, not a collision): providing a block-oriented
    // implementation of a scalar function is the same pattern the built-ins use
    initWithContextClassLoader(createDescriptorClassLoader(ScalarShadowTransformFunction.class.getName()), Set.of());
    assertSame(TransformFunctionFactory.getAllFunctions().get("reverse"), ScalarShadowTransformFunction.class);
  }

  @Test
  public void testServiceRegisteredBuiltInClassIsNoOp()
      throws Exception {
    // A descriptor shipping a class that is already registered under the same canonical name (here: the built-in
    // itself) is a no-op rather than a collision
    initWithContextClassLoader(createDescriptorClassLoader(AdditionTransformFunction.class.getName()), Set.of());
    assertSame(TransformFunctionFactory.getAllFunctions().get("add"), AdditionTransformFunction.class);
  }

  @Test
  public void testVersionSkewedDuplicateKeepsFirstDiscoveredCopy()
      throws Exception {
    // The context classloader defines its own copy of PluginServiceTransformFunction (child-first) while the
    // plugin realm resolves the test-classpath copy through parent delegation: same class name, different Class
    // objects. End to end, the duplicate registration must be skipped, keeping the first discovered copy
    initWithContextClassLoader(
        createChildFirstDescriptorClassLoader(PluginServiceTransformFunction.class.getName()), Set.of());
    Class<? extends TransformFunction> registered =
        TransformFunctionFactory.getAllFunctions().get("pluginrealmservicetransform");
    assertNotNull(registered);
    assertEquals(registered.getName(), PluginServiceTransformFunction.class.getName());
    assertNotSame(registered, PluginServiceTransformFunction.class);
  }

  @Test
  public void testVersionSkewedCopyOfBuiltInIsIgnored()
      throws Exception {
    // A discovered provider that is a different-classloader copy of an already-registered built-in class (e.g. a
    // fat plugin jar bundling pinot-core classes) must be skipped with a WARN — neither a collision failure nor an
    // override — keeping the built-in registration
    initWithContextClassLoader(
        createChildFirstDescriptorClassLoader(AdditionTransformFunction.class.getName()), Set.of());
    assertSame(TransformFunctionFactory.getAllFunctions().get("add"), AdditionTransformFunction.class);
  }

  @Test
  public void testRegistrationDoesNotInitOrEvaluateFunction()
      throws Exception {
    int constructionsBefore = DiscoveredServiceTransformFunction.CONSTRUCTION_COUNT.get();
    int initCallsBefore = DiscoveredServiceTransformFunction.INIT_COUNT.get();
    int evalCallsBefore = DiscoveredServiceTransformFunction.EVAL_COUNT.get();
    initWithContextClassLoader(createDescriptorClassLoader(DiscoveredServiceTransformFunction.class.getName()),
        Set.of());
    // Discovery constructs an instance to read its name, but never calls init() or evaluates it
    assertTrue(DiscoveredServiceTransformFunction.CONSTRUCTION_COUNT.get() > constructionsBefore);
    assertEquals(DiscoveredServiceTransformFunction.INIT_COUNT.get(), initCallsBefore);
    assertEquals(DiscoveredServiceTransformFunction.EVAL_COUNT.get(), evalCallsBefore);

    // Query execution constructs a fresh instance through the factory and initializes/evaluates it
    ExpressionContext expression =
        RequestContextUtils.getExpression(String.format("discovered_service_transform(%s)", INT_SV_COLUMN));
    TransformFunction transformFunction = TransformFunctionFactory.get(expression, _dataSourceMap);
    transformFunction.transformToIntValuesSV(_projectionBlock);
    assertEquals(DiscoveredServiceTransformFunction.INIT_COUNT.get(), initCallsBefore + 1);
    assertEquals(DiscoveredServiceTransformFunction.EVAL_COUNT.get(), evalCallsBefore + 1);
  }

  @Test
  public void testNoPartialRegistryPublicationUnderConcurrentReads()
      throws Exception {
    Set<String> builtInNames = _builtInFunctions.keySet();
    int numReaders = 4;
    ExecutorService executorService = Executors.newFixedThreadPool(numReaders);
    AtomicBoolean stopped = new AtomicBoolean();
    try {
      List<Future<?>> readers = new ArrayList<>(numReaders);
      for (int i = 0; i < numReaders; i++) {
        readers.add(executorService.submit(() -> {
          while (!stopped.get()) {
            Map<String, Class<? extends TransformFunction>> snapshot = TransformFunctionFactory.getAllFunctions();
            assertTrue(snapshot.keySet().containsAll(builtInNames),
                "Observed a registry snapshot missing built-in functions");
            assertSame(snapshot.get("add"), AdditionTransformFunction.class);
          }
        }));
      }
      ClassLoader descriptorClassLoader =
          createDescriptorClassLoader(DiscoveredServiceTransformFunction.class.getName());
      for (int i = 0; i < 100; i++) {
        initWithContextClassLoader(descriptorClassLoader, Set.of());
        TransformFunctionFactory.init(Set.of());
      }
      stopped.set(true);
      for (Future<?> reader : readers) {
        // Propagates any assertion failure from the reader threads
        reader.get(30, TimeUnit.SECONDS);
      }
    } finally {
      stopped.set(true);
      executorService.shutdownNow();
    }
  }

  @Test
  public void testGetAllFunctionsIsImmutable() {
    TransformFunctionFactory.init(Set.of());
    expectThrows(UnsupportedOperationException.class,
        () -> TransformFunctionFactory.getAllFunctions().put("foo", AdditionTransformFunction.class));
  }

  /// Writes a `META-INF/services` descriptor for [TransformFunction] listing the given lines into a
  /// fresh temporary directory, and returns an isolated [URLClassLoader] exposing it. The loader
  /// delegates to the test classpath for classes, so fixture classes resolve while the descriptor
  /// stays invisible to other tests and modules.
  private static ClassLoader createDescriptorClassLoader(String... descriptorLines)
      throws IOException {
    Path tempDir = Files.createTempDirectory("transform-function-descriptor");
    TEMP_DIRS.add(tempDir);
    Path descriptorFile = tempDir.resolve(SERVICE_DESCRIPTOR_PATH);
    Files.createDirectories(descriptorFile.getParent());
    Files.write(descriptorFile, List.of(descriptorLines), StandardCharsets.UTF_8);
    URLClassLoader descriptorClassLoader = new URLClassLoader(new URL[]{tempDir.toUri().toURL()},
        TransformFunctionFactoryTest.class.getClassLoader());
    DESCRIPTOR_CLASS_LOADERS.add(descriptorClassLoader);
    return descriptorClassLoader;
  }

  /// Like [#createDescriptorClassLoader(String...)], but the returned loader additionally defines its own copy of
  /// the provider class from the test-classpath bytes instead of delegating to the parent (via the shared
  /// [ChildFirstClassLoader] test utility), simulating a classloader shipping its own (version-skewed) copy of the
  /// class.
  private static ClassLoader createChildFirstDescriptorClassLoader(String providerClassName)
      throws IOException {
    Path tempDir = Files.createTempDirectory("transform-function-descriptor");
    TEMP_DIRS.add(tempDir);
    Path descriptorFile = tempDir.resolve(SERVICE_DESCRIPTOR_PATH);
    Files.createDirectories(descriptorFile.getParent());
    Files.write(descriptorFile, List.of(providerClassName), StandardCharsets.UTF_8);
    URLClassLoader descriptorClassLoader = new ChildFirstClassLoader(new URL[]{tempDir.toUri().toURL()},
        TransformFunctionFactoryTest.class.getClassLoader(), providerClassName);
    DESCRIPTOR_CLASS_LOADERS.add(descriptorClassLoader);
    return descriptorClassLoader;
  }

  /// Runs [TransformFunctionFactory#init(Set)] with the given thread context classloader installed,
  /// exercising the production application-classpath discovery path (`ServiceLoader.load` uses the
  /// context classloader), and restores the original context classloader afterwards.
  private static void initWithContextClassLoader(ClassLoader classLoader,
      Set<Class<TransformFunction>> transformFunctionClasses) {
    Thread currentThread = Thread.currentThread();
    ClassLoader originalClassLoader = currentThread.getContextClassLoader();
    currentThread.setContextClassLoader(classLoader);
    try {
      TransformFunctionFactory.init(transformFunctionClasses);
    } finally {
      currentThread.setContextClassLoader(originalClassLoader);
    }
  }

  @SuppressWarnings("unchecked")
  private static Class<TransformFunction> asTransformFunctionClass(Class<? extends TransformFunction> clazz) {
    return (Class<TransformFunction>) clazz;
  }

  // ------------------------------------------------------------------------------------------------------
  // Service provider fixtures. These are regular test classes with no META-INF/services descriptor on the
  // classpath; tests reference them from generated descriptors in isolated classloaders.
  // ------------------------------------------------------------------------------------------------------

  /// Valid provider used for happy-path discovery, canonicalization and evaluation tests. Returns the
  /// int argument plus one.
  public static class DiscoveredServiceTransformFunction extends BaseTransformFunction {
    static final AtomicInteger CONSTRUCTION_COUNT = new AtomicInteger();
    static final AtomicInteger INIT_COUNT = new AtomicInteger();
    static final AtomicInteger EVAL_COUNT = new AtomicInteger();

    public DiscoveredServiceTransformFunction() {
      CONSTRUCTION_COUNT.incrementAndGet();
    }

    @Override
    public String getName() {
      return "Discovered_Service_Transform";
    }

    @Override
    public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
      INIT_COUNT.incrementAndGet();
      super.init(arguments, columnContextMap);
    }

    @Override
    public TransformResultMetadata getResultMetadata() {
      return INT_SV_NO_DICTIONARY_METADATA;
    }

    @Override
    public int[] transformToIntValuesSV(ValueBlock valueBlock) {
      EVAL_COUNT.incrementAndGet();
      int length = valueBlock.getNumDocs();
      initIntValuesSV(length);
      int[] values = _arguments.get(0).transformToIntValuesSV(valueBlock);
      for (int i = 0; i < length; i++) {
        _intValuesSV[i] = values[i] + 1;
      }
      return _intValuesSV;
    }
  }

  /// Valid provider registered through the plugin realm descriptor.
  public static class PluginServiceTransformFunction extends BaseTransformFunction {
    @Override
    public String getName() {
      return "Plugin_Realm_Service_Transform";
    }

    @Override
    public TransformResultMetadata getResultMetadata() {
      return INT_SV_NO_DICTIONARY_METADATA;
    }
  }

  /// Explicitly configured provider claiming the same canonical name as
  /// [DiscoveredServiceTransformFunction].
  public static class ExplicitOverrideTransformFunction extends BaseTransformFunction {
    @Override
    public String getName() {
      return "discoveredServiceTransform";
    }

    @Override
    public TransformResultMetadata getResultMetadata() {
      return INT_SV_NO_DICTIONARY_METADATA;
    }
  }

  /// Legacy `pinot.server.transforms` style provider.
  public static class LegacyConfiguredTransformFunction extends BaseTransformFunction {
    @Override
    public String getName() {
      return "Legacy_Configured_Transform";
    }

    @Override
    public TransformResultMetadata getResultMetadata() {
      return INT_SV_NO_DICTIONARY_METADATA;
    }
  }

  /// Explicitly configured provider overriding the built-in `add` function.
  public static class AddOverrideTransformFunction extends BaseTransformFunction {
    @Override
    public String getName() {
      return "add";
    }

    @Override
    public TransformResultMetadata getResultMetadata() {
      return INT_SV_NO_DICTIONARY_METADATA;
    }
  }

  public static class DuplicateNameServiceTransformFunctionA extends BaseTransformFunction {
    @Override
    public String getName() {
      return "Duplicate_Service_Name";
    }

    @Override
    public TransformResultMetadata getResultMetadata() {
      return INT_SV_NO_DICTIONARY_METADATA;
    }
  }

  public static class DuplicateNameServiceTransformFunctionB extends BaseTransformFunction {
    @Override
    public String getName() {
      return "duplicateServiceName";
    }

    @Override
    public TransformResultMetadata getResultMetadata() {
      return INT_SV_NO_DICTIONARY_METADATA;
    }
  }

  /// Provider whose name collides with the built-in `add` function.
  public static class BuiltInNameServiceTransformFunction extends BaseTransformFunction {
    @Override
    public String getName() {
      return "ADD";
    }

    @Override
    public TransformResultMetadata getResultMetadata() {
      return INT_SV_NO_DICTIONARY_METADATA;
    }
  }

  public static class NullNameServiceTransformFunction extends BaseTransformFunction {
    @Override
    public String getName() {
      return null;
    }

    @Override
    public TransformResultMetadata getResultMetadata() {
      return INT_SV_NO_DICTIONARY_METADATA;
    }
  }

  public static class BlankNameServiceTransformFunction extends BaseTransformFunction {
    @Override
    public String getName() {
      return "   ";
    }

    @Override
    public TransformResultMetadata getResultMetadata() {
      return INT_SV_NO_DICTIONARY_METADATA;
    }
  }

  public static class ThrowingConstructorTransformFunction extends BaseTransformFunction {
    public ThrowingConstructorTransformFunction() {
      throw new IllegalStateException("Intentional constructor failure");
    }

    @Override
    public String getName() {
      return "Throwing_Constructor_Transform";
    }

    @Override
    public TransformResultMetadata getResultMetadata() {
      return INT_SV_NO_DICTIONARY_METADATA;
    }
  }

  public static class PrivateConstructorTransformFunction extends BaseTransformFunction {
    private PrivateConstructorTransformFunction() {
    }

    @Override
    public String getName() {
      return "Private_Constructor_Transform";
    }

    @Override
    public TransformResultMetadata getResultMetadata() {
      return INT_SV_NO_DICTIONARY_METADATA;
    }
  }

  /// Listed in a descriptor but does not implement [TransformFunction].
  public static class NotATransformFunction {
  }

  /// Provider whose name shadows the `reverse` scalar function.
  public static class ScalarShadowTransformFunction extends BaseTransformFunction {
    @Override
    public String getName() {
      return "reverse";
    }

    @Override
    public TransformResultMetadata getResultMetadata() {
      return INT_SV_NO_DICTIONARY_METADATA;
    }
  }

  /// Listed in a descriptor but violates the concrete-class requirement of the provider contract.
  public abstract static class AbstractServiceTransformFunction extends BaseTransformFunction {
    @Override
    public String getName() {
      return "Abstract_Service_Transform";
    }

    @Override
    public TransformResultMetadata getResultMetadata() {
      return INT_SV_NO_DICTIONARY_METADATA;
    }
  }
}
