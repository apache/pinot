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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/**
 * IMPORTANT READ
 *
 * To avoid (huge) jars in the codebase just for testing, we let Maven download the jars required for these tests.
 * In the pom.xml the maven-dependency-plugin is configured to download (and sometimes unpack)
 * artifacts during the generate-test-resources phase. They will be put in _pluginsDirectory
 *
 * As this is the target/test-classes/plugins, it will contain the static files under src/test/resoources/plugins,
 * most of the time just the pinot-plugin.properties for that plugin.
 */
public class ClassLoaderTest {
  private static final String ORIGINAL_PLUGIN_DIR = System.getProperty(PluginManager.PLUGINS_DIR_PROPERTY_NAME);

  // Relative to pinot-spi/pom.xml
  private static final Path PLUGINS_DIRECTORY = Path.of("target/test-classes/plugins").toAbsolutePath();

  // MathUtils is only used in pinot framework, should not be available in limited plugins
  private static final String COMMONS_MATH_UTILS = "org.apache.commons.math3.util.MathUtils";

  // IOUtils exists in all realms, they should use their own version
  private static final String COMMONS_IO_UTILS = "org.apache.commons.io.IOUtils";

  // TimeUtils exists in all realms, they should be imported from pinot classloader
  private static final String SPI_TIME_UTILS = "org.apache.pinot.spi.utils.TimeUtils";

  private static final String YAMMER_METRICS_REGISTRY = "org.apache.pinot.plugin.metrics.yammer.YammerMetricsRegistry";
  private static final String DROPWIZARD_METRICS_REGISTRY =
      "org.apache.pinot.plugin.metrics.dropwizard.DropwizardMetricsRegistry";

  @BeforeClass
  public void setup()
      throws IOException {
    System.setProperty(PluginManager.PLUGINS_DIR_PROPERTY_NAME, PLUGINS_DIRECTORY.toString());
  }

  @Test
  public void classRealms()
      throws Exception {
    assertNotNull(ClassLoader.getSystemClassLoader().loadClass(COMMONS_MATH_UTILS),
        "Expected " + COMMONS_MATH_UTILS + " to be loadable via SystemClassLoader");
    assertNotNull(ClassLoader.getSystemClassLoader().loadClass(COMMONS_IO_UTILS),
        "Expected " + COMMONS_IO_UTILS + " to be loadable via SystemClassLoader");
  }

  // pinot-dropwizard is a limited plugin, meaning it cannot access every class from the pinot realm
  @Test
  public void limitedPluginRealm()
      throws Exception {
    String pluginName = "pinot-dropwizard";

    // Create fresh pluginManager, so it can pick up the system property for the pluginDirectory
    PluginManager pluginManager = new PluginManager();

    // make sure pinot-dropwizard-0.10.0-shaded.jar has been downloaded
    assertTrue(
        Files.exists(PLUGINS_DIRECTORY.resolve(pluginName + "/pinot-dropwizard-0.10.0-shaded.jar")),
        "Plugin not found. Run 'mvn -f pinot-spi/pom.xml generate-test-resources' once to prepare this artifact");

    assertNotNull(pluginManager.loadClass(pluginName, DROPWIZARD_METRICS_REGISTRY),
        "Expected o.a.p.p.m.d.DropwizardMetricsRegistry to be available");
    assertNotNull(pluginManager.createInstance(pluginName, DROPWIZARD_METRICS_REGISTRY),
        "Expected to be able to create instance of o.a.p.p.m.d.DropwizardMetricsRegistry");

    assertNotNull(pluginManager.loadClass(pluginName, SPI_TIME_UTILS),
        "Expected o.a.p.spi.utils.TimeUtils to be available via dropwizard-realm");
    assertEquals(pluginManager.loadClass(pluginName, SPI_TIME_UTILS)
            .getProtectionDomain().getCodeSource().getLocation(),
        Path.of("target/classes").toUri().toURL(),
        "Expected o.a.p.spi.utils.TimeUtils to be loaded from pinot-realm");

    assertThrows("Class is part of a different plugin, so should not be accessible",
        ClassNotFoundException.class, () -> pluginManager.loadClass(pluginName, YAMMER_METRICS_REGISTRY));

    assertThrows("Class is dependency of pinot-spi, but is not an exported package",
        ClassNotFoundException.class, () -> pluginManager.loadClass(pluginName, COMMONS_MATH_UTILS));

    assertTrue(pluginManager.loadClass(pluginName, COMMONS_IO_UTILS).getProtectionDomain()
            .getCodeSource().getLocation().getPath().endsWith("pinot-dropwizard-0.10.0-shaded.jar"),
        "Expected o.a.c.i.IOUtils to be available via dropwizard-realm");
  }

  @Test
  public void unlimitedPluginRealm()
      throws Exception {
    String pluginName = "pinot-yammer";

    // Create fresh pluginManager, so it can pick up the system property for the pluginDirectory
    PluginManager pluginManager = new PluginManager();

    // pinot-yammer is an unlimited plugin, meaning it has access to every class from the pinot realm
    assertTrue(
        Files.exists(PLUGINS_DIRECTORY.resolve(pluginName + "/pinot-yammer-0.10.0-shaded.jar")),
        "Plugin not found. Run 'mvn -f pinot-spi/pom.xml generate-test-resources' once to prepare this artifact");
    assertNotNull(pluginManager.loadClass(pluginName, YAMMER_METRICS_REGISTRY));
    assertNotNull(pluginManager.createInstance(pluginName, YAMMER_METRICS_REGISTRY));

    assertNotNull(pluginManager.loadClass(pluginName, SPI_TIME_UTILS),
        "Expected o.a.p.spi.utils.TimeUtils to be available via yammer-realm");
    assertEquals(pluginManager.loadClass(pluginName, SPI_TIME_UTILS)
            .getProtectionDomain().getCodeSource().getLocation(),
        Path.of("target/classes").toUri().toURL(),
        "Expected o.a.p.spi.utils.TimeUtils to be loaded from pinot-realm");

    assertNotNull(pluginManager.loadClass(pluginName, COMMONS_MATH_UTILS),
        "o.a.c.m.u.MathUtils is dependency of pinot-spi, must be accessible for unlimited plugins");

    assertTrue(pluginManager.loadClass(pluginName, COMMONS_IO_UTILS).getProtectionDomain()
            .getCodeSource().getLocation().getPath().endsWith("pinot-yammer-0.10.0-shaded.jar"),
        "This is self-first, so class should come from pinot-yammer realm");

    assertThrows("Class is part of a different plugin, so should not be accessible",
        ClassNotFoundException.class, () -> pluginManager.loadClass(pluginName, DROPWIZARD_METRICS_REGISTRY));
  }

  @Test
  public void assemblyBasedRealm()
      throws Exception {
    String pluginName = "assemblybased-pinot-plugin";

    // Create fresh pluginManager, so it can pick up the system property for the pluginDirectory
    PluginManager pluginManager = new PluginManager();

    // pinot-yammer is an unlimited plugin, meaning it has access to every class from the pinot realm
    assertTrue(
        Files.exists(PLUGINS_DIRECTORY.resolve(
            pluginName + "/classes/org/apache/pinot/plugin/metrics/yammer/YammerMetricsRegistry.class")),
        "Class not found. Run 'mvn -f pinot-spi/pom.xml generate-test-resources' once to prepare this artifact");
    assertNotNull(pluginManager.loadClass(pluginName, YAMMER_METRICS_REGISTRY));
    assertNotNull(pluginManager.createInstance(pluginName, YAMMER_METRICS_REGISTRY));

    assertNotNull(pluginManager.loadClass(pluginName, SPI_TIME_UTILS),
        "Expected o.a.p.spi.utils.TimeUtils to be available via yammer-realm");
    assertEquals(pluginManager.loadClass("pinot-dropwizard", SPI_TIME_UTILS)
            .getProtectionDomain().getCodeSource().getLocation(),
        Path.of("target/classes").toUri().toURL(),
        "Expected o.a.p.spi.utils.TimeUtils to be loaded from pinot-realm");

    assertThrows("Class is dependency of pinot-spi, but is not an exported package",
        ClassNotFoundException.class, () -> pluginManager.loadClass(
            "pinot-dropwizard", COMMONS_MATH_UTILS));

    assertTrue(pluginManager.loadClass(pluginName, COMMONS_IO_UTILS).getProtectionDomain()
            .getCodeSource().getLocation().getPath().endsWith("commons-io-2.11.0.jar"),
        "This is self-first, so class should come from pinot-yammer realm");

    assertThrows("Class is part of a different plugin, so should not be accessible",
        ClassNotFoundException.class, () -> pluginManager.loadClass(pluginName, DROPWIZARD_METRICS_REGISTRY));
  }

  @Test
  public void classicPluginClassloader()
      throws Exception {
    String pluginName = "pinot-shaded-yammer";

    // Create fresh pluginManager, so it can pick up the system property for the pluginDirectory
    PluginManager pluginManager = new PluginManager();

    // pinot-shaded-yammer is a shaded jar using the legacy PluginClassloader instead of classRealm
    assertTrue(
        Files.exists(PLUGINS_DIRECTORY.resolve(pluginName + "/pinot-yammer-0.10.0-shaded.jar")),
        "Plugin not found. Run 'mvn -f pinot-spi/pom.xml generate-test-resources' once to prepare this artifact");

    assertNotNull(pluginManager.loadClass(pluginName, YAMMER_METRICS_REGISTRY),
        "Expected o.a.p.p.m.y.YammerMetricsRegistry to be available");
    assertNotNull(pluginManager.createInstance(pluginName, YAMMER_METRICS_REGISTRY),
        "Expected to be able to create instance of o.a.p.p.m.y.YammerMetricsRegistry");

    assertNotNull(pluginManager.loadClass(pluginName, SPI_TIME_UTILS),
        "Expected o.a.p.spi.utils.TimeUtils to be available via yammer-realm");
    assertEquals(pluginManager.loadClass(pluginName, SPI_TIME_UTILS)
            .getProtectionDomain().getCodeSource().getLocation(),
        Path.of("target/classes").toUri().toURL(),
        "Expected o.a.p.spi.utils.TimeUtils to be loaded from pinot-realm");

    assertNotNull(pluginManager.loadClass(pluginName, COMMONS_MATH_UTILS),
        "Class is dependency of pinot-spi, must be accessible for unlimited plugins");

    // THIS IS THE MAIN REASON FOR CLASSREALMS: classes are here loaded from pinot first, not from shaded plugin
    String urlPath = pluginManager.loadClass(pluginName, COMMONS_IO_UTILS)
        .getProtectionDomain().getCodeSource().getLocation().getPath();
    // unpredictable version, as it depends on this pinot-spi
    assertTrue(Pattern.matches(".*/commons-io/commons-io/[^/]+/commons-io-[.\\d]+.jar$", urlPath),
        "This is using the PluginClassloader, so class should come from the system class laoder");

    assertThrows("Class is part of a different plugin, so should not be accessible",
        ClassNotFoundException.class, () -> pluginManager.loadClass(pluginName, DROPWIZARD_METRICS_REGISTRY));
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    if (ORIGINAL_PLUGIN_DIR != null) {
      System.setProperty(PluginManager.PLUGINS_DIR_PROPERTY_NAME, ORIGINAL_PLUGIN_DIR);
    } else {
      System.clearProperty(PluginManager.PLUGINS_DIR_PROPERTY_NAME);
    }
  }
}
