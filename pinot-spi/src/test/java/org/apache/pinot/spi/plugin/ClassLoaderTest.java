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
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.plugin.PluginManager.PLUGINS_DIR_PROPERTY_NAME;


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

  private static final String ORIGINAL_PLUGIN_DIR = System.getProperty(PLUGINS_DIR_PROPERTY_NAME);

  // relative to pinot-spi/pom.xml
  private final Path _pluginsDirectory = Path.of("target/test-classes/plugins").toAbsolutePath();

  // MathUtils is only used in pinot framework, should not be available in limited plugins
  private final String _commonsMathUtils = "org.apache.commons.math3.util.MathUtils";

  // IOUtils exists in all realms, they should use their own version
  private final String _commonsIOUtils = "org.apache.commons.io.IOUtils";

  // TimeUtils exists in all realms, they should be imported from pinot classloader
  private final String _spiTimeUtils = "org.apache.pinot.spi.utils.TimeUtils";

  public final String _yammerMetricsRegistry = "org.apache.pinot.plugin.metrics.yammer.YammerMetricsRegistry";

  public final String _dropwizardMetricsRegistry =
      "org.apache.pinot.plugin.metrics.dropwizard.DropwizardMetricsRegistry";

  @BeforeClass
  public void setup()
      throws IOException {
    System.setProperty(PLUGINS_DIR_PROPERTY_NAME, _pluginsDirectory.toString());
  }

  @Test
  public void classRealms() throws Exception {
    Assert.assertNotNull(ClassLoader.getSystemClassLoader().loadClass(_commonsMathUtils),
        "Expected " + _commonsMathUtils + " to be loadable via SystemClassLoader");
    Assert.assertNotNull(ClassLoader.getSystemClassLoader().loadClass(_commonsIOUtils),
        "Expected " + _commonsIOUtils + " to be loadable via SystemClassLoader");
  }

  // pinot-dropwizard is a limited plugin, meaning it cannot access every class from the pinot realm
  @Test
  public void limitedPluginRealm() throws Exception {
    final String pluginName = "pinot-dropwizard";

    // Create fresh pluginManager, so it can pick up the system property for the pluginDirectory
    PluginManager pluginManager = new PluginManager();

    // make sure pinot-dropwizard-0.10.0-shaded.jar has been downloaded
    Assert.assertTrue(
        Files.exists(_pluginsDirectory.resolve(pluginName + "/pinot-dropwizard-0.10.0-shaded.jar")),
        "Plugin not found. Run 'mvn -f pinot-spi/pom.xml generate-test-resources' once to prepare this artifact");

    Assert.assertNotNull(pluginManager.loadClass(pluginName, _dropwizardMetricsRegistry),
        "Expected o.a.p.p.m.d.DropwizardMetricsRegistry to be available");
    Assert.assertNotNull(pluginManager.createInstance(pluginName, _dropwizardMetricsRegistry),
        "Expected to be able to create instance of o.a.p.p.m.d.DropwizardMetricsRegistry");

    Assert.assertNotNull(pluginManager.loadClass(pluginName, _spiTimeUtils),
        "Expected o.a.p.spi.utils.TimeUtils to be available via dropwizard-realm");
    Assert.assertEquals(pluginManager.loadClass(pluginName, _spiTimeUtils)
            .getProtectionDomain().getCodeSource().getLocation(),
        Path.of("target/classes").toUri().toURL(),
        "Expected o.a.p.spi.utils.TimeUtils to be loaded from pinot-realm");

    Assert.assertThrows("Class is part of a different plugin, so should not be accessible",
        ClassNotFoundException.class, () -> pluginManager.loadClass(pluginName, _yammerMetricsRegistry));

    Assert.assertThrows("Class is dependency of pinot-spi, but is not an exported package",
        ClassNotFoundException.class, () -> pluginManager.loadClass(pluginName, _commonsMathUtils));

    Assert.assertTrue(pluginManager.loadClass(pluginName, _commonsIOUtils).getProtectionDomain()
            .getCodeSource().getLocation().getPath().endsWith("pinot-dropwizard-0.10.0-shaded.jar"),
        "Expected o.a.c.i.IOUtils to be available via dropwizard-realm");
  }

  @Test
  public void unlimitedPluginRealm() throws Exception {
    final String pluginName = "pinot-yammer";

    // Create fresh pluginManager, so it can pick up the system property for the pluginDirectory
    PluginManager pluginManager = new PluginManager();

    // pinot-yammer is an unlimited plugin, meaning it has access to every class from the pinot realm
    Assert.assertTrue(
        Files.exists(_pluginsDirectory.resolve(pluginName + "/pinot-yammer-0.10.0-shaded.jar")),
        "Plugin not found. Run 'mvn -f pinot-spi/pom.xml generate-test-resources' once to prepare this artifact");
    Assert.assertNotNull(pluginManager.loadClass(pluginName, _yammerMetricsRegistry));
    Assert.assertNotNull(pluginManager.createInstance(pluginName, _yammerMetricsRegistry));

    Assert.assertNotNull(pluginManager.loadClass(pluginName, _spiTimeUtils),
        "Expected o.a.p.spi.utils.TimeUtils to be available via yammer-realm");
    Assert.assertEquals(pluginManager.loadClass(pluginName, _spiTimeUtils)
            .getProtectionDomain().getCodeSource().getLocation(),
        Path.of("target/classes").toUri().toURL(),
        "Expected o.a.p.spi.utils.TimeUtils to be loaded from pinot-realm");

    Assert.assertNotNull(pluginManager.loadClass(pluginName, _commonsMathUtils),
        "o.a.c.m.u.MathUtils is dependency of pinot-spi, must be accessible for unlimited plugins");

    Assert.assertTrue(pluginManager.loadClass(pluginName, _commonsIOUtils).getProtectionDomain()
            .getCodeSource().getLocation().getPath().endsWith("pinot-yammer-0.10.0-shaded.jar"),
        "This is self-first, so class should come from pinot-yammer realm");

    Assert.assertThrows("Class is part of a different plugin, so should not be accessible",
        ClassNotFoundException.class, () -> pluginManager.loadClass(pluginName, _dropwizardMetricsRegistry));
  }

  @Test
  public void assemblyBasedRealm() throws Exception {
    final String pluginName = "assemblybased-pinot-plugin";

    // Create fresh pluginManager, so it can pick up the system property for the pluginDirectory
    PluginManager pluginManager = new PluginManager();

    // pinot-yammer is an unlimited plugin, meaning it has access to every class from the pinot realm
    Assert.assertTrue(
        Files.exists(_pluginsDirectory.resolve(
            pluginName + "/classes/org/apache/pinot/plugin/metrics/yammer/YammerMetricsRegistry.class")),
        "Class not found. Run 'mvn -f pinot-spi/pom.xml generate-test-resources' once to prepare this artifact");
    Assert.assertNotNull(pluginManager.loadClass(pluginName, _yammerMetricsRegistry));
    Assert.assertNotNull(pluginManager.createInstance(pluginName, _yammerMetricsRegistry));

    Assert.assertNotNull(pluginManager.loadClass(pluginName, _spiTimeUtils),
        "Expected o.a.p.spi.utils.TimeUtils to be available via yammer-realm");
    Assert.assertEquals(pluginManager.loadClass("pinot-dropwizard", _spiTimeUtils)
            .getProtectionDomain().getCodeSource().getLocation(),
        Path.of("target/classes").toUri().toURL(),
        "Expected o.a.p.spi.utils.TimeUtils to be loaded from pinot-realm");

    Assert.assertThrows("Class is dependency of pinot-spi, but is not an exported package",
        ClassNotFoundException.class, () -> pluginManager.loadClass(
            "pinot-dropwizard", _commonsMathUtils));

    Assert.assertTrue(pluginManager.loadClass(pluginName, _commonsIOUtils).getProtectionDomain()
            .getCodeSource().getLocation().getPath().endsWith("commons-io-2.11.0.jar"),
        "This is self-first, so class should come from pinot-yammer realm");

    Assert.assertThrows("Class is part of a different plugin, so should not be accessible",
        ClassNotFoundException.class, () -> pluginManager.loadClass(pluginName, _dropwizardMetricsRegistry));
  }

  @Test
  public void classicPluginClassloader() throws Exception {
    final String pluginName = "pinot-shaded-yammer";

    // Create fresh pluginManager, so it can pick up the system property for the pluginDirectory
    PluginManager pluginManager = new PluginManager();

    // pinot-shaded-yammer is a shaded jar using the legacy PluginClassloader instead of classRealm
    Assert.assertTrue(
        Files.exists(_pluginsDirectory.resolve(pluginName + "/pinot-yammer-0.10.0-shaded.jar")),
        "Plugin not found. Run 'mvn -f pinot-spi/pom.xml generate-test-resources' once to prepare this artifact");

    Assert.assertNotNull(pluginManager.loadClass(pluginName, _yammerMetricsRegistry),
        "Expected o.a.p.p.m.y.YammerMetricsRegistry to be available");
    Assert.assertNotNull(pluginManager.createInstance(pluginName, _yammerMetricsRegistry),
        "Expected to be able to create instance of o.a.p.p.m.y.YammerMetricsRegistry");

    Assert.assertNotNull(pluginManager.loadClass(pluginName, _spiTimeUtils),
        "Expected o.a.p.spi.utils.TimeUtils to be available via yammer-realm");
    Assert.assertEquals(pluginManager.loadClass(pluginName, _spiTimeUtils)
            .getProtectionDomain().getCodeSource().getLocation(),
        Path.of("target/classes").toUri().toURL(),
        "Expected o.a.p.spi.utils.TimeUtils to be loaded from pinot-realm");

    Assert.assertNotNull(pluginManager.loadClass(pluginName, _commonsMathUtils),
        "Class is dependency of pinot-spi, must be accessible for unlimited plugins");

    // THIS IS THE MAIN REASON FOR CLASSREALMS: classes are here loaded from pinot first, not from shaded plugin
    String urlPath = pluginManager.loadClass(pluginName, _commonsIOUtils)
        .getProtectionDomain().getCodeSource().getLocation().getPath();
    // unpredictable version, as it depends on this pinot-spi
    Assert.assertTrue(Pattern.matches(".*/commons-io/commons-io/[^/]+/commons-io-[\\.\\d]+.jar$", urlPath),
        "This is using the PluginClassloader, so class should come from the system class laoder");

    Assert.assertThrows("Class is part of a different plugin, so should not be accessible",
        ClassNotFoundException.class, () -> pluginManager.loadClass(pluginName, _dropwizardMetricsRegistry));
  }

  @AfterClass
  public void tearDown() throws Exception {
    if (ORIGINAL_PLUGIN_DIR != null) {
      System.setProperty(PLUGINS_DIR_PROPERTY_NAME, ORIGINAL_PLUGIN_DIR);
    } else {
      System.clearProperty(PLUGINS_DIR_PROPERTY_NAME);
    }
  }
}
