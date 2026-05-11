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
import java.nio.file.Files;
import java.util.Set;
import java.util.jar.JarOutputStream;
import org.apache.pinot.query.planner.rules.RuleSetCustomizer;
import org.testng.Assert;
import org.testng.annotations.Test;


/// Verifies that {@link PluginManager} exports the SPI packages from the pinot
/// realm into each plugin realm, so plugin JARs can implement
/// {@link RuleSetCustomizer} without bundling or shading those classes.
///
/// This test lives in the {@code org.apache.pinot.spi.plugin} package (in a
/// different Maven module) to access the package-private {@link PluginManager}
/// constructor.
public class PluginRealmExportTest {

  @Test
  public void pluginRealmCanLoadRuleSetCustomizer()
      throws Exception {
    File tempDir = Files.createTempDirectory("pinot-spi-export-test").toFile();
    try {
      // Minimal new-style plugin: just pinot-plugin.properties + an empty JAR.
      Files.createFile(tempDir.toPath().resolve("pinot-plugin.properties"));
      try (JarOutputStream jos = new JarOutputStream(new FileOutputStream(new File(tempDir, "dummy.jar")))) {
        // intentionally empty
      }

      PluginManager pm = new PluginManager();
      pm.load("spi-export-test-plugin", tempDir);

      Set<ClassLoader> loaders = pm.getPluginClassLoaders();
      Assert.assertEquals(loaders.size(), 1, "Exactly one plugin realm should be registered");

      ClassLoader pluginRealm = loaders.iterator().next();

      // RuleSetCustomizer is in org.apache.pinot.query.planner.rules, which PluginManager
      // exports from the pinotRealm. Loading it from the plugin realm must succeed.
      Class<?> customizerClass = pluginRealm.loadClass(RuleSetCustomizer.class.getName());
      Assert.assertNotNull(customizerClass);
      Assert.assertTrue(customizerClass.isInterface(), "RuleSetCustomizer must be an interface");

      // RelOptRule is in org.apache.calcite.plan, also exported by PluginManager.
      Class<?> relOptRuleClass = pluginRealm.loadClass("org.apache.calcite.plan.RelOptRule");
      Assert.assertNotNull(relOptRuleClass);
    } finally {
      File[] files = tempDir.listFiles();
      if (files != null) {
        for (File f : files) {
          f.delete();
        }
      }
      tempDir.delete();
    }
  }
}
