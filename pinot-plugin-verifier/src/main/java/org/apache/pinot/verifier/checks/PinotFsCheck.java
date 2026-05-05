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
package org.apache.pinot.verifier.checks;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.verifier.PluginVerifier.CheckContext;


/// Each {@link PinotFS} is registered in production via
/// {@code PinotFSFactory.register(scheme, factoryClassName, config)}. We don't go through
/// {@code register} here because it requires per-scheme config (credentials, region, etc.) we
/// don't have; instead we instantiate the class directly through {@code PluginManager} —
/// which is what {@code register} does internally. Connecting to the actual storage backend
/// is the integration tests' job.
public final class PinotFsCheck implements Check {
  /// Map of pluginName → PinotFS FQCN. Plugin name matches {@code plugins/pinot-file-system/}.
  private static final Map<String, String> FILESYSTEMS = new LinkedHashMap<>() {{
    put("pinot-s3", "org.apache.pinot.plugin.filesystem.S3PinotFS");
    put("pinot-gcs", "org.apache.pinot.plugin.filesystem.GcsPinotFS");
    put("pinot-adls", "org.apache.pinot.plugin.filesystem.ADLSGen2PinotFS");
    put("pinot-hdfs", "org.apache.pinot.plugin.filesystem.HadoopPinotFS");
  }};

  @Override
  public String name() {
    return "File system plugins (PinotFS)";
  }

  @Override
  public Outcome run(CheckContext context) {
    int pass = 0;
    int fail = 0;
    for (Map.Entry<String, String> e : FILESYSTEMS.entrySet()) {
      String pluginName = e.getKey();
      String fqcn = e.getValue();
      if (!context.targets(fqcn)) {
        continue;
      }
      try {
        Object instance = context.createInstance(pluginName, fqcn);
        if (!(instance instanceof PinotFS)) {
          throw new IllegalStateException(
              fqcn + " was loaded but does not implement PinotFS — got " + instance.getClass().getName());
        }
        System.out.println("  PASS  " + fqcn);
        if (context.verbose()) {
          for (String line : context.describeCodeSource(instance.getClass())) {
            System.out.println("        " + line);
          }
        }
        pass++;
      } catch (Throwable t) {
        System.out.println("  FAIL  " + fqcn + " — " + t.getClass().getSimpleName() + ": " + t.getMessage());
        if (context.verbose()) {
          t.printStackTrace(System.out);
        }
        fail++;
      }
    }
    return new Outcome(pass, fail);
  }
}
