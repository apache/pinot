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

import java.util.List;
import org.apache.pinot.spi.annotations.metrics.PinotMetricsFactory;
import org.apache.pinot.verifier.PluginVerifier.CheckContext;


/// Asks {@code PluginManager.loadServices(PinotMetricsFactory.class)} for the set of metrics
/// factories the realm walk discovers. The post-PR migration path in
/// {@code PinotMetricUtils.initializePinotMetricsFactory} uses exactly this call, so a green
/// run here is the same coverage as a real broker initializing metrics.
public final class MetricsFactoryCheck implements Check {

  @Override
  public String name() {
    return "Metrics factory plugins (PinotMetricsFactory)";
  }

  @Override
  public Outcome run(CheckContext context) {
    int pass = 0;
    int fail = 0;
    List<PinotMetricsFactory> factories;
    try {
      factories = context.pluginManager().loadServices(PinotMetricsFactory.class);
    } catch (Throwable t) {
      System.out.println("  FAIL  PluginManager.loadServices(PinotMetricsFactory.class) threw "
          + t.getClass().getSimpleName() + ": " + t.getMessage());
      if (context.verbose()) {
        t.printStackTrace(System.out);
      }
      return new Outcome(0, 1);
    }
    if (factories.isEmpty()) {
      System.out.println("  FAIL  No PinotMetricsFactory implementations were discovered.");
      return new Outcome(0, 1);
    }
    for (PinotMetricsFactory factory : factories) {
      String fqcn = factory.getClass().getName();
      if (!context.targets(fqcn)) {
        continue;
      }
      try {
        // Trigger the same getMetricsFactoryName path that PinotMetricUtils logs.
        String displayName = factory.getMetricsFactoryName();
        System.out.println("  PASS  " + fqcn + " (factoryName='" + displayName + "')");
        if (context.verbose()) {
          for (String line : context.describeCodeSource(factory.getClass())) {
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
