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
package org.apache.pinot.common.utils;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.List;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class OomProtectionUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(OomProtectionUtils.class);

  private OomProtectionUtils() {
  }

  public static boolean hasDisableAdaptiveIhopFlag(List<String> jvmInputArgs) {
    if (jvmInputArgs == null || jvmInputArgs.isEmpty()) {
      return false;
    }
    for (String arg : jvmInputArgs) {
      if ("-XX:-G1UseAdaptiveIHOP".equals(arg)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Detect whether G1GC is active for the current JVM.
   * Prefer GC MXBean names, fall back to scanning JVM input args.
   */
  public static boolean isG1GcActive(List<String> jvmInputArgs) {
    try {
      for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
        String name = bean.getName();
        if (name != null && name.toLowerCase().contains("g1")) {
          return true;
        }
      }
    } catch (Throwable ignore) {
      // fall through to arg-based detection
    }

    if (jvmInputArgs != null) {
      for (String arg : jvmInputArgs) {
        if (arg != null && arg.contains("UseG1GC")) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * If the required GC flag is missing, disables OOM protection by flipping the config flag to false.
   * @return true if the method disabled OOM protection; false otherwise
   */
  public static boolean enforceIhopGcOrDisableOom(PinotConfiguration config, List<String> jvmInputArgs) {
    // Allow tests to bypass GC/IHOP enforcement while keeping OOM protection behavior as configured.
    if (shouldSkipGcEnforcement(config)) {
      LOGGER.info("Skipping GC/IHOP enforcement for tests; leaving OOM protection as configured.");
      return false;
    }
    // Ensure G1GC is active; Pinot's OOM protections are validated with G1GC only.
    if (!isG1GcActive(jvmInputArgs)) {
      LOGGER.warn("G1GC is not active. Disabling OOM protection. Please run with -XX:+UseG1GC.");
      config.setProperty(CommonConstants.Accounting.FULLY_QUALIFIED_CONFIG_OF_OOM_PROTECTION_KILLING_QUERY, false);
      return true;
    }

    boolean hasFlag = hasDisableAdaptiveIhopFlag(jvmInputArgs);
    if (!hasFlag) {
      LOGGER.warn("Required GC option -XX:-G1UseAdaptiveIHOP not found. Disabling OOM protection.");
      config.setProperty(CommonConstants.Accounting.FULLY_QUALIFIED_CONFIG_OF_OOM_PROTECTION_KILLING_QUERY, false);
      return true;
    }
    // Ensure IHOP threshold is below all OOM protection levels so GC triggers before protections
    // Default IHOP threshold is 45% if not explicitly set by `-XX:InitiatingHeapOccupancyPercent=45`
    double ihopRatio = 0.45;
    if (jvmInputArgs != null) {
      for (String arg : jvmInputArgs) {
        if (arg != null && arg.startsWith("-XX:InitiatingHeapOccupancyPercent=")) {
          int idx = arg.indexOf('=');
          if (idx > 0 && idx + 1 < arg.length()) {
            try {
              ihopRatio = Double.parseDouble(arg.substring(idx + 1)) / 100.0;
            } catch (Exception ignore) {
              // keep default on parse failures
            }
          }
        }
      }
    }
    // We want to ensure IHOP threshold is below all OOM protection levels so GC triggers before protections
    // So we add 20% to the default IHOP threshold
    double ihopRatioThreshold = ihopRatio + 0.20;
    PinotConfiguration accountingConfigs = config.subset(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX);
    double alarmingRatio = accountingConfigs.getProperty(
        CommonConstants.Accounting.CONFIG_OF_ALARMING_LEVEL_HEAP_USAGE_RATIO,
        CommonConstants.Accounting.DEFAULT_ALARMING_LEVEL_HEAP_USAGE_RATIO);
    double criticalRatio = accountingConfigs.getProperty(
        CommonConstants.Accounting.CONFIG_OF_CRITICAL_LEVEL_HEAP_USAGE_RATIO,
        CommonConstants.Accounting.DEFAULT_CRITICAL_LEVEL_HEAP_USAGE_RATIO);
    double panicRatio = accountingConfigs.getProperty(
        CommonConstants.Accounting.CONFIG_OF_PANIC_LEVEL_HEAP_USAGE_RATIO,
        CommonConstants.Accounting.DEFAULT_PANIC_LEVEL_HEAP_USAGE_RATIO);

    if (ihopRatioThreshold > alarmingRatio || ihopRatioThreshold > criticalRatio
        || ihopRatioThreshold > panicRatio) {
      int expectedIhopRatio = (int) Math.min(Math.min(alarmingRatio, criticalRatio), panicRatio) * 100 - 20;
      LOGGER.warn(
          "InitiatingHeapOccupancyPercent ratio {} is not below 20% of OOM thresholds (alarming={}, critical={}, "
              + "panic={}). Disabling OOM protection as GC may not be triggered or fast enough. Please ensure you "
              + "set a lower value than -XX:InitiatingHeapOccupancyPercent={} in the JVM options.",
          ihopRatio, alarmingRatio, criticalRatio, panicRatio, expectedIhopRatio);
      config.setProperty(CommonConstants.Accounting.FULLY_QUALIFIED_CONFIG_OF_OOM_PROTECTION_KILLING_QUERY, false);
      return true;
    }
    return false;
  }

  /**
   * Overload that reads JVM input arguments from RuntimeMXBean.
   */
  public static void enforceIhopGcOrDisableOom(PinotConfiguration config) {
    if (shouldSkipGcEnforcement(config)) {
      LOGGER.info("Skipping GC/IHOP enforcement for tests; leaving OOM protection as configured.");
      return;
    }
    if (config.getProperty(CommonConstants.Accounting.FULLY_QUALIFIED_CONFIG_OF_OOM_PROTECTION_KILLING_QUERY,
        CommonConstants.Accounting.DEFAULT_ENABLE_OOM_PROTECTION_KILLING_QUERY)) {
      try {
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        List<String> inputArgs = runtimeMXBean.getInputArguments();
        enforceIhopGcOrDisableOom(config, inputArgs);
      } catch (Exception e) {
        LOGGER.warn("Failed to inspect JVM input arguments for GC options; leaving OOM protection as configured.", e);
      }
    }
  }

  private static boolean shouldSkipGcEnforcement(PinotConfiguration config) {
    boolean viaConfig = config.getProperty(
        CommonConstants.Accounting.FULLY_QUALIFIED_CONFIG_OF_SKIP_OOM_GC_ENFORCEMENT_FOR_TESTS,
        CommonConstants.Accounting.DEFAULT_SKIP_OOM_GC_ENFORCEMENT_FOR_TESTS);
    if (viaConfig) {
      return true;
    }
    String sysProp = System.getProperty(CommonConstants.Accounting.SYS_PROP_OF_SKIP_OOM_GC_ENFORCEMENT_FOR_TESTS);
    return sysProp != null && Boolean.parseBoolean(sysProp);
  }
}
