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
package org.apache.pinot.core.transport;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.pinot.common.metrics.AbstractMetrics;
import org.apache.pinot.spi.utils.DataSizeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Utility class to inspect Netty constants and log their values, with the ability to check for specific conditions
/// and log warnings if they are not met.
public class NettyInspector {
  private static final Logger LOGGER = LoggerFactory.getLogger(NettyInspector.class);
  /// We use a CopyOnWriteArrayList to allow dynamic addition of checks at runtime, if needed.
  public static final CopyOnWriteArrayList<Check> CHECKS;
  /// We use a CopyOnWriteArrayList to allow dynamic addition of Netty instances at runtime,
  /// if needed (e.g. if we want to support other shaded versions of Netty).
  public static final CopyOnWriteArrayList<NettyInstance> KNOWN_INSTANCES;

  static {
    CHECKS = new CopyOnWriteArrayList<>(
        new Check[] {
            NettyInspector::checkDirectMemory
        }
    );
    KNOWN_INSTANCES = new CopyOnWriteArrayList<>(new NettyInstance[] {
        new NettyInstance.UnshadedNettyInstance(),
        new NettyInstance.GrpcNettyInstance()
    });
  }

  private NettyInspector() {
    // Private constructor to prevent instantiation
  }

  public static void registerMetrics(AbstractMetrics<?, ?, ?, ?> metrics) {
    for (NettyInstance instance : KNOWN_INSTANCES) {
      metrics.setOrUpdateGauge(instance.getName() + "NettyDirectMemoryUsed",
          instance::getUsedDirectMemory);
      metrics.setOrUpdateGauge(instance.getName() + "NettyDirectMemoryMax",
          instance::getMaxDirectMemory);
    }
  }

  /// Logs the values of all constants for all Netty instances, and logs warnings if any checks fail.
  public static void logAllChecks() {
    for (Map.Entry<NettyInstance, List<CheckResult>> entry : checkAllConstants().entrySet()) {
      NettyInstance instance = entry.getKey();
      List<CheckResult> results = entry.getValue();
      for (CheckResult result : results) {
        switch (result._status) {
          case PASS:
            // Do nothing
            break;
          case FAIL:
            LOGGER.warn("Netty instance '{}' check failed: {}", instance.getName(), result._message);
            break;
          case UNKNOWN:
            LOGGER.warn("Netty instance '{}' check unknown: {}", instance.getName(), result._message);
            break;
          default:
            LOGGER.warn("Netty instance '{}' check returned unexpected status: {}", instance.getName(), result._status);
            break;
        }
      }
    }
  }

  /// Checks all constants for all Netty instances and returns a map of instances to their check results.
  public static Map<NettyInstance, List<CheckResult>> checkAllConstants() {
    Map<NettyInstance, List<CheckResult>> results = new HashMap<>();
    for (NettyInstance instance : KNOWN_INSTANCES) {
      for (Check check : CHECKS) {
        CheckResult result = check.apply(instance);
        results.computeIfAbsent(instance, k -> new ArrayList<>()).add(result);
      }
    }
    return results;
  }

  public static CheckResult checkDirectMemory(NettyInstance instance) {
    if (instance.isExplicitTryReflectionSetAccessible()) {
      return CheckResult.SUCCESS;
    } else {
      String message = "Reflection access is disabled on the " + instance.getName() + " Netty instance. "
          + "Netty will probably use heap memory instead off-heap. "
          + "It is recommended to set -D" + instance.getShadePrefix()
          + "io.netty.tryReflectionSetAccessible=true.";
      return new CheckResult(message, CheckResult.Status.FAIL);
    }
  }

  public interface Check extends Function<NettyInstance, CheckResult> {
    /// Applies the check to the given Netty instance.
    /// @param nettyInstance Netty instance to check
    /// @return CheckResult indicating success, failure or unknown
    @Override
    CheckResult apply(NettyInstance nettyInstance);
  }

  public static class CheckResult {
    public static final CheckResult SUCCESS = new CheckResult(null, Status.PASS);
    @Nullable
    private final String _message;
    private final Status _status;

    public CheckResult(@Nullable String message, Status status) {
      _message = message;
      _status = status;
    }

    public enum Status {
      PASS,
      FAIL,
      UNKNOWN
    }
  }
}
